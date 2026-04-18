#!/usr/bin/env Rscript 
# ==============================================================================
# Analyze Structure Flooding Using SLR Scenarios (v2)
# ==============================================================================
#
# Purpose: Performs boolean ST_Intersects between pre-filtered structure tables
#          and region-partitioned SLR tables to identify flooded structures.
#
# Prerequisites:
#   1. Run load_structures_to_db_v2.R to create per-state structure tables
#   2. Run create_slr_region_tables.R to create per-region SLR tables
#
# Output: One table per state × scenario containing only flooded structures
#         Table naming: flooded_structures_{state}_{scenario}
#
# Usage:
#   Rscript analyze_structure_slr_flooding_v2.R <config_file.yaml>
#
# Author: Matt Schwaller
# Date: 2026-03-01
# ==============================================================================

library(RPostgres)
library(yaml)
library(glue)
library(DBI)

# ==============================================================================
# STATE-TO-REGION MAPPING
# ==============================================================================

STATE_TO_REGION <- list(
  "01" = "ms_al",    # Alabama
  "06" = "west",     # California
  "09" = "atlantic", # Connecticut
  "10" = "atlantic", # Delaware
  "12" = "florida",  # Florida
  "13" = "atlantic", # Georgia
  "22" = "la",       # Louisiana
  "23" = "atlantic", # Maine
  "24" = "atlantic", # Maryland
  "25" = "atlantic", # Massachusetts
  "28" = "ms_al",    # Mississippi
  "33" = "atlantic", # New Hampshire
  "34" = "atlantic", # New Jersey
  "36" = "atlantic", # New York
  "37" = "atlantic", # North Carolina
  "41" = "west",     # Oregon
  "44" = "atlantic", # Rhode Island
  "45" = "atlantic", # South Carolina
  "48" = "tx",       # Texas
  "51" = "atlantic", # Virginia
  "53" = "west"      # Washington
)

# State FIPS to abbreviation
FIPS_TO_STATE <- list(
  "01" = "AL", "06" = "CA", "09" = "CT", "10" = "DE", "12" = "FL",
  "13" = "GA", "22" = "LA", "23" = "ME", "24" = "MD", "25" = "MA",
  "28" = "MS", "33" = "NH", "34" = "NJ", "36" = "NY", "37" = "NC",
  "41" = "OR", "44" = "RI", "45" = "SC", "48" = "TX", "51" = "VA",
  "53" = "WA"
)

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

#' Initialize logging
init_log <- function(log_dir) {
  timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  log_file <- file.path(log_dir, glue("structure_slr_analysis_{timestamp}.txt"))
  dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)
  log_con <- file(log_file, open = "wt")
  writeLines(c(
    strrep("=", 78),
    "Structure × SLR Flooding Analysis Log (v2 - region-constrained)",
    glue("Started: {Sys.time()}"),
    strrep("=", 78)
  ), log_con)
  return(log_con)
}

#' Log message to both console and file
log_msg <- function(msg, log_con, verbose = TRUE) {
  timestamp <- format(Sys.time(), "%H:%M:%S")
  log_line <- glue("[{timestamp}] {msg}")
  if (verbose) cat(log_line, "\n")
  writeLines(log_line, log_con)
  flush(log_con)
}

#' Format time duration
format_duration <- function(seconds) {
  if (seconds < 60) {
    return(glue("{round(seconds, 1)} seconds"))
  } else if (seconds < 3600) {
    return(glue("{round(seconds/60, 1)} minutes"))
  } else {
    hours <- floor(seconds/3600)
    mins <- round((seconds %% 3600)/60)
    return(glue("{hours}h {mins}m"))
  }
}

#' Send notification via ntfy
send_notification <- function(title, message) {
  tryCatch({
    system(glue('curl -s -d "{message}" https://ntfy.sh/matt-tripper3-jobs -H "Title: {title}"'),
           ignore.stdout = TRUE)
  }, error = function(e) {})
}

# ==============================================================================
# MAIN PROCESSING
# ==============================================================================

main <- function() {

  # --- Parse command line arguments ---
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    stop("Usage: Rscript analyze_structure_slr_flooding_v2.R <config_file.yaml>")
  }

  config_file <- args[1]
  if (!file.exists(config_file)) {
    stop(glue("Config file not found: {config_file}"))
  }

  # --- Load configuration ---
  cat(glue("Loading configuration from: {config_file}\n"))
  config <- yaml::read_yaml(config_file)
  verbose <- config$mode$verbose

  # --- Expand file paths ---
  config$paths$log_dir <- path.expand(config$paths$log_dir)

  # --- Initialize logging ---
  log_con <- init_log(config$paths$log_dir)
  log_msg(glue("Configuration loaded: {config_file}"), log_con, verbose)
  log_msg(glue("Mode: {if (config$mode$test) 'TEST' else 'PRODUCTION'}"), log_con, verbose)

  # --- Connect to database ---
  log_msg("Connecting to database...", log_con, verbose)
  con <- dbConnect(
    Postgres(),
    dbname = config$database$name,
    host   = config$database$host,
    user   = Sys.getenv("USER")
  )
  log_msg(glue("Connected to database: {config$database$name}"), log_con, verbose)

  # --- Tune PostgreSQL for parallel spatial queries ---
  log_msg("Setting PostgreSQL parallel query parameters...", log_con, verbose)
  dbExecute(con, "SET max_parallel_workers_per_gather = 8")
  dbExecute(con, "SET work_mem = '4GB'")
  dbExecute(con, "SET effective_io_concurrency = 200")
  log_msg("  max_parallel_workers_per_gather = 8", log_con, verbose)
  log_msg("  work_mem = 1GB", log_con, verbose)
  log_msg("  effective_io_concurrency = 200", log_con, verbose)

  # --- Get config parameters ---
  scenarios <- config$slr_scenarios
  overwrite <- isTRUE(if (is.null(config$analysis$overwrite_existing)) TRUE
                       else config$analysis$overwrite_existing)
  results_pattern <- config$analysis$results_table_pattern

  log_msg(glue("SLR scenarios: {paste(scenarios, collapse=', ')}"), log_con, verbose)
  log_msg(glue("States: {paste(config$states, collapse=', ')}"), log_con, verbose)
  log_msg(glue("Overwrite existing: {overwrite}"), log_con, verbose)

  # --- Verify region-partitioned SLR tables exist ---
  log_msg("Verifying region-partitioned SLR tables...", log_con, verbose)
  missing_tables <- c()
  for (scenario in scenarios) {
    for (region in config$slr_regions) {
      region_table <- glue("slr_{scenario}_{region}")
      if (!dbExistsTable(con, region_table)) {
        missing_tables <- c(missing_tables, region_table)
      }
    }
  }
  if (length(missing_tables) > 0) {
    log_msg(glue("ERROR: Missing region tables: {paste(missing_tables, collapse=', ')}"), log_con, TRUE)
    log_msg("Run create_slr_region_tables.R first", log_con, TRUE)
    dbDisconnect(con)
    close(log_con)
    stop("Missing region-partitioned SLR tables")
  }
  log_msg("All region-partitioned SLR tables present", log_con, verbose)

  # --- Track overall statistics ---
  overall_start <- Sys.time()
  total_flooded <- 0
  total_checked <- 0
  combinations_processed <- 0

  # --- Process each state × scenario ---
  for (state_code in config$states) {

    state_abbr <- FIPS_TO_STATE[[state_code]]
    region <- STATE_TO_REGION[[state_code]]

    # Determine structure table name
    if (config$mode$test) {
      struct_table <- glue("usa_structures_{state_code}_test")
    } else {
      struct_table <- glue("usa_structures_{state_code}")
    }

    # Verify structure table exists
    if (!dbExistsTable(con, struct_table)) {
      log_msg(glue("WARNING: Structure table not found: {struct_table}. Skipping."),
              log_con, TRUE)
      next
    }

    # Get structure count for this state
    struct_count <- dbGetQuery(con, glue("SELECT COUNT(*) as n FROM {struct_table}"))$n

    for (scenario in scenarios) {

      analysis_start <- Sys.time()

      log_msg("", log_con, verbose)
      log_msg(strrep("-", 60), log_con, verbose)
      log_msg(glue("{state_code} ({state_abbr}) x SLR {scenario} | region={region} | {format(struct_count, big.mark=',')} structures"),
              log_con, verbose)
      log_msg(strrep("-", 60), log_con, verbose)

      # Region-partitioned SLR table
      slr_table <- glue("slr_{scenario}_{state_code}")

      # Results table
      results_table <- glue(results_pattern, state = state_code, scenario = scenario)

      # Check if results table already exists
      if (dbExistsTable(con, results_table)) {
        if (!overwrite) {
          log_msg(glue("  {results_table} already exists. Skipping."), log_con, verbose)
          next
        }
      }

      # Drop existing results table
      dbExecute(con, glue("DROP TABLE IF EXISTS {results_table} CASCADE"))

      # ---------------------------------------------------------------
      # Core query: ST_Intersects with region-constrained SLR table
      # Uses EXISTS for efficiency — returns only flooded structures,
      # stops checking a structure as soon as one SLR polygon matches
      # ---------------------------------------------------------------
      log_msg(glue("  Running ST_Intersects against {slr_table}..."), log_con, verbose)

      intersect_query <- glue('
        CREATE TABLE {results_table} AS
        SELECT s.*
        FROM {struct_table} s
        WHERE EXISTS (
          SELECT 1 FROM {slr_table} slr
          WHERE ST_Intersects(s.geom, slr.geom)
        )
      ')

      dbExecute(con, intersect_query)

      # Get flooded count
      flooded_count <- dbGetQuery(con, glue("SELECT COUNT(*) as n FROM {results_table}"))$n

      # Create spatial index on results
      index_name <- glue("{results_table}_geom_idx")
      dbExecute(con, glue("CREATE INDEX {index_name} ON {results_table} USING GIST(geom)"))

      analysis_elapsed <- as.numeric(difftime(Sys.time(), analysis_start, units = "secs"))

      flood_pct <- if (struct_count > 0) round(100 * flooded_count / struct_count, 2) else 0

      log_msg(glue("  Flooded: {format(flooded_count, big.mark=',')} / {format(struct_count, big.mark=',')} ({flood_pct}%)"),
              log_con, verbose)
      log_msg(glue("  Table: {results_table}"),
              log_con, verbose)
      log_msg(glue("  Time: {format_duration(analysis_elapsed)}"),
              log_con, verbose)

      # Track totals
      total_flooded <- total_flooded + flooded_count
      total_checked <- total_checked + struct_count
      combinations_processed <- combinations_processed + 1

      # Per-state notification
      send_notification(
        glue("{state_abbr} x SLR{scenario} complete"),
        glue("{format(flooded_count, big.mark=',')} flooded of {format(struct_count, big.mark=',')} ({flood_pct}%) in {format_duration(analysis_elapsed)}")
      )

    } # end scenario loop
  } # end state loop

  # --- Overall summary ---
  overall_elapsed <- as.numeric(difftime(Sys.time(), overall_start, units = "secs"))
  overall_pct <- if (total_checked > 0) round(100 * total_flooded / total_checked, 2) else 0

  dbDisconnect(con)

  log_msg("", log_con, verbose)
  log_msg(strrep("=", 78), log_con, verbose)
  log_msg("Structure flooding analysis complete!", log_con, verbose)
  log_msg(glue("  State x scenario combinations: {combinations_processed}"), log_con, verbose)
  log_msg(glue("  Total structures checked:      {format(total_checked, big.mark=',')}"), log_con, verbose)
  log_msg(glue("  Total flooded structures:      {format(total_flooded, big.mark=',')} ({overall_pct}%)"), log_con, verbose)
  log_msg(glue("  Total time:                    {format_duration(overall_elapsed)}"), log_con, verbose)
  log_msg(glue("Finished: {Sys.time()}"), log_con, verbose)
  log_msg(strrep("=", 78), log_con, verbose)
  close(log_con)

  send_notification(
    "All SLR Analysis Complete",
    glue("{format(total_flooded, big.mark=',')} flooded of {format(total_checked, big.mark=',')} across {combinations_processed} combinations in {format_duration(overall_elapsed)}")
  )

  if (isTRUE(config$flags$play_fanfare)) {
    tryCatch(beepr::beep(sound = 8), error = function(e) {})
  }
}

main()
