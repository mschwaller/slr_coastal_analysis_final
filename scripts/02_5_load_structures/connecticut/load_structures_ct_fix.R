#!/usr/bin/env Rscript
# ==============================================================================
# Load Connecticut FEMA Structures — Planning Region FIPS Fix
# ==============================================================================
#
# Purpose: Connecticut-specific reload of FEMA structures into megaSLR.
#          Connecticut reorganized its county-level FIPS codes in 2022,
#          replacing county codes (09001, 09003, ...) with planning region
#          codes (09110, 09120, ...). Census 2025 tracts use the new codes,
#          but the FEMA .gdb still uses the old county codes in CENSUSCODE.
#          A full GEOID equality JOIN produces zero matches.
#
#          This script uses a relaxed JOIN that matches on state prefix (2 digits)
#          + tract suffix (6 digits), skipping the 3-digit county/planning region
#          code. The tract_geoid column is populated from tract_10ft_intersections
#          and therefore uses the new planning region GEOIDs.
#
# Usage:
#   Rscript load_structures_ct_fix.R <config_file.yaml>
#
# Example:
#   Rscript load_structures_ct_fix.R YAML_config_files/structure_analysis_config_v3.yaml
#
# Author: Matt Schwaller
# Date: 2026-04-15
# ==============================================================================

library(RPostgres)
library(yaml)
library(glue)
library(DBI)

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

log_msg <- function(msg, log_con, verbose = TRUE) {
  timestamp <- format(Sys.time(), "%H:%M:%S")
  log_line <- glue("[{timestamp}] {msg}")
  if (verbose) cat(log_line, "\n")
  writeLines(log_line, log_con)
  flush(log_con)
}

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

send_notification <- function(title, message) {
  tryCatch({
    system(glue('curl -s -d "{message}" https://ntfy.sh/matt-tripper3-jobs -H "Title: {title}"'),
           ignore.stdout = TRUE)
  }, error = function(e) {})
}

# ==============================================================================
# MAIN
# ==============================================================================

main <- function() {

  # --- Parse arguments ---
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    stop("Usage: Rscript load_structures_ct_fix.R <config_file.yaml>")
  }

  config_file <- args[1]
  if (!file.exists(config_file)) {
    stop(glue("Config file not found: {config_file}"))
  }

  config <- yaml::read_yaml(config_file)
  verbose <- config$mode$verbose
  config$paths$structures_base_dir <- path.expand(config$paths$structures_base_dir)
  config$paths$log_dir <- path.expand(config$paths$log_dir)

  # --- Initialize logging ---
  dir.create(config$paths$log_dir, showWarnings = FALSE, recursive = TRUE)
  log_file <- file.path(config$paths$log_dir,
    glue("structure_loading_CT_fix_{format(Sys.time(), '%Y-%m-%d_%H-%M-%S')}.txt"))
  log_con <- file(log_file, open = "wt")

  log_msg(strrep("=", 70), log_con, verbose)
  log_msg("Connecticut Structure Loading — Planning Region FIPS Fix", log_con, verbose)
  log_msg(glue("Started: {Sys.time()}"), log_con, verbose)
  log_msg(strrep("=", 70), log_con, verbose)

  # --- Constants ---
  state_code   <- "09"
  state_abbr   <- "CT"
  final_table  <- "usa_structures_09"
  temp_table   <- "_temp_structures_09"
  prefilter_table <- config$prefilter$tract_table

  # --- Connect to database ---
  con <- dbConnect(Postgres(), dbname = config$database$name,
                   host = config$database$host, user = Sys.getenv("USER"))
  log_msg(glue("Connected to database: {config$database$name}"), log_con, verbose)

  # --- Verify pre-filter table ---
  prefilter_count <- dbGetQuery(con,
    glue("SELECT COUNT(*) as n FROM {prefilter_table} WHERE LEFT(geoid, 2) = '09'"))$n
  log_msg(glue("Pre-filter table: {prefilter_table} ({prefilter_count} CT tracts)"), log_con, verbose)
  if (prefilter_count == 0) {
    stop("No Connecticut tracts found in pre-filter table!")
  }

  process_start <- Sys.time()

  # --- Resolve .gdb path ---
  gdb_file <- file.path(config$paths$structures_base_dir, "CT_Structures.gdb")
  if (!file.exists(gdb_file)) {
    stop(glue("GDB file not found: {gdb_file}"))
  }
  log_msg(glue("GDB: {gdb_file}"), log_con, verbose)

  # =====================================================================
  # STAGE 1: ogr2ogr bulk load into temp table
  # =====================================================================
  log_msg("STAGE 1: Loading .gdb into temp table via ogr2ogr...", log_con, verbose)
  load_start <- Sys.time()

  dbExecute(con, glue("DROP TABLE IF EXISTS {temp_table} CASCADE"))

  ogr2ogr_cmd <- glue(
    'ogr2ogr -f PostgreSQL PG:dbname={config$database$name} ',
    '"{gdb_file}" "CT_Structures" ',
    '-nln {temp_table} ',
    '-t_srs EPSG:5070 ',
    '-lco GEOMETRY_NAME=geom ',
    '-progress ',
    '-overwrite'
  )

  ogr_result <- system(ogr2ogr_cmd, intern = FALSE)
  if (ogr_result != 0) {
    stop(glue("ogr2ogr failed with exit code {ogr_result}"))
  }

  temp_count <- dbGetQuery(con, glue("SELECT COUNT(*) as n FROM {temp_table}"))$n
  load_elapsed <- as.numeric(difftime(Sys.time(), load_start, units = "secs"))
  log_msg(glue("Loaded {format(temp_count, big.mark=',')} structures in {format_duration(load_elapsed)}"),
          log_con, verbose)

  # =====================================================================
  # STAGE 2: Filter with relaxed FIPS JOIN
  # =====================================================================
  log_msg("STAGE 2: Filtering with relaxed FIPS JOIN (state prefix + tract suffix)...",
          log_con, verbose)
  filter_start <- Sys.time()

  # Detect censuscode column case
  census_col <- dbGetQuery(con, glue("
    SELECT column_name FROM information_schema.columns
    WHERE table_name = '{temp_table}' AND lower(column_name) = 'censuscode'
  "))$column_name[1]
  cc <- if (census_col == tolower(census_col)) census_col else glue('"{census_col}"')
  log_msg(glue("Census code column: {cc}"), log_con, verbose)

  dbExecute(con, glue("DROP TABLE IF EXISTS {final_table} CASCADE"))

  # Match on state prefix (2 digits) + tract suffix (6 digits),
  # skipping the county/planning region digits (positions 3-5)
  filter_query <- glue('
    CREATE TABLE {final_table} AS
    SELECT s.*, t.geoid AS tract_geoid
    FROM {temp_table} s
    JOIN {prefilter_table} t
      ON LEFT(s.{cc}, 2) = LEFT(t.geoid, 2)
      AND RIGHT(s.{cc}, 6) = RIGHT(t.geoid, 6)
    WHERE LEFT(s.{cc}, 2) = \'09\'
  ')

  dbExecute(con, filter_query)

  filtered_count <- dbGetQuery(con, glue("SELECT COUNT(*) as n FROM {final_table}"))$n
  filter_elapsed <- as.numeric(difftime(Sys.time(), filter_start, units = "secs"))
  pct_kept <- round(100 * filtered_count / temp_count, 1)

  log_msg(glue("Filtered: {format(temp_count, big.mark=',')} -> {format(filtered_count, big.mark=',')} ",
               "structures ({pct_kept}% kept)"),
          log_con, verbose)
  log_msg(glue("Filter completed in {format_duration(filter_elapsed)}"), log_con, verbose)

  # =====================================================================
  # STAGE 3: Spatial index
  # =====================================================================
  log_msg("STAGE 3: Creating spatial index...", log_con, verbose)
  index_start <- Sys.time()
  dbExecute(con, glue("CREATE INDEX {final_table}_geom_idx ON {final_table} USING GIST(geom)"))
  index_elapsed <- as.numeric(difftime(Sys.time(), index_start, units = "secs"))
  log_msg(glue("Index created in {format_duration(index_elapsed)}"), log_con, verbose)

  # =====================================================================
  # STAGE 4: Cleanup
  # =====================================================================
  log_msg("STAGE 4: Dropping temp table...", log_con, verbose)
  dbExecute(con, glue("DROP TABLE IF EXISTS {temp_table} CASCADE"))

  # --- Summary ---
  process_elapsed <- as.numeric(difftime(Sys.time(), process_start, units = "secs"))

  log_msg("", log_con, verbose)
  log_msg(strrep("=", 70), log_con, verbose)
  log_msg("Connecticut structure loading complete!", log_con, verbose)
  log_msg(glue("  Loaded:   {format(temp_count, big.mark=',')} structures from .gdb"), log_con, verbose)
  log_msg(glue("  Filtered: {format(filtered_count, big.mark=',')} in SLR tracts ({pct_kept}%)"), log_con, verbose)
  log_msg(glue("  Table:    {final_table}"), log_con, verbose)
  log_msg(glue("  Note:     tract_geoid uses new planning region GEOIDs"), log_con, verbose)
  log_msg(glue("  Time:     {format_duration(process_elapsed)}"), log_con, verbose)
  log_msg(strrep("=", 70), log_con, verbose)
  close(log_con)

  dbDisconnect(con)

  send_notification(
    "CT structures loaded (FIPS fix)",
    glue("{format(filtered_count, big.mark=',')} of {format(temp_count, big.mark=',')} kept ({pct_kept}%) in {format_duration(process_elapsed)}")
  )
}

main()
