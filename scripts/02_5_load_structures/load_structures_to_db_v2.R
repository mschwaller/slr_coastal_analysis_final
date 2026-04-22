#!/usr/bin/env Rscript
# ==============================================================================
# Load FEMA Structure Polygons into PostgreSQL Database (v2)
# ==============================================================================
#
# Purpose: Imports FEMA USA Structures geodatabase files into megaSLR database
#          with tract-based pre-filtering to keep only structures in SLR-affected
#          Census tracts. Uses ogr2ogr for bulk loading (avoids R memory limits)
#          and SQL JOINs for filtering.
#
# Pipeline per state:
#   1. ogr2ogr loads full state .gdb into a temp table
#   2. SQL JOIN against public.tract_10ft_intersections filters to coastal structures
#   3. Create spatial index on filtered table
#   4. Drop temp table
#
# Usage:
#   Rscript load_structures_to_db_v2.R <config_file.yaml>
#
# Example:
#   Rscript load_structures_to_db_v2.R YAML_config_files/structure_analysis_config_v2.yaml
#
# Author: Matt Schwaller
# Date: 2026-02-28
# ==============================================================================

library(RPostgres)
library(yaml)
library(glue)
library(DBI)

# ==============================================================================
# STATE FIPS TO ABBREVIATION MAPPING
# ==============================================================================

FIPS_TO_STATE <- list(
  "01" = "AL",  "06" = "CA",  "09" = "CT",  "10" = "DE",  "11" = "DC",
  "12" = "FL",  "13" = "GA",  "22" = "LA",  "23" = "ME",  "24" = "MD",
  "25" = "MA",  "28" = "MS",  "33" = "NH",  "34" = "NJ",  "36" = "NY",
  "37" = "NC",  "41" = "OR",  "42" = "PA",  "44" = "RI",  "45" = "SC",
  "48" = "TX",  "51" = "VA",  "53" = "WA"
)

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

#' Initialize logging
init_log <- function(log_dir) {
  timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  log_file <- file.path(log_dir, glue("structure_loading_{timestamp}.txt"))
  dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)
  log_con <- file(log_file, open = "wt")
  writeLines(c(
    strrep("=", 78),
    "FEMA Structure Loading Log (v2 - ogr2ogr + tract filter)",
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
    stop("Usage: Rscript load_structures_to_db_v2.R <config_file.yaml>")
  }
  
  config_file <- args[1]
  if (!file.exists(config_file)) {
    stop(glue("Config file not found: {config_file}"))
  }
  
  # --- Load configuration ---
  cat(glue("Loading configuration from: {config_file}\n"))
  config <- yaml::read_yaml(config_file)
  verbose <- config$mode$verbose
  
  # --- Expand ~ in all file paths ---
  config$paths$structures_base_dir <- path.expand(config$paths$structures_base_dir)
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
  
  # --- Verify pre-filter table exists ---
  prefilter_table <- config$prefilter$tract_table
  check_q <- glue("SELECT COUNT(*) as n FROM {prefilter_table}")
  prefilter_count <- dbGetQuery(con, check_q)$n
  if (prefilter_count == 0) {
    stop(glue("Pre-filter table {prefilter_table} is empty or does not exist!"))
  }
  log_msg(glue("Pre-filter table: {prefilter_table} ({format(prefilter_count, big.mark=',')} tracts)"),
          log_con, verbose)
  
  # --- Track overall statistics ---
  overall_start <- Sys.time()
  total_loaded   <- 0
  total_filtered <- 0
  states_processed <- 0
  
  # --- Process each state ---
  for (state_code in config$states) {
    
    process_start <- Sys.time()
    
    log_msg("", log_con, verbose)
    log_msg(strrep("=", 60), log_con, verbose)
    log_msg(glue("Processing state: {state_code} ({FIPS_TO_STATE[[state_code]]})"),
            log_con, verbose)
    log_msg(strrep("=", 60), log_con, verbose)
    
    # --- Resolve state abbreviation and file paths ---
    state_abbr <- FIPS_TO_STATE[[state_code]]
    if (is.null(state_abbr)) {
      log_msg(glue("ERROR: Unknown FIPS code: {state_code}"), log_con, TRUE)
      next
    }
    
    gdb_file   <- glue("{state_abbr}_Structures.gdb")
    gdb_path   <- file.path(config$paths$structures_base_dir, gdb_file)
    layer_name <- glue("{state_abbr}_Structures")
    
    if (!file.exists(gdb_path)) {
      log_msg(glue("ERROR: GDB file not found: {gdb_path}"), log_con, TRUE)
      next
    }
    log_msg(glue("GDB: {gdb_path}"), log_con, verbose)
    
    # --- Determine table names ---
    if (config$mode$test) {
      test_county  <- config$test_counties[[state_code]]
      final_table  <- glue("usa_structures_{state_code}_test")
      log_msg(glue("TEST MODE: Will filter to county {test_county}"), log_con, verbose)
    } else {
      final_table <- glue("usa_structures_{state_code}")
    }
    temp_table <- glue("_temp_structures_{state_code}")
    
    # --- Check if final table already exists ---
    if (dbExistsTable(con, final_table)) {
      # skip_if_exists takes priority: don't touch the table
      if (isTRUE(config$load_structures$skip_if_exists)) {
        log_msg(glue("Table {final_table} already exists. Skipping (skip_if_exists=TRUE)"),
                log_con, verbose)
        next
      }
      # overwrite_existing: default TRUE if missing or garbled
      overwrite <- isTRUE(if (is.null(config$load_structures$overwrite_existing)) TRUE 
                          else config$load_structures$overwrite_existing)
      if (overwrite) {
        log_msg(glue("Table {final_table} already exists. Will overwrite (overwrite_existing=TRUE)"),
                log_con, verbose)
      } else {
        log_msg(glue("Table {final_table} already exists. Skipping (overwrite_existing=FALSE)"),
                log_con, verbose)
        next
      }
    }
    
    # =====================================================================
    # STAGE 1: ogr2ogr bulk load into temp table
    # =====================================================================
    log_msg("STAGE 1: Loading .gdb into temp table via ogr2ogr...", log_con, verbose)
    load_start <- Sys.time()
    
    # Drop temp table if it exists from a prior failed run
    dbExecute(con, glue("DROP TABLE IF EXISTS {temp_table} CASCADE"))
    
    ogr2ogr_cmd <- glue(
      'ogr2ogr -f PostgreSQL PG:dbname={config$database$name} ',
      '"{gdb_path}" "{layer_name}" ',
      '-nln {temp_table} ',
      '-t_srs EPSG:5070 ',
      '-lco GEOMETRY_NAME=geom ',
      '-progress ',
      '-overwrite'
    )
    
    log_msg(glue("Running: ogr2ogr ..."), log_con, verbose)
    ogr_result <- system(ogr2ogr_cmd, intern = FALSE)
    
    if (ogr_result != 0) {
      log_msg(glue("ERROR: ogr2ogr failed with exit code {ogr_result}"), log_con, TRUE)
      next
    }
    
    # Get row count from temp table
    temp_count <- dbGetQuery(con, glue("SELECT COUNT(*) as n FROM {temp_table}"))$n
    load_elapsed <- as.numeric(difftime(Sys.time(), load_start, units = "secs"))
    log_msg(glue("Loaded {format(temp_count, big.mark=',')} structures in {format_duration(load_elapsed)}"),
            log_con, verbose)
    total_loaded <- total_loaded + temp_count
    
    # =====================================================================
    # STAGE 2: Filter to SLR-affected tracts via SQL JOIN
    # =====================================================================
    log_msg("STAGE 2: Filtering to SLR-affected tracts...", log_con, verbose)
    filter_start <- Sys.time()
    
    # Drop final table if it exists
    dbExecute(con, glue("DROP TABLE IF EXISTS {final_table} CASCADE"))
    
    # Detect the actual column name (ogr2ogr lowercases, st_write preserves case)
    census_col_q <- glue("
      SELECT column_name FROM information_schema.columns
      WHERE table_name = '{temp_table}' AND lower(column_name) = 'censuscode'
    ")
    census_col <- dbGetQuery(con, census_col_q)$column_name[1]
    if (is.na(census_col)) {
      log_msg("ERROR: No CENSUSCODE column found in temp table", log_con, TRUE)
      dbExecute(con, glue("DROP TABLE IF EXISTS {temp_table} CASCADE"))
      next
    }
    # Quote if mixed-case, plain if lowercase
    cc <- if (census_col == tolower(census_col)) census_col else glue('"{census_col}"')
    log_msg(glue("Census code column: {cc}"), log_con, verbose)
    
    if (config$mode$test) {
      # Test mode: filter to specific county AND SLR tracts
      filter_query <- glue('
        CREATE TABLE {final_table} AS
        SELECT s.*, t.geoid AS tract_geoid
        FROM {temp_table} s
        JOIN {prefilter_table} t
          ON s.{cc} = t.geoid
        WHERE LEFT(s.{cc}, 5) = \'{state_code}{test_county}\'
      ')
    } else {
      # Production mode: filter to all SLR tracts for this state
      filter_query <- glue('
        CREATE TABLE {final_table} AS
        SELECT s.*, t.geoid AS tract_geoid
        FROM {temp_table} s
        JOIN {prefilter_table} t
          ON s.{cc} = t.geoid
        WHERE LEFT(s.{cc}, 2) = \'{state_code}\'
      ')
    }
    
    dbExecute(con, filter_query)
    
    filtered_count <- dbGetQuery(con, glue("SELECT COUNT(*) as n FROM {final_table}"))$n
    filter_elapsed <- as.numeric(difftime(Sys.time(), filter_start, units = "secs"))
    pct_kept <- round(100 * filtered_count / temp_count, 1)
    
    log_msg(glue("Filtered: {format(temp_count, big.mark=',')} -> {format(filtered_count, big.mark=',')} ",
                 "structures ({pct_kept}% kept)"),
            log_con, verbose)
    log_msg(glue("Filter completed in {format_duration(filter_elapsed)}"), log_con, verbose)
    total_filtered <- total_filtered + filtered_count
    
    # =====================================================================
    # STAGE 3: Create spatial index
    # =====================================================================
    if (config$load_structures$create_spatial_index) {
      log_msg("STAGE 3: Creating spatial index...", log_con, verbose)
      index_start <- Sys.time()
      
      index_name <- glue("{final_table}_geom_idx")
      dbExecute(con, glue('CREATE INDEX {index_name} ON {final_table} USING GIST(geom)'))
      
      index_elapsed <- as.numeric(difftime(Sys.time(), index_start, units = "secs"))
      log_msg(glue("Index created in {format_duration(index_elapsed)}"), log_con, verbose)
    }
    
    # =====================================================================
    # STAGE 4: Cleanup temp table
    # =====================================================================
    log_msg("STAGE 4: Dropping temp table...", log_con, verbose)
    dbExecute(con, glue("DROP TABLE IF EXISTS {temp_table} CASCADE"))
    
    # --- State summary ---
    process_elapsed <- as.numeric(difftime(Sys.time(), process_start, units = "secs"))
    states_processed <- states_processed + 1
    
    log_msg("", log_con, verbose)
    log_msg(glue("State {state_code} ({state_abbr}) complete!"), log_con, verbose)
    log_msg(glue("  Loaded:   {format(temp_count, big.mark=',')} structures from .gdb"),
            log_con, verbose)
    log_msg(glue("  Filtered: {format(filtered_count, big.mark=',')} in SLR tracts ({pct_kept}%)"),
            log_con, verbose)
    log_msg(glue("  Table:    {final_table}"), log_con, verbose)
    log_msg(glue("  Time:     {format_duration(process_elapsed)}"), log_con, verbose)
    
    # Notify per state
    send_notification(
      glue("{state_abbr} structures loaded"),
      glue("{format(filtered_count, big.mark=',')} of {format(temp_count, big.mark=',')} kept ({pct_kept}%) in {format_duration(process_elapsed)}")
    )
  }
  
  # --- Overall summary ---
  overall_elapsed <- as.numeric(difftime(Sys.time(), overall_start, units = "secs"))
  overall_pct <- if (total_loaded > 0) round(100 * total_filtered / total_loaded, 1) else 0
  
  dbDisconnect(con)
  
  log_msg("", log_con, verbose)
  log_msg(strrep("=", 78), log_con, verbose)
  log_msg("Structure loading complete!", log_con, verbose)
  log_msg(glue("  States processed:   {states_processed}"), log_con, verbose)
  log_msg(glue("  Total loaded:       {format(total_loaded, big.mark=',')} structures from .gdb files"),
          log_con, verbose)
  log_msg(glue("  Total after filter: {format(total_filtered, big.mark=',')} structures ({overall_pct}%)"),
          log_con, verbose)
  log_msg(glue("  Total time:         {format_duration(overall_elapsed)}"), log_con, verbose)
  log_msg(glue("Finished: {Sys.time()}"), log_con, verbose)
  log_msg(strrep("=", 78), log_con, verbose)
  close(log_con)
  
  send_notification(
    "All Structure Loading Complete",
    glue("{format(total_filtered, big.mark=',')} structures across {states_processed} states in {format_duration(overall_elapsed)}")
  )
  
  if (config$flags$play_fanfare) {
    tryCatch(beepr::beep(sound = 8), error = function(e) {})
  }
}

# Run main function
main()