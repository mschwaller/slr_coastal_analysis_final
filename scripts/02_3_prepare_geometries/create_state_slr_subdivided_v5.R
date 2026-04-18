#!/usr/bin/env Rscript
# ==============================================================================
# Create Per-State Subdivided SLR Tables (v5)
# ==============================================================================
#
# Purpose: Creates per-state SLR tables by clipping regional SLR polygons to
#          each state's census tract extent and subdividing complex polygons
#          using ST_Subdivide. This dramatically improves ST_Intersects
#          performance by producing smaller polygons with tighter bounding boxes.
#
# v4 changes:
#   1. Two-stage spatial filter: In addition to the coarse bounding box filter,
#      an EXISTS clause ensures each SLR polygon actually overlaps at least one
#      of the state's census tracts. This eliminates neighbor-state polygons
#      captured by the bbox (e.g., Delaware's bbox captures NJ/MD/VA coastline).
#   2. Supports batch YAML overrides for parallel execution. Run 2-3 instances
#      with different config files, each processing a subset of states.
#
# Prerequisites:
#   1. Region-partitioned SLR tables (e.g., slr_0ft_atlantic) must exist
#   2. Census tracts table (census_tracts_2025) must exist with spatial index
#
# Output: Per-state subdivided SLR tables
#         Table naming: slr_{scenario}_{state_code}
#         Example: slr_0ft_09, slr_3ft_09
#
# Usage:
#   # Single run (all states):
#   Rscript create_state_slr_subdivided_v5.R structure_analysis_config_v3.yaml
#
#   # Parallel batches (run each in a separate screen session):
#   screen -S batch1
#   Rscript create_state_slr_subdivided_v5.R config_batch_atlantic.yaml
#   # Ctrl+A, d
#   screen -S batch2
#   Rscript create_state_slr_subdivided_v5.R config_batch_gulf.yaml
#   # Ctrl+A, d
#   screen -S batch3
#   Rscript create_state_slr_subdivided_v5.R config_batch_west.yaml
#
# Author: Matt Schwaller
# Date: 2026-04
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

init_log <- function(log_dir, batch_label = NULL) {
  timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  suffix <- if (!is.null(batch_label)) glue("_{batch_label}") else ""
  log_file <- file.path(log_dir, glue("create_state_slr_subdivided_v5{suffix}_{timestamp}.txt"))
  dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)
  log_con <- file(log_file, open = "wt")
  writeLines(c(
    strrep("=", 78),
    "Create Per-State Subdivided SLR Tables (v5 - two-stage filter)",
    if (!is.null(batch_label)) glue("Batch: {batch_label}") else "Batch: all states",
    glue("Started: {Sys.time()}"),
    strrep("=", 78)
  ), log_con)
  return(log_con)
}

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
# MAIN PROCESSING
# ==============================================================================

main <- function() {
  
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    stop("Usage: Rscript create_state_slr_subdivided_v5.R <config_file.yaml> [--state XX]")
  }
  
  config_file <- args[1]
  if (!file.exists(config_file)) {
    stop(glue("Config file not found: {config_file}"))
  }
  
  # Parse optional --state override
  state_override <- NULL
  if (length(args) >= 3 && args[2] == "--state") {
    state_override <- args[3]
  }
  
  cat(glue("Loading configuration from: {config_file}\n"))
  config <- yaml::read_yaml(config_file)
  verbose <- config$mode$verbose
  config$paths$log_dir <- path.expand(config$paths$log_dir)
  use_make_valid <- config$use_make_valid
  
  if (is.null(use_make_valid)) use_make_valid <- TRUE
  
  # Apply --state override: replace states list with single state
  if (!is.null(state_override)) {
    if (!state_override %in% names(STATE_TO_REGION)) {
      stop(glue("Unknown state FIPS code: {state_override}"))
    }
    config$states <- list(state_override)
    state_abbr <- FIPS_TO_STATE[[state_override]]
    cat(glue("  --state override: processing only {state_override} ({state_abbr})\n"))
  }
  
  # Batch label: use state abbr if --state override, else config value, else NULL
  batch_label <- if (!is.null(state_override)) {
    FIPS_TO_STATE[[state_override]]
  } else {
    config$batch_label  # NULL if not set
  }
  
  log_con <- init_log(config$paths$log_dir, batch_label)
  log_msg(glue("Configuration loaded: {config_file}"), log_con, verbose)
  
  # --- Subdivide parameters (with defaults) ---
  max_vertices <- if (!is.null(config$subdivide$max_vertices)) {
    config$subdivide$max_vertices
  } else {
    256
  }
  buffer_meters <- if (!is.null(config$subdivide$buffer_meters)) {
    config$subdivide$buffer_meters
  } else {
    1000  # ~1km buffer around tract extent
  }
  overwrite <- isTRUE(if (!is.null(config$subdivide$overwrite_existing)) {
    config$subdivide$overwrite_existing
  } else {
    TRUE
  })
  
  # Census tracts table for bounding box and EXISTS check
  tracts_table <- if (!is.null(config$subdivide$tracts_table)) {
    config$subdivide$tracts_table
  } else {
    "census_tracts_2025"
  }
  
  log_msg(glue("ST_Subdivide max_vertices: {max_vertices}"), log_con, verbose)
  log_msg(glue("Bounding box source: {tracts_table} (census tracts)"), log_con, verbose)
  log_msg(glue("Bounding box buffer: {buffer_meters}"), log_con, verbose)
  log_msg(glue("Overwrite existing: {overwrite}"), log_con, verbose)
  log_msg(glue("Use ST_MakeValid: {use_make_valid}"), log_con, verbose)
  log_msg(glue("SLR scenarios: {paste(config$slr_scenarios, collapse=', ')}"), log_con, verbose)
  log_msg(glue("States: {paste(config$states, collapse=', ')}"), log_con, verbose)
  if (!is.null(batch_label)) {
    log_msg(glue("Batch label: {batch_label}"), log_con, verbose)
  }
  
  log_msg("Connecting to database...", log_con, verbose)
  con <- dbConnect(
    Postgres(),
    dbname = config$database$name,
    host   = config$database$host,
    user   = Sys.getenv("USER")
  )
  log_msg(glue("Connected to database: {config$database$name}"), log_con, verbose)
  
  # Verify census tracts table exists
  if (!dbExistsTable(con, tracts_table)) {
    stop(glue("Census tracts table '{tracts_table}' not found in database."))
  }
  log_msg(glue("Verified: {tracts_table} exists"), log_con, verbose)
  
  # Verify spatial index on census tracts (needed for EXISTS performance)
  idx_check <- dbGetQuery(con, glue("
    SELECT indexname FROM pg_indexes
    WHERE tablename = '{tracts_table}' AND indexdef LIKE '%gist%'
  "))
  if (nrow(idx_check) > 0) {
    log_msg(glue("Verified: spatial index on {tracts_table} ({idx_check$indexname[1]})"), log_con, verbose)
  } else {
    log_msg(glue("WARNING: No GIST index found on {tracts_table} — EXISTS clause may be slow"), log_con, verbose)
  }
  
  # Tune PostgreSQL for subdivide operations
  # ST_MakeValid is not parallel-safe — disable parallel workers
  dbExecute(con, "SET max_parallel_workers_per_gather = 0")
  dbExecute(con, "SET work_mem = '8GB'")
  dbExecute(con, "SET effective_io_concurrency = 200")
  log_msg("PostgreSQL: parallel_workers=0, work_mem=8GB, io_concurrency=200", log_con, verbose)
  
  overall_start <- Sys.time()
  tables_created <- 0
  tables_skipped <- 0
  
  for (state_code in config$states) {
    
    state_abbr <- FIPS_TO_STATE[[state_code]]
    region <- STATE_TO_REGION[[state_code]]
    
    # send a ntfy notification to start
    send_notification(
      glue("Subdivide: Starting {state_abbr}"),
      glue("State {state_code} ({state_abbr}) | region: {region}")
    )
    state_start <- Sys.time()
    
    for (scenario in config$slr_scenarios) {
      
      state_slr_table <- glue("slr_{scenario}_{state_code}")
      region_slr_table <- glue("slr_{scenario}_{region}")
      
      log_msg("", log_con, verbose)
      log_msg(strrep("-", 60), log_con, verbose)
      log_msg(glue("{state_code} ({state_abbr}) | {scenario} | source: {region_slr_table}"),
              log_con, verbose)
      log_msg(strrep("-", 60), log_con, verbose)
      
      if (dbExistsTable(con, state_slr_table)) {
        if (!overwrite) {
          log_msg(glue("  {state_slr_table} already exists. Skipping."), log_con, verbose)
          tables_skipped <- tables_skipped + 1
          next
        }
      }
      
      step_start <- Sys.time()
      
      dbExecute(con, glue("DROP TABLE IF EXISTS {state_slr_table} CASCADE"))
      
      # -----------------------------------------------------------------
      # Create per-state subdivided SLR table (two-stage filter):
      #
      #   Stage 1 (coarse): Bounding box filter — only SLR polygons whose
      #     bbox overlaps the state's census tract extent (+ buffer). This
      #     is fast because it uses the spatial index on the region table.
      #
      #   Stage 2 (fine): EXISTS filter — among those bbox matches, keep
      #     only polygons that actually overlap at least one of the state's
      #     census tracts. This eliminates neighbor-state polygons captured
      #     by the bbox (e.g., Delaware's bbox captures NJ/MD/VA coastline).
      #     Uses the spatial index on census_tracts_2025 so it's efficient.
      #
      #   Then: ST_Subdivide breaks complex polygons into pieces with
      #     <= max_vertices vertices each.
      #
      # v5: A fefw bug fixes
      # v4: Added EXISTS clause (stage 2) to tighten state coverage.
      # v3: Bounding box from census_tracts_2025 (not usa_structures).
      # -----------------------------------------------------------------
      log_msg(glue("  Two-stage filter: bbox + EXISTS on {tracts_table} (statefp={state_code})"),
              log_con, verbose)
      log_msg(glue("  Subdividing with max {max_vertices} vertices..."),
              log_con, verbose)
      
      geom_expr <- if (use_make_valid) {
        glue("ST_Subdivide(ST_MakeValid(r.geom), {max_vertices})")
      } else {
        glue("ST_Subdivide(r.geom, {max_vertices})")
      }
      
      create_query <- glue('
        CREATE TABLE {state_slr_table} AS
        SELECT {geom_expr} AS geom
        FROM {region_slr_table} r
        WHERE r.geom && (
          SELECT ST_Expand(ST_Extent(geom), {buffer_meters})
          FROM {tracts_table}
          WHERE statefp = \'{state_code}\'
        )
        AND EXISTS (
          SELECT 1 FROM {tracts_table} t
          WHERE t.statefp = \'{state_code}\'
          AND t.geom && r.geom
        )
      ')
      
      dbExecute(con, create_query)
      
      poly_count <- dbGetQuery(con, glue(
        "SELECT COUNT(*) AS n FROM {state_slr_table}"
      ))$n
      
      clip_elapsed <- as.numeric(difftime(Sys.time(), step_start, units = "secs"))
      log_msg(glue("  Subdivided polygons: {format(poly_count, big.mark=',')}"),
              log_con, verbose)
      log_msg(glue("  Subdivide time: {format_duration(clip_elapsed)}"), log_con, verbose)
      
      log_msg("  Creating spatial index...", log_con, verbose)
      idx_start <- Sys.time()
      index_name <- glue("{state_slr_table}_geom_idx")
      dbExecute(con, glue("CREATE INDEX {index_name} ON {state_slr_table} USING GIST(geom)"))
      dbExecute(con, glue("ANALYZE {state_slr_table}"))
      idx_elapsed <- as.numeric(difftime(Sys.time(), idx_start, units = "secs"))
      log_msg(glue("  Index + analyze time: {format_duration(idx_elapsed)}"),
              log_con, verbose)
      
      total_elapsed <- as.numeric(difftime(Sys.time(), step_start, units = "secs"))
      log_msg(glue("  Total: {format_duration(total_elapsed)}"), log_con, verbose)
      log_msg(glue("  Table: {state_slr_table}"), log_con, verbose)
      
      tables_created <- tables_created + 1
      
    } # end scenario loop
    
    # Notify completion of state
    state_elapsed <- as.numeric(difftime(Sys.time(), state_start, units = "secs"))
    send_notification(
      glue("Subdivide: {state_abbr} complete"),
      glue("State {state_code} ({state_abbr}) finished in {format_duration(state_elapsed)}")
    )
    
  } # end state loop
  
  overall_elapsed <- as.numeric(difftime(Sys.time(), overall_start, units = "secs"))
  dbDisconnect(con)
  
  log_msg("", log_con, verbose)
  log_msg(strrep("=", 78), log_con, verbose)
  log_msg("Per-state subdivided SLR table creation complete!", log_con, verbose)
  log_msg(glue("  Tables created: {tables_created}"), log_con, verbose)
  log_msg(glue("  Tables skipped: {tables_skipped}"), log_con, verbose)
  log_msg(glue("  Total time: {format_duration(overall_elapsed)}"), log_con, verbose)
  log_msg(glue("Finished: {Sys.time()}"), log_con, verbose)
  log_msg(strrep("=", 78), log_con, verbose)
  close(log_con)
  
  send_notification(
    glue("State SLR Subdivide Complete{if (!is.null(batch_label)) paste0(' (', batch_label, ')') else ''}"),
    glue("{tables_created} tables created in {format_duration(overall_elapsed)}")
  )
  
  if (isTRUE(config$flags$play_fanfare)) {
    tryCatch(beepr::beep(sound = 8), error = function(e) {})
  }
}

main()
