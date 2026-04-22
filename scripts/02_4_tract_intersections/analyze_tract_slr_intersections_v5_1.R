#!/usr/bin/env Rscript
# ==============================================================================
# Census Tracts × SLR Intersection Analysis (v5)
# ==============================================================================
#
# Purpose: Computes area-based intersections between Census tracts and
#          pre-subdivided, state-partitioned SLR flood polygons.
#
# v5 changes:
#   1. Removed state_substate chunking — loops over states (statefp) directly
#   2. Uses ST_Union + ST_Intersection for exact area calculation (handles
#      overlapping source polygons correctly, e.g. Keys tracts with ~1000 polys)
#   3. Supports --state XX override for parallel execution (one screen per state)
#   4. Writes directly to output table via INSERT (no temp tables / UNION ALL)
#
# Prerequisites:
#   1. Census tracts loaded (census_tracts_2025 with statefp, spatial index)
#   2. Subdivided SLR tables: slr_{scenario}_{fips}
#
# Output: One table per SLR scenario (e.g., tract_0ft_intersections)
#
# Usage:
#   # All states, all scenarios:
#   Rscript analyze_tract_slr_intersections_v5.R config.yaml
#
#   # Single state (for parallel launches):
#   Rscript analyze_tract_slr_intersections_v5.R config.yaml --state 12
#
# Author: Matt Schwaller
# Date: 2026-04
# ==============================================================================

library(DBI)
library(RPostgres)
library(glue)
library(yaml)

# ==============================================================================
# STATE LOOKUP
# ==============================================================================

FIPS_TO_STATE <- list(
  "01" = "AL", "06" = "CA", "09" = "CT", "10" = "DE", "11" = "DC", "12" = "FL",
  "13" = "GA", "22" = "LA", "23" = "ME", "24" = "MD", "25" = "MA",
  "28" = "MS", "33" = "NH", "34" = "NJ", "36" = "NY", "37" = "NC",
  "41" = "OR", "42" = "PA", "44" = "RI", "45" = "SC", "48" = "TX", "51" = "VA",
  "53" = "WA"
)

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

format_duration <- function(seconds) {
  if (seconds < 60) {
    return(glue("{round(seconds, 1)} seconds"))
  } else if (seconds < 3600) {
    return(glue("{round(seconds/60, 1)} minutes"))
  } else {
    hours <- floor(seconds / 3600)
    mins <- round((seconds %% 3600) / 60)
    return(glue("{hours}h {mins}m"))
  }
}

init_log <- function(log_dir, batch_label = NULL) {
  timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  suffix <- if (!is.null(batch_label)) glue("_{batch_label}") else ""
  log_file <- file.path(log_dir, glue("tract_slr_analysis_v5{suffix}_{timestamp}.txt"))
  dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)
  log_con <- file(log_file, open = "wt")
  writeLines(c(
    strrep("=", 78),
    "Census Tracts x SLR Intersection Analysis (v5 - per-state, no substates)",
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

send_notification <- function(title, message, topic) {
  tryCatch({
    system(glue('curl -s -d "{message}" https://ntfy.sh/{topic} -H "Title: {title}"'),
           ignore.stdout = TRUE)
  }, error = function(e) {})
}

# ==============================================================================
# PROCESS ONE SCENARIO FOR ONE STATE
# ==============================================================================

process_state_scenario <- function(state_code, scenario, config, con, log_con, output_table) {

  verbose <- config$mode$verbose
  tracts_table <- config$tracts_table
  state_abbr <- FIPS_TO_STATE[[state_code]]
  slr_table <- glue("slr_{scenario}_{state_code}")

  step_start <- Sys.time()

  log_msg(glue("  {state_code} ({state_abbr}) × {scenario} → {slr_table}"), log_con, verbose)

  # Verify SLR table exists
  if (!dbExistsTable(con, slr_table)) {
    log_msg(glue("    ✗ Missing: {slr_table}"), log_con, TRUE)
    return(list(state = state_code, status = "failed", rows = 0))
  }

  tryCatch({

    dbExecute(con, glue("
      INSERT INTO {output_table} (geoid, namelsad, statefp, tract_area_ha, slr_area_ha)
      SELECT
        t.geoid,
        t.namelsad,
        t.statefp,
        ST_Area(t.geom) / 10000 AS tract_area_ha,
        ST_Area(ST_Intersection(ST_Union(s.geom), t.geom)) / 10000 AS slr_area_ha
      FROM {tracts_table} t
      JOIN {slr_table} s ON ST_Intersects(s.geom, t.geom)
      WHERE t.statefp = '{state_code}'
      GROUP BY t.geoid, t.namelsad, t.statefp, t.geom
    "))

    row_count <- dbGetQuery(con, glue(
      "SELECT COUNT(*) FROM {output_table} WHERE statefp = '{state_code}'"
    ))[[1]]

    elapsed <- as.numeric(difftime(Sys.time(), step_start, units = "secs"))
    log_msg(glue("    ✓ {format(row_count, big.mark = ',')} tracts in {format_duration(elapsed)}"),
            log_con, verbose)

    return(list(state = state_code, status = "complete", rows = row_count))

  }, error = function(e) {
    elapsed <- as.numeric(difftime(Sys.time(), step_start, units = "secs"))
    log_msg(glue("    ✗ FAILED ({format_duration(elapsed)}): {e$message}"), log_con, TRUE)
    return(list(state = state_code, status = "failed", rows = 0))
  })
}

# ==============================================================================
# PROCESS ONE SCENARIO (ALL STATES)
# ==============================================================================

process_scenario <- function(scenario, states, config, con, log_con, state_override = NULL) {

  verbose <- config$mode$verbose
  base_output_table <- glue(config$analysis$output_table_pattern)
  overwrite <- isTRUE(config$analysis$overwrite_existing)
  ntfy_topic <- config$notifications$ntfy_topic

  # In --state mode, write to per-state table; in full mode, write to combined table
  if (!is.null(state_override)) {
    output_table <- glue("{base_output_table}_{state_override}")
  } else {
    output_table <- base_output_table
  }

  scenario_start <- Sys.time()

  log_msg("", log_con, verbose)
  log_msg(strrep("=", 60), log_con, verbose)
  log_msg(glue("SCENARIO: SLR {scenario} → {output_table}"), log_con, verbose)
  log_msg(strrep("=", 60), log_con, verbose)

  # --- Check for existing output ---
  if (dbExistsTable(con, output_table)) {
    if (!overwrite) {
      log_msg(glue("  {output_table} already exists. Skipping (overwrite_existing = false)."),
              log_con, verbose)
      return(list(scenario = scenario, status = "skipped", rows = NA, elapsed = 0))
    }
    log_msg(glue("  {output_table} exists — will overwrite."), log_con, verbose)
  }

  # --- Create empty output table ---
  dbExecute(con, glue("DROP TABLE IF EXISTS {output_table} CASCADE"))
  dbExecute(con, glue("
    CREATE TABLE {output_table} (
      geoid TEXT,
      namelsad TEXT,
      statefp TEXT,
      tract_area_ha DOUBLE PRECISION,
      slr_area_ha DOUBLE PRECISION
    )
  "))

  # --- Process each state ---
  n_states <- length(states)
  failed_states <- c()
  total_rows <- 0

  for (i in seq_along(states)) {
    state_code <- states[i]
    log_msg(glue("  ({i}/{n_states})"), log_con, verbose)

    result <- process_state_scenario(state_code, scenario, config, con, log_con, output_table)

    if (result$status == "failed") {
      failed_states <- c(failed_states, state_code)
    } else {
      total_rows <- total_rows + result$rows
    }
  }

  # --- Create index ---
  log_msg("  Creating index...", log_con, verbose)
  dbExecute(con, glue("CREATE INDEX {output_table}_geoid_idx ON {output_table}(geoid)"))
  dbExecute(con, glue("ANALYZE {output_table}"))

  # --- Scenario summary ---
  scenario_elapsed <- as.numeric(difftime(Sys.time(), scenario_start, units = "secs"))

  if (length(failed_states) > 0) {
    log_msg(glue("  ⚠ {length(failed_states)} states failed: {paste(failed_states, collapse = ', ')}"),
            log_con, TRUE)
  }

  log_msg(glue("  ✓✓ SLR {scenario} complete: {format(total_rows, big.mark = ',')} tracts in {format_duration(scenario_elapsed)}"),
          log_con, verbose)

  return(list(scenario = scenario, status = "complete", rows = total_rows,
              elapsed = scenario_elapsed, failed = length(failed_states)))
}

# ==============================================================================
# MAIN
# ==============================================================================

main <- function() {

  # --- Parse command line arguments ---
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    stop("Usage: Rscript analyze_tract_slr_intersections_v5.R <config.yaml> [--state XX]")
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

  # --- Load configuration ---
  config <- yaml::read_yaml(config_file)
  verbose <- config$mode$verbose
  config$paths$log_dir <- path.expand(config$paths$log_dir)

  # --- Apply --state override ---
  if (!is.null(state_override)) {
    if (!state_override %in% names(FIPS_TO_STATE)) {
      stop(glue("Unknown state FIPS code: {state_override}"))
    }
    config$states <- list(state_override)
    state_abbr <- FIPS_TO_STATE[[state_override]]
    cat(glue("  --state override: processing only {state_override} ({state_abbr})\n"))
  }

  states <- unlist(config$states)

  # Batch label for logging
  batch_label <- if (!is.null(state_override)) {
    FIPS_TO_STATE[[state_override]]
  } else {
    config$batch_label
  }

  # --- Initialize logging ---
  log_con <- init_log(config$paths$log_dir, batch_label)
  log_msg(glue("Configuration loaded: {config_file}"), log_con, verbose)
  log_msg(glue("Scenarios: {paste(config$slr_scenarios, collapse = ', ')}"), log_con, verbose)
  log_msg(glue("States: {paste(states, collapse = ', ')}"), log_con, verbose)

  # --- Connect to database ---
  con <- dbConnect(
    Postgres(),
    dbname = config$database$name,
    host   = config$database$host,
    user   = Sys.getenv("USER")
  )
  log_msg(glue("Connected to {config$database$name}"), log_con, verbose)

  # PostgreSQL tuning — conservative for parallel R launches
  dbExecute(con, "SET max_parallel_workers_per_gather = 2")
  dbExecute(con, "SET work_mem = '2GB'")
  dbExecute(con, "SET effective_io_concurrency = 200")
  log_msg("PostgreSQL: parallel_workers=2, work_mem=2GB, io_concurrency=200", log_con, verbose)

  # --- Verify tracts table ---
  tracts_table <- config$tracts_table
  if (!dbExistsTable(con, tracts_table)) {
    stop(glue("Census tracts table '{tracts_table}' not found."))
  }

  # --- Loop over scenarios ---
  overall_start <- Sys.time()
  scenario_results <- list()

  for (scenario in config$slr_scenarios) {
    result <- process_scenario(scenario, states, config, con, log_con, state_override)
    scenario_results[[scenario]] <- result
  }

  # --- Overall summary ---
  overall_elapsed <- as.numeric(difftime(Sys.time(), overall_start, units = "secs"))
  dbDisconnect(con)

  log_msg("", log_con, verbose)
  log_msg(strrep("=", 78), log_con, verbose)
  log_msg("Census Tracts x SLR intersection analysis complete!", log_con, verbose)
  log_msg(glue("Total time: {format_duration(overall_elapsed)}"), log_con, verbose)

  for (r in scenario_results) {
    row_str <- if (!is.na(r$rows)) format(r$rows, big.mark = ",") else "—"
    log_msg(glue("  SLR {r$scenario}: {r$status} | {row_str} rows | {format_duration(r$elapsed)}"),
            log_con, verbose)
  }

  log_msg(strrep("=", 78), log_con, verbose)
  close(log_con)

  send_notification(
    glue("Tract SLR Analysis Complete{if (!is.null(batch_label)) paste0(' (', batch_label, ')') else ''}"),
    glue("{length(config$slr_scenarios)} scenarios in {format_duration(overall_elapsed)}"),
    config$notifications$ntfy_topic
  )

  if (isTRUE(config$flags$play_fanfare)) {
    tryCatch(beepr::beep(sound = 8), error = function(e) {})
  }
}

main()
