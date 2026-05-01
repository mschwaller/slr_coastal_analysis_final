#!/usr/bin/env Rscript
# ==============================================================================
# Export tract_{scenario}_intersections Tables to GeoPackage Files (v2)
# ==============================================================================
#
# Purpose: Exports per-state, per-scenario tract × SLR intersection data from
#          the megaSLR database to GeoPackage (.gpkg) files. Each output GPKG
#          contains all 11 SLR scenarios as separate layers (SLR_0ft through
#          SLR_10ft), with one GPKG per state. Mirrors the organization of
#          export_flooded_structures_v2.R for consistency across the pipeline.
#
# Input tables (one per scenario, covering all states):
#   tract_{scenario}_intersections  (e.g., tract_0ft_intersections)
#
# Output naming:
#   flooded_tracts_SS.gpkg (SS = state abbreviation)
#   Layers: SLR_0ft, SLR_1ft, ..., SLR_10ft
#
# Each row contains:
#   geoid, namelsad, statefp, countyfp, aland, awater, tract_area_ha, slr_area_ha, geom
#
# Usage:
#   Rscript export_flooded_tracts_v2.R <config_file.yaml>
#
# Example:
#   Rscript export_flooded_tracts_v2.R YAML_config_files/tracts_analysis_config_v2.yaml
#
# Author: Matt Schwaller
# Date: 2026-04-30
# ==============================================================================

library(sf)
library(DBI)
library(RPostgres)
library(yaml)
library(glue)

# ==============================================================================
# STATE FIPS TO ABBREVIATION MAPPING
# ==============================================================================

STATE_LOOKUP <- c(
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
  dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)
  timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  log_file <- file.path(log_dir, glue("export_flooded_tracts_{timestamp}.txt"))
  log_con <- file(log_file, open = "wt")
  writeLines(c(
    strrep("=", 78),
    "Flooded Tracts GeoPackage Export Log (v2)",
    glue("Started: {Sys.time()}"),
    strrep("=", 78)
  ), log_con)
  flush(log_con)
  return(log_con)
}

#' Log message to both console and file, with flush
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

#' Format file size
format_size <- function(bytes) {
  if (bytes < 1024) return(glue("{bytes} B"))
  if (bytes < 1024^2) return(glue("{round(bytes / 1024, 1)} KB"))
  if (bytes < 1024^3) return(glue("{round(bytes / 1024^2, 1)} MB"))
  return(glue("{round(bytes / 1024^3, 1)} GB"))
}

#' Send notification via ntfy
send_notification <- function(title, message, topic) {
  tryCatch({
    system(glue('curl -s -d "{message}" https://ntfy.sh/{topic} -H "Title: {title}"'),
           ignore.stdout = TRUE)
  }, error = function(e) {})
}

# ==============================================================================
# MAIN
# ==============================================================================

main <- function() {

  # --- Parse command line arguments ---
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    stop("Usage: Rscript export_flooded_tracts_v2.R <config_file.yaml>")
  }

  config_file <- args[1]
  if (!file.exists(config_file)) {
    stop(glue("Config file not found: {config_file}"))
  }

  # --- Load configuration ---
  cat(glue("Loading configuration from: {config_file}\n"))
  config <- yaml::read_yaml(config_file)
  verbose <- isTRUE(config$mode$verbose)

  # --- Resolve paths ---
  log_dir <- path.expand(config$paths$log_dir)

  # Output directory: use config$paths$tracts_export_dir if present, else default
  if (!is.null(config$paths$tracts_export_dir)) {
    output_dir <- path.expand(config$paths$tracts_export_dir)
  } else {
    output_dir <- file.path(Sys.getenv("HOME"),
                            "claude_projects/slr_analysis/exports/flooded_tracts_gpkg")
  }
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  # --- ntfy topic ---
  ntfy_topic <- if (!is.null(config$notifications$ntfy_topic)) {
    config$notifications$ntfy_topic
  } else {
    "matt-tripper3-jobs"
  }

  # --- Initialize logging ---
  log_con <- init_log(log_dir)
  log_msg(glue("Configuration loaded: {config_file}"), log_con, verbose)
  log_msg(glue("Database:   {config$database$name}"), log_con, verbose)
  log_msg(glue("Output dir: {output_dir}"), log_con, verbose)
  log_msg(glue("States:     {length(config$states)}"), log_con, verbose)
  log_msg(glue("Scenarios:  {paste(config$slr_scenarios, collapse = ', ')}"),
          log_con, verbose)

  # --- Connect to database ---
  log_msg("Connecting to database...", log_con, verbose)
  con <- dbConnect(
    Postgres(),
    dbname = config$database$name,
    host   = config$database$host,
    user   = Sys.getenv("USER")
  )
  log_msg(glue("Connected to database: {config$database$name}"), log_con, verbose)

  # --- Verify input tables exist ---
  log_msg("Verifying tract intersection tables...", log_con, verbose)
  missing_tables <- c()
  for (scenario in config$slr_scenarios) {
    table_name <- glue("tract_{scenario}_intersections")
    if (!dbExistsTable(con, table_name)) {
      missing_tables <- c(missing_tables, table_name)
    }
  }
  if (length(missing_tables) > 0) {
    log_msg(glue("ERROR: Missing tables: {paste(missing_tables, collapse = ', ')}"),
            log_con, TRUE)
    dbDisconnect(con)
    close(log_con)
    stop("Cannot proceed: required tract intersection tables are missing.")
  }
  log_msg("All tract intersection tables present", log_con, verbose)

  # --- Track overall statistics ---
  total_start <- Sys.time()
  states_done <- 0
  states_failed <- 0
  export_log <- data.frame(
    state = character(), scenario = character(),
    tracts = integer(), stringsAsFactors = FALSE
  )

  # --- Process each state ---
  for (fips in config$states) {

    abbrev <- STATE_LOOKUP[[fips]]
    if (is.null(abbrev)) {
      log_msg(glue("ERROR: Unknown FIPS code: {fips}"), log_con, TRUE)
      states_failed <- states_failed + 1
      next
    }

    gpkg_file <- file.path(output_dir, glue("flooded_tracts_{abbrev}.gpkg"))

    log_msg("", log_con, verbose)
    log_msg(strrep("-", 60), log_con, verbose)
    log_msg(glue("{abbrev} (FIPS {fips})"), log_con, verbose)
    log_msg(strrep("-", 60), log_con, verbose)

    # Remove existing file to start fresh
    if (file.exists(gpkg_file)) file.remove(gpkg_file)

    state_start <- Sys.time()
    state_ok <- TRUE
    state_total_tracts <- 0

    for (scenario in config$slr_scenarios) {

      table_name <- glue("tract_{scenario}_intersections")
      layer_name <- glue("SLR_{scenario}")

      # Read from PostGIS, filtering to this state
      tryCatch({
        query <- glue(
          "SELECT t.geoid, t.namelsad, t.statefp, c.countyfp, c.aland, c.awater, ",
          "t.tract_area_ha, t.slr_area_ha, t.geom ",
          "FROM {table_name} t ",
          "JOIN census_tracts_2025 c ON t.geoid = c.geoid ",
          "WHERE t.statefp = '{fips}'"
        )
        layer_data <- st_read(con, query = query, quiet = TRUE)

        n_tracts <- nrow(layer_data)

        if (n_tracts == 0) {
          log_msg(glue("  {layer_name}: 0 tracts (no intersections at this scenario)"),
                  log_con, verbose)
          next
        }

        # Append to existing GPKG, or create new one if first layer
        if (file.exists(gpkg_file)) {
          st_write(layer_data, gpkg_file, layer = layer_name,
                   append = TRUE, quiet = TRUE)
        } else {
          st_write(layer_data, gpkg_file, layer = layer_name, quiet = TRUE)
        }

        log_msg(glue("  {layer_name}: {format(n_tracts, big.mark = ',')} tracts"),
                log_con, verbose)

        export_log <- rbind(export_log, data.frame(
          state = abbrev, scenario = scenario,
          tracts = n_tracts, stringsAsFactors = FALSE
        ))
        state_total_tracts <- state_total_tracts + n_tracts

      }, error = function(e) {
        log_msg(glue("  ERROR exporting {table_name} for {abbrev}: {e$message}"),
                log_con, TRUE)
        state_ok <<- FALSE
      })
    }

    state_elapsed <- as.numeric(difftime(Sys.time(), state_start, units = "secs"))

    if (state_ok && file.exists(gpkg_file)) {
      file_size_str <- format_size(file.info(gpkg_file)$size)
      log_msg(glue("  File: {basename(gpkg_file)}"), log_con, verbose)
      log_msg(glue("  Size: {file_size_str} | Time: {format_duration(state_elapsed)}"),
              log_con, verbose)
      states_done <- states_done + 1

      # Per-state notification
      send_notification(
        glue("{abbrev} tracts GPKG exported"),
        glue("{format(state_total_tracts, big.mark = ',')} tract rows across {length(config$slr_scenarios)} scenarios, {file_size_str} in {format_duration(state_elapsed)}"),
        ntfy_topic
      )
    } else if (!state_ok) {
      log_msg("  FAILED during export", log_con, TRUE)
      states_failed <- states_failed + 1
    } else {
      # state_ok was TRUE but no file was created — all scenarios had zero tracts
      log_msg(glue("  NOTE: {abbrev} has no intersecting tracts at any scenario"),
              log_con, TRUE)
      states_failed <- states_failed + 1
    }
  }

  dbDisconnect(con)

  # --- Summary ---
  total_elapsed <- as.numeric(difftime(Sys.time(), total_start, units = "secs"))
  gpkg_files <- list.files(output_dir, pattern = "\\.gpkg$", full.names = TRUE)
  total_bytes <- sum(file.info(gpkg_files)$size)

  log_msg("", log_con, verbose)
  log_msg(strrep("=", 78), log_con, verbose)
  log_msg("Export Complete", log_con, verbose)
  log_msg(glue("  States exported:          {states_done}"), log_con, verbose)
  log_msg(glue("  States failed / no data:  {states_failed}"), log_con, verbose)
  log_msg(glue("  Total tract-scenario rows: {format(sum(export_log$tracts), big.mark = ',')}"),
          log_con, verbose)
  log_msg(glue("  Total size:                {format_size(total_bytes)}"),
          log_con, verbose)
  log_msg(glue("  Total time:                {format_duration(total_elapsed)}"),
          log_con, verbose)
  log_msg(glue("  Output dir:                {output_dir}"), log_con, verbose)
  log_msg(strrep("=", 78), log_con, verbose)

  # List exported files
  log_msg("", log_con, verbose)
  log_msg("Exported files:", log_con, verbose)
  for (f in sort(gpkg_files)) {
    log_msg(glue("  {basename(f)}  ({format_size(file.info(f)$size)})"),
            log_con, verbose)
  }

  log_msg(glue("Finished: {Sys.time()}"), log_con, verbose)
  close(log_con)

  # Final notification
  send_notification(
    "All Tract GPKG Exports Complete",
    glue("{states_done} states, {format(sum(export_log$tracts), big.mark = ',')} rows, {format_size(total_bytes)} in {format_duration(total_elapsed)}"),
    ntfy_topic
  )
}

main()
