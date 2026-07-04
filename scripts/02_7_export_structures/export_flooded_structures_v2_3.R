#!/usr/bin/env Rscript
# ==============================================================================
# Export flooded_structures Tables to GeoPackage Files (v2)
# ==============================================================================
#
# Purpose: Exports per-state, per-scenario flooded_structures_FF_Xft tables
#          from the megaSLR database to GeoPackage (.gpkg) files. Each output
#          GPKG contains all 11 SLR scenarios as separate layers (SLR_0ft
#          through SLR_10ft), with one GPKG per state.
#
# v2 changes (vs v1):
#   - YAML-driven configuration (consistent with other pipeline scripts)
#   - Uses Sys.getenv("USER") for portability (no hardcoded user)
#   - Timestamped log file in paths.log_dir with flush per write
#   - ntfy.sh push notifications per state and at overall completion
#
# v2_2 changes (vs v2_1):
#   - Export query uses an explicit column whitelist instead of SELECT *,
#     restricting output to fields with documented provenance. Eight source
#     columns are dropped (35 -> 27 attribute fields, plus geometry):
#       * Unpopulated per Yang et al. 2024 (USA Structures), Table 2:
#           sec_occ, outbldg, h_adj_elev, l_adj_elev
#       * Absent from the published schema, no published methodology:
#           b_code, pop_median, pop_ci95_lower, pop_ci95_upper
#
# v2_3 changes (vs v2_2):
#   - Also drop prop_cnty from the whitelist. It is a later FEMA
#     distribution add-on (not in Yang et al. 2024 Table 2) present in only
#     18 of 23 state tables; its absence in CA, MD, MA, NJ, PA caused the
#     v2_2 export to fail for those states. Dropping it yields an identical
#     26-field schema (25 Yang Table 2 fields + tract_geoid) + geom across
#     all 23 states.
#
# Output naming: flooded_structures_SS.gpkg (SS = state abbreviation)
# Layer naming:  SLR_Xft (X = 0 through 10)
#
# Usage:
#   Rscript export_flooded_structures_v2_3.R <config_file.yaml>
#
# Example:
#   Rscript export_flooded_structures_v2_3.R YAML_config_files/structure_analysis_config_v3.yaml
#
# Author: Matt Schwaller
# Date: 2026-04-15 (v2_2: 2026-07-03; v2_3: 2026-07-04)
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
  log_file <- file.path(log_dir, glue("export_flooded_structures_{timestamp}.txt"))
  log_con <- file(log_file, open = "wt")
  writeLines(c(
    strrep("=", 78),
    "Flooded Structures GeoPackage Export Log (v2)",
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

  # --- Parse command line arguments ---
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    stop("Usage: Rscript export_flooded_structures_v2.R <config_file.yaml>")
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

  # Output directory: use config$paths$structures_export_dir if present, else default
  if (!is.null(config$paths$structures_export_dir)) {
    output_dir <- path.expand(config$paths$structures_export_dir)
  } else {
    output_dir <- file.path(Sys.getenv("HOME"),
                            "claude_projects/slr_analysis/exports/gpkg")
  }
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

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

  # --- Track overall statistics ---
  total_start <- Sys.time()
  states_done <- 0
  states_failed <- 0
  export_log <- data.frame(
    state = character(), scenario = character(),
    structures = integer(), stringsAsFactors = FALSE
  )

  # --- Process each state ---
  for (fips in config$states) {

    abbrev <- STATE_LOOKUP[[fips]]
    if (is.null(abbrev)) {
      log_msg(glue("ERROR: Unknown FIPS code: {fips}"), log_con, TRUE)
      states_failed <- states_failed + 1
      next
    }

    gpkg_file <- file.path(output_dir, glue("flooded_structures_{abbrev}.gpkg"))

    log_msg("", log_con, verbose)
    log_msg(strrep("-", 60), log_con, verbose)
    log_msg(glue("{abbrev} (FIPS {fips})"), log_con, verbose)
    log_msg(strrep("-", 60), log_con, verbose)

    # Remove existing file to start fresh
    if (file.exists(gpkg_file)) file.remove(gpkg_file)

    state_start <- Sys.time()
    state_ok <- TRUE
    state_total_structures <- 0

    for (scenario in config$slr_scenarios) {

      table_name <- glue("flooded_structures_{fips}_{scenario}")
      layer_name <- glue("SLR_{scenario}")

      # Check if table exists
      if (!dbExistsTable(con, table_name)) {
        log_msg(glue("  WARNING: {table_name} does not exist, skipping"),
                log_con, verbose)
        next
      }

      # Read from PostGIS and write to GPKG
      #
      # Explicit column whitelist (not SELECT *) to restrict the export to
      # fields with documented provenance. Nine source columns are omitted:
      #   - Unpopulated per Yang et al. 2024 (USA Structures), Table 2:
      #       sec_occ, outbldg, h_adj_elev, l_adj_elev
      #   - Absent from the published USA Structures schema and lacking any
      #     published derivation methodology:
      #       b_code, pop_median, pop_ci95_lower, pop_ci95_upper
      #   - prop_cnty: a later FEMA distribution add-on (not in Yang et al.
      #     2024 Table 2) that is present in only 18 of 23 state tables;
      #     dropped so all states export an identical schema.
      # Retained: 25 documented Yang Table 2 fields + tract_geoid + geometry
      #   = 26 attribute fields + geom.
      # Using an explicit list also prevents any future undocumented source
      # columns from silently reappearing in the export.
      tryCatch({
        query <- glue(
          "SELECT ",
          # --- Esri / geodatabase defaults ---
          "objectid, shape_area, shape_length, ",
          # --- Building identity & occupancy ---
          "build_id, occ_cls, prim_occ, ",
          # --- Address ---
          "prop_addr, prop_city, prop_st, prop_zip, ",
          # --- Structure measurements ---
          "height, sqmeters, sqfeet, ",
          # --- Location / census codes ---
          "fips, censuscode, ",
          # --- Provenance / imagery ---
          "prod_date, source, usng, longitude, latitude, ",
          "image_name, image_date, val_method, remarks, uuid, ",
          # --- Pipeline-added ---
          "tract_geoid, ",
          # --- Geometry (must be last) ---
          "geom ",
          "FROM {table_name}"
        )
        layer_data <- st_read(con, query = query, quiet = TRUE)

        n_structures <- nrow(layer_data)

        if (n_structures == 0) {
          log_msg(glue("  WARNING: {table_name} is empty, skipping"),
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

        log_msg(glue("  {layer_name}: {format(n_structures, big.mark = ',')} structures"),
                log_con, verbose)

        export_log <- rbind(export_log, data.frame(
          state = abbrev, scenario = scenario,
          structures = n_structures, stringsAsFactors = FALSE
        ))
        state_total_structures <- state_total_structures + n_structures

      }, error = function(e) {
        log_msg(glue("  ERROR exporting {table_name}: {e$message}"),
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
        glue("{abbrev} GPKG exported"),
        glue("{format(state_total_structures, big.mark = ',')} structures across {length(config$slr_scenarios)} scenarios, {file_size_str} in {format_duration(state_elapsed)}")
      )
    } else {
      log_msg("  FAILED or no data", log_con, TRUE)
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
  log_msg(glue("  States exported:               {states_done}"), log_con, verbose)
  log_msg(glue("  States failed:                 {states_failed}"), log_con, verbose)
  log_msg(glue("  Total structure-scenario rows: {format(sum(export_log$structures), big.mark = ',')}"),
          log_con, verbose)
  log_msg(glue("  Total size:                    {format_size(total_bytes)}"),
          log_con, verbose)
  log_msg(glue("  Total time:                    {format_duration(total_elapsed)}"),
          log_con, verbose)
  log_msg(glue("  Output dir:                    {output_dir}"), log_con, verbose)
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
    "All GPKG Exports Complete",
    glue("{states_done} states, {format(sum(export_log$structures), big.mark = ',')} rows, {format_size(total_bytes)} in {format_duration(total_elapsed)}")
  )
}

main()
