#!/usr/bin/env Rscript
# ==============================================================================
# Louisiana Per-Tract SLR Intersection Analysis — Multi-Pass (v2)
# ==============================================================================
#
# Purpose: Processes Louisiana (FIPS 22) tract × SLR intersections using a
#          multi-pass strategy to handle complex coastal geometries efficiently.
#
# Strategy:
#   Pass 1 (--pass 1): Short timeout (default 30s). Processes all tracts,
#          completing ~96% quickly. Failed tracts are logged for Pass 2.
#
#   Pass 2 (--pass 2): No timeout (default). Retries only tracts that
#          failed Pass 1, using ST_Union with no time limit. Can be run
#          with --skip to exclude specific tracts handled separately
#          (e.g., in a dedicated psql session).
#
# Usage:
#   # Pass 1: fast sweep
#   Rscript analyze_tract_slr_louisiana_2.R config.yaml --pass 1
#
#   # Pass 2: retry failures with no timeout
#   Rscript analyze_tract_slr_louisiana_2.R config.yaml --pass 2
#
#   # Pass 2 with specific scenarios and skipping a tract:
#   Rscript analyze_tract_slr_louisiana_2.R config.yaml --pass 2 --scenarios 0ft,1ft,2ft --skip 22113951100
#
#   # Optional: compare ST_Union vs SUM for validation
#   Rscript analyze_tract_slr_louisiana_2.R config.yaml --validate --scenarios 0ft
#
# Output: Per-state tables (e.g., tract_0ft_intersections_22) with columns:
#   geoid, namelsad, statefp, tract_area_ha, slr_area_ha
#
# Author: Matt Schwaller
# Date: 2026-04
# ==============================================================================

library(DBI)
library(RPostgres)
library(glue)
library(yaml)

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

init_log <- function(log_dir, pass) {
  timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  log_file <- file.path(log_dir, glue("tract_slr_LA_pass{pass}_{timestamp}.txt"))
  dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)
  log_con <- file(log_file, open = "wt")
  writeLines(c(
    strrep("=", 78),
    glue("Louisiana Per-Tract SLR Intersection Analysis — Pass {pass}"),
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
# QUERY BUILDERS
# ==============================================================================

# ST_Union query — exact
build_union_query <- function(tracts_table, slr_table, geoid) {
  glue("
    SELECT
      t.geoid,
      t.namelsad,
      t.statefp,
      ST_Area(t.geom) / 10000 AS tract_area_ha,
      ST_Area(ST_Intersection(ST_Union(s.geom), t.geom)) / 10000 AS slr_area_ha
    FROM {tracts_table} t
    JOIN {slr_table} s ON ST_Intersects(s.geom, t.geom)
    WHERE t.geoid = '{geoid}'
    GROUP BY t.geoid, t.namelsad, t.statefp, t.geom
  ")
}

# SUM query — used only for validation (--validate mode)
build_sum_query <- function(tracts_table, slr_table, geoid) {
  glue("
    SELECT
      t.geoid,
      t.namelsad,
      t.statefp,
      ST_Area(t.geom) / 10000 AS tract_area_ha,
      SUM(ST_Area(ST_Intersection(s.geom, t.geom))) / 10000 AS slr_area_ha
    FROM {tracts_table} t
    JOIN {slr_table} s ON ST_Intersects(s.geom, t.geom)
    WHERE t.geoid = '{geoid}'
    GROUP BY t.geoid, t.namelsad, t.statefp, t.geom
  ")
}

# ==============================================================================
# TABLE MANAGEMENT
# ==============================================================================

ensure_output_table <- function(con, output_table) {
  # Create if not exists — safe for multi-pass and parallel use
  dbExecute(con, glue("
    CREATE TABLE IF NOT EXISTS {output_table} (
      geoid TEXT,
      namelsad TEXT,
      statefp TEXT,
      tract_area_ha DOUBLE PRECISION,
      slr_area_ha DOUBLE PRECISION
    )
  "))
}

get_completed_tracts <- function(con, output_table) {
  # Return GEOIDs already in the output table (skip on re-run)
  tryCatch({
    result <- dbGetQuery(con, glue("SELECT geoid FROM {output_table}"))
    return(result$geoid)
  }, error = function(e) {
    return(character(0))
  })
}

get_failed_tracts_from_log <- function(log_dir, pass_number, scenarios = NULL) {
  # Parse previous pass log files to find failed/timed-out tracts per scenario
  # Returns a named list: list("0ft" = c("22057021000", ...), "1ft" = c(...))

  log_files <- list.files(log_dir,
    pattern = glue("tract_slr_LA_pass{pass_number}_.*\\.txt$"),
    full.names = TRUE)

  if (length(log_files) == 0) {
    stop(glue("No Pass {pass_number} log files found in {log_dir}"))
  }

  # Use the most recent log file
  log_file <- sort(log_files, decreasing = TRUE)[1]
  cat(glue("Reading failures from: {basename(log_file)}"), "\n")

  lines <- readLines(log_file)

  failed <- list()
  current_scenario <- NULL

  for (line in lines) {
    # Detect scenario header
    scenario_match <- regmatches(line, regexpr("SCENARIO: (\\S+)", line))
    if (length(scenario_match) > 0) {
      current_scenario <- sub("SCENARIO: (\\S+).*", "\\1", scenario_match)
    }

    # Detect failures (FAILED, TIMEOUT, or InterruptedException)
    if (grepl("FAILED|TIMEOUT|InterruptedException", line) && !is.null(current_scenario)) {
      geoid <- regmatches(line, regexpr("22\\d{9}", line))
      if (length(geoid) > 0) {
        failed[[current_scenario]] <- c(failed[[current_scenario]], geoid)
      }
    }
  }

  # Filter to requested scenarios if specified
  if (!is.null(scenarios)) {
    failed <- failed[intersect(names(failed), scenarios)]
  }

  total <- sum(sapply(failed, length))
  cat(glue("Found {total} failed tracts across {length(failed)} scenarios"), "\n")
  return(failed)
}

# ==============================================================================
# CORE PROCESSING
# ==============================================================================

process_tract <- function(con, tracts_table, slr_table, output_table, geoid,
                          i = NA, n = NA, log_con, verbose) {

  tract_start <- Sys.time()
  label <- if (!is.na(i)) glue("({i}/{n}) ") else ""

  query <- build_union_query(tracts_table, slr_table, geoid)

  tryCatch({
    result <- dbGetQuery(con, query)
    elapsed <- as.numeric(difftime(Sys.time(), tract_start, units = "secs"))

    if (nrow(result) > 0) {
      dbExecute(con, glue("
        INSERT INTO {output_table} (geoid, namelsad, statefp, tract_area_ha, slr_area_ha)
        VALUES ('{result$geoid}', '{gsub(\"'\", \"''\", result$namelsad)}', '{result$statefp}',
                {result$tract_area_ha}, {result$slr_area_ha})
      "))

      if (elapsed > 60) {
        log_msg(glue("  {label}⚠ SLOW {geoid}: {format_duration(elapsed)} | slr_area={round(result$slr_area_ha, 1)} ha"),
                log_con, verbose)
      } else {
        log_msg(glue("  {label}✓ {geoid}: {format_duration(elapsed)}"),
                log_con, verbose)
      }
      return(list(status = "ok", geoid = geoid, elapsed = elapsed))
    } else {
      # No SLR overlap — silent skip
      return(list(status = "no_overlap", geoid = geoid, elapsed = elapsed))
    }

  }, error = function(e) {
    elapsed <- as.numeric(difftime(Sys.time(), tract_start, units = "secs"))
    is_timeout <- grepl("canceling statement|InterruptedException|statement timeout",
                        e$message, ignore.case = TRUE)
    if (is_timeout) {
      log_msg(glue("  {label}⏱ TIMEOUT {geoid} ({format_duration(elapsed)})"),
              log_con, TRUE)
      return(list(status = "timeout", geoid = geoid, elapsed = elapsed))
    } else {
      log_msg(glue("  {label}✗ FAILED {geoid} ({format_duration(elapsed)}): {e$message}"),
              log_con, TRUE)
      return(list(status = "failed", geoid = geoid, elapsed = elapsed))
    }
  })
}

# ==============================================================================
# VALIDATION MODE
# ==============================================================================

run_validation <- function(con, config, scenarios, log_con, verbose) {
  # Compare ST_Union vs SUM on slow-but-successful tracts from Pass 1
  tracts_table <- config$tracts_table

  log_msg("", log_con, verbose)
  log_msg("VALIDATION MODE: Comparing ST_Union vs SUM", log_con, verbose)
  log_msg(strrep("=", 60), log_con, verbose)

  for (scenario in scenarios) {
    slr_table <- glue("slr_{scenario}_22")
    output_table <- glue("{glue(config$analysis$output_table_pattern)}_22")

    # Get tracts that completed but took >10s (good validation candidates)
    completed <- tryCatch({
      dbGetQuery(con, glue("
        SELECT geoid, slr_area_ha FROM {output_table}
        ORDER BY slr_area_ha DESC
        LIMIT 25
      "))
    }, error = function(e) {
      log_msg(glue("  Cannot read {output_table}: {e$message}"), log_con, verbose)
      return(data.frame())
    })

    if (nrow(completed) == 0) {
      log_msg(glue("  No completed tracts in {output_table} to validate"), log_con, verbose)
      next
    }

    log_msg(glue(""), log_con, verbose)
    log_msg(glue("SCENARIO: {scenario} — validating {nrow(completed)} tracts"), log_con, verbose)
    log_msg(sprintf("  %-15s %12s %12s %8s", "GEOID", "UNION_ha", "SUM_ha", "PCT_DIFF"), log_con, verbose)

    for (j in seq_len(nrow(completed))) {
      geoid <- completed$geoid[j]
      union_area <- completed$slr_area_ha[j]

      sum_result <- tryCatch({
        dbGetQuery(con, build_sum_query(tracts_table, slr_table, geoid))
      }, error = function(e) data.frame())

      if (nrow(sum_result) > 0) {
        sum_area <- sum_result$slr_area_ha
        pct_diff <- if (union_area > 0) {
          round(100 * (sum_area - union_area) / union_area, 3)
        } else { 0 }
        log_msg(sprintf("  %-15s %12.1f %12.1f %+7.3f%%", geoid, union_area, sum_area, pct_diff),
                log_con, verbose)
      }
    }
  }
}

# ==============================================================================
# MAIN
# ==============================================================================

main <- function() {

  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    stop("Usage: Rscript analyze_tract_slr_louisiana_2.R <config.yaml> --pass N [--scenarios 0ft,1ft,...] [--skip GEOID1,GEOID2,...] [--validate]")
  }

  config_file <- args[1]
  if (!file.exists(config_file)) {
    stop(glue("Config file not found: {config_file}"))
  }

  # Parse arguments
  pass <- 1
  scenario_filter <- NULL
  validate_mode <- FALSE
  skip_tracts <- character(0)

  i <- 2
  while (i <= length(args)) {
    if (args[i] == "--pass" && i < length(args)) {
      pass <- as.integer(args[i + 1])
      i <- i + 2
    } else if (args[i] == "--scenarios" && i < length(args)) {
      scenario_filter <- unlist(strsplit(args[i + 1], ","))
      i <- i + 2
    } else if (args[i] == "--skip" && i < length(args)) {
      skip_tracts <- unlist(strsplit(args[i + 1], ","))
      i <- i + 2
    } else if (args[i] == "--validate") {
      validate_mode <- TRUE
      i <- i + 1
    } else {
      stop(glue("Unknown argument: {args[i]}"))
    }
  }

  config <- yaml::read_yaml(config_file)
  verbose <- config$mode$verbose
  config$paths$log_dir <- path.expand(config$paths$log_dir)
  tracts_table <- config$tracts_table
  ntfy_topic <- config$notifications$ntfy_topic

  # Determine which scenarios to run
  scenarios <- if (!is.null(scenario_filter)) {
    scenario_filter
  } else {
    config$slr_scenarios
  }

  # Timeouts per pass
  timeout_secs <- switch(as.character(pass),
    "1" = 30,
    "2" = 0,  # 0 = no timeout
    30
  )

  log_con <- init_log(config$paths$log_dir, pass)

  con <- dbConnect(
    Postgres(),
    dbname = config$database$name,
    host   = config$database$host,
    user   = Sys.getenv("USER")
  )
  on.exit(dbDisconnect(con), add = TRUE)

  # PostgreSQL tuning
  pg_parallel <- 8
  dbExecute(con, glue("SET max_parallel_workers_per_gather = {pg_parallel}"))
  dbExecute(con, "SET work_mem = '8GB'")
  dbExecute(con, "SET effective_io_concurrency = 200")
  dbExecute(con, glue("SET statement_timeout = '{timeout_secs}s'"))

  log_msg(glue("Pass {pass} | timeout={timeout_secs}s | parallel_workers={pg_parallel}"), log_con, verbose)
  log_msg(glue("Scenarios: {paste(scenarios, collapse = ', ')}"), log_con, verbose)
  if (length(skip_tracts) > 0) {
    log_msg(glue("Skipping {length(skip_tracts)} tracts: {paste(skip_tracts, collapse = ', ')}"), log_con, verbose)
  }

  # Validation mode
  if (validate_mode) {
    run_validation(con, config, scenarios, log_con, verbose)
    close(log_con)
    return(invisible(NULL))
  }

  # Get tract list for passes that process all tracts vs. only failures
  la_tracts <- dbGetQuery(con, glue("
    SELECT geoid FROM {tracts_table}
    WHERE statefp = '22'
    ORDER BY geoid
  "))
  n_total <- nrow(la_tracts)
  log_msg(glue("Louisiana: {n_total} total tracts"), log_con, verbose)

  # For Pass 2, get the failed tracts from previous pass
  failed_by_scenario <- NULL
  if (pass == 2) {
    failed_by_scenario <- get_failed_tracts_from_log(
      config$paths$log_dir, 1, scenarios)
  }

  overall_start <- Sys.time()

  for (scenario in scenarios) {

    scenario_start <- Sys.time()
    slr_table <- glue("slr_{scenario}_22")
    output_table <- glue("{glue(config$analysis$output_table_pattern)}_22")

    log_msg("", log_con, verbose)
    log_msg(strrep("=", 60), log_con, verbose)
    log_msg(glue("SCENARIO: {scenario} → {output_table}"), log_con, verbose)
    log_msg(strrep("=", 60), log_con, verbose)

    # Verify SLR table exists
    slr_exists <- dbGetQuery(con, glue("
      SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = '{slr_table}'
      ) AS exists_flag
    "))$exists_flag
    if (!slr_exists) {
      log_msg(glue("  ⚠ SLR table {slr_table} not found — skipping"), log_con, verbose)
      next
    }

    # Create output table if needed (safe for multi-pass)
    ensure_output_table(con, output_table)

    # Determine which tracts to process
    completed_geoids <- get_completed_tracts(con, output_table)

    if (pass == 1) {
      # Process all tracts, skipping any already completed
      tracts_to_process <- setdiff(la_tracts$geoid, completed_geoids)
    } else if (pass == 2) {
      # Retry failures from Pass 1
      if (is.null(failed_by_scenario[[scenario]])) {
        log_msg(glue("  No Pass 1 failures for {scenario} — skipping"), log_con, verbose)
        next
      }
      tracts_to_process <- setdiff(failed_by_scenario[[scenario]], completed_geoids)
    }

    n_process <- length(tracts_to_process)
    n_skipped_completed <- length(completed_geoids)
    n_skipped_manual <- length(intersect(tracts_to_process, skip_tracts))
    tracts_to_process <- setdiff(tracts_to_process, skip_tracts)
    n_process <- length(tracts_to_process)

    log_msg(glue("  Processing {n_process} tracts ({n_skipped_completed} already completed, {n_skipped_manual} manually skipped)"),
            log_con, verbose)

    if (n_process == 0) next

    # Process tracts
    successes <- 0
    failures <- c()
    timeouts <- c()

    for (j in seq_along(tracts_to_process)) {
      geoid <- tracts_to_process[j]

      result <- process_tract(con, tracts_table, slr_table, output_table, geoid,
                              i = j, n = n_process,
                              log_con = log_con, verbose = verbose)

      if (result$status == "ok") {
        successes <- successes + 1
      } else if (result$status == "timeout") {
        timeouts <- c(timeouts, geoid)
      } else if (result$status == "failed") {
        failures <- c(failures, geoid)
      }
    }

    # Ensure index exists (CREATE IF NOT EXISTS via conditional)
    idx_name <- glue("{output_table}_geoid_idx")
    idx_exists <- dbGetQuery(con, glue("
      SELECT EXISTS (
        SELECT 1 FROM pg_indexes WHERE indexname = '{idx_name}'
      ) AS exists_flag
    "))$exists_flag
    if (!idx_exists) {
      dbExecute(con, glue("CREATE INDEX {idx_name} ON {output_table}(geoid)"))
    }
    dbExecute(con, glue("ANALYZE {output_table}"))

    scenario_elapsed <- as.numeric(difftime(Sys.time(), scenario_start, units = "secs"))
    log_msg("", log_con, verbose)
    log_msg(glue("  ✓✓ {scenario} complete: {successes}/{n_process} succeeded in {format_duration(scenario_elapsed)}"),
            log_con, verbose)
    if (length(timeouts) > 0) {
      log_msg(glue("  Timeouts ({length(timeouts)}): {paste(timeouts, collapse = ', ')}"),
              log_con, verbose)
    }
    if (length(failures) > 0) {
      log_msg(glue("  Failures ({length(failures)}): {paste(failures, collapse = ', ')}"),
              log_con, verbose)
    }

    # Notify per scenario
    send_notification(
      glue("LA Pass {pass}: {scenario}"),
      glue("{successes}/{n_process} ok, {length(timeouts)} timeout, {length(failures)} fail | {format_duration(scenario_elapsed)}"),
      ntfy_topic
    )
  }

  overall_elapsed <- as.numeric(difftime(Sys.time(), overall_start, units = "secs"))

  log_msg("", log_con, verbose)
  log_msg(strrep("=", 78), log_con, verbose)
  log_msg(glue("Louisiana Pass {pass} complete: {format_duration(overall_elapsed)}"), log_con, verbose)
  log_msg(strrep("=", 78), log_con, verbose)
  close(log_con)

  send_notification(
    glue("LA Tract SLR Pass {pass} Complete"),
    glue("{length(scenarios)} scenarios in {format_duration(overall_elapsed)}"),
    ntfy_topic
  )

  if (isTRUE(config$flags$play_fanfare)) {
    tryCatch(beepr::beep(sound = 8), error = function(e) {})
  }
}

main()
