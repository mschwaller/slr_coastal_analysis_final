# **************************************************
#                     DETAILS
#
# Purpose:   Merged pipeline (runs on TRIPPER3). For each state:
#              1. Load FEMA footprints from the source .gdb, validate geometry,
#                 drop the undocumented modeled population fields.
#              2. Flag each footprint 0/1 for every SLR scenario, using the
#                 exported flooded_structures_SS.gpkg as a build_id lookup.
#              3. Determine 2010 block group, 2020 block group, and 2020
#                 census place (GEOID + name) for each footprint via a
#                 centroid spatial join, then attach those geography columns
#                 back onto the full-polygon footprints by build_id.
#              4. Save one full-polygon output file per state (geometry +
#                 SLR flags + geography codes + occupancy).
#            After the loop, write total and single-family-dwelling (SFD)
#            summary tables for each geography level.
#
#            Centroids are used only as the in-memory join vehicle for the
#            census spatial join; they are never written to disk.
#
# Author:    Nora Schwaller
# Assisted:  Claude (Anthropic), claude.ai
# Merged/adapted for TRIPPER3 from 2-01_flooded_structures_nc.R and
#   2-02_geocode_footprints_2.R
#   Merge, CRS/validity/snap revisions assisted by Claude Opus 4.8 High
#   (Anthropic), claude.ai, 2026-07-03.
# Started:   MM/DD/YYYY
# Updated:   2026-07-03
#
# Run on TRIPPER3 (long job) inside screen; tee console for tailing:
#   screen -S slr_geocode
#   cd ~/claude_projects/slr_analysis
#   Rscript 2-01_flooded_structures_geocoded.R \
#     2>&1 | tee ~/Science/Nora_SLR/SLR_log_files/geocode_console.log
#   # detach: Ctrl-A then D   |   reattach: screen -r slr_geocode
# ntfy.sh push notifications fire per state and at completion
#   (topic: matt-tripper3-jobs).
# **************************************************


# *************
# 1. Setup ----

SCRIPT_VERSION <- "v14 (2026-07-03)"
cat("==== 2-01_flooded_structures_geocoded", SCRIPT_VERSION, "====\n")

footprints_dir <- "~/Science/Nora_SLR/house_footprints/"
slr_dir        <- "~/claude_projects/slr_analysis/exports/flooded_structures_gpkg/"
out_dir        <- "~/claude_projects/slr_analysis/exports/SLR_states/"

footprints_dir <- path.expand(footprints_dir)
slr_dir        <- path.expand(slr_dir)
out_dir        <- path.expand(out_dir)
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

library(sf)
library(dplyr)
library(tigris)

options(tigris_use_cache = TRUE)
set.seed(123)

# Working CRS for spatial ops (EPSG:5070, NAD83 / Conus Albers Equal Area) -
# the project standard. Projecting footprints here before st_centroid avoids
# planar-on-degrees centroid error and the longlat st_centroid warning.
WORKING_CRS <- 5070

# Max distance (meters) for snapping a centroid with NO block-group match to
# the nearest block group. Block groups should cover all land, so an NA match
# usually means the centroid fell just outside a generalized (cb = TRUE)
# coastline. Snap within this distance; beyond it, leave NA (and log it).
BG_SNAP_MAX_M <- 500

# ntfy.sh push notification (matches the rest of the pipeline's job monitoring).
# Uses system2 with argument vectors so message/title text (which can contain
# quotes, backticks, or other shell metacharacters - e.g. an error string) is
# passed as data and never interpreted by a shell.
send_notification <- function(title, message) {
  tryCatch({
    system2("curl",
            args = c("-s",
                     "-d", message,
                     "-H", paste0("Title: ", title),
                     "https://ntfy.sh/matt-tripper3-jobs"),
            stdout = FALSE, stderr = FALSE)
  }, error = function(e) {})
}

# FEMA attribute whitelist: the same 26 FEMA attribute fields kept in
# the v2_2 GPKG export. Trimming the .gdb read to this set keeps these state
# files column-consistent with the published GeoPackages. The eight dropped
# source columns (sec_occ, outbldg, h_adj_elev, l_adj_elev, b_code,
# pop_median, pop_ci95_lower, pop_ci95_upper) simply fall outside this list.
# Note: tract_geoid and geom from the export whitelist are excluded here -
# tract_geoid is a pipeline-added column that lives in the PostGIS tables, not
# the raw .gdb; geom is carried automatically by sf. build_id / occ_cls /
# prim_occ are retained, so the flag join and SFD breakout still work.
# Matched case-insensitively and defensively (missing fields are skipped).
FEMA_KEEP <- c(
  "objectid", "shape_area", "shape_length",
  "build_id", "occ_cls", "prim_occ",
  "prop_addr", "prop_city", "prop_st", "prop_zip", "prop_cnty",
  "height", "sqmeters", "sqfeet",
  "fips", "censuscode",
  "prod_date", "source", "usng", "longitude", "latitude",
  "image_name", "image_date", "val_method", "remarks", "uuid"
)

# states = any state with both a footprints gdb and a flooded structures gpkg
gdb_files <- list.files(footprints_dir, pattern = "_Structures\\.gdb$")
states    <- sub("_Structures\\.gdb$", "", gdb_files)
states    <- states[file.exists(paste0(slr_dir, "flooded_structures_", states, ".gpkg"))]

cat("States to process:", paste(states, collapse = ", "), "\n")

# tracks duplicate build_id rows dropped per state (from boundary st_join ties)
dedup_log <- data.frame(state = character(), n_before = integer(),
                        n_after = integer(), n_dropped = integer())

# tracks footprints dropped because their geometry could not be repaired
invalid_log <- data.frame(state = character(), build_id = character(),
                          reason = character(), stringsAsFactors = FALSE)

# tracks centroids with no block-group match that were snapped to the nearest
# block group (or left NA if beyond BG_SNAP_MAX_M)
bg_snap_log <- data.frame(state = character(), build_id = character(),
                          which_bg = character(), assigned_geoid = character(),
                          dist_m = numeric(), within_threshold = logical(),
                          stringsAsFactors = FALSE)

# accumulates non-spatial data across all states for the summary tables in step 3
all_data_list <- list()

#' Snap centroids with a missing block-group GEOID to the nearest block group.
#'
#' Only operates on rows where `geoid_col` is NA. For each, finds the nearest
#' polygon in `bg` and its distance; assigns the GEOID if within max_m, else
#' leaves NA. Returns a list(centroids, log) where log rows describe each snap
#' attempt (assigned or not).
#'
#' @param centroids sf points (must carry build_id) in a projected CRS (meters)
#' @param bg        sf block-group polygons with the GEOID in column `geoid_col`
#' @param geoid_col name of the GEOID column on centroids (e.g. "blkgrp_2020")
#' @param which_bg  label for the log (e.g. "blkgrp_2020")
#' @param st        state abbreviation for the log
#' @param max_m     max snap distance in meters
snap_na_bg <- function(centroids, bg, geoid_col, which_bg, st, max_m) {
  na_idx <- which(is.na(centroids[[geoid_col]]))
  log_rows <- bg_snap_log[0, ]   # empty frame with correct columns
  if (length(na_idx) == 0) return(list(centroids = centroids, log = log_rows))

  na_pts   <- centroids[na_idx, ]
  nearest  <- st_nearest_feature(na_pts, bg)
  dists    <- as.numeric(st_distance(na_pts, bg[nearest, ], by_element = TRUE))
  cand     <- bg[[geoid_col]][nearest]      # bg was renamed to geoid_col upstream
  within   <- dists <= max_m

  # assign only those within threshold; others stay NA
  centroids[[geoid_col]][na_idx[within]] <- cand[within]

  log_rows <- data.frame(
    state           = st,
    build_id        = na_pts$build_id,
    which_bg        = which_bg,
    assigned_geoid  = ifelse(within, cand, NA_character_),
    dist_m          = round(dists, 1),
    within_threshold = within,
    stringsAsFactors = FALSE
  )
  list(centroids = centroids, log = log_rows)
}


# *************
# 2. Loop by state ----

for (st in states) {

  state_start <- Sys.time()
  cat("\n----", st, "----\n")

  tryCatch({

  # --- load footprints ---
  gdb_path   <- paste0(footprints_dir, st, "_Structures.gdb")
  layer_name <- st_layers(gdb_path)$name[1]
  fp         <- st_read(gdb_path, layer = layer_name, quiet = TRUE)

  # Lowercase column names WITHOUT breaking the sf geometry pointer.
  # A blanket names(fp) <- tolower(names(fp)) renames the geometry column
  # (e.g. "Shape" -> "shape") but leaves attr(fp, "sf_column") pointing at the
  # old name, so the next sf op errors with "sf_column does not point to a
  # geometry column". Instead: capture the geometry column, lowercase the rest,
  # then re-set geometry explicitly to a known name.
  geom_col <- attr(fp, "sf_column")
  non_geom <- setdiff(names(fp), geom_col)
  names(fp)[match(non_geom, names(fp))] <- tolower(non_geom)
  names(fp)[names(fp) == geom_col] <- "geometry"
  st_geometry(fp) <- "geometry"

  # --- validity: check first, fix only the invalid, drop unfixable residue ---
  # st_is_valid is far cheaper than st_make_valid, and invalid footprints are
  # rare, so this avoids running the expensive fix on the ~99%+ that are valid.
  # An invalid polygon can also yield a centroid placed outside the footprint
  # (silently geocoded to the wrong geography), so validating up front protects
  # correctness, not just speed.
  valid0 <- st_is_valid(fp)
  n_invalid <- sum(!valid0, na.rm = TRUE)
  if (n_invalid > 0 || anyNA(valid0)) {
    bad <- which(!valid0 | is.na(valid0))
    cat("  ", length(bad), "invalid geometr(ies); repairing...\n")
    # st_make_valid can return a GEOMETRYCOLLECTION (polygon + stray line/point);
    # extract polygonal parts so the result stays type-compatible with the sf
    # column before reinsertion.
    repaired <- st_make_valid(fp[bad, ])
    repaired <- st_collection_extract(repaired, "POLYGON", warn = FALSE)
    fp[bad, ] <- repaired

    # re-check; anything still invalid is dropped and logged
    valid1 <- st_is_valid(fp[bad, ])
    still_bad <- bad[!valid1 | is.na(valid1)]
    if (length(still_bad) > 0) {
      invalid_log <- rbind(invalid_log, data.frame(
        state    = st,
        build_id = fp$build_id[still_bad],
        reason   = st_is_valid(fp[still_bad, ], reason = TRUE),
        stringsAsFactors = FALSE
      ))
      cat("   WARNING:", length(still_bad),
          "geometr(ies) could not be repaired - dropped (logged)\n")
      fp <- fp[-still_bad, ]
    }
  }

  # --- drop empty geometries (e.g. a repaired collection with no polygon part) ---
  # These would yield empty centroids that NA out on join AND crash the nearest-
  # block-group snap (st_nearest_feature errors on empty geometry). Log and drop.
  empty_mask <- st_is_empty(fp)
  if (any(empty_mask)) {
    invalid_log <- rbind(invalid_log, data.frame(
      state    = st,
      build_id = fp$build_id[empty_mask],
      reason   = "empty geometry after validity repair",
      stringsAsFactors = FALSE
    ))
    cat("   WARNING:", sum(empty_mask),
        "empty geometr(ies) after repair - dropped (logged)\n")
    fp <- fp[!empty_mask, ]
  }

  # --- project to working CRS (EPSG:5070) to match prior-stage layers ---
  fp <- st_transform(fp, WORKING_CRS)

  # --- trim to the 26-field FEMA whitelist (matches v2_2 export schema) ---
  # Any whitelist field absent from this state's gdb is skipped; report both
  # keeps and misses for transparency. Do NOT name the geometry column in the
  # select(): on an sf object geometry is "sticky" and retained automatically,
  # and naming it explicitly errors ("Can't select columns that don't exist")
  # in some sf/dplyr versions because the sticky geometry is outside tidyselect
  # scope. select(all_of(keep_now)) keeps geometry on its own.
  keep_now <- intersect(FEMA_KEEP, names(fp))
  missing  <- setdiff(FEMA_KEEP, names(fp))
  if (length(missing) > 0) {
    cat("   NOTE: whitelist field(s) not in", st, "gdb:",
        paste(missing, collapse = ", "), "\n")
  }
  fp <- fp %>% select(all_of(keep_now))
  # safety net: if a version somehow dropped geometry, restore it explicitly
  if (!inherits(fp, "sf") || is.null(attr(fp, "sf_column"))) {
    stop("geometry column lost during whitelist trim for ", st)
  }
  cat("   kept", length(keep_now), "FEMA attribute columns\n")

  # --- load all SLR layers from the exported gpkg (build_id lookup only) ---
  gpkg_path   <- paste0(slr_dir, "flooded_structures_", st, ".gpkg")
  layer_names <- st_layers(gpkg_path)$name

  slr_layers <- setNames(
    lapply(layer_names, function(lyr) {
      df <- st_read(gpkg_path, layer = lyr, quiet = TRUE)
      names(df) <- tolower(names(df))
      st_drop_geometry(df)
    }),
    layer_names
  )

  # --- flag footprints by SLR layer (1 = build_id present in that layer) ---
  for (lyr in layer_names) {
    fp[[lyr]] <- as.integer(fp$build_id %in% slr_layers[[lyr]]$build_id)
  }

  cat("  ", nrow(fp), "footprints,", length(layer_names), "SLR layers flagged\n")

  # --- centroids: in-memory join vehicle for census geographies (not saved) ---
  centroids <- st_centroid(fp %>% select(build_id))

  # --- pull census geographies for this state ---
  # tigris id-column naming varies by vintage/case/format. Detect the id column
  # by preferring an exact known variant, then falling back to a fuzzy prefix
  # match; normalize to a bare GEOID by stripping any Census "<level>US" prefix
  # so 2010/2020 share one format for downstream joins.
  #   Known variants: GEOID, GEO_ID, GEOID10, GEOID20, GEO_ID10, GEO_ID20
  #   (case-insensitive). Exact-first avoids grabbing a prefixed/FQ column such
  #   as GEOIDFQ when a bare GEOID also exists.
  GEOID_VARIANTS <- c("GEOID", "GEO_ID", "GEOID10", "GEOID20", "GEO_ID10", "GEO_ID20")
  pick_geoid <- function(x, label) {
    nm <- names(x)
    exact <- nm[toupper(nm) %in% GEOID_VARIANTS]
    hit <- if (length(exact) > 0) exact[1]
           else grep("^GEO_?ID", nm, value = TRUE, ignore.case = TRUE)[1]
    if (is.na(hit) || length(hit) == 0)
      stop(label, ": no GEOID/GEO_ID column found. Columns present: ",
           paste(nm, collapse = ", "))
    hit
  }
  pick_name <- function(x, label) {
    hit <- grep("^NAME", names(x), value = TRUE, ignore.case = TRUE)[1]
    if (is.na(hit)) stop(label, ": no NAME column found. Columns present: ",
                         paste(names(x), collapse = ", "))
    hit
  }
  # strip a leading Census geo-prefix like "1500000US" -> bare GEOID
  strip_geoid_prefix <- function(v) sub("^[0-9]+US", "", as.character(v))

  bg10_raw <- block_groups(state = st, year = 2010, cb = TRUE)
  blkgrp_2010 <- bg10_raw[, pick_geoid(bg10_raw, "block_groups 2010")]
  names(blkgrp_2010)[1] <- "blkgrp_2010"
  blkgrp_2010$blkgrp_2010 <- strip_geoid_prefix(blkgrp_2010$blkgrp_2010)
  blkgrp_2010 <- st_transform(blkgrp_2010, st_crs(centroids))

  bg20_raw <- block_groups(state = st, year = 2020, cb = TRUE)
  blkgrp_2020 <- bg20_raw[, pick_geoid(bg20_raw, "block_groups 2020")]
  names(blkgrp_2020)[1] <- "blkgrp_2020"
  blkgrp_2020$blkgrp_2020 <- strip_geoid_prefix(blkgrp_2020$blkgrp_2020)
  blkgrp_2020 <- st_transform(blkgrp_2020, st_crs(centroids))

  pl20_raw <- places(state = st, year = 2020, cb = TRUE)
  places_2020 <- pl20_raw[, c(pick_geoid(pl20_raw, "places 2020"),
                              pick_name(pl20_raw, "places 2020"))]
  names(places_2020)[1:2] <- c("places_2020", "places_2020_name")
  places_2020$places_2020 <- strip_geoid_prefix(places_2020$places_2020)
  places_2020 <- st_transform(places_2020, st_crs(centroids))

  # --- spatial join: attach GEOIDs (and place name) to centroids ---
  centroids <- centroids %>%
    st_join(blkgrp_2010) %>%
    st_join(blkgrp_2020) %>%
    st_join(places_2020)

  # --- drop duplicates from boundary ties (point touching >1 polygon) ---
  n_before  <- nrow(centroids)
  centroids <- centroids %>%
    group_by(build_id) %>%
    slice_sample(n = 1) %>%
    ungroup()
  n_after   <- nrow(centroids)
  n_dropped <- n_before - n_after

  dedup_log <- rbind(dedup_log, data.frame(
    state = st, n_before = n_before, n_after = n_after, n_dropped = n_dropped
  ))

  cat("  ", n_after, "footprints geocoded (", n_dropped, "duplicate(s) dropped)\n")

  # --- snap NA block-group matches to nearest BG (both vintages, NA-only) ---
  # places NA is left as-is (unincorporated land is legitimately place-less).
  snap10 <- snap_na_bg(centroids, blkgrp_2010, "blkgrp_2010", "blkgrp_2010", st, BG_SNAP_MAX_M)
  centroids <- snap10$centroids
  snap20 <- snap_na_bg(centroids, blkgrp_2020, "blkgrp_2020", "blkgrp_2020", st, BG_SNAP_MAX_M)
  centroids <- snap20$centroids
  bg_snap_log <- rbind(bg_snap_log, snap10$log, snap20$log)
  n_snapped <- sum(snap10$log$within_threshold) + sum(snap20$log$within_threshold)
  n_left_na <- sum(!snap10$log$within_threshold) + sum(!snap20$log$within_threshold)
  if (nrow(snap10$log) + nrow(snap20$log) > 0) {
    cat("   BG snap: ", n_snapped, "assigned to nearest,",
        n_left_na, "left NA (beyond", BG_SNAP_MAX_M, "m)\n")
  }

  # --- attach geography columns back onto the FULL-POLYGON footprints ---
  # centroids were derived from fp and carry build_id; after dedup there is
  # exactly one centroid row per build_id. Align to fp by build_id (match)
  # rather than a join - simpler and no risk of row multiplication.
  # Guard: match() needs build_id to be non-NA and unique in fp.
  if (anyNA(fp$build_id)) {
    warning(st, ": ", sum(is.na(fp$build_id)),
            " footprint(s) have NA build_id; their geography will be NA")
  }
  if (anyDuplicated(fp$build_id) > 0) {
    warning(st, ": duplicate build_id in footprints; match() takes the first")
  }
  geo <- centroids %>% st_drop_geometry()
  idx <- match(fp$build_id, geo$build_id)
  fp$blkgrp_2010      <- geo$blkgrp_2010[idx]
  fp$blkgrp_2020      <- geo$blkgrp_2020[idx]
  fp$places_2020      <- geo$places_2020[idx]
  fp$places_2020_name <- geo$places_2020_name[idx]

  # --- accumulate non-spatial rows for summary tables ---
  all_data_list[[st]] <- fp %>% st_drop_geometry()

  # --- save full-polygon file (geometry + flags + geography + occupancy) ---
  out_path <- paste0(out_dir, st, "_footprints_slr_geocoded.gpkg")
  st_write(fp, out_path, layer = paste0(st, "_footprints_slr_geocoded"),
           delete_dsn = TRUE, quiet = TRUE)

  state_elapsed <- round(as.numeric(difftime(Sys.time(), state_start, units = "mins")), 1)
  cat("  saved ->", out_path, "  (", state_elapsed, "min )\n")
  send_notification(
    paste0(st, " geocoded"),
    paste0(nrow(fp), " footprints, ", length(layer_names),
           " SLR layers, ", state_elapsed, " min")
  )

  }, error = function(e) {
    # isolate failures: log and skip this state so the overnight run continues.
    # per-state .gpkg files already written are unaffected; only this state's
    # contribution to the summary tables is lost.
    cat("   ERROR processing", st, "-> skipped:", conditionMessage(e), "\n")
    send_notification(paste0(st, " FAILED"), conditionMessage(e))
  })
}


# *************
# 3. Save dedup log ----

log_path <- paste0(out_dir, "dedup_log.csv")
write.csv(dedup_log, log_path, row.names = FALSE)
cat("\nDedup log saved ->", log_path, "\n")

# record of footprints dropped for unrepairable geometry (may be empty)
invalid_path <- paste0(out_dir, "dropped_invalid_geometries.csv")
write.csv(invalid_log, invalid_path, row.names = FALSE)
cat("Invalid-geometry drop log saved ->", invalid_path,
    "(", nrow(invalid_log), "row(s) )\n")

# record of NA block-group matches snapped to nearest BG (or left NA)
bg_snap_path <- paste0(out_dir, "na_blockgroup_snapped.csv")
write.csv(bg_snap_log, bg_snap_path, row.names = FALSE)
cat("Block-group snap log saved ->", bg_snap_path,
    "(", nrow(bg_snap_log), "row(s),",
    sum(bg_snap_log$within_threshold), "assigned )\n")


# *************
# 4. SLR summary tables by geography ----
# for each geography level: total property count + total 1's per SLR layer,
# plus single-family-dwelling (SFD) counts, grouped by that geography's GEOID

all_data <- bind_rows(all_data_list)
slr_cols <- grep("^SLR_", names(all_data), value = TRUE)

# SFD summaries: for each geography, count all properties, then separately count
# the SFD-only subset, then join the two together. Filtering to SFD rows BEFORE
# grouping means the SFD counts are computed exactly like the totals - a plain
# sum of each SLR flag within the group - so they can't misalign.
sfd_data <- all_data %>% filter(prim_occ == "Single Family Dwelling")


# ---- 2010 block groups ----
total_2010 <- all_data %>%
  group_by(blkgrp_2010) %>%
  summarise(n_total = n(),
            across(all_of(slr_cols), ~ sum(.x, na.rm = TRUE)),
            .groups = "drop")

sfd_2010 <- sfd_data %>%
  group_by(blkgrp_2010) %>%
  summarise(n_sfd_total = n(),
            across(all_of(slr_cols), ~ sum(.x, na.rm = TRUE), .names = "sfd_{.col}"),
            .groups = "drop")

summary_2010 <- left_join(total_2010, sfd_2010, by = "blkgrp_2010")
summary_2010[is.na(summary_2010)] <- 0   # geographies with no SFDs -> 0

write.csv(summary_2010, paste0(out_dir, "summary_blkgrp_2010.csv"), row.names = FALSE)


# ---- 2020 block groups ----
total_2020 <- all_data %>%
  group_by(blkgrp_2020) %>%
  summarise(n_total = n(),
            across(all_of(slr_cols), ~ sum(.x, na.rm = TRUE)),
            .groups = "drop")

sfd_2020 <- sfd_data %>%
  group_by(blkgrp_2020) %>%
  summarise(n_sfd_total = n(),
            across(all_of(slr_cols), ~ sum(.x, na.rm = TRUE), .names = "sfd_{.col}"),
            .groups = "drop")

summary_2020 <- left_join(total_2020, sfd_2020, by = "blkgrp_2020")
summary_2020[is.na(summary_2020)] <- 0

write.csv(summary_2020, paste0(out_dir, "summary_blkgrp_2020.csv"), row.names = FALSE)


# ---- 2020 census places ----
total_places <- all_data %>%
  group_by(places_2020) %>%
  summarise(n_total = n(),
            across(all_of(slr_cols), ~ sum(.x, na.rm = TRUE)),
            .groups = "drop")

sfd_places <- sfd_data %>%
  group_by(places_2020) %>%
  summarise(n_sfd_total = n(),
            across(all_of(slr_cols), ~ sum(.x, na.rm = TRUE), .names = "sfd_{.col}"),
            .groups = "drop")

summary_places <- left_join(total_places, sfd_places, by = "places_2020")
summary_places[is.na(summary_places)] <- 0

# reattach the readable place name (dropped by grouping on GEOID alone)
place_names <- all_data %>% distinct(places_2020, places_2020_name)
summary_places <- summary_places %>%
  left_join(place_names, by = "places_2020") %>%
  relocate(places_2020_name, .after = places_2020)

write.csv(summary_places, paste0(out_dir, "summary_places_2020.csv"), row.names = FALSE)

cat("\nSummary tables saved to", out_dir, "\n")

send_notification(
  "SLR geocode complete",
  paste0(length(states), " states, ", nrow(all_data),
         " footprints total. Summaries written.")
)
