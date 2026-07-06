# **************************************************
# resummarize_slr_states.R
#
# Purpose: Regenerate the three summary CSVs (blkgrp_2010, blkgrp_2020,
#          places_2020) from the already-written per-state geocoded
#          GeoPackages, WITHOUT re-running the full geocoding pipeline.
#
# Memory-safe: reads ONE state's gpkg at a time (attributes only, geometry
# dropped), summarizes it, keeps only the small per-state summary tables, and
# binds those at the end. Block groups and places do not cross state lines, so
# a plain row-bind of per-state summaries is exact.
#
# Occupancy breakouts: for each configured category, produces n_<prefix>_total
# and <prefix>_SLR_0ft..10ft, counted by filtering to the subset BEFORE
# grouping. A preflight verifies each configured string exists in the first
# gpkg and stops if any don't match (avoids silent all-zero columns).
#
# Assisted by Claude Opus 4.8 High (Anthropic), claude.ai, 2026-07-05
# **************************************************

SCRIPT_VERSION <- "resummarize v3 (breakouts) (2026-07-05)"
cat("====", SCRIPT_VERSION, "====\n")

library(sf)
library(dplyr)

states_dir <- path.expand("~/claude_projects/slr_analysis/exports/SLR_states/")

# Occupancy breakouts (must match the main script). Each produces its own
# n_<prefix>_total and <prefix>_SLR_* columns. occ_cls == "Residential" OVERLAPS
# the prim_occ categories (an SFD is also Residential), so res_ is inclusive of
# the others and the category columns are not mutually exclusive.
OCC_BREAKOUTS <- list(
  sfd   = list(field = "prim_occ", value = "Single Family Dwelling"),
  manuf = list(field = "prim_occ", value = "Manufactured Home"),
  mfd   = list(field = "prim_occ", value = "Multi - Family Dwelling"),
  res   = list(field = "occ_cls",  value = "Residential")
)

gpkgs <- list.files(states_dir, pattern = "_footprints_slr_geocoded\\.gpkg$",
                    full.names = TRUE)
cat("Found", length(gpkgs), "state gpkg files\n")

# summarise one geography within a single state's attribute table: total +
# per-SLR totals, plus each occupancy breakout's n_<prefix>_total and
# <prefix>_SLR_* (filtered to the subset before grouping).
summarize_one <- function(df, geo_col, slr_cols, breakouts) {
  out <- df %>%
    group_by(.data[[geo_col]]) %>%
    summarise(n_total = n(),
              across(all_of(slr_cols), ~ sum(.x, na.rm = TRUE)),
              .groups = "drop")
  for (pfx in names(breakouts)) {
    b   <- breakouts[[pfx]]
    sub <- df[df[[b$field]] == b$value & !is.na(df[[b$field]]), , drop = FALSE]
    sub_sum <- sub %>%
      group_by(.data[[geo_col]]) %>%
      summarise(!!paste0("n_", pfx, "_total") := n(),
                across(all_of(slr_cols), ~ sum(.x, na.rm = TRUE),
                       .names = paste0(pfx, "_{.col}")),
                .groups = "drop")
    out <- left_join(out, sub_sum, by = geo_col)
    fill_cols <- c(paste0("n_", pfx, "_total"), paste0(pfx, "_", slr_cols))
    for (cc in fill_cols) if (cc %in% names(out)) out[[cc]][is.na(out[[cc]])] <- 0L
  }
  out
}

# --- preflight: verify breakout strings against the FIRST gpkg ---
if (length(gpkgs) > 0) {
  pf <- st_read(gpkgs[1], quiet = TRUE) %>% st_drop_geometry()
  cat("Preflight: checking occupancy breakout strings against",
      basename(gpkgs[1]), "...\n")
  for (pfx in names(OCC_BREAKOUTS)) {
    b <- OCC_BREAKOUTS[[pfx]]
    if (!(b$field %in% names(pf)))
      stop("Preflight: field '", b$field, "' (breakout '", pfx, "') not found.")
    if (!(b$value %in% pf[[b$field]]))
      stop("Preflight: value '", b$value, "' not found in ", b$field,
           " (breakout '", pfx, "'). Present values include: ",
           paste(head(sort(unique(pf[[b$field]])), 20), collapse = " | "))
    cat("   OK:", pfx, "->", b$field, "==", shQuote(b$value), "\n")
  }
  rm(pf)
  cat("Preflight passed.\n")
}

# accumulators for the small per-state summary tables
acc_2010   <- list()
acc_2020   <- list()
acc_places <- list()
place_names_list <- list()
slr_cols <- NULL

for (g in gpkgs) {
  st_abbr <- sub("_footprints_slr_geocoded\\.gpkg$", "", basename(g))
  cat("  summarizing", st_abbr, "...\n")

  df <- st_read(g, quiet = TRUE) %>% st_drop_geometry()
  if (is.null(slr_cols)) slr_cols <- grep("^SLR_", names(df), value = TRUE)

  acc_2010[[st_abbr]]   <- summarize_one(df, "blkgrp_2010", slr_cols, OCC_BREAKOUTS)
  acc_2020[[st_abbr]]   <- summarize_one(df, "blkgrp_2020", slr_cols, OCC_BREAKOUTS)
  acc_places[[st_abbr]] <- summarize_one(df, "places_2020", slr_cols, OCC_BREAKOUTS)
  place_names_list[[st_abbr]] <- df %>% distinct(places_2020, places_2020_name)

  rm(df); gc()
}

summary_2010   <- bind_rows(acc_2010)
summary_2020   <- bind_rows(acc_2020)
summary_places <- bind_rows(acc_places)

place_names <- bind_rows(place_names_list) %>% distinct(places_2020, places_2020_name)
summary_places <- summary_places %>%
  left_join(place_names, by = "places_2020") %>%
  relocate(places_2020_name, .after = places_2020)

# sanity check: no breakout per-SLR count may exceed its SLR total
base_slr <- grep("^SLR_", names(summary_2020), value = TRUE)
violations <- 0
for (pfx in names(OCC_BREAKOUTS)) {
  for (sc in base_slr) {
    bcol <- paste0(pfx, "_", sc)
    if (bcol %in% names(summary_2020))
      violations <- violations +
        sum(summary_2020[[bcol]] > summary_2020[[sc]], na.rm = TRUE)
  }
}
cat("Sanity check: rows where any breakout_SLR > SLR total:", violations,
    "(should be 0)\n")

write.csv(summary_2010,   paste0(states_dir, "summary_blkgrp_2010.csv"), row.names = FALSE)
write.csv(summary_2020,   paste0(states_dir, "summary_blkgrp_2020.csv"), row.names = FALSE)
write.csv(summary_places, paste0(states_dir, "summary_places_2020.csv"), row.names = FALSE)

cat("\nSummaries regenerated in", states_dir, "\n")
