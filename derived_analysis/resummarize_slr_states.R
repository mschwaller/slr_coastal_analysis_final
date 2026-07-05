# **************************************************
# resummarize_slr_states.R
#
# Purpose: Regenerate the three summary CSVs (blkgrp_2010, blkgrp_2020,
#          places_2020) from the already-written per-state geocoded
#          GeoPackages, WITHOUT re-running the full geocoding pipeline.
#
# Memory-safe design: the previous version (and the summary block in the main
# script) did bind_rows() on ALL states' rows at once - ~70M rows across 23
# states - which exhausts RAM and gets the process OOM-killed (no R error,
# just a silent stop after the last log write). This version instead:
#   - reads ONE state's gpkg at a time (attributes only, geometry dropped)
#   - computes that state's per-geography summary immediately
#   - keeps only the small summary tables (a few thousand rows per state)
#   - binds those small tables at the end
# Block groups and places do not cross state lines, so each geography's counts
# come entirely from a single state - no cross-state aggregation is needed,
# a plain row-bind of the per-state summaries is exact.
#
# Assisted by Claude Opus 4.8 High (Anthropic), claude.ai, 2026-07-04
# **************************************************

SCRIPT_VERSION <- "resummarize v2 memory-safe (2026-07-04)"
cat("====", SCRIPT_VERSION, "====\n")

library(sf)
library(dplyr)

states_dir <- path.expand("~/claude_projects/slr_analysis/exports/SLR_states/")

gpkgs <- list.files(states_dir, pattern = "_footprints_slr_geocoded\\.gpkg$",
                    full.names = TRUE)
cat("Found", length(gpkgs), "state gpkg files\n")

# summarise one geography within a single state's attribute table:
# total count + per-SLR flooded totals, and the SFD-only equivalents, joined.
summarize_one <- function(df, sfd_df, geo_col, slr_cols) {
  total <- df %>%
    group_by(.data[[geo_col]]) %>%
    summarise(n_total = n(),
              across(all_of(slr_cols), ~ sum(.x, na.rm = TRUE)),
              .groups = "drop")
  sfd <- sfd_df %>%
    group_by(.data[[geo_col]]) %>%
    summarise(n_sfd_total = n(),
              across(all_of(slr_cols), ~ sum(.x, na.rm = TRUE), .names = "sfd_{.col}"),
              .groups = "drop")
  out <- left_join(total, sfd, by = geo_col)
  # geographies with no SFDs -> NA after join -> 0
  sfd_out <- c("n_sfd_total", paste0("sfd_", slr_cols))
  for (cc in sfd_out) if (cc %in% names(out)) out[[cc]][is.na(out[[cc]])] <- 0L
  out
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

  # read attributes only (drop geometry immediately to save memory)
  df <- st_read(g, quiet = TRUE) %>% st_drop_geometry()

  if (is.null(slr_cols)) slr_cols <- grep("^SLR_", names(df), value = TRUE)

  sfd_df <- df %>% filter(prim_occ == "Single Family Dwelling")

  acc_2010[[st_abbr]]   <- summarize_one(df, sfd_df, "blkgrp_2010", slr_cols)
  acc_2020[[st_abbr]]   <- summarize_one(df, sfd_df, "blkgrp_2020", slr_cols)
  acc_places[[st_abbr]] <- summarize_one(df, sfd_df, "places_2020", slr_cols)
  place_names_list[[st_abbr]] <- df %>% distinct(places_2020, places_2020_name)

  rm(df, sfd_df); gc()   # free this state's rows before the next
}

# bind the small per-state summaries (each geography lives in exactly one state,
# so no re-aggregation is needed)
summary_2010 <- bind_rows(acc_2010)
summary_2020 <- bind_rows(acc_2020)
summary_places <- bind_rows(acc_places)

# reattach readable place names
place_names <- bind_rows(place_names_list) %>% distinct(places_2020, places_2020_name)
summary_places <- summary_places %>%
  left_join(place_names, by = "places_2020") %>%
  relocate(places_2020_name, .after = places_2020)

# --- sanity check: no per-SLR SFD count may exceed its total ---
violations <- 0
for (sc in slr_cols) {
  violations <- violations +
    sum(summary_2020[[paste0("sfd_", sc)]] > summary_2020[[sc]], na.rm = TRUE)
}
cat("Sanity check: rows where sfd_SLR > SLR total:", violations, "(should be 0)\n")

write.csv(summary_2010,   paste0(states_dir, "summary_blkgrp_2010.csv"), row.names = FALSE)
write.csv(summary_2020,   paste0(states_dir, "summary_blkgrp_2020.csv"), row.names = FALSE)
write.csv(summary_places, paste0(states_dir, "summary_places_2020.csv"), row.names = FALSE)

cat("\nSummaries regenerated in", states_dir, "\n")
