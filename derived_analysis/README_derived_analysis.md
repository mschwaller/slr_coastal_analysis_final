---
editor_options:
  markdown:
    wrap: 72
---

# derived_analysis

Downstream analysis built **on top of** the published megaSLR dataset.
This directory geocodes the exported flooded-structure GeoPackages to
Census geographies and produces per-geography summary tables of flooded
structures by sea level rise (SLR) scenario, broken out by total
properties and single-family dwellings (SFDs).

> **Relationship to the published dataset.** The scripts and outputs
> here are **not** part of the *Scientific Data* / Dryad deliverable.
> They consume the published per-state structure GeoPackages (produced
> by `scripts/02_7_export_structures/`) and generate derived products
> used to build maps and tables for the manuscript. The published
> dataset is complete at pipeline stage 2.8; nothing in this directory
> is required to reproduce it.

## What this does

For each of the 23 coastal states, the pipeline:

1.  Loads the original FEMA USA Structures footprints from the source
    `.gdb`, validates and repairs geometry, and trims to the 26-field
    attribute whitelist matching the published GeoPackage schema.
2.  Flags each footprint 0/1 for every SLR scenario (0–10 ft) using the
    exported `flooded_structures_SS.gpkg` files as a `build_id` lookup.
3.  Assigns each footprint to a 2010 Census block group, a 2020 Census
    block group, and a 2020 Census place (GEOID + name) via a centroid
    spatial join, snapping centroids with no block-group match to the
    nearest block group within a distance threshold.
4.  Writes one full-polygon GeoPackage per state carrying the footprint
    geometry, SLR flags, and the four geography columns.

Each state is summarised as it is processed and only the small
per-geography summary tables are retained; after all states finish,
these are combined into summary tables (2010 block groups, 2020 block
groups, 2020 places) with total and single-family-dwelling counts,
overall and flooded at each SLR increment. Summarising per state (rather
than concatenating every state's rows) keeps peak memory to a single
state's footprints.

## Scripts

| Script | Purpose |
|----|----|
| `flooded_structures_geocoded.R` | Main pipeline: load → validate → flag → geocode → per-state GeoPackage → summary tables |
| `resummarize_slr_states.R` | Regenerates only the three summary CSVs from existing per-state GeoPackages (no full re-run needed) |

## Inputs

-   **Exported structure GeoPackages** — `flooded_structures_SS.gpkg`,
    one per state, with `SLR_0ft`–`SLR_10ft` layers, produced by
    pipeline stage 2.7. Used only as a `build_id` lookup to flag
    flooding.
-   **FEMA USA Structures `.gdb` files** — `SS_Structures.gdb`, the
    original per-state footprints. Source of geometry and all
    attributes.
-   **Census geographies** — 2010 block groups, 2020 block groups, and
    2020 places, retrieved via `tigris` (cartographic boundary files,
    `cb = TRUE`).

## Outputs

Written to `~/claude_projects/slr_analysis/exports/SLR_states/`:

-   `SS_footprints_slr_geocoded.gpkg` (one per state) — footprint
    polygons + 26 FEMA attribute fields + `SLR_0ft`–`SLR_10ft` flags +
    `blkgrp_2010`, `blkgrp_2020`, `places_2020`, `places_2020_name`.
-   `summary_blkgrp_2010.csv`, `summary_blkgrp_2020.csv`,
    `summary_places_2020.csv` — per-geography counts: `n_total`,
    `n_sfd_total`, `SLR_0ft`–`SLR_10ft` (total flooded per level), and
    `sfd_SLR_0ft`–`sfd_SLR_10ft` (SFDs flooded per level).
-   `dedup_log.csv` — boundary-tie duplicates dropped per state.
-   `dropped_invalid_geometries.csv` — footprints dropped for
    unrepairable or empty geometry, with reason.
-   `na_blockgroup_snapped.csv` — centroids with no block-group match,
    snapped to the nearest block group or left NA, with distances.

## Method notes

**Coordinate reference system.** All spatial operations use EPSG:5070
(NAD83 / Conus Albers Equal Area), the project standard. FEMA footprints
(native EPSG:4326) and `tigris` Census geographies are transformed to
EPSG:5070 before centroid computation and spatial joins, so distances
and centroids are in meters.

**Geography assignment via centroids.** Each footprint's block group and
place are determined by where its centroid falls. Centroids are computed
in memory only and are not written to disk; the resulting geography
codes are attached back onto the full-polygon footprints by `build_id`.

**Block-group snapping.** Block groups cover all land, so a centroid
with no 2010 or 2020 block-group match usually fell just outside a
generalized (`cb = TRUE`) coastline. Such centroids are snapped to the
nearest block group within `BG_SNAP_MAX_M` (default 500 m) and logged;
beyond that distance the value is left NA. Census places do **not**
cover unincorporated land, so an NA place value is expected and left
as-is.

**Census GEOID normalization.** `tigris` returns different id-column
names and formats by vintage: 2010 cartographic files use a prefixed
`GEO_ID` (e.g. `1500000US010150022002`) while 2020 files use a bare
`GEOID` (e.g. `010150022002`). The id column is detected across known
variants (`GEOID`, `GEO_ID`, `GEOID10`, `GEOID20`, `GEO_ID10`,
`GEO_ID20`) and any Census `<level>US` prefix is stripped so 2010 and
2020 GEOIDs share a bare format for downstream joins.

**Single-family dwellings.** SFDs are identified by
`prim_occ == "Single Family Dwelling"` (a FEMA `prim_occ` value;
`occ_cls` holds only broad categories such as Residential). SFD summary
counts are computed by filtering to SFD rows *before* grouping, so each
`sfd_SLR_Xft` count is the number of single-family dwellings in that
geography flooded at level X and can never exceed the corresponding
total.

**Per-state isolation.** Each state is processed inside a `tryCatch` so
a single failure (e.g. a corrupt `.gdb` or a `tigris` download error) is
logged and skipped rather than aborting the whole run.

## Requirements

-   R 4.x with packages: `sf`, `dplyr`, `tigris`
-   Internet access for `tigris` Census downloads (cached after first
    fetch via `options(tigris_use_cache = TRUE)`)
-   The exported structure GeoPackages (stage 2.7) and the FEMA `.gdb`
    files

## Running

Long job — run inside `screen` on TRIPPER3 with console output teed to a
log:

``` bash
screen -S slr_geocode
cd ~/claude_projects/slr_analysis
Rscript flooded_structures_geocoded.R \
  2>&1 | tee ~/Science/Nora_SLR/SLR_log_files/geocode_$(date +%Y%m%d_%H%M).log
# detach: Ctrl-A then D   |   reattach: screen -r slr_geocode
```

The script prints its version banner at startup and fires `ntfy.sh` push
notifications (topic `matt-tripper3-jobs`) per state and at completion.
Each message is prefixed with a timestamp and the state abbreviation
(e.g.
`2026-07-05 14:32 | AL | 2771976 footprints, 11 SLR layers, 12.4 min`).

To regenerate only the summary tables from existing per-state
GeoPackages (seconds, no full re-run):

``` bash
Rscript resummarize_slr_states.R
```

## Provenance for the manuscript

The summary tables and geocoded GeoPackages here are the source of the
maps and tables in the *Scientific Data* manuscript. This directory is
the reproducible record of how those figures were generated from the
published dataset; the manuscript methods section describes the same
steps in condensed form for readers.
