---
editor_options: 
  markdown: 
    wrap: 72
---

# slr_coastal_analysis_final

Geospatial pipeline for analyzing U.S. coastal structure and Census
tract exposure to NOAA sea level rise (SLR) inundation scenarios. The
pipeline intersects NOAA SLR inundation polygons with 2025 Census TIGER
tract boundaries and FEMA building footprints across 21 coastal states
at 11 SLR scenarios (0–10 ft).

## Repository Structure

```         
slr_coastal_analysis_final/
├── scripts/
│   ├── 02_1_download_noaa_slr/       # Download and ingest NOAA SLR shapefiles
│   ├── 02_2_ingest_tracts/           # Ingest Census TIGER tract boundaries
│   ├── 02_3_prepare_geometries/      # Subdivide SLR polygons for performance
│   ├── 02_4_tract_intersections/     # Compute tract × SLR intersections
│   │   └── louisiana/                # Louisiana-specific dedicated script
│   ├── 02_5_load_structures/         # Load FEMA building footprints
│   │   └── connecticut/              # Connecticut FIPS fix script
│   ├── 02_6_structure_flooding/      # Identify flooded structures per scenario
│   ├── 02_7_export_structures/       # Export flooded structures to GeoPackage
│   └── 02_8_export_tracts/           # Export flooded tracts to GeoPackage
├── config/                           # YAML configuration files
├── docs/                             # Full pipeline documentation
└── README.md
```

Directory numbering parallels the section numbering in the pipeline
documentation (`docs/NOAA_SLR_methods_pipeline_v3.md`).

## Pipeline Summary

| Section | Step | Script | Output |
|--------------------|-----------------|------------------|------------------|
| 2.1 | Download NOAA SLR shapefiles | `NOAA_SLR_downloader_v3.R` + `ogr2ogr` | 66 tables (`slr_Xft_region`) |
| 2.2 | Ingest Census tracts | `wget` + `shp2pgsql` | 1 table (`census_tracts_2025`) |
| 2.3 | Subdivide SLR polygons | `create_state_slr_subdivided_v5.R` | 231 tables (`slr_Xft_FF`) |
| 2.4 | Tract intersections | `analyze_tract_slr_intersections_v5_1.R` | 11 tables (`tract_Xft_intersections`) |
| 2.5 | Load FEMA structures | `load_structures_to_db_v2.R` | 21 tables (`usa_structures_FF`) |
| 2.6 | Structure flooding analysis | `analyze_structure_slr_flooding_v2_1.R` | 231 tables (`flooded_structures_FF_Xft`) |
| 2.7 | Export flooded structures | `export_flooded_structures_v2_1.R` | 21 GeoPackage files |
| 2.8 | Export flooded tracts | `export_flooded_tracts_v1.R` | 21 GeoPackage files |

Naming conventions: `FF` = 2-digit state FIPS code, `X` = SLR scenario
(0–10 ft), `SS` = 2-letter state abbreviation, `region` = NOAA region
name.

## Data Sources

-   **NOAA SLR inundation polygons** — 6 NOAA regions, 11 scenarios
    (0–10 ft). Downloaded via `NOAA_SLR_downloader_v3.R`.
-   **2025 Census TIGER tract boundaries** — 49,502 tracts across 21
    coastal states. Downloaded from the Census Bureau TIGER/Line FTP
    server.
-   **FEMA USA Structures** — National building footprint dataset
    distributed as state-level `.gdb` files. 72.9M structures
    nationally; 14.1M retained after tract-based pre-filtering.

## Requirements

**Hardware:** The pipeline was developed and run on a 32-core AMD
Threadripper workstation with 128 GB RAM (TRIPPER3). Long-running steps
(Sections 2.3, 2.4, 2.6) benefit significantly from parallel processing.

**Software:** - PostgreSQL 14+ with PostGIS 3+ - R 4.x with packages:
`DBI`, `RPostgres`, `sf`, `glue`, `yaml`, `parallel` - GDAL command-line
tools (`ogr2ogr`, `shp2pgsql`)

**Database:** All pipeline steps operate on a PostgreSQL/PostGIS
database named `megaSLR`. Connection parameters are set in each YAML
config file. Database credentials are managed via `~/.pgpass` and are
not stored in this repository.

**CRS:** All data are reprojected to EPSG:5070 (NAD83 / Conus Albers
Equal Area) on ingest. Source CRS: NOAA polygons and Census tracts are
EPSG:4269; FEMA structures are EPSG:4326.

## Configuration

Each pipeline step is driven by a YAML config file in `config/`:

| Config file                         | Used by sections   |
|-------------------------------------|--------------------|
| `NOAA_downloads_config_v1.yaml`     | 2.1                |
| `tracts_analysis_config_v2.yaml`    | 2.4, 2.8           |
| `structure_analysis_config_v3.yaml` | 2.3, 2.5, 2.6, 2.7 |

Edit the config files to set database host, credentials path, state
lists, scenario lists, output paths, and parallelism settings before
running.

## Documentation

Full pipeline documentation is in
`docs/NOAA_SLR_methods_pipeline_v3.md`. This covers data sources,
methodology, performance optimizations, known issues (including the
Connecticut FIPS fix and Louisiana geometry handling), and output
schemas.

## Special Cases

**Louisiana (Section 2.4):** Dense coastal geometry required a dedicated
per-tract processing script
(`scripts/02_4_tract_intersections/louisiana/`) with extended timeouts
and skip logic.

**Connecticut (Section 2.5):** Post-2022 Census planning region FIPS
codes conflict with legacy county codes in the FEMA `.gdb`. A relaxed
JOIN script (`scripts/02_5_load_structures/connecticut/`) resolves this
using state prefix and tract suffix matching.

## Coverage

21 coastal states: AL, CA, CT, DE, FL, GA, LA, MA, MD, ME, MS, NC, NH,
NJ, NY, OR, RI, SC, TX, VA, WA

## Authors

Nora Schwaller (UCSD) \
Mathew Schwaller

## Data Archival

Final spatial data products will be archived via Dryad. Code archived in
this GitHub repository.
