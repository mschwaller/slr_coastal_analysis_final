# Data Sources and Pipeline Description Document

# Choose Your Own Disaster: A Dataset to Assess Sea Level Rise and Inundation of US Coastal Tracts and Structures

# 1. Overview

Sea level rise (SLR) appears to be an inevitable consequence of atmospheric carbon dioxide content that has increased by 50% since pre-industrial levels, and that continues to increase at an accelerating pace. While there is considerable uncertainty about the timing and magnitude of SLR it is clear that coastal infrastructure will be increasingly and often catastrophically inundated within the span of one or 2 human lifetimes. To help predict these impacts we combine 3 open source datasets to evaluate inundation of coastal Census tracts and FEMA structures of the conterminous United States at levels of SLR from 0 to 10 feet. As described in detail below, the sources for this assessment include a NOAA SLR dataset, Census tracts from the TIGER database, and FEMA's inventory of U.S. structures. These sources are combined into a derived dataset that identifies which tracts and structures are inundated at incremental levels of SLR. The derived dataset can be used to "create your own disaster" (CYOD) for SLR levels of 0 to 10 feet above current Mean Higher High Water conditions.

This document describes the source data and the data processing pipeline used to create the CYOD dataset.

**Scale:** 21 coastal states · 11 SLR scenarios (0ft–10ft) · \~49,500 Census tracts · \~101K–3.8M flooded structures (varying by SLR level)\
**Infrastructure:** PostgreSQL/PostGIS on an Ubuntu 32-core, 128 GB RAM AMD Threadripper workstation\
**Primary language:** R (with PostGIS for spatial computation)

## 1.1 Data Sources Overview

Below is a brief description of the source data, with additional detail provided in subsequent sections.

### NOAA Sea Level Rise Polygons

-   **Source:** NOAA Office for Coastal Management, Digital Coast
-   **Format:** Shapefiles, partitioned by coastal region
-   **Scenarios used:** 0ft (current tidal flooding) through 10ft
-   **Geometry note:** These polygons can be extremely complex — some regions contain polygons with 2M+ vertices, particularly along Louisiana's coastline

### Census Tracts

-   **Source:** US Census Bureau TIGER/Line files
-   **Usage:** Primary unit of analysis for SLR area intersection; also supports downstream demographic and zoning analysis via GEOID joins
-   **Known issue:** Connecticut uses post-2022 planning region FIPS codes in Census tract tables, but FEMA structure files reference pre-2022 county FIPS codes. Matching requires state prefix + tract suffix rather than full GEOID equality.

### FEMA USA Structures

-   **Source:** FEMA USA Structures geodatabases
-   **Format:** Per-state file geodatabases containing building footprint polygons and additional attributes
-   **Coverage:** All 21 coastal states

## 1.2 Pipeline Architecture Overview

This is a brief description of the database and scripts used to generate the CYOD dataset, with additional details provided in subsequent sections.

**Coordinate Reference System (CRS)**. The native CRS for the NOAA SLR shapefiles is EPSG:4269, the North American Datum 1983 (NAD83), which is expressed as geographical coordinates in latitude and longitude. This CRS is not convenient because length and area of SLR shapefile polygons are calculated in degrees (length) and degrees^2^ (area). We transform the EPSG:4269 coordinates to EPSG:5070 which uses the same NAD83 geoid but maps the coordinates to the Conus Albers projection which is expressed in meters of northing and easting. Using EPSG:5070 expresses the SLR multipolygons in meters (length) and meters^2^ (area). Using this equal area CRS also speeds up the spatial features operations compared to working with degrees of latitude and longitude. Thus, EPSG:5070 is the target CRS applied to all SLR polygons, Census tracts (native CRS EPSG:4269), and FEMA structures (native CRS EPSG:4326); EPSG:5070 is also used in all scripts, queries and calculations.

### Data Ingest

-   All geospatial data used in the CYOD pipeline are stored and processed in a Postgres database (with PostGIS extensions) named megaSLR. Once source data are ingested, the database provides persistent storage and access. The database supports records and fields defined as spatial features (point, line, polygon, multipolygon) and also supports an extensive set of spatial query commands (`ST_Subdivide`, `ST_Intersection`, `ST_Intersects`, `ST_Union`, `ST_Area` and so on). Database operations are optimized for performance and allow multiple CPU cores to execute a single query in parallel, thus reducing execution time for CPU-intensive workloads.

-   **SLR shapefiles** were acquired from the NOAA website <https://coast.noaa.gov/slrdata/Ancillary/index.html> using the R script `NOAA_SLR_downloader_v3.R,` with parameters for this script set in the configuration file `NOAA_downloads_config_v1.yaml`. The shapefiles are stored in subdirectories as defined in the yaml config file. A set of bash commands including GDAL's `ogr2ogr()` was used to ingest the shapefiles into megaSLR as psql tables, with one table per SLR level, per coastal region (using the 6 NOAA-defined SLR regions: atlantic, florida, la, ms_al, tx and west), from 0ft to 10ft; a total of 66 SLR tables consisting of 11,446,885 SLR polygons, with a table naming convention of slr_Xft_region (with X = 0 to 10, and region = the 6 SLR regions defined earlier).

-   **Census Tracts** were acquired for the coastal US states from the website <https://www2.census.gov/geo/tiger/TIGER2025/TRACT> using bash wget and saved to disk as ESRI shapefiles. A set of bash commands including GDAL's `shp2pgsql()` were used to ingest the shapefiles into megaSLR as a single psql table named census_tracts_2025 with 49,502 tract polygons.

-   **FEMA Structures** were acquired from the site <https://gis-fema.hub.arcgis.com/pages/usa-structures> using bash wget and saved to disk in the file's native ESRI geodatabase (.gdb) format. The script `load_structures_to_db_v2.R` was used to import the structures into the megaSLR database using GDAL's `ogr2ogr()`, with one structures table for each of 21 coastal states. This script is controlled by the config file `structure_analysis_config_v3.yaml`. Note that the load_structures script used tract-based pre-filtering to keep only structures in SLR-affected Census tracts, and is therefore dependent on the presence the 10 ft Census tract table `tract_10ft_intersections` (see Section 2.4 for how this table and associated intersections tables were generated). The 21 structures tables consist of 14,101,005 structure polygons, named usa_structures_FF with FF = the 2-digit FIPS code for the coastal US states.

### Pre-Processing

-   **SLR polygons were subdivided** using the script `create_state_slr_subdivided_v5.R` and config file `structure_analysis_config_v3.yaml`. The script creates per-state SLR tables by clipping regional SLR polygons to each state's Census tract extent and crucially by subdividing complex SLR polygons using `ST_Subdivide`. Subdividing complex SLR polygons dramatically improves `ST_Intersects` performance by producing smaller polygons with tighter bounding boxes, which 1) greatly reduces the number of false-positive spatial index hits, and 2) reduces the computational cost of exact geometry tests against polygons that may have millions of vertices. For example, 98% of Louisiana's 0ft SLR polygons have fewer than 50 vertices, but a small number of polygons with \>1k vertices (and one polygon with almost 7M vertices) dominates the computational load. After subdivision, each SLR polygon has a maximum of 256 vertices. The total geometric information is identical — subdivision is lossless — but the spatial index efficiently filters candidates using tight bounding boxes around spatially compact sub-polygons, instead of huge bounding boxes spanning complex coastal geometries. The naming convention for the 231 subdivided tables is slr_Xft_FF with X = 0 to 10 and FF = the 21 FIPS codes for the coastal US states (231 tables).

```         
 Louisiana SLR: distribution of the number of vertices per SLR polygon.
 
    before subdivision                after subdivision
 # vertices | # polygons           # vertices | # polygons 
------------+--------------       ------------+--------------
 0-10       |      1172301         0-10       |      1749474
 11-50      |       182774         11-50      |       475009
 51-100     |        14060         51-100     |       175764
 101-256    |         6830         101-256    |       598155
 257-1K     |         2414         total      |      2998402
 1K-10K     |          696
 10K-100K   |          152
 100K-1M    |          116
 1M+        |           31     
 total      |      1379374
```

### Census Tracts and FEMA Structures: Intersections with SLR Levels

-   **Census tracts intersections with SLR levels** were computed using the script `analyze_tract_slr_intersections_v5_1.R` and the config file `tracts_analysis_config_v2.yaml`. The script computes area-based intersections between Census tracts and pre-subdivided, state-partitioned SLR flood polygons. The script generated 11 database tables with the naming convention `tract_Xft_intersections` with X = 0 to 10.
-   **FEMA structures intersections with SLR levels** were computed using the script `analyze_structure_slr_flooding_v2_1.R` and the config file `structure_analysis_config_v3.yaml`. The script performs boolean `ST_Intersects` between pre-filtered structure tables and per-state, per-SLR subdivided tables (`slr_Xft_FF`) to identify flooded structures at each SLR level. The script generated 231 database tables with the naming convention `flooded_structures_FF_Xft`, where FF = 2-digit state FIPS code and X = SLR level (0 to 10, in feet).

### Data Export

-   **Export of flooded structures** is performed using the script `export_flooded_structures_v2.R`. This script saves flooded_structures tables from megaSLR to disk as Open Geospatial Consortium formatted GeoPackage files with one GPKG per state, and with SLR_0ft to SLR_10ft as separate layers in each GPKG. The file naming convention for the exported files is flooded_structures_SS.gpkg where SS = one of AL, CA, CT, DE, FL, GA, LA, MA, MD, ME, MS, NC, NH, NJ, NY, OR, RI, SC, TX, VA, WA.
-   **Export of flooded tracts** is performed using the script `export_flooded_tracts_v1.R` and the config file `tracts_analysis_config_v2.yaml`. This script reads tract intersection tables from megaSLR, filters by state, and exports to GeoPackage files with one GPKG per state and SLR_0ft to SLR_10ft as separate layers. The file naming convention is `flooded_tracts_SS.gpkg` where SS = state abbreviation.

The CYOD processing pipeline is summarized below as an illustration with color representations as follows: coral for external data sources, teal for all database tables, purple for exported files

![](images/pipeline_flow2.png)

# 2. Processing Pipeline Details

The table below is roadmap for the processing pipeline indicating the source files, the scripts and processes that act upon these files, the megaSLR tables or exported files that are generated by the scripts and processes, and the naming convention used for the database tables and exported files. The first column of the roadmap table also identifies subsequent sections of this document in parentheses, from (2.1) to (2.8), where additional details are provided on the sequential steps of the pipeline inputs, processes, and outputs.

+-------------------------------------------------+-------------------------------------------------------------------------------+------------------------------+-----------------------------------------------------------+-------------------+
| Input                                           | Script / Process                                                              | Output Table / File          | Naming Convention                                         | \# Tables / Files |
+=================================================+===============================================================================+==============================+===========================================================+===================+
| NOAA SLR shapefiles\                            | `NOAA_SLR_downloader_v3.R` + `ogr2ogr` +`NOAA_downloads_config_v1.yaml`       | `slr_Xft_region`             | X = 0–10, region = atlantic, florida, la, ms_al, tx, west | 66 tables         |
| \                                               |                                                                               |                              |                                                           |                   |
| (**2.1** Add NOAA SLR Shapefiles)               |                                                                               |                              |                                                           |                   |
+-------------------------------------------------+-------------------------------------------------------------------------------+------------------------------+-----------------------------------------------------------+-------------------+
| Census TIGER shapefiles\                        | bash `wget` + `shp2pgsql`                                                     | `census_tracts_2025`         | —                                                         | 1 table           |
| \                                               |                                                                               |                              |                                                           |                   |
| (**2.2** AddCensus Tracts)                      |                                                                               |                              |                                                           |                   |
+-------------------------------------------------+-------------------------------------------------------------------------------+------------------------------+-----------------------------------------------------------+-------------------+
| `slr_Xft_region`\                               | `create_state_slr_subdivided_v5.R` + `structure_analysis_config_v3.yaml`      | `slr_Xft_FF`                 | X = 0–10, FF = 21 FIPS codes                              | 231 tables        |
| \                                               |                                                                               |                              |                                                           |                   |
| (**2.3** Subdivide SLR Polygons)                |                                                                               |                              |                                                           |                   |
+-------------------------------------------------+-------------------------------------------------------------------------------+------------------------------+-----------------------------------------------------------+-------------------+
| `slr_Xft_FF` + `census_tracts_2025`\            | `analyze_tract_slr_intersections_v5_1.R` + `tracts_analysis_config_v3.yaml`   | `tract_Xft_intersections`    | X = 0–10                                                  | 11 tables         |
| \                                               |                                                                               |                              |                                                           |                   |
| (**2.4** Tract Intersections with SLR Polygons) |                                                                               |                              |                                                           |                   |
+-------------------------------------------------+-------------------------------------------------------------------------------+------------------------------+-----------------------------------------------------------+-------------------+
| FEMA .gdb + `tract_10ft_intersections`\         | `load_structures_to_db_v2.R` + `structure_analysis_config_v3.yaml`            | `usa_structures_FF`          | FF = 21 FIPS codes                                        | 21 tables         |
| \                                               |                                                                               |                              |                                                           |                   |
| (**2.5** Add Structures)                        |                                                                               |                              |                                                           |                   |
+-------------------------------------------------+-------------------------------------------------------------------------------+------------------------------+-----------------------------------------------------------+-------------------+
| `usa_structures_FF` + `slr_Xft_FF`\             | `analyze_structure_slr_flooding_v2_1.R` + `structure_analysis_config_v3.yaml` | `flooded_structures_FF_Xft`  | FF = 21 FIPS codes, X = 0–10                              | 231 tables        |
| \                                               |                                                                               |                              |                                                           |                   |
| (**2.6** Add Flooded Structures Tables)         |                                                                               |                              |                                                           |                   |
+-------------------------------------------------+-------------------------------------------------------------------------------+------------------------------+-----------------------------------------------------------+-------------------+
| `flooded_structures_FF_Xft`\                    | `export_flooded_structures_v2.R` + `tracts_analysis_config_v3.yaml`           | `flooded_structures_SS.gpkg` | SS = state abbreviation                                   | 21 files          |
| \                                               |                                                                               |                              |                                                           |                   |
| (**2.7** Export Flooded Structures)             |                                                                               |                              |                                                           |                   |
+-------------------------------------------------+-------------------------------------------------------------------------------+------------------------------+-----------------------------------------------------------+-------------------+
| `tract_Xft_intersections`\                      | `export_flooded_tracts_v1.R` + `tracts_analysis_config_v2.yaml` \`            | `flooded_tracts_SS.gpkg`     | SS = state abbreviation                                   | 21 files          |
| \                                               |                                                                               |                              |                                                           |                   |
| (**2.8** Export Flooded Tracts)                 |                                                                               |                              |                                                           |                   |
+-------------------------------------------------+-------------------------------------------------------------------------------+------------------------------+-----------------------------------------------------------+-------------------+

## 2.1 Add NOAA SLR Shapefiles to megaSLR

The [NOAA SLR Viewer website](https://coast.noaa.gov/slrdata/Ancillary/NOAA_OCM_SLR_MergedPolys_Shapefiles_0225/index.html) provides an interactive map of potential coastal flood areas and flood depth for sea level rise measured from 0 to 10 feet. In addition to the interactive website, NOAA provides sea level rise vector polygons in shapefile format (.shp) and other ancillary information that have been merged by region.

As explained in the metadata file [`NOAA OCM SLR 1to10ft metadata.xml`](https://coast.noaa.gov/slrdata/Ancillary/NOAA_OCM_SLR_1to10ft_metadata.xml):

*These data were created as part of the National Oceanic and Atmospheric Administration Office for Coastal Management's efforts to create an online mapping viewer depicting potential sea level rise and its associated impacts on the nation's coastal areas. The purpose of the mapping viewer is to provide coastal managers and scientists with a preliminary look at sea level rise (slr) and coastal flooding impacts. The viewer is a screening-level tool that uses nationally consistent data sets and analyses. Data and maps provided can be used at several scales to help gauge trends and prioritize actions for different scenarios. The Sea Level Rise and Coastal Flooding Impacts Viewer may be accessed at: <https://www.coast.noaa.gov/slr>*

*These data depict the potential inundation of coastal areas resulting from a projected 1 to 10 feet rise in sea level above current Mean Higher High Water (MHHW) conditions. The process used to produce the data can be described as a modified bathtub approach that attempts to account for both local/regional tidal variability as well as hydrological connectivity. The process uses two source datasets to derive the final inundation rasters and polygons and accompanying low-lying polygons for each iteration of sea level rise: the Digital Elevation Model (DEM) of the area and a tidal surface model that represents spatial tidal variability. The tidal model is created using the NOAA National Geodetic Survey's VDATUM datum transformation software (<http://vdatum.noaa.gov>) in conjunction with spatial interpolation/extrapolation methods and represents the MHHW tidal datum in orthometric values (North American Vertical Datum of 1988). The model used to produce these data does not account for erosion, subsidence, or any future changes in an area's hydrodynamics. It is simply a method to derive data in order to visualize the potential scale, not exact location, of inundation from sea level rise.*

*The purpose of these data is to show potential sea level rise inundation ranging from 1 to 10 feet above current Mean Higher High Water (MHHW) for the area. Although the water surface mapped represents a particular increase in sea level in feet above MHHW, the actual cell values in the raster dataset represent depth in meters.*

The process to derive the inundation rasters and polygons and low-lying area polygons is as follows:

1.  A tidal surface is generated using NOAA VDATUM tool and various spatial interpolation/extrapolation routines, dependent upon the area being mapped. The surface generated represents the spatial variability of offsets between MHHW, a tidal datum and NAVD88, an orthometric datum. Each iteration (1-10ft) of slr is added to this base surface and subsequently used for mapping.

2.  Using the DEM and the tidal surface (for each iteration of slr), raster calculations are made using ArcGIS Spatial Analyst Raster Calculation tool to generate multiple rasters, one 32-bit floating point raster representing depth of inundation and one 8-bit single value raster representing the extent of inundation.

3.  The hydrologic connectivity of the single value raster is evaluated using an 8-sided neighborhood rule in ArcGIS using the RegionGroup tool. The output raster from this process is then converted to a vector polygon feature class for further analysis. Using this 'base' feature class, a new feature class is created representing hydrologically connected areas. The 'base' feature class is also used to create a feature class representing unconnected 'low-lying' areas.

### SLR File-Naming Convention

As noted above, the NOAA vector polygon datasets (shapefiles) fall into one of two classes:

1.  **Sea level rise (SLR)** class of vector polygons that are hydrologically connected to one another and to the sea
2.  **Low-lying** class of vector polygons that are not hydrologically connected to the sea but are connected to one another and are below the shapefile's defined sea level

These two classes can be distinguished from each other by the website display file-naming convention and the url file path naming convention on this website: <https://coast.noaa.gov/slrdata/Ancillary/NOAA_OCM_SLR_MergedPolys_Shapefiles_0225/index.html>

Files that map to shapefiles and ancillary data for low-lying areas use the display file-naming convention as in this example:

```         
Florida merged low 3.0 ft.shp
```

An example of the url for downloading this file uses this convention:

```         
https://coast.noaa.gov/slrdata/Ancillary/NOAA_OCM_SLR_MergedPolys_Shapefiles_0225/Florida_merged_low_3_0ft.shp
```

The term "slr" (for sea level rise) is used in the naming convention for the corresponding web display name and download url:

```         
Florida merged slr 3.0 ft.shp (web page name)
https://coast.noaa.gov/slrdata/Ancillary/NOAA_OCM_SLR_MergedPolys_Shapefiles_0225/Florida_merged_slr_3_0ft.shp (download url)
```

### URL Naming Convention

The base url for downloading any or all of the SLR or low-lying vector polygon shapefiles:

```         
https://coast.noaa.gov/slrdata/Ancillary/NOAA_OCM_SLR_MergedPolys_Shapefiles_0225
```

The url filename is specific to each downloadable vector polygon shapefile and ancillary file. General pattern:

```         
<location_name>_merged_<polygon_class>_<sea_level_height>_0ft.<extension>
```

**EXCEPT** TX (Texas) AND LA (Louisiana) **HAVE A DIFFERENT NAMING CONVENTION**:

```         
<location_name>_merged_<polygon_class>_<sea_level_height>ft.<extension>
```

Note that for these 2 cases, the `_0` part is missing!

**`<location_name>`** may take these values: - Alaska, Atlantic, Caribbean, Florida, LA (Louisiana), MS_AL (Mississippi and Alabama), Pacific, TX (Texas), West

**`<polygon_class>`** may take these values: - `slr` (sea level rise as defined above) - `low` (low-lying areas as defined above)

**`<sea_level_height>`** may take these values: - 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 (integer values between 0 and 10 feet above Mean Higher High Water)

**`<extension>`** may take these values: - cpg, dbf, prj, sbn, sbx, shp, shp.xml, shx

### Downloading the NOAA SLR Shapefiles

The extension names correspond to all of the available files for a given location, polygon class, and sea level height. For example, to download all of the 3 foot sea level rise (slr) files associated with a shapefile vector polygon for Mississippi and Alabama (MS_AL), append the base url to these filenames:

```         
MS_AL_merged_slr_3_0_ft.cpg
MS_AL_merged_slr_3_0_ft.dbf
MS_AL_merged_slr_3_0_ft.prj
MS_AL_merged_slr_3_0_ft.sbn
MS_AL_merged_slr_3_0_ft.sbx
MS_AL_merged_slr_3_0_ft.shp
MS_AL_merged_slr_3_0_ft.shp.xml
MS_AL_merged_slr_3_0_ft.shx
```

### R Download Script

The R script to download the NOAA SLR vector polygon shapefiles and ancillary data is `NOAA_SLR_downloader_v3.R`, which is controlled by the yaml config file `NOAA_downloads_config_v1.yaml`.

**Note:** The only SLR coastal shapefiles for the conterminous U.S. were downloaded. Shapefiles over Hawaii (Pacific) and, Alaska and the Caribbean were not downloaded.

### Add SLR Tables to the megaSLR Database

SLR shapefiles were ingested into megaSLR using GDAL's `ogr2ogr` with reprojection to EPSG:5070. Here is the code block from `fn_build_table_raw()` that creates the command and that was used to ingest the SLR shapefiles into megaSLR tables:

``` r
  # Build ogr2ogr command
  cmd <- c(
    "-f", "PostgreSQL",
    pg_conn_str,
    shQuote(path),
    "-sql", shQuote(sql),
    "-nln", table_name,
    "-t_srs", "EPSG:5070",
    "-nlt", "MULTIPOLYGON",
    "-progress",
    "-lco", "GEOMETRY_NAME=geom",
    "-lco", "SPATIAL_INDEX=NONE",
    "-lco", "DIM=2",
    "-gt", "65536",
    "--config", "PG_USE_COPY", "YES"
  )
```

As noted above (Section 1.2) the ingest of the NOAA SLR shapefiles into megaSLR results in table per SLR level, per coastal region (using the 6 NOAA-defined SLR regions: atlantic, florida, la, ms_al, tx and west), from 0ft to 10ft. Thus there is a total of 66 SLR tables consisting of 11,446,885 SLR polygons, with a table naming convention of slr_Xft_region (with X = 0 to 10, and region = the 6 SLR regions defined earlier).

## 2.2 Download Census Tracts and Add to megaSLR Database

Census tracts are small, relatively permanent statistical subdivisions of a county, typically containing 1,200 to 8,000 people (ideally 4,000) with similar population characteristics, economic status, and living conditions. Established for the U.S. Census Bureau to analyze neighborhoods, they are designed to be stable over time for comparative studies, with boundaries often following visible features like roads or rivers. Each census tract is assigned an 11-digit identifier within a state/county framework (i.e., 2-digit state FIPS code + 3-digit county FIPS code + 6-digit tract code). Tract polygons are available as shapefiles from the Census web site:

<https://www2.census.gov/geo/tiger/TIGER2025/TRACT/>

The 2025 TIGER/Line Shapefiles provide the geographic boundaries for census tracts, but do not include population counts within the shapefile attributes themselves. The 2025 TIGER/Line files, released in September 2025, contain geographic identifiers (GEOIDs), ANSI codes, and feature geometry as of January 1, 2025.

### One Step Download and Add Census Tracts into the megaSLR Database

The following ocean coastal states were loaded into the Postgres database megaSLR on the TRIPPER3 workstation. In the table below the 2-digit numeric codes are the FIPS codes for each state.

```         
FIPS
CODE  State Name

01    Alabama
06    California
09    Connecticut  
10    Delaware
12    Florida
13    Georgia
22    Louisiana
23    Maine
24    Maryland
25    Massachusetts
28    Mississippi
33    New Hampshire
34    New Jersey
36    New York
37    North Carolina
41    Oregon
44    Rhode Island
45    South Carolina
48    Texas
51    Virginia
53    Washington
```

We used the following Linux bash commands to import the TIGER2025 tract shapefiles using wget and to unzip them. The PostGIS shp2pgsql command is used as a data loader that converts the ESRI shapefiles into SQL statements and its output is piped to the `psql` command, which then executes the generated SQL against in the megaSLR database. So the overall effect is to convert all the Census tracts shapefiles into a single Postgres database table: census_tracts_2025. The final step is to create a spatial index in the table to improve geographic search performance.

``` bash
BASE_URL="https://www2.census.gov/geo/tiger/TIGER2025/TRACT"
COASTAL_FIPS="01 02 06 09 10 12 13 15 22 23 24 25 28 33 34 36 37 41 44 45 48 51 53 72 78"

mkdir -p ~/Desktop/census_tracts_2025
cd ~/Desktop/census_tracts_2025

# Download each coastal state
for fips in $COASTAL_FIPS; do
  echo "Downloading FIPS ${fips}..."
  wget ${BASE_URL}/tl_2025_${fips}_tract.zip
  unzip -o tl_2025_${fips}_tract.zip
  rm tl_2025_${fips}_tract.zip
done

echo "All coastal states downloaded!"

# Load each state separately
# NOT loading AK, HI and Caribbean
for fips in 01 06 09 10 12 13 22 23 24 25 28 33 34 36 37 41 44 45 48 51 53; do
  echo "Loading FIPS ${fips}..."
  shp2pgsql -s 4269:5070 -a tl_2025_${fips}_tract.shp census_tracts_2025 | psql -U matt -d megaSLR
done

# Create the spatial index
psql -U matt -d megaSLR -c "CREATE INDEX census_tracts_2025_geom_idx ON census_tracts_2025 USING GIST (geom);"
```

The tracts shapefiles are replicated into a single table in megaSLR named `census_tracts_2025` with the following attributes:

```         
Attributes from the TIGER shapefiles
statefp
countyfp
tractce
geoid
geoidfq
name
namelsad
mtfcc
funcstat
aland
awater
intptlat
intptlon

Attributes added on ingest into megaSLR
gid
geom
```

STATEFP: The two-digit Federal Information Processing Series (FIPS) code for the state.

COUNTYFP: The three-digit FIPS code for the county.

GEOID: A unique, fully qualified 11-digit geographic identifier (State + County + Tract) used to join the shapefile with demographic and economic data.

GEOIDFQ: (Fully Qualified Geographic Identifier) is a 16-character alphanumeric code used to join spatial boundary data to tabular data from data.census.gov

TRACTCE: The six-digit census tract code.

NAME: The decimal-formatted name of the census tract (e.g., "101.01").

NAMELSAD: The full name of the tract with the legal/statistical area description (e.g., "Census Tract 101.01").

MTFCC: The MAF/TIGER Feature Class Code (typically G5020 for census tracts).

FUNCSTAT: a one-character code defining the functional/legal status of geographic entities this code indicates whether the tract is a statistical entity (typically 'S') or a legal entity (if applicable).

ALAND & AWATER: Area measurements of land and water in square meters.

INTPTLAT & INTPTLON: The latitude and longitude of the internal point (centroid) of the tract.

GID: an auto-increment primary key added by `shp2pgsql`

GEOM: the PostGIS geometry column created during ingest (reprojected to EPSG:5070)

Tract polygons provide a fairly fine-grained geographic resolution of neighborhoods. As such they are useful targets for studying inundation as a result of sea level rise. Tract population and other features can be found by using the tract GEOID as a key to extract such information from other databases.

## 2.3 Subdivide SLR Polygons

As mentioned above (Section 1.2) SLR polygons were subdivided to improve performance using the script `create_state_slr_subdivided_v5.R` and config file `structure_analysis_config_v3.yaml`. The script creates 231 per-state SLR tables (`slr_Xft_FF`) by clipping regional SLR polygons to each state's census tract extent and subdividing complex polygons using PostGIS's `ST_Subdivide` function.

The script processes each state × scenario combination as follows:

1.  **Bounding box clip:** For each state, the spatial extent of that state's census tracts (from `census_tracts_2025`, filtered by `statefp`) is computed with a small buffer defined in the yaml config file. Typically the buffer is set to 1000 m. Only regional SLR polygons whose bounding boxes overlap this extent are selected. This eliminates SLR polygons from other states within the same NOAA region.

2.  **Geometry repair:** `ST_MakeValid` is applied to each polygon before subdivision, controlled by the YAML parameter `use_make_valid` (default: `TRUE`). This prevents topological errors — particularly self-intersections in Louisiana's complex coastal geometries — from causing `ST_Subdivide` to fail. Because `ST_MakeValid` is not parallel-safe in PostGIS, `max_parallel_workers_per_gather` is set to 0 during this step.

3.  **Subdivision:** `ST_Subdivide` decomposes each polygon into sub-polygons with at most 256 vertices (configurable via `max_vertices`). This is a lossless operation — the union of subdivided pieces exactly equals the original polygon. The subdivision dramatically improves downstream `ST_Intersects` performance by producing compact sub-polygons with tight bounding boxes, enabling the GiST spatial index to efficiently filter candidates.

4.  **Spatial indexing:** A GiST index is created on each output table, followed by `ANALYZE` to update PostgreSQL query planner statistics.

The bounding box is derived from census tracts rather than from FEMA structure tables. This ensures complete SLR polygon coverage for the tract intersection analysis and eliminates a circular dependency in the pipeline: structure loading depends on tract intersections (for prefiltering), which depend on subdivided SLR tables. Using census tracts as the bounding box source makes the pipeline strictly linear. The slightly larger inland extent of census tracts does not capture additional SLR polygons since NOAA flood polygons exist only along the coast.

Key YAML parameters in the `subdivide` section of `structure_analysis_config_v3.yaml`:

``` r
subdivide:
  max_vertices: 256                   # Max vertices per subdivided polygon
  buffer_meters: 1000                 # Buffer around census tract extent
  overwrite_existing: true            # Overwrite existing tables
  tracts_table: "census_tracts_2025"  # Bounding box source table
use_make_valid: true                  # Apply ST_MakeValid before ST_Subdivide
```

PostgreSQL session settings used during subdivision (hard coded in the R script):

``` r
  dbExecute(con, "SET max_parallel_workers_per_gather = 0")
  dbExecute(con, "SET work_mem = '8GB'")
  dbExecute(con, "SET effective_io_concurrency = 200")
```

### Prerequisites

-   **Region-partitioned SLR tables** must exist in `megaSLR`: `slr_Xft_atlantic`, `slr_Xft_florida`, `slr_Xft_la`, `slr_Xft_ms_al`, `slr_Xft_tx`, `slr_Xft_west` for X = 0 through 10.

-   **Census tracts table** (`census_tracts_2025`) must exist with a GIST spatial index on the `geom` column and a `statefp` column. The script checks for the GIST index at startup and warns if it's missing.

### Inputs

-   **Config file** (YAML): Specifies database connection, list of states, SLR scenarios, subdivide parameters (`max_vertices`, `buffer_meters`), and processing flags. The master config is `structure_analysis_config_v3.yaml`.

-   **Region SLR tables**: Source polygons partitioned by coastal region. The state-to-region mapping is hardcoded in the script (e.g., Delaware → `atlantic`, Texas → `tx`, Florida → `florida`).

### Outputs

Per-state subdivided SLR tables named `slr_{scenario}_{state_fips}`:

-   Example: `slr_0ft_10` (Delaware, 0ft scenario)
-   231 tables total (11 scenarios × 21 states)
-   Each table has a single `geom` column with a GIST spatial index
-   `ANALYZE` is run on each table after index creation

### Logging and notifications

-   Log files are written to `paths.log_dir` with timestamps and batch labels in the filename.
-   ntfy.sh push notifications are sent at state start/finish and at overall completion to the `matt-tripper3-jobs` topic.

### Single-state command-line override

v5 accepts an optional `--state XX` argument that overrides the config file's `states` list with a single FIPS code:

``` bash
Rscript create_state_slr_subdivided_v5.R config.yaml --state 13
```

This processes only Georgia (FIPS 13), using all other settings from the config file. The batch label is automatically set to the state abbreviation (e.g., "GA"), so log files and ntfy notifications are tagged per-state. The `--state` flag is validated against the hardcoded `STATE_TO_REGION` mapping and will error on an unknown FIPS code.

This feature enables the `launch_subdivide_parallel.R` launcher script to spawn one bash screen session per state from a single master YAML config, with no need for per-state YAML files.

If `--state` is omitted, the script processes all states listed in the config, exactly as before.

## 2.4 Census Tract Intersections with SLR Polygons

The script `analyze_tract_slr_intersections_v5_1.R` and its companion `tracts_analysis_config_v2.yaml` compute area-based intersections between Census tracts and SLR flood polygons. The Census tracts source is the megaSLR table `census_tracts_2025`. The pre-subdivided, state-partitioned SLR flood polygons defined in Section 2.3 are the second source for the analyze tract script. The output megaSLR tables have the naming convention tract_Xft_intersections with X=0 to 10. Each table has one row per tract, and includes the following fields:

GEOID, NAMELSAD, STATEFP: As defined in Section 2.2

TRACT_AREA_HA: Tract area in hectare

SLR_AREA_HA: Tract area inundated by SLR in hectare

### Method

For each state and SLR scenario, the script `analyze_tract_slr_intersections_v5_1.R` executes a single SQL query that joins all census tracts in the state (filtered by `statefp`) against the corresponding subdivided SLR table (`slr_{scenario}_{fips}`). For each tract that intersects at least one SLR polygon, the query computes the total area of the tract in hectares and the area of the tract inundated by SLR in hectares. The inundated area is computed as `ST_Area(ST_Intersection(ST_Union(s.geom), t.geom))`. The `ST_Union` step is necessary because NOAA source polygons can overlap within a scenario — for example, in the Florida Keys, a single tract may intersect nearly 1,000 subdivided SLR polygons, some derived from overlapping source features. Without the union, summing individual intersection areas would overcount the area of inundation.

Results for each state are inserted into a per-state output table (e.g., `tract_0ft_intersections_12` for Florida). After all states complete, per-state tables are combined into the final `tract_Xft_intersections` tables.

### Parallel execution

The `analyze_tract_slr_intersections_v5_1.R` script supports a `--state XX` command-line override for parallel execution. In `--state` mode, each instance writes to its own per-state output table, avoiding write contention between parallel instances.

The launcher script `launch_tract_intersections_parallel_v1.R` automates parallel execution by spawning one detached `screen` session per state:

``` bash
# Launch all states from config:
Rscript launch_tract_intersections_parallel_v1.R config.yaml

# Launch a specific subset of states:
Rscript launch_tract_intersections_parallel_v1.R config.yaml 10 12 13 22 37 48 51
```

When state FIPS codes are provided on the command line, the launcher processes only those states; otherwise it uses the full state list from the YAML config. The launcher performs a pre-launch memory check — estimating \~6 GB per concurrent session (3 PostgreSQL processes × 2 GB `work_mem`) plus 16 GB reserved for the OS and PostgreSQL shared buffers — and aborts with a log message if available memory is insufficient. Screen sessions close automatically when their R script completes, so finished sessions do not inflate the resource check for subsequent launches.

Each state gets a screen session named `tract_XX` (e.g., `tract_12` for Florida):

``` bash
screen -ls             # list all sessions
screen -r tract_12     # attach to FL session
Ctrl+A, d             # detach
```

#### Two-wave execution strategy

Due to concerns about memory limits, the launcher script `launch_tract_intersections_parallel_v1.R` was run in two waves to prioritize the faster states:

**Wave 1** — 14 states with small or simple coastal geometries, launched simultaneously:

```         
01 06 09 23 24 25 28 33 34 36 41 44 45 53
AL CA CT ME MD MA MS NH NJ NY OR RI SC WA
```

**Wave 2** — 7 states with complex coastlines and higher SLR polygon density, launched after wave 1 completes or sufficient memory becomes available:

```         
10 12 13 22 37 48 51
DE FL GA LA NC TX VA
```

The `ST_Union` computation is the primary bottleneck in wave 2, particularly for tracts with dense SLR polygon coverage (e.g., Florida Keys, Louisiana coast).

#### Louisiana: bespoke per-tract multi-pass processing

Louisiana required special handling due to the extreme complexity of its deltaic coastal geometry, a complexity that resulted in unbearably long run times. The standard per-state query — which joins all 1,388 Louisiana tracts against the subdivided SLR table in a single SQL statement — could not complete within practical time limits. The `ST_Union` aggregation on densely overlapping SLR polygons in coastal parishes caused individual tract computations to exceed several hours, and GEOS `InterruptedException` errors terminated queries that hit the PostgreSQL statement timeout. Note that `statement_timeout` is not precise for these queries: GEOS only checks for interrupt signals at internal checkpoints during polygon union operations, so actual runtimes can exceed the configured timeout by several minutes.

The script `analyze_tract_slr_louisiana_2_2.R` addresses this with per-tract processing and a multi-pass timeout strategy. The script accepts the following command-line options:

``` bash
Rscript analyze_tract_slr_louisiana_2_2.R <config.yaml> --pass N
    [--scenarios 0ft,1ft,...] [--skip GEOID1,GEOID2,...] [--validate]
```

-   `--pass N`: Selects the processing pass (1 or 2). Pass 1 uses a 30-second timeout. Pass 2 uses no timeout and retries only the tracts that failed Pass 1 (identified automatically by parsing the most recent Pass 1 log file).
-   `--scenarios`: Comma-separated list of SLR scenarios to process, overriding the full list in the YAML config. Useful for running a subset of scenarios or parallelizing Pass 2 across multiple `screen` sessions.
-   `--skip`: Comma-separated list of tract GEOIDs to exclude from processing. Skipped tracts are logged. This allows specific tracts to be handled separately (e.g., in a dedicated `psql` session with different timeout and resource settings).
-   `--validate`: Runs in validation mode instead of processing. Compares `ST_Union` and `SUM(ST_Area(ST_Intersection(...)))` results on the 25 largest completed tracts per scenario, reporting the percentage difference per tract.

The script uses `CREATE TABLE IF NOT EXISTS` and skips tracts already present in the output table, making it safe to re-run across passes without data loss or duplication.

**Execution as performed.** Louisiana was processed in two passes plus targeted `psql` sessions for the most computationally expensive tracts:

**Pass 1** (30-second timeout): Processed all 1,388 tracts individually across all 11 SLR scenarios using the `ST_Union` method. The number of timeouts varied by scenario, reflecting the complexity of SLR geometry at each level: 50 tracts timed out at 0ft (the most complex geometry), decreasing steadily to 1 tract at 10ft — 118 tract×scenario timeouts total. Many Pass 1 timeouts were caused by PostgreSQL buffer cache pressure rather than inherent geometric complexity: when 1,388 tracts are processed sequentially, SLR table data is evicted from shared buffers between queries, and the same tracts often complete quickly in Pass 2 with fewer tracts and warm cache.

``` bash
screen -S la_pass1
Rscript analyze_tract_slr_louisiana_2_2.R config.yaml --pass 1
```

**Pass 2** (10-minute timeout, then no timeout; set by hard-coding `timeout_secs` on Line 381): Retried only the tracts that failed Pass 1. An initial Pass 2 run with a 10-minute timeout (600 sec) resolved all timeouts at SLR scenarios 3ft through 10ft. The remaining timeouts — 15 unique tracts at 0ft, 5 tracts at 1ft, and 2 tracts at 2ft (=22 tract×SLR_scenario combinations) — were concentrated in parishes with the most complex coastal geometry (Assumption, Iberia, Lafourche, Plaquemines, St. Bernard, St. James, St. John the Baptist, St. Martin, St. Mary, Terrebonne, Vermilion).

A final Pass 2 run with no timeout (`timeout_secs = 0` on Line 381) was used to complete the remaining holdout tracts. One tract — 22113951100 in Vermilion Parish — was separated out using `--skip` and processed in three dedicated `psql` sessions (one per remaining SLR scenario: 0ft, 1ft, 2ft), run simultaneously in separate `screen` sessions while the R script handled the other 14 holdout tracts in parallel:

``` bash
# R script: 14 holdout tracts, skip the worst one
screen -S la_pass2
Rscript analyze_tract_slr_louisiana_2_2.R config.yaml --pass 2 --skip 22113951100

# Dedicated psql sessions for tract 22113951100 (one per scenario)
screen -S la_22113951100_0ft
psql megaSLR
SET max_parallel_workers_per_gather = 4;
SET work_mem = '4GB';
SET statement_timeout = 0;
INSERT INTO tract_0ft_intersections_22 (geoid, namelsad, statefp, tract_area_ha, slr_area_ha)
SELECT t.geoid, t.namelsad, t.statefp,
       ST_Area(t.geom)/10000, ST_Area(ST_Intersection(ST_Union(s.geom), t.geom))/10000
FROM census_tracts_2025 t
JOIN slr_0ft_22 s ON ST_Intersects(s.geom, t.geom)
WHERE t.geoid = '22113951100'
GROUP BY t.geoid, t.namelsad, t.statefp, t.geom;
```

Per-tract runtimes for the holdout tracts ranged from 10 minutes to nearly 9 hours for a given SLR_level. The two most expensive tracts were 22109001400 and 22109001502 in Vermilion Parish (8h 52m and 5h 20m respectively at 0ft), followed by 22113951100 also in Vermilion Parish (4h 4m at 0ft and 1ft). All Louisiana tracts were ultimately completed using the exact `ST_Union` method.

**SUM vs. ST_Union validation.** Testing the SUM approximation (`SUM(ST_Area(ST_Intersection(...)))`, which skips `ST_Union`) revealed that the overcount from overlapping NOAA source polygons varies substantially by location. Measured differences ranged from 0.1% (tract 22023970102 in Assumption Parish) to 0.24% (tract 22109001400 in Vermilion Parish) to 27% (tract 22047952701 in Iberville Parish, in the Atchafalaya Basin). The large variance makes the SUM method unsuitable as a general-purpose fallback without per-tract validation, reinforcing the decision to complete all tracts with `ST_Union`.

**Monotonic row growth with SLR level (and an exception)**. There is steady growth in the number of inundated tracts with increasing SLR height over all coastal states: 6,746 tracts at 0ft, 6,945 at 1ft, 7,179 at 2ft, 7,404 at 3ft, 7,747 at 4ft, 8,206 at 5ft, 8,734 at 6ft, 9,232 at 7ft, 9,631 at 8ft, 9,957 at 9ft, 10,255 at 10ft. This monotonic behavior is expected as SLR inundates progressively more tracts at increasing SLR heights; any deviation may indicate errors in the pipeline methodology or the source data.

Note that 7 Maine tracts appear in the 9ft tract intersection table `tract_9ft_intersections` but not the 10ft table `tract_10ft_intersections`, producing minor non-monotonic behavior in tract counts for that state. All 7 tracts have flooded areas below 0.008% of tract area — orders of magnitude below the meaningful resolution of the source data. [NOAA's documentation](https://coast.noaa.gov/data/digitalcoast/pdf/slr-faq.pdf) notes that the underlying lidar DEMs have a vertical accuracy of ≤10 cm RMSE and recommends rounding results to the nearest one-foot; sub-foot polygon boundary differences between the 9ft and 10ft scenarios are therefore within the expected uncertainty of the product. Most importantly, *none of the flooded areas within these 7 tracts contain any flooded structures*. While the 7 tract-scenario intersections are retained in the tract tables as, they are attributable to sub-pixel geometry precision effects at polygon boundaries rather than genuine inundation differences between scenarios.

These are the 7 Maine tracts identified by `geoid` that are found in the table `tract_9ft_intersections` but not in the table `tract_10ft_intersections` along with the 9 ft flooded area in hectare `slr_area_ha`, the total tract area `tract_area_ha`, the percent of the tract flooded at this level `pct_flooded`, and the number of structures found within the SLR flooded area for each tract `flooded_structures`. Note that these 7 tracts are also included in the GeoPackage file `flooded_tracts_ME.gpkg` (see Section 2.8).

```         
    geoid    | slr_area_ha | tract_area_ha | pct_flooded | flooded_structures
-------------+-------------+---------------+-------------+-------------------
 23019021500 |      0.0356 |      39811.81 |    0.000089 |        0
 23019020500 |      0.0451 |      21033.32 |    0.000215 |        0
 23029955100 |     15.3188 |     384335.99 |    0.003986 |        0
 23019007100 |      0.0617 |       1513.17 |    0.004078 |        0
 23019007200 |      0.5217 |       9685.11 |    0.005387 |        0
 23019940000 |      0.2992 |       5013.01 |    0.005968 |        0
 23019008001 |      1.9406 |      24980.61 |    0.007769 |        0
```

### PostgreSQL tuning

Session settings for the main `analyze_tract_slr_intersections_v5_1.R` script are tuned for concurrent execution across multiple states:

``` r
  dbExecute(con, "SET max_parallel_workers_per_gather = 2")
  dbExecute(con, "SET work_mem = '2GB'")
  dbExecute(con, "SET effective_io_concurrency = 200")
```

With 32 cores, 64 threads, and 128 GB RAM, up to 14 concurrent state instances can run within these settings. Each instance uses at most 3 PostgreSQL processes (1 leader + 2 parallel workers), each allocated 2 GB `work_mem`.

The Louisiana per-tract script uses higher settings when running as a single instance:

``` r
  dbExecute(con, "SET max_parallel_workers_per_gather = 8")
  dbExecute(con, "SET work_mem = '8GB'")
  dbExecute(con, "SET effective_io_concurrency = 200")
```

Note that `max_parallel_workers_per_gather` only benefits the `ST_Intersects` join phase; the `ST_Union` aggregation in GEOS is single-threaded.

### Combining per-state output tables

After all parallel `--state` instances complete, per-state tables are combined into the final output tables. For each scenario:

``` sql
CREATE TABLE tract_0ft_intersections AS
  SELECT * FROM tract_0ft_intersections_01
  UNION ALL
  SELECT * FROM tract_0ft_intersections_06
  UNION ALL
  ...
  SELECT * FROM tract_0ft_intersections_53;

CREATE INDEX tract_0ft_intersections_geoid_idx ON tract_0ft_intersections(geoid);
ANALYZE tract_0ft_intersections;
```

This is repeated for all 11 scenarios (0ft through 10ft). Per-state tables are dropped after successful combination.

### Prerequisites

-   **Subdivided SLR tables**: `slr_{scenario}_{fips}` for all 21 states × 11 scenarios (231 tables, produced by Section 2.3).
-   **Census tracts table** (`census_tracts_2025`) with GiST spatial index on `geom` and `statefp` column.

### Outputs

One table per SLR scenario:

-   Naming: `tract_{scenario}_intersections` (e.g., `tract_0ft_intersections`, `tract_3ft_intersections`)
-   11 tables total (0ft through 10ft)
-   Columns: `geoid`, `namelsad`, `statefp`, `tract_area_ha`, `slr_area_ha`
-   Index on `geoid` for downstream joins

### Scripts

-   `analyze_tract_slr_intersections_v5_1.R` — main analysis script (per-state processing, `--state` override)
-   `launch_tract_intersections_parallel_v1.R` — launcher for parallel execution (resource check, screen sessions)
-   `analyze_tract_slr_louisiana_2_2.R` — Louisiana per-tract multi-pass analysis (`--pass`, `--scenarios`, `--skip`, `--validate`)
-   `combine_tract_intersections.sql` — combines per-state output tables into final `tract_Xft_intersections` tables with index creation, then verifies row counts

### Logging and notifications

-   Log files are written to `paths.log_dir` with timestamps and batch labels.
-   Louisiana per-tract logs include per-pass filenames (e.g., `tract_slr_LA_pass1_2026-04-10_20-43-51.txt`) with per-tract timing, slow-tract warnings (\>60s), and failure/timeout details. Pass 2 logs parse the Pass 1 log to identify retry targets.
-   ntfy.sh push notifications are sent per-scenario and at overall completion.

## 2.5 Add Structures Filtered by SLR Tracts to the Database

The FEMA [USA Structures](https://gis-fema.hub.arcgis.com/pages/usa-structures) dataset is described as the "nation's first comprehensive inventory of all structures larger than 450 square feet for use in Flood Insurance, Mitigation, Emergency Preparedness and Response." The dataset is distributed as per-state ESRI file geodatabases (.gdb), with each structure record containing building footprint geometry and a `CENSUSCODE` field that stores the 11-character Census tract GEOID.

The main challenge for structure-level SLR analysis is scale: state-level .gdb files contain millions of structure polygons — Texas alone has over 12 million FEMA structures — the vast majority of which are far removed from coastal flooding. Loading and spatially indexing all structures nationally would be prohibitively expensive and wasteful. Instead, the pipeline uses tract-based pre-filtering to retain only structures in Census tracts known to intersect SLR inundation, reducing the dataset by 84–89% before any spatial operations are performed.

### Pre-filter source: `tract_10ft_intersections`

The pre-filter table is `tract_10ft_intersections` (produced in Section 2.4), which contains all Census tracts that intersect any SLR polygon at the maximum 10ft scenario. Because SLR inundation is monotonically inclusive — any area flooded at N feet is also flooded at N+1 feet — the 10ft tract set is a guaranteed superset of all lower scenarios. Loading structures once against the 10ft envelope means the `usa_structures_FF` tables never need to be rebuilt when running analyses at different SLR levels.

### Pre-filter mechanism: attribute JOIN on `CENSUSCODE`

The FEMA `CENSUSCODE` field stores the same 11-character tract GEOID format as the `geoid` column in the intersection tables. This enables a simple equality JOIN for pre-filtering — no spatial join is needed. An earlier version of the load script (v1) ran an expensive `ST_Intersects` spatial join against `census_tracts_2025` to assign each structure to a tract; this was eliminated once the `CENSUSCODE` field was identified.

### Pipeline per state

The script `load_structures_to_db_v2.R` processes each state through four stages:

1.  **ogr2ogr bulk load:** The full state .gdb is streamed directly into a PostgreSQL temp table (`_temp_structures_FF`) via `ogr2ogr`, with reprojection from the source CRS (EPSG:4326) to EPSG:5070 on the fly. This replaces the v1 approach of using `sf::st_read()` to load entire states into R memory, which consumed 30–50 GB for large states like Florida. Using `ogr2ogr` keeps R's memory footprint minimal.

2.  **SQL filter:** A `CREATE TABLE ... AS SELECT ... JOIN` against `tract_10ft_intersections` retains only structures whose `CENSUSCODE` matches a tract in the pre-filter table. The filtered table includes all original FEMA columns plus a `tract_geoid` column populated from the JOIN. The script auto-detects the case of the `CENSUSCODE` column (ogr2ogr lowercases it; `sf::st_write` preserves case) and handles either.

3.  **Spatial index:** A GIST index is created on the `geom` column of the filtered table.

4.  **Cleanup:** The temp table is dropped.

### Connecticut FIPS code exception

Connecticut reorganized its county-level FIPS codes in 2022, replacing county codes (09001, 09003, ...) with planning region codes (09110, 09120, ...). The Census 2025 tracts and `tract_10ft_intersections` use the new planning region codes, but the FEMA .gdb still uses the old Connecticut county codes in `CENSUSCODE`. A full 11-digit GEOID equality JOIN produces zero matches for Connecticut yielding an empty table `usa_structures_09`.

The companion script `load_structures_ct_fix.R` handles this by using a relaxed JOIN that matches on state prefix (2 digits) + tract suffix (6 digits), skipping the 3-digit county/planning region code:

``` sql
JOIN tract_10ft_intersections t
  ON LEFT(s.censuscode, 2) = LEFT(t.geoid, 2)
  AND RIGHT(s.censuscode, 6) = RIGHT(t.geoid, 6)
WHERE LEFT(s.censuscode, 2) = '09'
```

The `tract_geoid` column in the resulting `usa_structures_09` table is populated from `tract_10ft_intersections` and therefore uses the new planning region GEOIDs, which is correct for downstream joins against the tract intersection tables. This script is run after the main `load_structures_to_db_v2.R` completes, overwriting the empty Connecticut table `usa_structures_09`.

### Filtering effectiveness

Tract-level filtering is far more effective than county-level filtering. Testing with four Miami-area counties (622,242 structures) demonstrated the difference:

```         
Filter                    Structures kept   Reduction
No filter (4 counties)    622,242           —
10ft tract filter         ~97,700           ~84%
0ft tract filter          ~69,500           ~89%
```

County-level filtering would have eliminated zero structures in this test, since all four counties are coastal. The tract filter does the real work.

### Configuration

The script is controlled by `structure_analysis_config_v3.yaml`. Key parameters:

``` yaml
prefilter:
  tract_table: "public.tract_10ft_intersections"

load_structures:
  skip_if_exists: false         # Skip state if usa_structures_FF already exists
  overwrite_existing: true      # Drop and rebuild existing tables
  create_spatial_index: true    # Build GIST index after filtering
```

Overwrite behavior is controlled entirely by YAML flags — no interactive prompts that would break in a `screen` session. If `overwrite_existing` is missing or garbled, it defaults to `TRUE`.

### Running

``` bash
# Main load — all 21 states (CT will produce 0 structures)
screen -S structures
cd ~/claude_projects/slr_analysis/structures/code/01_prepare
Rscript load_structures_to_db_v2.R \
  YAML_config_files/structure_analysis_config_v3.yaml \
  2>&1 | tee ~/Science/Nora_SLR/SLR_log_files/structure_loading_console.log

# CT fix — reload with relaxed FIPS JOIN
Rscript load_structures_ct_fix.R \
  YAML_config_files/structure_analysis_config_v3.yaml \
  2>&1 | tee ~/Science/Nora_SLR/SLR_log_files/ct_fix_console.log
```

### Prerequisites

-   **Census tract intersection tables** must exist: specifically `tract_10ft_intersections` (or whatever table is specified by `prefilter.tract_table` in the YAML config). The script verifies the table exists and is non-empty at startup.
-   **FEMA .gdb files** must be present in the directory specified by `paths.structures_base_dir`, following the naming convention `{SS}_Structures.gdb` (e.g., `FL_Structures.gdb`).

### Outputs

Per-state structure tables in the megaSLR `public` schema:

-   Naming convention: `usa_structures_FF` where FF is the 2-digit state FIPS code (e.g., `usa_structures_12` for Florida)
-   21 tables total
-   Columns: all original [FEMA attributes](https://gis-fema.hub.arcgis.com/datasets/fedmaps::usa-structures/about) plus `tract_geoid` from the pre-filter JOIN

```         
 usa_structures_FF table columns:
 
 build_id            outbldg             latitude       
 occ_cls             height              image_name     
 prim_occ            sqmeters            image_date     
 prop_addr           sqfeet              val_method     
 prop_city           h_adj_elev          remarks        
 shape_length        l_adj_elev          uuid           
 sec_occ             fips                b_code         
 shape_area          censuscode          pop_ci95_upper 
 objectid            prod_date           pop_median     
 prop_st             source              pop_ci95_lower 
 prop_zip            usng                geom           
 prop_cnty           longitude           tract_geoid    
```

-   GIST spatial index on `geom`
-   Total: 14,101,005 structure polygons across all 21 states (72,913,047 loaded from .gdb; 18.9% retained after filtering, plus 335,157 from the CT fix)

```         
State  Loaded       Filtered    Kept %    State  Loaded       Filtered    Kept %
AL     2,771,976      203,863    7.4%     NH       561,848      45,065    8.0%
CA    10,931,401    1,005,204    9.2%     NJ     2,581,846     852,463   33.0%
CT     1,182,437      335,157   28.3%     NY     5,015,922     717,418   14.3%
DE       383,131      234,385   61.2%     NC     5,299,008     671,634   12.7%
FL     7,792,420    3,989,084   51.2%     OR     1,699,221     187,887   11.1%
GA     4,342,910      233,933    5.4%     RI       367,168     160,575   43.7%
LA     2,439,867      989,769   40.6%     SC     2,604,892     550,598   21.1%
ME       764,623      290,063   37.9%     TX    12,311,384     828,427    6.7%
MD     1,737,639      532,451   30.6%     VA     3,467,120     833,505   24.0%
MA     2,091,488      604,952   28.9%     WA     2,829,967     666,774   23.6%
MS     1,736,779      167,798    9.7%
```

### Logging and notifications

-   Log files are written to `paths.log_dir` with timestamps in the filename.
-   ntfy.sh push notifications are sent per-state with structure counts, filter percentages, and timing (e.g., "FL structures loaded: 1,234,567 of 7,890,123 kept (15.6%) in 12m 30s").
-   Overall summary notification is sent at completion.

### Scripts

-   `load_structures_to_db_v2.R` — main loading script (ogr2ogr → SQL filter → GIST index)
-   `load_structures_ct_fix.R` — Connecticut reload with relaxed FIPS JOIN
-   `structure_analysis_config_v3.yaml` — shared configuration file

## 2.6 Generate Flooded Structures Tables

The script `analyze_structure_slr_flooding_v2_1.R` and its companion config file `structure_analysis_config_v3.yaml` determine which pre-filtered structures (from Section 2.5) intersect SLR inundation polygons (from Section 2.3) at each of the 11 SLR scenarios. The script generates 231 output tables with the naming convention `flooded_structures_FF_Xft`, where FF = 2-digit state FIPS code and X = SLR level (0 to 10, in feet).

Each output row contains the full FEMA record for a structure that touches SLR inundation at the given scenario, including the building footprint geometry, the original FEMA attributes, and the `tract_geoid` column carried forward from Section 2.5. All metadata is preserved in every scenario table (not just 0ft), so each table is self-contained and usable without downstream joins — particularly important for collaborators working in ArcGIS who may load individual scenario layers independently.

### Method

For each (state, scenario) combination, the script executes a single SQL query that uses an EXISTS subquery with `ST_Intersects` against the per-state subdivided SLR table:

``` sql
CREATE TABLE flooded_structures_{FF}_{Xft} AS
SELECT s.*
FROM usa_structures_{FF} s
WHERE EXISTS (
  SELECT 1 FROM slr_{Xft}_{FF} slr
  WHERE ST_Intersects(s.geom, slr.geom)
);
```

This is a boolean test — does this structure touch any SLR polygon at this scenario? — not an area calculation. No `ST_Union` or `ST_Intersection` is needed, which makes the structure flooding analysis dramatically faster than the tract intersection analysis in Section 2.4. Both the structures table (from Section 2.5) and the subdivided SLR table (from Section 2.3) have GIST spatial indexes with tight bounding boxes, so the EXISTS subquery short-circuits as soon as any intersecting SLR polygon is found.

### PostgreSQL tuning

The script uses aggressive parallel query settings, since it runs as a single instance against one state × scenario at a time:

``` r
  dbExecute(con, "SET max_parallel_workers_per_gather = 8")
  dbExecute(con, "SET work_mem = '1GB'")
  dbExecute(con, "SET effective_io_concurrency = 200")
```

`max_parallel_workers_per_gather = 8` allows PostgreSQL to parallelize each `ST_Intersects` join across 8 cores, which is effective because the join is CPU-bound and embarrassingly parallel over the structures table.

### Performance

The full 231-table run completes in approximately 20 minutes on TRIPPER3. Per-query runtimes scale with state size and SLR scenario:

-   Small states at low SLR (e.g., AL at 0ft, 204k structures): \~1 second
-   Mid-sized states (e.g., GA, MS, OR): 1–3 seconds per scenario
-   Large states at high SLR (e.g., VA at 10ft, 834k structures): \~11 seconds
-   Florida at 10ft (3.99M structures): the longest single query in the run

This contrasts sharply with the tract intersection analysis in Section 2.4, where `ST_Union` on overlapping NOAA polygons caused individual Louisiana tracts to run for hours. The structure flooding analysis avoids `ST_Union` entirely because it does not compute areas.

### Prerequisites

-   **Pre-filtered structure tables** `usa_structures_FF` for all 21 states (Section 2.5).
-   **Subdivided SLR tables** `slr_{Xft}_{FF}` for all 21 states × 11 scenarios (231 tables, Section 2.3).
-   Both table sets must have GIST spatial indexes on their `geom` columns.

### Outputs

Per-state, per-scenario flooded structures tables:

-   Naming: `flooded_structures_FF_Xft` where FF = 2-digit state FIPS code, X = SLR level (0 to 10)
-   231 tables total (21 states × 11 scenarios)
-   Columns: all original FEMA fields plus `tract_geoid` and `geom`
-   Monotonic row counts: for any given state, the number of flooded structures increases with SLR level (e.g., AL: 4,602 at 0ft → 30,345 at 10ft; VA: 9,091 at 0ft → 218,405 at 10ft)
-   Total across all state × scenario combinations: **17,576,504 flooded structure rows** out of 155,111,055 structure × scenario combinations checked (11.33%)

### Running

``` bash
screen -S flooding
cd ~/claude_projects/slr_analysis/structures/code/02_analyze
Rscript analyze_structure_slr_flooding_v2_1.R \
  ~/claude_projects/slr_analysis/structures/YAML_config_files/structure_analysis_config_v3.yaml \
  2>&1 | tee ~/Science/Nora_SLR/SLR_log_files/flooding_analysis_console.log
# Ctrl+A, D to detach
```

### Logging and notifications

-   Log files are written to `paths.log_dir` with timestamps in the filename.
-   Each (state, scenario) combination logs the structure count, flooded count, percentage, and elapsed time.
-   ntfy.sh push notifications fire per state and at overall completion to the `matt-tripper3-jobs` topic.

### Scripts

-   `analyze_structure_slr_flooding_v2_1.R` — main analysis script
-   `structure_analysis_config_v3.yaml` — shared configuration file

------------------------------------------------------------------------

## 2.7 Export Flooded Structures as GeoPackage Files

The script `export_flooded_structures_v2.R` reads each `flooded_structures_FF_Xft` table from megaSLR and writes it to a GeoPackage (.gpkg) file, producing one GPKG per state with all 11 SLR scenarios as separate layers within the file. The output is intended for delivery to downstream consumers — principally Nora Schwaller at UCSD, who uses the exports in ArcGIS for a zoning implications analysis.

### Output format and layer organization

The export produces 21 GeoPackage files, one per coastal state, using the naming convention `flooded_structures_SS.gpkg` where SS is the two-letter state abbreviation. Each file contains 11 layers named `SLR_0ft` through `SLR_10ft`, corresponding to the 11 input tables for that state:

```         
flooded_structures_FL.gpkg
 ├── SLR_0ft    (from flooded_structures_12_0ft)
 ├── SLR_1ft    (from flooded_structures_12_1ft)
 ├── SLR_2ft    (from flooded_structures_12_2ft)
 │   ...
 └── SLR_10ft   (from flooded_structures_12_10ft)
```

GeoPackage was chosen as the export format for several reasons:

-   **Broad compatibility:** GeoPackage is an Open Geospatial Consortium standard that works natively in ArcGIS, QGIS, and R's `sf` package without format conversion.
-   **Multi-layer support:** Unlike shapefiles, a single GPKG file can hold multiple layers, allowing all 11 SLR scenarios for a state to live in one portable file.
-   **No file size limits:** Shapefiles are capped at 2 GB per file, which would be restrictive for large states like Florida with \~4 million flooded structures at 10ft. GeoPackage has no such limit.
-   **Attribute preservation:** All original FEMA fields are preserved alongside the geometry, unlike shapefiles' 10-character column name limit.

### Method

For each state in the YAML `states` list, the script:

1.  Removes any pre-existing GPKG file for the state (to start fresh)
2.  Loops over the 11 SLR scenarios, reading each `flooded_structures_FF_Xft` table from PostgreSQL via `sf::st_read` and writing it as a new layer in the GPKG via `sf::st_write`
3.  Uses `append = TRUE` on all writes after the first to add layers to the same file without overwriting existing layers
4.  Logs per-layer structure counts and final file size

Tables that do not exist or are empty are skipped with a warning; this does not halt processing of subsequent scenarios for the same state.

### Connecticut note

Connecticut's `tract_geoid` column in `flooded_structures_09_Xft` uses the new planning region GEOIDs (09110, 09120, ...) from `tract_10ft_intersections`, not the old county-based GEOIDs (09001, 09003, ...) that appear in the FEMA `CENSUSCODE` field. This is the correct behavior for downstream joins against the Census tract intersection tables (Section 2.4), but ArcGIS users should be aware that joining to FEMA-native county-based identifiers would require the relaxed state-prefix-plus-tract-suffix match described in Section 2.5.

### Configuration

The script is controlled by `structure_analysis_config_v3.yaml`, sharing the same config used by the earlier pipeline stages. Relevant keys:

``` yaml
database:
  name: "megaSLR"
  host: "localhost"

states: ["01","06","09","10","12","13","22","23","24","25","28","33","34","36","37","41","44","45","48","51","53"]
slr_scenarios: ["0ft","1ft","2ft","3ft","4ft","5ft","6ft","7ft","8ft","9ft","10ft"]

paths:
  log_dir: "~/Science/Nora_SLR/SLR_log_files"
  structures_export_dir: "~/claude_projects/slr_analysis/exports/flooded_structures_gpkg"
```

The `paths.structures_export_dir` key specifies the output directory for the GPKG files. If omitted, the script defaults to `~/claude_projects/slr_analysis/exports/gpkg`. A parallel A parallel `paths.tracts_export_dir` key is used by the tracts export script (Section 2.8).

### Running

``` bash
screen -S export
cd ~/claude_projects/slr_analysis/structures/code/03_export
Rscript export_flooded_structures_v2.R \
  ~/claude_projects/slr_analysis/structures/YAML_config_files/structure_analysis_config_v3.yaml \
  2>&1 | tee ~/Science/Nora_SLR/SLR_log_files/export_console.log
```

### Prerequisites

-   **Flooded structures tables** `flooded_structures_FF_Xft` for all 21 states × 11 scenarios (231 tables, Section 2.6).
-   **Output directory** specified by `paths.structures_export_dir`, or the hard-coded default. The script creates the directory if it does not exist.
-   R packages: `sf`, `DBI`, `RPostgres`, `yaml`, `glue`.

### Outputs

-   21 GeoPackage files named `flooded_structures_SS.gpkg`
-   11 layers per file, named `SLR_0ft` through `SLR_10ft`
-   Each layer contains the full FEMA record plus `tract_geoid` and `geom`
-   Files are written to `paths.structures_export_dir` (default: `~/claude_projects/slr_analysis/exports/gpkg`)

### Logging and notifications

-   Timestamped log file `export_flooded_structures_{timestamp}.txt` in `paths.log_dir`, with `flush()` per write for real-time tailing.
-   ntfy.sh push notifications fire per-state (structure count, layer count, file size, elapsed time) and at overall completion with aggregate statistics.

### Scripts

-   `export_flooded_structures_v2.R` — main export script (YAML-driven, logging, ntfy notifications)
-   `structure_analysis_config_v3.yaml` — shared configuration file

## 2.8 Export Flooded Tracts as GeoPackage Files

The script `export_flooded_tracts_v1.R` reads each `tract_{scenario}_intersections` table from megaSLR, filters by state, and writes the results to GeoPackage (.gpkg) files organized per state. This mirrors the per-state organization used for flooded structures exports in Section 2.7, so that downstream users encounter a consistent pattern across both datasets: one GPKG per state, with SLR scenarios as layers.

### Output format and layer organization

The export produces 21 GeoPackage files, one per coastal state, using the naming convention `flooded_tracts_SS.gpkg` where SS is the two-letter state abbreviation. Each file contains 11 layers named `SLR_0ft` through `SLR_10ft`, with each layer containing only the tracts for that state at that SLR scenario:

```         
flooded_tracts_FL.gpkg
 ├── SLR_0ft    (1,493 tracts from tract_0ft_intersections WHERE statefp = '12')
 ├── SLR_1ft    (1,568 tracts)
 ├── SLR_2ft    (1,626 tracts)
 │   ...
 └── SLR_10ft   (2,935 tracts)
```

Each row contains `geoid`, `namelsad`, `statefp`, `tract_area_ha`, `slr_area_ha`, and `geom`. Users who need a derived field such as percent flooded can compute it as `slr_area_ha / tract_area_ha`.

### Organizational consistency with structures exports

The tract intersection tables in megaSLR are organized by SLR scenario (11 tables, each covering all 21 states), while the flooded structures tables are organized by state × scenario (231 tables). To avoid this asymmetry propagating into the exported data — which would force users to learn two different organizational conventions — the tract export script filters each scenario table by `statefp` at export time and writes the results into per-state GPKGs. This means both datasets follow the same convention: pull one state's GPKG, find all 11 SLR scenarios as layers inside it.

### Method

For each state in the YAML `states` list, the script:

1.  Removes any pre-existing GPKG file for the state (to start fresh)
2.  Loops over the 11 SLR scenarios, reading from each `tract_{scenario}_intersections` table with a `WHERE statefp = '{fips}'` filter, and writing the result as a new layer in the GPKG via `sf::st_write`
3.  Uses `append = TRUE` on all writes after the first to add layers to the same file
4.  Logs per-layer tract counts and final file size

The `statefp` filter is efficient — it's an indexed column on the tract intersection tables — and tract counts per state per scenario are small (typically tens to low thousands), so the export completes in seconds.

### Population data

The exported tract tables do not include population fields. Census population data is well-documented, widely available, and straightforward to join by `geoid` using tools such as `tidycensus` (R), ArcGIS Living Atlas, or direct downloads from data.census.gov. Embedding a specific ACS vintage in the export would create a staleness risk: the ACS 5-year estimates are updated annually each December, and a frozen population column would become outdated relative to the latest available data. Keeping the tract exports focused on the SLR intersection geometry — the novel contribution of this dataset — allows downstream users to join whichever population vintage is most appropriate for their analysis.

### Configuration

The script is controlled by `tracts_analysis_config_v2.yaml`, the same config used by the tract intersection analysis in Section 2.4. Relevant keys:

``` yaml
database:
  name: "megaSLR"
  host: "localhost"

states:
 - "01"   # AL
 - "06"   # CA
 # ... all 21 coastal states

slr_scenarios:
 - "0ft"
 - "1ft"
 # ... through "10ft"

paths:
  log_dir: "~/Science/Nora_SLR/SLR_log_files"
  tracts_export_dir: "~/claude_projects/slr_analysis/exports/flooded_tracts_gpkg"
```

The `paths.tracts_export_dir` key specifies the output directory. If omitted, the script defaults to `~/claude_projects/slr_analysis/exports/flooded_tracts_gpkg`.

### Running

``` bash
screen -S tract_export
cd ~/claude_projects/slr_analysis/tracts/code/04_export
Rscript export_flooded_tracts_v1.R \
  ~/claude_projects/slr_analysis/tracts/YAML_config_files/tracts_analysis_config_v2.yaml \
  2>&1 | tee ~/Science/Nora_SLR/SLR_log_files/tracts_export_console.log
```

### Prerequisites

-   **Tract intersection tables** `tract_{scenario}_intersections` for all 11 scenarios (produced by Section 2.4). The script verifies all 11 tables exist at startup and fails fast if any are missing.
-   **Output directory** specified by `paths.tracts_export_dir`, or the hard-coded default. The script creates the directory if it does not exist.
-   R packages: `sf`, `DBI`, `RPostgres`, `yaml`, `glue`.

### Outputs

-   21 GeoPackage files named `flooded_tracts_SS.gpkg`
-   11 layers per file, named `SLR_0ft` through `SLR_10ft`
-   Each layer contains `geoid`, `namelsad`, `statefp`, `tract_area_ha`, `slr_area_ha`, and `geom`
-   Total size: 8.1 MB across all 21 files
-   Total export time: \~14 seconds

### Logging and notifications

-   Timestamped log file `export_flooded_tracts_{timestamp}.txt` in `paths.log_dir`, with `flush()` per write for real-time tailing.
-   ntfy.sh push notifications fire per-state (tract count, file size, elapsed time) and at overall completion with aggregate statistics.

### Scripts

-   `export_flooded_tracts_v1.R` — main export script (YAML-driven, logging, ntfy notifications)
-   `tracts_analysis_config_v2.yaml` — shared configuration file (also used by tract intersection analysis)
