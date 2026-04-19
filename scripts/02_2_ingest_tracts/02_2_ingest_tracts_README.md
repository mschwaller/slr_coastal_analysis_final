---
editor_options: 
  markdown: 
    wrap: 72
---

# 02_2 Ingest Census Tracts

This step downloads 2025 Census TIGER tract shapefiles and loads them
into PostgreSQL. There is no R script for this step — it is performed
with `wget`, `shp2pgsql`, and `psql` directly. See Section 2.2 of
`docs/NOAA_SLR_methods_pipeline_v3.md` for full details.

## Commands

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
# NOT loading AK (02), HI (15), or Caribbean (72, 78)
for fips in 01 06 09 10 12 13 22 23 24 25 28 33 34 36 37 41 44 45 48 51 53; do
  echo "Loading FIPS ${fips}..."
  shp2pgsql -s 4269:5070 -a tl_2025_${fips}_tract.shp census_tracts_2025 | psql -U matt -d megaSLR
done

# Create the spatial index
psql -U matt -d megaSLR -c "CREATE INDEX census_tracts_2025_geom_idx ON census_tracts_2025 USING GIST (geom);"
```

## Notes

-   Source CRS is EPSG:4269 (NAD83); `-s 4269:5070` reprojects to
    EPSG:5070 (NAD83 / Conus Albers Equal Area) on load.
-   The `-a` flag appends each state to the same `census_tracts_2025`
    table. The table must be created before the first state is loaded,
    or the first state loaded with `-c` (create) instead of `-a`.
-   COASTAL_FIPS includes AK, HI, and Caribbean for download
    completeness, but those are excluded from the load loop.
-   Output: 1 table (`census_tracts_2025`), 49,502 tracts across 21
    states.
