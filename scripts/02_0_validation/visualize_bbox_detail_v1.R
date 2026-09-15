# visualize_bbox_detail_v1.R
#
# Build a Google Earth detail view of the NOAA SLR 0, 1 and 2 ft extents plus
# Nora's 4,776 structures, for an arbitrary geographic bounding box.
#
# Companion to visualize_4776_scenario_check_v1.R, which works by Census tract.
# This version takes a lat/lon box instead, for looking at a specific area.
#
# Note on the envelope: a rectangle in EPSG:4326 is NOT a rectangle in
# EPSG:5070 (Conus Albers).  The box is densified with ST_Segmentize before
# transformation so its edges follow the correct curves; transforming the four
# corners alone would clip material near the box edges.
#
# Requires nora_list (see diagnostic_4776_anomaly.md step 1).

library(DBI)
library(RPostgres)
library(sf)
library(glue)

con <- dbConnect(Postgres(), dbname = "megaSLR", host = "localhost")

# --- parameters -------------------------------------------------------------

xmin <- -90.4032
xmax <- -90.3815
ymin <-  29.5600
ymax <-  29.5740

label <- "bbox_lockport_south"   # used for the output directory and .kmz name

simplify_m  <- 5     # ST_SimplifyPreserveTopology tolerance, metres.  Lower
                     # than the tract-scale script (10 m) because this is a
                     # much smaller area and detail matters more here.
min_area_m2 <- 50    # drop extent pieces smaller than this

outdir <- path.expand("~/Science/Nora_SLR/la_anomalies/kml")
dt     <- file.path(outdir, label)
dir.create(dt, showWarnings = FALSE, recursive = TRUE)
unlink(list.files(dt, full.names = TRUE))

stopifnot(dbGetQuery(con,
  "SELECT to_regclass('public.nora_list') IS NOT NULL AS e")$e[1])

# --- the area of interest, densified then projected -------------------------

aoi <- glue("
  ST_Transform(
    ST_Segmentize(
      ST_SetSRID(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 4326), 4326)::geography::geometry,
      100),
    5070)")

dims <- dbGetQuery(con, glue("
  SELECT round((ST_XMax(g) - ST_XMin(g))::numeric, 0) AS width_m,
         round((ST_YMax(g) - ST_YMin(g))::numeric, 0) AS height_m,
         round((ST_Area(g)/10000)::numeric, 1)        AS ha
  FROM (SELECT {aoi} AS g) x"))
message(glue("AOI: {dims$width_m} x {dims$height_m} m, {dims$ha} ha"))

# --- SLR extents ------------------------------------------------------------

for (s in c(0, 1, 2)) {
  message(glue("fetching SLR {s} ft ..."))
  g <- st_read(con, query = glue("
    WITH a AS (SELECT {aoi} AS geom)
    SELECT ST_SimplifyPreserveTopology(
             ST_Intersection(a.geom, p.geom), {simplify_m}) AS geom
    FROM a JOIN slr_{s}ft_22 p
      ON a.geom && p.geom AND ST_Intersects(a.geom, p.geom)
    WHERE ST_Area(ST_Intersection(a.geom, p.geom)) > {min_area_m2}"),
    quiet = TRUE)

  g <- g[!st_is_empty(g), ]
  message(glue("  {nrow(g)} pieces, {round(as.numeric(sum(st_area(g)))/10000, 2)} ha"))
  if (nrow(g) == 0) next

  g <- st_transform(g, 4326)
  g$Name <- glue("SLR {s} ft")
  g$Description <- if (s == 0)
    "Current MHHW extent (NOAA InPort 48105) - a SEPARATE product from the 1-10 ft series"
  else
    glue("Newly inundated land at {s} ft above MHHW (NOAA InPort 48106)")

  st_write(g[, c("Name", "Description")],
           file.path(dt, glue("extent_{s}ft.kml")),
           driver = "KML", delete_dsn = TRUE, quiet = TRUE)
}

# --- structures within the box ----------------------------------------------
# Centroids computed server-side in EPSG:5070: some FEMA footprints carry
# duplicate vertices that s2 rejects once the data is in EPSG:4326.
# Name is omitted so Google Earth does not print a label beside every pushpin;
# the balloon still carries the detail on click.

pts <- st_read(con, query = glue("
  WITH a AS (SELECT {aoi} AS geom)
  SELECT n.build_id, n.tract_geoid,
         concat(n.f0,n.f1,n.f2,n.f3,n.f4,n.f5,n.f6,n.f7,n.f8,n.f9,n.f10) AS pattern,
         s.occ_cls, s.prim_occ, round(s.sqmeters::numeric,0) AS sqm,
         s.prop_addr, s.prop_city,
         ST_Centroid(s.geom) AS geom
  FROM nora_list n
  JOIN usa_structures_22 s USING (build_id), a
  WHERE s.geom && a.geom AND ST_Intersects(s.geom, a.geom)"), quiet = TRUE)

message(glue("structures in box: {nrow(pts)}"))
if (nrow(pts) > 0) print(table(pts$pattern))

if (nrow(pts) > 0) {
  pts <- st_transform(pts, 4326)
  pts$Description <- with(pts, paste0(
    "build_id: ", build_id,
    "<br/>tract: ", tract_geoid,
    "<br/>pattern (0-10 ft): ", pattern,
    "<br/>class: ", occ_cls, " / ", prim_occ,
    "<br/>footprint: ", sqm, " m2",
    "<br/>address: ", prop_addr, " ", prop_city))

  # one file per flag pattern, so they can be toggled independently
  for (pat in sort(unique(pts$pattern))) {
    sub <- pts[pts$pattern == pat, ]
    st_write(sub[, "Description", drop = FALSE],
             file.path(dt, glue("structures_{pat}_n{nrow(sub)}.kml")),
             driver = "KML", delete_dsn = TRUE, quiet = TRUE)
    message(glue("  pattern {pat}: {nrow(sub)}"))
  }
}

# --- the box itself, for reference ------------------------------------------

box <- st_read(con, query = glue("
  SELECT 'AOI' AS \"Name\",
         '{xmin}, {ymin} to {xmax}, {ymax}' AS \"Description\",
         {aoi} AS geom"), quiet = TRUE)
st_write(st_transform(box, 4326), file.path(dt, "aoi_box.kml"),
         driver = "KML", delete_dsn = TRUE, quiet = TRUE)

# --- bundle -----------------------------------------------------------------

kmz <- file.path(outdir, glue("la_{label}.kmz"))
if (file.exists(kmz)) file.remove(kmz)
old <- setwd(dt)
zip(zipfile = kmz, files = basename(list.files(dt, pattern = "\\.kml$")), flags = "-q")
setwd(old)

message("\nWrote: ", kmz)
message("GDAL 'Unable to open ... to obtain file list' warnings from the KML ",
        "driver are harmless; the files are written correctly.")

dbDisconnect(con)
