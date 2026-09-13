# visualize_4776_scenario_check_v1.R
#
# Build Google Earth visualizations of the Section 5 result from the "4,776
# anomaly" diagnosis: Nora's 4,776 Louisiana structures fall inside the NOAA
# SLR 2 ft extent (4,776 of 4,776) but almost none inside the 1 ft extent (50),
# which is what identified the 2 ft layer as the one actually intersected.
#
# Two products, because two different questions are being asked:
#
#   la_4776_overview.kmz   statewide point cloud, no extents.  Small and fast.
#                          Answers "where are these structures?"  Points are
#                          split into one folder per flag pattern so the 1,882
#                          known anomalies can be toggled against the rest.
#
#   la_4776_detail.kmz     one or two tracts with the 0, 1 and 2 ft extents
#                          drawn.  Answers "why did this happen?"  At this zoom
#                          the 2 ft boundary visibly wraps the point cloud while
#                          the 1 ft extent sits well inside it.
#
# Structures are exported as POINTS (server-side ST_Centroid, in EPSG:5070
# before transform -- some FEMA footprints carry duplicate vertices that the s2
# engine rejects once the data is in EPSG:4326).
#
# The extents must be clipped and simplified.  Clipped to the 22 affected
# tracts the three scenarios hold roughly 513,000 / 71,000 / 110,000 subdivided
# pieces, which Google Earth will not render.  Restricting to one or two tracts,
# simplifying, and dropping sub-threshold slivers brings this to a manageable
# size; counts are reported before anything is written so the thresholds can be
# adjusted.
#
# Requires nora_list (see diagnostic_4776_anomaly.md step 1).

library(DBI)
library(RPostgres)
library(sf)
library(glue)

con <- dbConnect(Postgres(), dbname = "megaSLR", host = "localhost")

outdir <- path.expand("~/Science/Nora_SLR/la_anomalies/kml")
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)

# --- parameters -------------------------------------------------------------

detail_tracts <- c("22057021700")   # Lockport, 463 structures. Add
                                    # "22057021500" (Larose, 664) for a wider
                                    # view; each tract added costs render time.

simplify_m    <- 10     # ST_SimplifyPreserveTopology tolerance, metres
min_area_m2   <- 200    # drop extent pieces smaller than this

stopifnot(dbGetQuery(con,
  "SELECT to_regclass('public.nora_list') IS NOT NULL AS e")$e[1])

tr_list <- paste0("'", detail_tracts, "'", collapse = ",")

# --- 1. overview: statewide points by flag pattern ---------------------------

pts <- st_read(con, query = "
  SELECT n.build_id, n.tract_geoid,
         concat(n.f0,n.f1,n.f2,n.f3,n.f4,n.f5,n.f6,n.f7,n.f8,n.f9,n.f10) AS pattern,
         s.occ_cls, s.prim_occ, round(s.sqmeters::numeric,0) AS sqm,
         s.prop_city,
         ST_Centroid(s.geom) AS geom
  FROM nora_list n
  JOIN usa_structures_22 s USING (build_id)", quiet = TRUE)

message(glue("points: {nrow(pts)}"))
print(table(pts$pattern))

pts <- st_transform(pts, 4326)

# Human-readable label per pattern, so the Google Earth folder list explains
# itself without reference to the write-up.
lab <- c(
  "10111111111" = "A_wet0_DRY1_wet2up_1882_the_known_anomaly",
  "00111111111" = "B_dry0_dry1_wet2up_2844_monotonic",
  "01111111111" = "C_dry0_wet1up_48_monotonic",
  "11111111111" = "D_wet_at_all_levels_2_monotonic"
)
pts$grp <- ifelse(pts$pattern %in% names(lab), lab[pts$pattern], paste0("Z_other_", pts$pattern))

pts$Name <- paste0(pts$build_id)
pts$Description <- with(pts, paste0(
  "build_id: ", build_id,
  "<br/>tract: ", tract_geoid,
  "<br/>pattern (0-10 ft): ", pattern,
  "<br/>class: ", occ_cls, " / ", prim_occ,
  "<br/>footprint: ", sqm, " m2",
  "<br/>city: ", prop_city))

ov <- file.path(outdir, "overview")
dir.create(ov, showWarnings = FALSE)
unlink(list.files(ov, full.names = TRUE))

for (g in sort(unique(pts$grp))) {
  sub <- pts[pts$grp == g, ]
  f <- file.path(ov, glue("{g}.kml"))
  st_write(sub[, c("Name", "Description")], f,
           driver = "KML", delete_dsn = TRUE, quiet = TRUE)
  message(glue("  {g}: {nrow(sub)}"))
}

old <- setwd(ov)
zip(zipfile = file.path(outdir, "la_4776_overview.kmz"),
    files = basename(list.files(ov, pattern = "\\.kml$")), flags = "-q")
setwd(old)

# --- 2. detail: extents plus points for the selected tract(s) ---------------

dt <- file.path(outdir, "detail")
dir.create(dt, showWarnings = FALSE)
unlink(list.files(dt, full.names = TRUE))

for (s in c(0, 1, 2)) {
  message(glue("fetching SLR {s} ft for detail view ..."))
  g <- st_read(con, query = glue("
    WITH t AS (SELECT geom FROM census_tracts_2025 WHERE geoid IN ({tr_list}))
    SELECT ST_SimplifyPreserveTopology(
             ST_Intersection(t.geom, p.geom), {simplify_m}) AS geom
    FROM t JOIN slr_{s}ft_22 p
      ON t.geom && p.geom AND ST_Intersects(t.geom, p.geom)
    WHERE ST_Area(ST_Intersection(t.geom, p.geom)) > {min_area_m2}"),
    quiet = TRUE)

  g <- g[!st_is_empty(g), ]
  message(glue("  {nrow(g)} pieces after simplify/filter, ",
               "{round(as.numeric(sum(st_area(g)))/10000, 1)} ha"))

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

dpts <- pts[pts$tract_geoid %in% detail_tracts, ]
message(glue("detail points: {nrow(dpts)}"))
if (nrow(dpts) > 0)
  st_write(dpts[, c("Name", "Description")],
           file.path(dt, "structures.kml"),
           driver = "KML", delete_dsn = TRUE, quiet = TRUE)

tr <- st_read(con, query = glue("
  SELECT geoid AS \"Name\", geoid AS \"Description\", geom
  FROM census_tracts_2025 WHERE geoid IN ({tr_list})"), quiet = TRUE)
st_write(st_transform(tr, 4326), file.path(dt, "tract_boundary.kml"),
         driver = "KML", delete_dsn = TRUE, quiet = TRUE)

old <- setwd(dt)
zip(zipfile = file.path(outdir, "la_4776_detail.kmz"),
    files = basename(list.files(dt, pattern = "\\.kml$")), flags = "-q")
setwd(old)

# --- 3. same content as a GeoPackage, for ArcGIS ----------------------------

gpkg <- file.path(outdir, "la_4776_scenario_check.gpkg")
if (file.exists(gpkg)) file.remove(gpkg)

st_write(st_transform(pts, 5070), gpkg, layer = "structures_4776",
         quiet = TRUE, append = FALSE)
for (s in c(0, 1, 2)) {
  g <- st_read(con, query = glue("
    WITH t AS (SELECT geom FROM census_tracts_2025 WHERE geoid IN ({tr_list}))
    SELECT {s} AS slr_ft, ST_Intersection(t.geom, p.geom) AS geom
    FROM t JOIN slr_{s}ft_22 p
      ON t.geom && p.geom AND ST_Intersects(t.geom, p.geom)"), quiet = TRUE)
  g <- g[!st_is_empty(g), ]
  if (nrow(g) > 0)
    st_write(g, gpkg, layer = glue("slr_{s}ft_extent"), quiet = TRUE, append = FALSE)
}

message("\nWrote:")
message("  ", file.path(outdir, "la_4776_overview.kmz"))
message("  ", file.path(outdir, "la_4776_detail.kmz"))
message("  ", gpkg)
message("\nIn Google Earth, set colours per folder: right-click a folder -> ",
        "Properties -> Style, Color. Suggested: 0 ft pale blue, 1 ft mid blue, ",
        "2 ft dark blue with ~40% opacity, structures red.")

dbDisconnect(con)
