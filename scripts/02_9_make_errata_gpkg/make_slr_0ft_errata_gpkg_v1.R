# make_slr_0ft_errata_gpkg_v1.R
#
# Generate suspected_SLR_0ft_errata.gpkg, the errata layer accompanying the
# derived dataset (Section 2.10.2).
#
# The file identifies Louisiana structures whose flooding flags are believed
# to be in error, and states for each which scenario value is suspect and in
# which direction.  The two classes are corrected in OPPOSITE directions:
#
#   Class 1 (inland corridor).  Structures inside the NOAA SLR 0 ft (MHHW)
#     extent that visual inspection of current satellite imagery shows to be
#     on dry ground, often several hundred metres from any watercourse.  The
#     SLR 0 ft value is believed to be a FALSE POSITIVE; the SLR 1 ft value
#     (unflooded) is believed correct, and SLR 2 ft and above correctly
#     re-flag these locations as genuine inundation reaches them.
#
#   Class 2 (open water).  Structures lying in permanent open water, at a
#     measured distance of exactly zero from mapped water, which are flagged
#     unflooded at SLR 1 ft.  The SLR 1 ft value is believed to be a FALSE
#     NEGATIVE; these structures should be flagged flooded at every scenario.
#
# Class 3 (water margin) structures are NOT included.  Their behaviour is
# consistent with sliver effects at vectorized polygon boundaries, which is a
# property of the source data rather than an identifiable error, and no
# defensible per-structure threshold separates them from correctly flagged
# marginal cases.
#
# No flags in the published dataset are altered.  This file records what was
# found so that users can exclude or downweight affected records as suits
# their analysis.

library(DBI)
library(RPostgres)
library(sf)
library(glue)

con <- dbConnect(Postgres(), dbname = "megaSLR", host = "localhost")

outdir <- path.expand("~/Science/Nora_SLR/la_anomalies")
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
gpkg <- file.path(outdir, "suspected_SLR_0ft_errata.gpkg")
if (file.exists(gpkg)) file.remove(gpkg)

# --- class membership -------------------------------------------------------
# Class 1 tract membership rests on visual inspection of current satellite
# imagery, not on a computed rule, so the tracts are listed explicitly rather
# than derived.  Class 2 is identifiable from the data: all its structures lie
# at distance zero from mapped water, and 22023990000 is a Census water tract.

class1_tracts <- c(
  "22057021500",   # Larose
  "22057021700",   # Lockport
  "22057021602",   # between Lockport and Larose
  "22057021603"    # west of Raceland -- MIXED, see note below
)

# Tract 22057021603 contains both dry-ground structures and structures on the
# margin of Lake Fields; inspection could not separate them at tract level.
# Records from it are flagged so users can treat them more cautiously.
mixed_tracts <- c("22057021603")

class2_tracts <- c("22023990000", "22093040500", "22097960400")

c1 <- paste0("'", class1_tracts, "'", collapse = ",")
c2 <- paste0("'", class2_tracts, "'", collapse = ",")
cm <- paste0("'", mixed_tracts,  "'", collapse = ",")

# --- build the errata layer -------------------------------------------------
# la_anom_dist is produced by export_la_subset_for_nora_v2.R and holds the
# per-structure distance to TIGER 2025 areal and linear water.

stopifnot(dbGetQuery(con,
  "SELECT to_regclass('public.la_anom_dist') IS NOT NULL AS e")$e[1])

errata <- st_read(con, query = glue("
  SELECT s.build_id,
         s.tract_geoid,
         CASE WHEN d.tract_geoid IN ({c1}) THEN 1 ELSE 2 END AS errata_class,
         CASE WHEN d.tract_geoid IN ({c1})
              THEN 'inland_0ft_false_positive'
              ELSE 'open_water_1ft_false_negative' END AS category,
         CASE WHEN d.tract_geoid IN ({c1}) THEN '0ft' ELSE '1ft' END AS suspect_scenario,
         CASE WHEN d.tract_geoid IN ({c1}) THEN 'flooded' ELSE 'unflooded' END AS published_value,
         CASE WHEN d.tract_geoid IN ({c1}) THEN 'unflooded' ELSE 'flooded' END AS believed_value,
         CASE WHEN d.tract_geoid IN ({cm}) THEN true ELSE false END AS mixed_tract,
         round(d.dist_water_m::numeric, 1) AS dist_water_m,
         '1011111111' AS flag_pattern,
         CASE
           WHEN d.tract_geoid IN ({cm}) THEN
             'Tract contains a mix of dry-ground structures and structures on the margin of Lake Fields; the two could not be separated at tract level. The SLR 0 ft flag is suspect for the dry-ground cases only. Treat with caution.'
           WHEN d.tract_geoid IN ({c1}) THEN
             'Structure lies within the NOAA SLR 0 ft (MHHW) extent but is on dry ground in current satellite imagery. The SLR 0 ft flooded flag is believed to be a false positive; SLR 1 ft and above are believed correct.'
           ELSE
             'Structure lies in permanent open water (distance to mapped water = 0) and should be flooded at every scenario. The SLR 1 ft unflooded flag is believed to be a false negative.'
         END AS note,
         s.occ_cls, s.prim_occ, s.sqmeters, s.prop_addr, s.prop_city,
         s.geom
  FROM la_anom_dist d
  JOIN usa_structures_22 s USING (build_id)
  WHERE d.tract_geoid IN ({c1}) OR d.tract_geoid IN ({c2})"), quiet = TRUE)

message(glue("errata records: {nrow(errata)}"))
print(table(errata$category))
cat("\nby tract:\n")
print(table(errata$tract_geoid))
cat("\nmixed-tract records: ", sum(errata$mixed_tract), "\n")

# sanity: expected 1,440 in class 1 and 37 in class 2
stopifnot(nrow(errata) == sum(table(errata$errata_class)))
message(glue("class 1: {sum(errata$errata_class == 1)}  (expected 1440)"))
message(glue("class 2: {sum(errata$errata_class == 2)}  (expected 37)"))

st_write(errata, gpkg, layer = "suspected_errata_structures",
         quiet = TRUE, append = FALSE)

# --- affected tract boundaries, for context ---------------------------------

tr <- st_read(con, query = glue("
  SELECT t.geoid,
         CASE WHEN t.geoid IN ({c1}) THEN 1 ELSE 2 END AS errata_class,
         CASE WHEN t.geoid IN ({cm}) THEN true ELSE false END AS mixed_tract,
         t.geom
  FROM census_tracts_2025 t
  WHERE t.geoid IN ({c1}) OR t.geoid IN ({c2})"), quiet = TRUE)

st_write(tr, gpkg, layer = "errata_tracts", quiet = TRUE, append = FALSE)

# --- companion README -------------------------------------------------------
# GeoPackage attribute-only tables are awkward to write portably, so the class
# definitions travel as a sibling markdown file instead.

readme <- glue("
# suspected_SLR_0ft_errata.gpkg

Accompanies the megaSLR derived dataset.  See Section 2.10.2 of the methods
documentation for the full analysis.

No flags in the published dataset have been altered.  This file records
structures whose flooding flags are believed to be in error, so that users can
exclude or downweight them.

## Layers

- `suspected_errata_structures` ({nrow(errata)} records) - FEMA USA Structures
  footprints, EPSG:5070
- `errata_tracts` ({nrow(tr)} records) - Census tract boundaries for context

## Classes

**Class 1, `inland_0ft_false_positive` ({sum(errata$errata_class == 1)} structures).**
Structures inside the NOAA SLR 0 ft (MHHW) extent that current satellite
imagery shows to be on dry ground, often several hundred metres from any
watercourse.  Tracts: {paste(class1_tracts, collapse = ', ')} (Lockport-Larose
corridor, Lafourche Parish).  The SLR 0 ft flooded flag is believed to be a
FALSE POSITIVE.  SLR 1 ft (unflooded) is believed correct; SLR 2 ft and above
correctly re-flag these locations.

**Class 2, `open_water_1ft_false_negative` ({sum(errata$errata_class == 2)} structures).**
Structures in permanent open water, at measured distance zero from mapped
water, flagged unflooded at SLR 1 ft.  Tracts: {paste(class2_tracts, collapse = ', ')}.
The SLR 1 ft unflooded flag is believed to be a FALSE NEGATIVE; these
structures should be flooded at every scenario.

## Caveats

- Tract {paste(mixed_tracts, collapse = ', ')} contains both dry-ground
  structures and structures on the margin of Lake Fields, which could not be
  separated at tract level.  Its {sum(errata$mixed_tract)} records carry
  `mixed_tract = TRUE`.
- Class 1 is a LOWER BOUND.  The underlying test detects only structures whose
  flags are non-monotonic across scenarios.  If the MHHW surface over-inundates
  this corridor it will also over-flag structures that remain flooded at
  SLR 1 ft and above, and those cases are not detectable by this method.
- Class 1 membership rests on visual inspection of current satellite imagery,
  which is evidence of present-day conditions rather than of the conditions
  the MHHW surface was intended to represent.
- Structures on the immediate margins of water bodies (Class 3 in Section
  2.10.2) are NOT included.  Their behaviour is consistent with sliver effects
  at independently vectorized polygon boundaries, and no defensible
  per-structure threshold separates them from correctly flagged cases.

Generated {format(Sys.Date())} by make_slr_0ft_errata_gpkg_v1.R
")

writeLines(readme, file.path(outdir, "suspected_SLR_0ft_errata_README.md"))

message("\nWrote:")
message("  ", gpkg)
message("  ", file.path(outdir, "suspected_SLR_0ft_errata_README.md"))
print(st_layers(gpkg))

dbDisconnect(con)
