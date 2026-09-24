# build_tract_flooding_summary_v1.R
#
# Build the tract flooding summary: one row per Census tract per SLR scenario,
# covering all 23 states, with inundated area and structure counts.
#
# Produces:
#   1. a database table  tract_flooding_summary
#   2. a GeoPackage      tract_flooding_summary.gpkg   (single spatial layer)
#   3. a CSV             tract_flooding_summary.csv    (attributes only)
#
# Schema is as agreed with Nora (see proposed_tract_flooding_summary_table.md).
#
# Note on geometry: the table is long format, so tract geometry repeats once per
# scenario — up to 11 times for a tract that floods at every level. That is the
# price of a single layer that can be filtered to one scenario and mapped
# directly. If the resulting file is unwieldy, set split_layers = TRUE below to
# write one layer per scenario instead, which mirrors the existing
# flooded_tracts_gpkg exports and removes the duplication.

library(DBI)
library(RPostgres)
library(sf)
library(glue)

# --- parameters -------------------------------------------------------------

db_name   <- "megaSLR"
db_host   <- "localhost"

out_dir   <- path.expand("~/Science/Nora_SLR/release_0225/gpkg_tract_structures_exports")
out_stem  <- "tract_flooding_summary"

table_name <- "tract_flooding_summary"   # database table to (re)create

split_layers      <- FALSE   # FALSE = one long-format layer
                             # TRUE  = one layer per scenario, SLR_0ft ... SLR_10ft
write_csv         <- TRUE
negligible_ha     <- 0.001   # slr_area_ha below this is flagged negligible

# The seven Maine tracts whose 9 ft inundation derives from the ST_MakeValid
# ribbon artifact (Section 2.10.1). They appear at 9 ft and no other scenario.
artifact_9ft_me <- c("23019007100", "23019007200", "23019008001", "23019020500",
                     "23019021500", "23019940000", "23029955100")

states <- c("01","06","09","10","11","12","13","22","23","24","25","28",
            "33","34","36","37","41","42","44","45","48","51","53")

stusps <- c("01"="AL","06"="CA","09"="CT","10"="DE","11"="DC","12"="FL",
            "13"="GA","22"="LA","23"="ME","24"="MD","25"="MA","28"="MS",
            "33"="NH","34"="NJ","36"="NY","37"="NC","41"="OR","42"="PA",
            "44"="RI","45"="SC","48"="TX","51"="VA","53"="WA")

scenarios <- 0:10

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
con <- dbConnect(Postgres(), dbname = db_name, host = db_host)

# --- state abbreviation lookup, as a table so the join happens server-side ---

dbExecute(con, "DROP TABLE IF EXISTS state_fips_lookup")
dbExecute(con, "CREATE TEMP TABLE state_fips_lookup (statefp text PRIMARY KEY, stusps text)")
dbWriteTable(con, "state_fips_lookup",
             data.frame(statefp = names(stusps), stusps = unname(stusps),
                        stringsAsFactors = FALSE),
             append = TRUE, row.names = FALSE, temporary = TRUE)

# --- build the summary table ------------------------------------------------
# Structure counts come from two sources per state-scenario:
#   usa_structures_{FF}                  -> n_structures        (pre-filtered)
#   flooded_structures_{FF}_{N}ft        -> n_structures_flooded
# Both are aggregated by tract_geoid before joining, so a tract with no
# structures yields NULL and is coalesced to 0.

dbExecute(con, glue("DROP TABLE IF EXISTS {table_name}"))
dbExecute(con, glue("
  CREATE TABLE {table_name} (
    geoid                  text,
    namelsad               text,
    statefp                text,
    stusps                 text,
    scenario_ft            int,
    tract_area_ha          double precision,
    slr_area_ha            double precision,
    pct_area_flooded       double precision,
    n_structures           int,
    n_structures_flooded   int,
    pct_structures_flooded double precision,
    data_flag              text
  )"))

me_list <- paste0("'", artifact_9ft_me, "'", collapse = ",")

for (ff in states) {
  for (s in scenarios) {

    tbl_struct <- glue("usa_structures_{ff}")
    tbl_flood  <- glue("flooded_structures_{ff}_{s}ft")
    tbl_tract  <- glue("tract_{s}ft_intersections")

    # a missing flooded-structures table means no structures flooded there
    have_flood <- !is.na(dbGetQuery(con,
      glue("SELECT to_regclass('public.{tbl_flood}') AS t"))$t[1])

    flood_join <- if (have_flood) glue("
      LEFT JOIN (SELECT tract_geoid, count(*) AS n
                 FROM {tbl_flood} GROUP BY 1) f ON f.tract_geoid = t.geoid")
                  else "LEFT JOIN (SELECT NULL::text AS tract_geoid, 0 AS n) f ON false"

    n <- dbExecute(con, glue("
      INSERT INTO {table_name}
      SELECT t.geoid, t.namelsad, t.statefp, l.stusps, {s},
             t.tract_area_ha,
             t.slr_area_ha,
             100.0 * t.slr_area_ha / NULLIF(t.tract_area_ha, 0),
             COALESCE(a.n, 0),
             COALESCE(f.n, 0),
             100.0 * COALESCE(f.n, 0) / NULLIF(a.n, 0),
             CASE
               WHEN {s} = 9 AND t.geoid IN ({me_list}) THEN 'artifact_9ft_me'
               WHEN t.slr_area_ha < {negligible_ha}    THEN 'negligible_area'
               WHEN COALESCE(a.n, 0) = 0               THEN 'no_structures'
               ELSE NULL
             END
      FROM {tbl_tract} t
      JOIN state_fips_lookup l ON l.statefp = t.statefp
      LEFT JOIN (SELECT tract_geoid, count(*) AS n
                 FROM {tbl_struct} GROUP BY 1) a ON a.tract_geoid = t.geoid
      {flood_join}
      WHERE t.statefp = '{ff}'"))

    message(glue("  {stusps[[ff]]} {s} ft: {n} rows"))
  }
}

dbExecute(con, glue("CREATE INDEX ON {table_name} (geoid, scenario_ft)"))
dbExecute(con, glue("CREATE INDEX ON {table_name} (scenario_ft)"))
dbExecute(con, glue("ANALYZE {table_name}"))

# --- summary ----------------------------------------------------------------

cat("\n=== rows per scenario ===\n")
print(dbGetQuery(con, glue("
  SELECT scenario_ft, count(*) AS n_tracts,
         round(sum(slr_area_ha)::numeric, 0) AS total_slr_ha,
         sum(n_structures_flooded) AS n_flooded
  FROM {table_name} GROUP BY 1 ORDER BY 1")), row.names = FALSE)

cat("\n=== data_flag counts ===\n")
print(dbGetQuery(con, glue("
  SELECT COALESCE(data_flag, '(none)') AS data_flag, count(*)
  FROM {table_name} GROUP BY 1 ORDER BY 2 DESC")), row.names = FALSE)

cat("\n=== totals ===\n")
print(dbGetQuery(con, glue("
  SELECT count(*) AS total_rows, count(DISTINCT geoid) AS distinct_tracts
  FROM {table_name}")), row.names = FALSE)

# --- export -----------------------------------------------------------------

gpkg <- file.path(out_dir, glue("{out_stem}.gpkg"))
if (file.exists(gpkg)) file.remove(gpkg)

if (!split_layers) {

  message("\nfetching summary with geometry (single layer) ...")
  g <- st_read(con, query = glue("
    SELECT s.*, c.geom
    FROM {table_name} s
    JOIN census_tracts_2025 c ON c.geoid = s.geoid
    ORDER BY s.geoid, s.scenario_ft"), quiet = TRUE)

  message(glue("  {nrow(g)} features"))
  st_write(g, gpkg, layer = out_stem, quiet = TRUE, append = FALSE)

} else {

  for (s in scenarios) {
    g <- st_read(con, query = glue("
      SELECT s.*, c.geom
      FROM {table_name} s
      JOIN census_tracts_2025 c ON c.geoid = s.geoid
      WHERE s.scenario_ft = {s}
      ORDER BY s.geoid"), quiet = TRUE)
    if (nrow(g) == 0) next
    st_write(g, gpkg, layer = glue("SLR_{s}ft"), quiet = TRUE, append = TRUE)
    message(glue("  SLR_{s}ft: {nrow(g)} features"))
  }
}

if (write_csv) {
  d <- dbGetQuery(con, glue("
    SELECT geoid, namelsad, statefp, stusps, scenario_ft,
           tract_area_ha, slr_area_ha, pct_area_flooded,
           n_structures, n_structures_flooded, pct_structures_flooded, data_flag
    FROM {table_name} ORDER BY geoid, scenario_ft"))
  csv <- file.path(out_dir, glue("{out_stem}.csv"))
  write.csv(d, csv, row.names = FALSE, na = "")
  message(glue("\nwrote {csv}  ({nrow(d)} rows)"))
}

message(glue("wrote {gpkg}"))
print(st_layers(gpkg))

dbDisconnect(con)
