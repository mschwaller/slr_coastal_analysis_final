# Diagnostic record: the "4,776 anomaly"

**Question.** Nora reported 4,776 Louisiana structures that intersect the NOAA
SLR 1 ft polygons in ArcGIS but carry an unflooded flag at 1 ft in the megaSLR
database, across 28 source polygons.

**Conclusion.** The megaSLR flags are correct. All 4,776 structures fall inside
the **2 ft** extent and almost none fall inside the 1 ft extent, so the
intersection appears to have been run against the 2 ft layer. The LA/TX
filename normalization documented in Section 2.1 is a plausible route to that
substitution.

**Elapsed effort.** Six queries, none longer than a few minutes once the
geometry was restricted sensibly.

------------------------------------------------------------------------

## 1. Characterize the reported set

Nora's export was loaded and the eleven flag columns concatenated into a
pattern string.

``` sql
CREATE TABLE nora_list (
  build_id integer, county text, tract_geoid text,
  f0 int, f1 int, f2 int, f3 int, f4 int, f5 int,
  f6 int, f7 int, f8 int, f9 int, f10 int);

\copy nora_list FROM '/home/matt/Science/Nora_SLR/nora_subset.txt' WITH (FORMAT csv, ENCODING 'UTF8')
```

The file carries a UTF-8 BOM, which breaks integer parsing on the first row:

``` bash
sed -i '1s/^\xEF\xBB\xBF//' /home/matt/Science/Nora_SLR/nora_subset.txt
```

**Result: 4,776 rows, 4,776 distinct `build_id`, four distinct flag patterns.**

| pattern (0→10 ft) | count | monotonic | 1 ft flag |
|----|----|----|----|
| `00111111111` | 2,844 | yes | 0 |
| `10111111111` | 1,882 | **no** | 0 |
| `01111111111` | 48 | yes | 1 |
| `11111111111` | 2 | yes | 1 |

**Implication.** Only 1,882 are non-monotonic — the already-documented
Lockport/Larose set (Section 2.10.2). The remaining 2,894 behave correctly.
Critically, **every pattern is flooded from position 2 onward**: all 4,776 are
flagged at 2 ft and above. That observation drove the rest of the diagnosis.

The reported discrepancy is therefore 4,726 (those with a 0 at 1 ft), not
4,776; 50 of the structures are flagged at 1 ft and were never in dispute.

------------------------------------------------------------------------

## 2. Are the structures in the loaded set?

Tests whether Nora intersected the full FEMA release rather than the
10 ft-prefiltered `usa_structures_22` (989,769 rows).

``` sql
SELECT count(*) AS total,
       count(*) FILTER (WHERE s.build_id IS NULL) AS missing
FROM nora_list n
LEFT JOIN usa_structures_22 s USING (build_id);
```

**Result: 4,776 total, 0 missing.**

**Implication.** Same structure set. The pre-filter is not involved.

------------------------------------------------------------------------

## 3. Are the flooding tables internally consistent?

Every structure intersecting `slr_1ft_22` should appear in
`flooded_structures_22_1ft`, since that is how the table is defined. A non-zero
result would mean flags were dropped.

``` sql
SELECT count(DISTINCT s.build_id) AS should_be_flooded_but_not
FROM usa_structures_22 s
JOIN slr_1ft_22 p ON s.geom && p.geom AND ST_Intersects(s.geom, p.geom)
WHERE NOT EXISTS (SELECT 1 FROM flooded_structures_22_1ft f
                  WHERE f.build_id = s.build_id);
```

**Result: 0.**

And the converse, that the 1,882 do not intersect the 1 ft layer:

``` sql
SELECT count(*) AS anomalies_inside_1ft FROM g1_la g
WHERE EXISTS (SELECT 1 FROM slr_1ft_22 p
              WHERE g.geom && p.geom AND ST_Intersects(g.geom, p.geom));
```

**Result: 0.**

**Implication.** Flags and geometry agree in both directions. The 1,882 are not
a subset of the 4,776 — they sit a median 252 m outside the 1 ft extent — so
subtracting one from the other does not describe a real third population.

------------------------------------------------------------------------

## 4. Was anything lost between the regional and subdivided layers?

The subdivided per-state tables (`slr_1ft_22`) are produced from the regional
source table (`slr_1ft_la`) by clip-and-subdivide (Section 2.3). If subdivision
lost coverage, Nora's intersection against the regional shapefile would find
structures the flags never tested.

First, whether repair is needed at all — `ST_MakeValid` on large polygons is
expensive and was avoided once this returned clean:

``` sql
SELECT count(*) AS n_large,
       count(*) FILTER (WHERE NOT ST_IsValid(geom)) AS n_invalid
FROM slr_1ft_la WHERE ST_NPoints(geom) > 100000;
```

**Result: 131 large polygons, 0 invalid.**

Then both layers measured against the same structures in one pass:

``` sql
SELECT count(*) AS n,
       count(*) FILTER (WHERE hit_regional) AS in_regional,
       count(*) FILTER (WHERE hit_sub)      AS in_subdivided
FROM (
  SELECT n.build_id,
         EXISTS (SELECT 1 FROM slr_1ft_la p
                 WHERE s.geom && p.geom AND ST_Intersects(s.geom, p.geom)) AS hit_regional,
         EXISTS (SELECT 1 FROM slr_1ft_22 p
                 WHERE s.geom && p.geom AND ST_Intersects(s.geom, p.geom)) AS hit_sub
  FROM nora_list n JOIN usa_structures_22 s USING (build_id)) x;
```

**Result: n = 4,776; in_regional = 50; in_subdivided = 50.**

**Implication.** The two layers agree exactly, and both agree with the flags —
the 50 are precisely the 48 + 2 structures already flagged at 1 ft.
Subdivision is not lossy, and 4,726 of Nora's structures do not intersect the
NOAA 1 ft layer in *any* form held in the database.

------------------------------------------------------------------------

## 5. Which layer do they intersect?

Prompted by the observation in step 1 that all four patterns are flooded from
2 ft up. Run on a random sample of 200 for speed; the signal did not require
the full set.

``` sql
WITH s200 AS (
  SELECT s.geom
  FROM (SELECT * FROM nora_list ORDER BY random() LIMIT 200) n
  JOIN usa_structures_22 s USING (build_id))
SELECT 0 AS scen, count(*) FILTER (WHERE EXISTS (
         SELECT 1 FROM slr_0ft_22 p WHERE g.geom && p.geom AND ST_Intersects(g.geom,p.geom))) FROM s200 g
UNION ALL SELECT 1, count(*) FILTER (WHERE EXISTS (
         SELECT 1 FROM slr_1ft_22 p WHERE g.geom && p.geom AND ST_Intersects(g.geom,p.geom))) FROM s200 g
UNION ALL SELECT 2, count(*) FILTER (WHERE EXISTS (
         SELECT 1 FROM slr_2ft_22 p WHERE g.geom && p.geom AND ST_Intersects(g.geom,p.geom))) FROM s200 g
ORDER BY 1;
```

**Result, out of 200 sampled:**

| SLR layer | intersecting | scaled to 4,776 | expected |
|----|----|----|----|
| 0 ft | 77 | ~1,840 | 1,882 (the `10111…` set) ✓ |
| 1 ft | 5 | ~119 | 50 (within sampling noise) ✓ |
| **2 ft** | **200** | **4,776** | — |

**Implication.** Every sampled structure falls inside the 2 ft extent. The 0 ft
and 1 ft counts reproduce values already known independently from the flag
patterns, confirming the query behaves correctly. This is not a coverage
difference between two versions of the 1 ft layer — it is a different scenario.

------------------------------------------------------------------------

## 6. How the substitution could occur

`NOAA_SLR_downloader_v3.R` requests LA and TX files under NOAA's naming
convention but writes them locally under the general one, inserting the missing
`_0`:

``` r
if (l_name == "TX" || l_name == "LA") {
  url_part      <- paste0(l_name, "_merged_", vector_type, "_", slr_height, "ft.", extension)
  url_dest_part <- str_replace(url_part, "ft.", "_0ft.")
  dest_path     <- path(download_directory, url_dest_part)
}
```

``` bash
ls /home/nora/Science/Nora_SLR/SLR_downloads_1_ft | grep -i "LA_merged_slr_1"
# LA_merged_slr_1_0ft.shp   (etc.)
```

So the same scenario carries two names depending on provenance:

| Scenario | At NOAA (LA/TX) | On TRIPPER3 |
|----|----|----|
| 1 ft | `LA_merged_slr_1ft.shp` | `LA_merged_slr_1_0ft.shp` |
| 2 ft | `LA_merged_slr_2ft.shp` | `LA_merged_slr_2_0ft.shp` |

Nora reported using `LA_merged_slr_1ft.shp`, which is not the name any file on
TRIPPER3 carries. Working from a mix of sources makes a one-scenario offset
easy. **Not confirmed** — the numbers show which layer was intersected, not
which file was opened.

------------------------------------------------------------------------

## Method notes

-   **Restrict before measuring.** The full-state version of step 4 ran for
    hours; restricted to Nora's 4,776 `build_id` it took minutes. Sampling 200
    in step 5 answered a categorical question in seconds.
-   **Check validity before reaching for `ST_MakeValid`.** It is not
    indexable, is evaluated per candidate polygon, and is the single most
    common cause of a hung session in this project. `slr_1ft_la` needed none.
-   **Watch for the BOM** on anything exported from Excel.
-   **Use `screen`** for anything unbounded, particularly over a remote
    connection. A `reptyr` rescue attempt mid-session cancelled the query it
    was meant to protect.
-   **The decisive clue was in the data, not the geometry.** All four flag
    patterns being flooded from 2 ft up pointed at the answer before any
    spatial query was written.
