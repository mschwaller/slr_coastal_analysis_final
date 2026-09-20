# Pipeline doc: pending edits

Current version: **v4.4**.
Superseded items from the pre-Louisiana-correction list have been struck.

Status key: **[ ]** pending, **[x]** done, **[—]** no longer applicable.

---

## Done in v4.4

- [x] §2.4 tract count at 1 ft: 7,060 → 7,108
- [x] §2.9 flooded structures at 1 ft: 139,868 → 146,431
- [x] §2.9 total flooded structure rows: 17,608,770 → 17,615,333
- [x] §2.9 total tract×scenario rows: 93,525 → 93,573
- [x] §2.10 intro: 2,185 → 303, plus a note that figures were recomputed
- [x] §2.10.1 restructured into Spikes and Dips
- [x] §2.10.1 dip scan added: 4 cases, Table 2.10.1.A
- [x] §2.10.1 SC two-scenario V-shape (`45035010400`)
- [x] §2.10.1 TX degenerate intersection (`48489990000`)
- [x] §2.10.1 symmetry note: false positives under the spike test, a false
      negative in the TX case
- [x] §2.10.1 sliver structure claim softened to what was tested
- [x] §2.10.2 rewritten: 303 structures, no Louisiana
- [x] §2.10.3 errata section removed

## No longer applicable

- [—] §2.10.2 Louisiana rewrite — the section no longer has Louisiana content
- [—] Errata GeoPackage regeneration — retired
- [—] Louisiana positional classes, distance-to-water analysis, Lockport/Larose
      corridor, the 7,867 lower-bound argument
- [—] Verify the "~130 km of the coastline" claim — the sentence is gone
- [—] Class 3 grouped-by-position note — gone with the rewrite

---

## Still pending

### Small corrections

- [ ] **Figure 2.10.1.A caption**: "located 120 west of other vertices" →
      "120 km west".
- [ ] **Ribbon width**: §2.10.1 says "approximately 4 m wide". Measurements give
      7.89 m maximum thickness and ~3.7 m mean width over 120.3 km. State which
      is meant, or give both.
- [ ] **§2.10.2 typo carried over**: check for "identifyies" and "down-weigh" —
      these were in the retired §2.10.3 and should be gone, but worth a grep.
- [ ] **§2.9 GeoPackage sizes**: "23 files, 11 GB" and "23 files, 8.4 MB" were
      measured before the Louisiana rebuild. The LA structures file grew; re-measure
      and update both here and in the §2.7 / §2.8 totals (lines ~1046 and ~1145).

### §2.1 — source data

- [ ] **Where the truncated originals are kept.** The damaged 0225 files are at
      `release_0225/truncated_LA_1ft_0225/`, and the substituted files carry
      `00_README_LA_1ft_substitution.txt` alongside them. State this so the
      deposit is self-describing.
- [ ] **Release comparison (0225 vs 0426).** Atlantic (11 files, 2–11% smaller),
      Florida (11 files, 0.5–6% smaller) and West (11 files, 23–38% *larger*)
      were regenerated between releases; LA, TX and MS_AL are byte-identical.
      Supports the substitution argument and is a useful observation about NOAA
      release stability in its own right.
- [ ] **Null-geometry audit.** A sweep of all 66 regional tables for rows with
      attributes but no geometry found damage in exactly two: `slr_1ft_la`
      (153,617 of 732,810 rows, 21% — the truncation) and `slr_0ft_la` (6 rows).
      Worth reporting, and worth recommending as a standard post-ingest check: it
      verifies what actually landed in the database, which the file-size check
      cannot.
- [ ] **Curiosity, low impact: six null-geometry rows in `slr_0ft_la`.**
      All six carry `gridcode = 9014`, `id` values 1, 2, 5, 6, 30, 31, and
      `shape_area` at or near the layer's median (7.3 × 10⁻¹⁰ sq deg) — typical
      fragments, not outliers, total missing area on the order of square meters.
      The low `id` values argue against truncation at the end of the file. Cause
      not determined; no measurable effect. Note that the LA 0 ft shapefile is
      2,147,483,560 bytes — 88 bytes under the shapefile format's 2 GB limit —
      in both the 0225 and 0426 releases.
- [ ] **Stray Pacific file.** `Pacific_merged_slr_3_0ft.shp` sits in the 0225
      download directory (from an early config listing Pacific in
      `location_names`) but was never ingested. Note it so the file count in the
      deposit reconciles: 67 files present, 66 used.
- [ ] **Scope statement.** The 0426 archive also contains `_low_` polygons plus
      Alaska and Caribbean regions. State explicitly that the pipeline ingests
      `slr` (hydrologically connected) polygons only, and that unconnected
      low-lying areas are excluded by design.

### §2.5 — pre-filter

- [ ] **The union-of-scenarios fix was recommended but not applied.** The
      pipeline still uses `tract_10ft_intersections`
      (`prefilter.tract_table` in `structure_analysis_config_v3.yaml`). Decide
      whether to apply it now or leave it documented as a known limitation.
      Applying it would require reloading structures for the 7 Maine tracts and
      re-running their flooding tables.

### New material

- [ ] **Tract flooding summary table.** Proposed in
      `proposed_tract_flooding_summary_table.md`, pending Nora's comment. If
      adopted it needs its own section, probably after §2.8.

### Deposit

- [ ] **Deposit README**: which release each source file came from, why LA 1 ft
      differs, what `truncated_LA_1ft_0225/` contains, and that the 0225 release
      is no longer retrievable from NOAA.
- [ ] **Decide** whether the 0225 source shapefiles go into the Dryad deposit
      (~33 GB) or a separate archive.
- [ ] **Note on the export directory name**: the GeoPackage exports live under
      `release_0225/`, but the LA 1 ft layer derives from the 0426 file. A README
      at `gpkg_tract_structures_exports/` level would prevent the path name
      implying a purity the contents do not have.

### Configs to restore

- [ ] `structure_analysis_config_v3.yaml` — `states` and `slr_scenarios` are
      scoped to LA / 1 ft (or LA / all scenarios after the export fix). Restore
      the full lists.
- [ ] `tracts_analysis_config_v2.yaml` — `states` scoped to LA. Restore the full
      23-state list.
- [ ] Both configs: export paths now point at
      `~/Science/Nora_SLR/release_0225/gpkg_tract_structures_exports/...`. This
      is a genuine correction and should stay.
- [ ] `create_state_subdivisions` path in `structure_analysis_config_v3.yaml`
      still points at the old `~/claude_projects/slr_analysis/...` repo layout.
