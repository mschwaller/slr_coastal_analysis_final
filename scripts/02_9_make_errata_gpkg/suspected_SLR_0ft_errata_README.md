# suspected_SLR_0ft_errata.gpkg

Accompanies the megaSLR derived dataset.  See Section 2.10.2 of the methods
documentation for the full analysis.

No flags in the published dataset have been altered.  This file records
structures whose flooding flags are believed to be in error, so that users can
exclude or downweight them.

## Layers

- `suspected_errata_structures` (1477 records) - FEMA USA Structures
  footprints, EPSG:5070
- `errata_tracts` (7 records) - Census tract boundaries for context

## Classes

**Class 1, `inland_0ft_false_positive` (1440 structures).**
Structures inside the NOAA SLR 0 ft (MHHW) extent that current satellite
imagery shows to be on dry ground, often several hundred metres from any
watercourse.  Tracts: 22057021500, 22057021700, 22057021602, 22057021603 (Lockport-Larose
corridor, Lafourche Parish).  The SLR 0 ft flooded flag is believed to be a
FALSE POSITIVE.  SLR 1 ft (unflooded) is believed correct; SLR 2 ft and above
correctly re-flag these locations.

**Class 2, `open_water_1ft_false_negative` (37 structures).**
Structures in permanent open water, at measured distance zero from mapped
water, flagged unflooded at SLR 1 ft.  Tracts: 22023990000, 22093040500, 22097960400.
The SLR 1 ft unflooded flag is believed to be a FALSE NEGATIVE; these
structures should be flooded at every scenario.

## Caveats

- Tract 22057021603 contains both dry-ground
  structures and structures on the margin of Lake Fields, which could not be
  separated at tract level.  Its 253 records carry
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

Generated 2026-08-10 by make_slr_0ft_errata_gpkg_v1.R
