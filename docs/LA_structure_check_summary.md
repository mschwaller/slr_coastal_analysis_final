---
editor_options: 
  markdown: 
    wrap: 72
---

# LA structure check 

Regarding the 4,776-structure list. I ran a check against the database
and I think the discrepancy comes down to which SLR layer got
intersected.

Every one of your 4,776 structures falls inside the **2 ft** extent.
Almost none fall inside the 1 ft extent:

| SLR layer | Your structures that intersect it |
|-----------|-----------------------------------|
| 0 ft      | \~1,880                           |
| 1 ft      | \~50                              |
| **2 ft**  | **4,776 (all of them)**           |

Consistent with that, all 4,776 flagged flooded at 2 ft and above are in
the database, and they carry only four flag patterns:

| pattern (0→10 ft) | count | notes |
|------------------------|------------------------|------------------------|
| `00111111111` | 2,844 | dry at 0 and 1 ft, flooded 2 ft up — monotonic, no problem |
| `10111111111` | 1,882 | the Lockport/Larose anomaly we already knew about |
| `01111111111` | 48 | flooded from 1 ft up — monotonic, correctly flagged |
| `11111111111` | 2 | flooded at every level — correctly flagged |

So of the 4,776, only the 1,882 are genuinely non-monotonic. The other
2,894 behave exactly as they should.

I checked the database and the pipeline that generated the gpkg files is
internally consistent. Every structure in the database that intersects
the 1 ft layer is flagged at 1 ft, and every structure flagged at 1 ft
intersects it with no gaps in either direction. I also confirmed that
all 4,776 of your structures are in our loaded structure database table,
so nothing was excluded upstream. And the raw NOAA regional file in
consistent with the per-state version that I had to pre-process, so
nothing was lost in that pre-processing step.

That leaves the layer itself as the explanation. My best guess is a
filename mix-up. I'm guessing that you used the 2 ft file instead of the
1 ft file to do the intersection. This mix-up may be due to a NOAA file
naming inconsistently. Most regions use:

```         
<location>_merged_<class>_<height>_0ft.shp     e.g. Atlantic_merged_slr_1_0ft.shp
```

but **Louisiana and Texas drop the `_0`**:

```         
<location>_merged_<class>_<height>ft.shp       e.g. LA_merged_slr_1ft.shp
```

So in NOAA's original LA files, `LA_merged_slr_1ft.shp` is the 1 ft
layer and `LA_merged_slr_2ft.shp` is the 2 ft layer. But on TRIPPER3 the
LA and TX files appear as `LA_merged_slr_1_0ft.shp` ... I renamed them
on ingest to harmonize the naming with the other regions.

If you have a mix of original NOAA downloads and files from TRIPPER3, it
would be very easy to end up one scenario off. That's consistent with
what the numbers show.

## Suggested next steps

1.  **Check which file you actually loaded.** If it was
    `LA_merged_slr_2ft.shp` (or anything with a `2` in the height
    position), that fully explains the result and there's nothing
    further to investigate.

2.  **Re-try your analysis with TRIPPER3 copies rather than what you've
    got.** These are the exact files the published flags were computed
    from, so results will be directly comparable. They're in
    `/home/nora/Science/Nora_SLR/SLR_downloads_1_ft/`, named
    `LA_merged_slr_1_0ft.*` — note the `_0`, which is the renamed form.
    Happy to copy any other scenarios you need.

3.  **If you'd rather use original NOAA files**, that's fine — just be
    aware the LA and TX filenames won't have the `_0`, so
    `LA_merged_slr_1ft.shp` is the 1 ft layer. Worth spot-checking
    against the TRIPPER3 copy before relying on it.

4.  **The 1,882 are the real finding** and are already written up.
    They're flooded at 0 ft, dry at 1 ft, and flooded from 2 ft up — the
    0 ft flag looks to be the wrong one, and the details are in
    `LA_slr_0ft_anomaly_review.gpkg` and Section 2.10.2 of the methods
    doc.

I added a note about the LA/TX renaming to the pipeline documentation so
we can remember this for next time.
