# Analysis — `BCEF_CONSO` term‑0 mismatch vs target

## Symptom

| Source | Row | EAD_RA_RATE (term 0) |
|--------|-----|----------------------|
| Target (`TS_EAD_FWD_25Q4_v1_small.csv`) | `BCEF_CONSO_Q;C;0`        | `0,987451416` |
| Engine output                            | `BCEF_CONSO_TF_Q;C;0`    | `0,987377015` |

Absolute difference `0.987451416 − 0.987377015 = 0.000074401` (~`7.5e‑5`, relative).
The curve **shape** is correct; only the trailing decimals differ.

> Note the id also differs (`BCEF_CONSO_Q` vs `BCEF_CONSO_TF_Q`): the engine deliberately
> includes `RATE_TYPE` in `EAD_MATRIX_ID` per the spec, while the sample target dropped it.
> That is a naming choice (see Q1 / Q19), **not** the numeric cause analysed here.

## Root cause: input‑data vintage, not a formula bug

Term 0 is quarter 1. The validated formulas for the first quarter are:

```
CRD_Q1     = mean(M1, M2, M3)          (block average)
RA_STAT_Q1 = M1 + M2/2                 (half‑weight window, q = 1)
RA_FI_Q1   = M1 + M2/2
RE_Q1      = M1 + M2/2
RA_Q1      = -(RA_STAT_Q1 + RA_FI_Q1 + RE_Q1) / CRD_Q1
EAD(t0)    = VECTOR_FACTORED_Q1 = 1 − RA_Q1
```

### CONSO baseline inputs in the provided `Inputs_RA.xlsx` (sheet `RA_BCEF`)

| Metric  | M1    | M2    | M3    |
|---------|-------|-------|-------|
| CRD     | −8128 | −7962 | −7795 |
| RA STAT | 70    | 61    | 59    |
| RA FI   | 0     | 0     | 0     |
| RE      | 0     | 0     | 0     |

### Hand calculation from those inputs

```
CRD_Q1     = (−8128 − 7962 − 7795) / 3 = −7961.6667
RA_STAT_Q1 = 70 + 61/2                 = 100.5
RA_FI_Q1   = 0 ;  RE_Q1 = 0
RA_Q1      = −(100.5 + 0 + 0) / −7961.6667 = 0.012622985
EAD(t0)    = 1 − 0.012622985              = 0.987377015
```

This equals the engine output **to the last digit** (`0,987377015`). So the engine is
computing the formula **correctly** from the data it was given. This is not the locale bug
and not a formula error.

### Why the target differs

Same formula, different underlying numbers. To produce the target `0.987451416`, the term‑0
`RA` would have to be `1 − 0.987451416 = 0.012548584`. With the same CRD that requires:

```
RA_STAT_Q1 (target) = 0.012548584 × 7961.6667 ≈ 99.91
```

i.e. the target's CONSO `RA STAT` (and/or `CRD`) monthly series are slightly different
(`≈ 99.9` vs our `100.5`). **The provided `Inputs_RA.xlsx` is a different / stale vintage than
the file that generated the target.**

## Consistent with prior findings

- `BCEF_MORTGAGE_Q` Central matches the target to ~`1e‑5` (its monthly series happen to align).
- `BCEF_CONSO_Q` drifts at the ~5th decimal here (`7.4e‑5`) because its series do **not** align.
- The deep tail diverges further because the provided CONSO exposure runs off differently than
  the target's (e.g. MORTGAGE CRD is still `−1860` at month 360 in this file; the target
  plateaus from term 30).

This is the documented input‑vintage mismatch (see [`MISSING_INPUTS.md`](../MISSING_INPUTS.md),
Item 2).

## Does the decimal separator (comma) matter?

Concern raised: *the RA values are decimals and the decimal separator is a comma.*

Checked directly in the file — it does **not** affect the result:

- **The values are stored as numbers, not text.** Every value cell is `t="n"` (e.g. CONSO CRD
  = `-8128, -7962, -7795, …`; RA STAT = `70, 61, 59, …`), stored canonically. There is **no
  comma anywhere** in the sheet data (`inlineStr` cells are only the text labels). The comma is
  Excel's **French display locale**, not the stored value; spark-excel reads the underlying
  number (`-8128`), never the string `"-8,128"`.
- **`usePlainNumberFormat=true`** (commit `8efdf24`) makes POI render numeric cells invariantly
  (`.` decimal, no grouping), so locale display never reaches the computation.
- **`RA` is a ratio, so a uniform decimal scale cancels.** Even if every value were `1/1000` of
  the stored integer (comma = decimal): `RA_Q1 = -(0.1005)/(-7.961667) = 0.012623`, giving
  `EAD = 0.987377015` — **identical**. The `÷1000` cancels between numerator and denominator.

**The only failure mode** is a value arriving as *text* `"70,5"` on a build **without**
`usePlainNumberFormat`: the old `toDouble` strips the comma → `705` (10× wrong). That is already
neutralised by the `usePlainNumberFormat` fix. Ensure the BNP build includes commit `8efdf24`;
verify with `debug=true` that the `TRACE` shows sensible magnitudes (`CRD≈-7961.67`,
`RA_STAT≈100.5`), not values 10×/1000× off or zero.

## Conclusion

- **No code change required.** Formula, period aggregation, the locale fix, and the RA
  key‑trim are all correct — verified against a by‑hand calculation that reproduces the engine
  output exactly.
- **Exact byte match is blocked on data:** it needs the correct‑vintage `RA_BCEF` monthly
  series — the one that actually produced `TS_EAD_FWD_25Q4_v1_small.csv` (exposure that runs
  off like the target). That is Item 2 in `MISSING_INPUTS.md` and the RA‑vintage business ask.

## How to reproduce this check

1. Set `debug = true` in `localRun/tseadfwd/application.conf`, run, and read the
   `TRACE - BCEF_CONSO_TF_Q / C` table — period 1 shows `CRD = −7961.67`, `RA_STAT = 100.5`,
   `RA = 0.012623`, `EAD_RA_RATE = 0.987377015`.
2. Or recompute the four lines above from the M1–M3 table by hand.
