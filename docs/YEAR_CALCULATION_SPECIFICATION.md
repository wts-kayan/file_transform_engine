# EAD FWD — Year (Annual) Calculation Specification

Authoritative description of how the engine computes the **yearly** EAD term structure
(`EAD_MATRIX_ID` ending in `_Y`). This documents the **implemented behaviour**, which is the source
of truth: where it intentionally deviates from `Schema_EAD_FWD_20260601_v5.xlsx` (`Annual Freq -
COMPUTATION`), that is called out under [Deviations from the v5 schema](#deviations-from-the-v5-schema).

Core code: **`PrimaryViewYearly`** — the standalone yearly formula core (own aggregation, period loop,
and RA-detail formulas); `PrimaryView` — shared frequency-agnostic primitives (`vectorFactored`,
`termGrid`/`termSeries`, constants) plus the `freq` dispatch that routes the `Yearly` case to
`PrimaryViewYearly`; and `PrimaryMapper` — parsing, leg selection, and the shock window. The quarterly
path lives in `PrimaryView` and is **not** described here; only the parts that differ are noted.

---

## 0. Inputs and the global pre-rule

Per `(PERIMETER, SEGMENT, RATE_TYPE, FWL_TYPE, METRIC)` the input is a monthly series `M1…M361`
(30y horizon = `30·12 + 1`). `FWL_TYPE ∈ {BASELINE, STRESS (+), STRESS (-)}`,
`METRIC ∈ {CRD, RA STAT, RA FI, RE}`.

**Flat extrapolation (schema preamble).** If a series is shorter than `M361`, it is forward-filled
to 361 months holding the last observed value (`PrimaryView.padForward`, `INPUT_MONTHS = 361`).

Branching is driven by `FWL_TO_BE_APPLIED`:

| Flag | Path | Steps |
|------|------|-------|
| `NO`  | BASELINE only | 1 → 2 (RA detail) → 3 (vector) → 4 (factored) |
| `YES` | BASELINE + ADVERSE + EXTREME + OPTIMISTIC | 1 → 2 (shock) → 3 (rate) → 4 (RA detail) → 5 (total) → 6 (vector) → 7 (factored) |

---

## 1. STEP 1 — Annual averaging convention

Each annual value is the **MEAN** of its monthly values over the year window — `SUM(months) /
divisor` — and this applies to **every** metric: CRD **and** RA STAT / RA FI / RE alike.

| Year (term) | Months | Divisor |
|-------------|--------|---------|
| Y1 (term 0) | M1 … M6 | 6 |
| Y2 (term 1) | M7 … M18 | 12 |
| Y3 (term 2) | M19 … M30 | 12 |
| Yₙ (term n−1), n ≥ 2 | M(12n−17) … M(12n−6) | 12 |

> Y1 is a half-year (mid-year start, 6 months); every later year is a full 12-month, non-overlapping
> block. The windows do **not** overlap and the boundary months are **not** half-weighted (that is a
> quarterly-only convention).

Code: `PrimaryViewYearly.aggregate` — `SUM(window months) / len` for all metrics (CRD and RA alike).
A term whose window exceeds the available months yields no value (the series stops there).

> **Why the mean (not a raw sum) matters.** The RA detail is `−RA_metric / CRD`. Because both the
> numerator and the denominator are divided by the same divisor, the divisor cancels and the detail
> equals `−ΣRA_metric / ΣCRD`. Averaging only CRD while leaving RA metrics as raw sums (the previous
> behaviour) inflated every yearly RA by the divisor (×6 in Y1, ×12 in Y2+), which drove the
> non-Central scenarios over the run-off cap and froze the curve. See
> [`docs/OPEN_QUESTIONS.md` Q33].

The term → period → year mapping: `term` is in years (`YEARLY_STEP = 1.0`); period `p = term + 1`
(term 0 = period 1 = Y1).

---

## 2. RA detail per scenario (STEP 2–5)

Notation per year `i`, all aggregates being the STEP-1 annual means:

```
stat_det      = −RA_STAT_base / CRD_base
fire_base_det = −(RA_FI_base + RE_base) / CRD_base
```

### FWL = NO  (`PrimaryViewYearly.statOnlyRa`)
```
RA_i = stat_det                        # RA STAT only; FI and RE are EXCLUDED (schema FWL=NO STEP 2)
```

### FWL = YES, Central  (`PrimaryViewYearly.centralRa`)
```
RA_i = stat_det + fire_base_det        # = −(RA_STAT + RA_FI + RE)_base / CRD_base
```

### FWL = YES, non-Central  (`PrimaryViewYearly.scenarioRa`)
The caller (`PrimaryMapper.matrixRows`) fixes the stress leg **and** the sign by scenario:

| Scenario | Stress leg | `shockSign` |
|----------|-----------|-------------|
| ADVERSE  | STRESS (−) | −1 |
| EXTREME  | STRESS (−) | −1 |
| OPTIMISTIC | STRESS (+) | +1 |

The shock detrend uses **each leg's OWN CRD** (schema STEP 2):
```
shock_fi = (−RA_FI_leg / CRD_leg) − (−RA_FI_base / CRD_base)
shock_re = (−RE_leg   / CRD_leg) − (−RE_base   / CRD_base)
```

The per-year macro delta (STEP 3, see §3):
```
delta_i = (Macro_scen_i − Macro_central_i) · macro_delta_scale
```

RA total (STEP 4 + 5 combined — RA STAT stays BASELINE across all scenarios):
```
RA_i = stat_det + fire_base_det + shockSign · (shock_fi + shock_re) · delta_i
```

So ADVERSE/EXTREME subtract the STRESS(−) shock and OPTIMISTIC adds the STRESS(+) shock.

**Equivalent macro-blended form (how the analysis worked-steps render it).** Because
`shock_fi + shock_re ≡ str − fire`, where `fire = −(FI+RE)/CRD` on **BASELINE** and
`str = −(FI+RE)/CRD` on the scenario's **STRESS leg**, the same RA is:
```
Rate = shockSign · delta_i            # +delta Optimistic, −delta Adverse/Extreme
RA_i = stat_det + fire + Rate · (str − fire)
```
i.e. the baseline FI/RE detrend blended toward the stress-leg detrend by the macro weight. At
`Rate = 0` (Central) it collapses to the baseline detrend. This is the form shown in the
`YearAnalysisDriver` worked blocks because it makes the STRESS(+/−) data explicit; it is
arithmetically identical to the shock form above — `fire_base_det` is still BASELINE.

---

## 3. STEP 3 — Macro delta (shock multiplier)

`delta_i = (Macro_scen_i − Macro_central_i) · macro_delta_scale`, read for the matrix's
`MACRO_VARIABLE` from the scenario file (`PrimaryMapper.macroDeltaArray`). `macro_delta_scale` is the
config knob `tseadfwd_app.macro_delta_scale` (unit conversion of the raw macro difference into the
unit the shock expects).

**Shock window / projection horizon** (`PrimaryMapper.deltaPath`, `shockWindowFor`). The shock applies
from the as-of quarter **up to and including** `as_of + PROJECTION_HORIZON` (per the PARAMETRAGE
column; config fallback otherwise). For a yearly term the delta is sampled 4 quarters per year
(`step = 4`); **past the horizon end the delta is `0`** — the shock stops, it is not held flat.

---

## 4. STEP 6–7 — Vector and vector factored (final EAD TS)

```
VECTOR_i        = 1 − RA_i
EAD_RA_RATE_i   = Π(VECTOR_1 … VECTOR_i)          # cumulative product, per scenario
```
`PrimaryView.vectorFactored` + `termSeries`.

**Grid & run-off.**
- Computed year by year while `term ≤ COMPUTED_HORIZON_Y (30)`; output grid is `0 … FLAT_MAX_Y (50)`
  by 1, plus the tail term `100`. Past the computed horizon the last value is held flat.
- `CRD_i == 0` → `RA_i = 0` (exposure fully run off, `VECTOR = 1`).
- `RA_i ≥ RUNOFF_RA_CAP (1.0)` → treated as run-off: the period is dropped and the curve freezes at
  the last good value (`computeRa` stops). A loss rate ≥ 100% is non-physical.

---

## Worked example — `BCEF_MORTGAGE_TF_Y`, Adverse, term 2 (Y3)

Annual means over M19–M30 (÷12), STRESS(−) leg, `shockSign = −1`:
```
stat_det      = −RA_STAT_base / CRD_base
fire_base_det = −(RA_FI_base + RE_base) / CRD_base
shock_fi      = −RA_FI_leg/CRD_leg + RA_FI_base/CRD_base
shock_re      = −RE_leg/CRD_leg   + RE_base/CRD_base
RA(2)         = stat_det + fire_base_det − (shock_fi + shock_re) · delta_2
VECTOR(2)     = 1 − RA(2)
EAD_RA_RATE(2)= EAD_RA_RATE(1) · VECTOR(2)
```
The `YearAnalysisDriver` job emits this breakdown (Markdown + CSV) for every matrix/scenario/term;
each `EAD_RA_RATE` is reconciled against the production engine output by construction.

---

## Deviations from the v5 schema

The engine is the source of truth; the following are **intentional** differences from
`Schema_EAD_FWD_20260601_v5.xlsx` (`Annual Freq - COMPUTATION`):

1. **Optimistic sign (STEP 4).** The v5 cell shows a `−` for *all three* scenarios. The engine
   **adds** the STRESS(+) shock for Optimistic (`shockSign = +1`) and subtracts it for
   Adverse/Extreme (`shockSign = −1`). The schema's literal all-minus is treated as a transcription
   error.
2. **Rate scaling (STEP 3 / 4).** The v5 text computes `Rate = (Macro_scen − Macro_base) × 100` then
   multiplies by `Rate / 100` (a no-op cancel). The engine instead applies a single configurable
   `macro_delta_scale` to the raw macro difference and does **not** hard-code the ×100 / ÷100.

The averaging convention of §1 (mean of every metric) **matches** the v5 schema and was corrected to
do so for the yearly path.

---

## Cross-references

- Yearly formula core: `src/main/scala/com.bnp.str.tseadfwd/mapping/PrimaryViewYearly.scala`
- Shared primitives + quarterly core + freq dispatch: `…/mapping/PrimaryView.scala`
- Orchestration / leg selection / shock window: `…/mapping/PrimaryMapper.scala`
- Yearly analysis generator: `…/job/YearAnalysisDriver.scala` (config block `YEAR_ANALYSIS`)
- Open items & decision history: `docs/OPEN_QUESTIONS.md` (Q32 FWL=NO, Q33 shock formula/sign)
