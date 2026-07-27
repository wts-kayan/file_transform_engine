# EAD FWD — Year (Annual) Calculation Specification

> # ⛔ SUPERSEDED — DO NOT USE
> **This document describes the OLD *pure-stress* yearly model, which the engine NO LONGER implements.**
> The yearly non-Central path was rewritten to the **OAT-10Y forward-looking sensitivity model**
> (reconstructed from `docs/EDB_EAD_FWD_BCEF_reconstruction.xlsx`). This file is kept only as
> historical reference for the prior behaviour.
>
> **Authoritative spec → [`docs/YEAR_CALCULATION_SPECIFICATION.md`](./YEAR_CALCULATION_SPECIFICATION.md).**
>
> Everything below that concerns **non-Central scenarios** is OBSOLETE and contradicts the engine:
> the engine now keeps RA STAT and the CRD denominator at **baseline**, adjusts only FI/RE by an
> OAT-spread-scaled sensitivity, and produces **Adverse ≠ Extreme** — the opposite of the
> pure-stress claims here. The shared parts (STEP-1 aggregation, FWL=NO, Central, VECTOR, cumulative
> product, grid/run-off) are still accurate.

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

| Flag | Path | Steps (yearly) |
|------|------|-------|
| `NO`  | BASELINE only | 1 (annual mean) → RA = −RA_STAT/CRD → VECTOR → factored |
| `YES` | Central (BASELINE) + ADVERSE/EXTREME (STRESS−) + OPTIMISTIC (STRESS+) | 1 (annual mean of the scenario's leg) → RA = −(STAT+FI+RE)/CRD on that leg → VECTOR → factored |

> The yearly non-Central path is **pure stress** (each scenario computed on its own leg), so the
> schema's separate shock (STEP 2) and rate (STEP 3) steps do not apply here — see §2 and
> [Deviations](#deviations-from-the-v5-schema).

---

## 1. STEP 1 — Annual aggregation convention

The two metric kinds are combined **differently** over the year window:
- **CRD** → **MEAN**, `SUM(window) / divisor` (the average exposure over the year).
- **RA STAT / RA FI / RE** → **raw SUM** over the window (the annual loss; no divisor).

So the RA detail `−RA_metric / CRD` is **annual-loss-SUM ÷ average-exposure**.

| Year (term) | Months | CRD divisor | RA metrics |
|-------------|--------|-------------|------------|
| Y1 (term 0) | M1 … M6 | 6 | SUM(M1..M6) |
| Y2 (term 1) | M7 … M18 | 12 | SUM(M7..M18) |
| Y3 (term 2) | M19 … M30 | 12 | SUM(M19..M30) |
| Yₙ (term n−1), n ≥ 2 | M(12n−17) … M(12n−6) | 12 | SUM over window |

> Y1 is a half-year (mid-year start, 6 months); every later year is a full 12-month, non-overlapping
> block. The windows do **not** overlap and the boundary months are **not** half-weighted (that is a
> quarterly-only convention).

Code: `PrimaryViewYearly.aggregate` — `if (isCrd) SUM(window)/len else SUM(window)`.
A term whose window exceeds the available months yields no value (the series stops there).

> **Sum-of-RA vs mean (history).** RA detail is `−RA_metric / CRD`. With RA metrics summed and CRD
> averaged, the loss rate is `−ΣRA / (ΣCRD/divisor)` = `−divisor·ΣRA / ΣCRD` — i.e. an *annual* loss
> rate (≈ divisor × a monthly rate). An earlier iteration averaged **every** metric (so the divisor
> cancelled to `−ΣRA/ΣCRD`); that produced a much smaller per-month-style rate and is what this
> change reverses. The non-Central freeze that the all-mean version avoided is here sidestepped
> instead by the **pure-stress** non-Central model (§2), which carries no macro shock to amplify. See
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

### FWL = YES, non-Central  (`PrimaryViewYearly.scenarioRa`) — **PURE STRESS**

> **Yearly-specific.** The quarterly path uses a baseline + macro-weighted shock (see `PrimaryView`);
> the yearly path does **not**. This is the deliberate divergence the standalone `PrimaryViewYearly`
> exists for.

The scenario's loss rate is computed **entirely on its own stress leg** — exactly `centralRa` applied
to the leg's metrics:
```
RA_i = −(RA_STAT_leg + RA_FI_leg + RE_leg) / CRD_leg
```

| Scenario | Stress leg |
|----------|-----------|
| ADVERSE  | STRESS (−) |
| EXTREME  | STRESS (−) |
| OPTIMISTIC | STRESS (+) |

Consequences:
- **No baseline term and no macro shock/Rate** — the macro variable does **not** enter the yearly
  FI/RE, and the per-year `delta`/`macro_delta_scale` are unused on the yearly path (§3 applies to
  quarterly only).
- **ADVERSE == EXTREME** (both read STRESS(−)).
- **RA STAT is taken from the leg**, not baseline — `PrimaryMapper` reads the leg's RA STAT
  (`series(legFwl, METRIC_RA_STAT)`) for this path. (In the sample the STRESS lines carry the same
  RA STAT as baseline, so this matches numerically, but it is genuinely sourced from the leg.)

`PrimaryMapper.matrixRows`/`termRowsFor` dispatch this by frequency: `Yearly` → the pure-stress call
above; everything else → the quarterly `PrimaryView.scenarioRa`.

---

## 3. STEP 3 — Macro delta (shock multiplier) — *quarterly only*

> Not used on the yearly path (pure stress, §2). This section describes the quarterly shock and the
> shared `deltaPath`/shock-window code, retained here for reference.

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

## Worked example — `BCEF_MORTGAGE_TF_Y`, Adverse, term 1 (Y2)

Pure stress: annual means over M7–M18 (÷12), **all metrics from the STRESS(−) leg**:
```
RA(1)         = −(RA_STAT(−) + RA_FI(−) + RE(−)) / CRD(−)
              = −(337.292230 + 252.313760 + 151.388256) / −86377.517513 = 0.00857855
VECTOR(1)     = 1 − 0.00857855 = 0.99142145
EAD_RA_RATE(1)= EAD_RA_RATE(0) · VECTOR(1) = 0.99507015 · 0.99142145 = 0.98653389
```
The `YearAnalysisDriver` job emits this breakdown (Markdown + CSV) for every matrix/scenario/term;
each `EAD_RA_RATE` is reconciled against the production engine output by construction (84/84 MATCH on
the sample).

---

## Deviations from the v5 schema

The engine is the source of truth; the following are **intentional** differences from
`Schema_EAD_FWD_20260601_v5.xlsx` (`Annual Freq - COMPUTATION`):

1. **Pure-stress non-Central (STEP 2–5).** The v5 schema builds non-Central RA_FI_RE as
   `RA_FI_RE(BASELINE) − shock(STRESS)·Rate/100` — a baseline detail plus a macro-weighted shock.
   The yearly engine instead computes each non-Central scenario **entirely on its stress leg**:
   `RA = −(RA_STAT + RA_FI + RE)/CRD` from STRESS(−) (Adverse/Extreme) or STRESS(+) (Optimistic). So
   on the yearly path the **macro variable / `Rate` plays no part**, **ADVERSE == EXTREME**, and
   **RA STAT comes from the leg** (not baseline as schema STEP 5 says). *(Quarterly still follows the
   schema's baseline + shock·Rate form.)*
2. **Optimistic sign (quarterly only).** In the quarterly shock the engine **adds** the STRESS(+)
   shock (`shockSign = +1`) and subtracts STRESS(−) (`−1`); the v5 cell shows `−` for all three.
   (Moot on the yearly path, which has no shock term.)
3. **Rate scaling (quarterly only).** The v5 text does `Rate = (Macro_scen − Macro_base) × 100` then
   `× Rate/100` (a no-op cancel); the quarterly engine applies a single configurable
   `macro_delta_scale` instead. (Moot on the yearly path.)
4. **RA metrics summed, not averaged (STEP 1).** The v5 example writes `RA_STAT_Y1 = SUM(M1..M6)/6`
   (a mean). The engine instead **sums** the RA flow metrics (RA STAT/FI/RE) over the window and only
   **averages CRD**, so RA = annual-loss-SUM ÷ average-exposure (an annual rate, ≈ divisor × a monthly
   rate). See §1.

---

## Cross-references

- Yearly formula core: `src/main/scala/com.bnp.str.tseadfwd/mapping/PrimaryViewYearly.scala`
- Shared primitives + quarterly core + freq dispatch: `…/mapping/PrimaryView.scala`
- Orchestration / leg selection / shock window: `…/mapping/PrimaryMapper.scala`
- Yearly analysis generator: `…/job/YearAnalysisDriver.scala` (config block `YEAR_ANALYSIS`)
- Open items & decision history: `docs/OPEN_QUESTIONS.md` (Q32 FWL=NO, Q33 shock formula/sign)
