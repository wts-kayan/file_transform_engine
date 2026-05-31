# Functional Specification — EAD FWD Term Structure Engine

**Project:** `file_transform_engine` (module `tseadfwd`)
**Purpose:** Automate the computation of the **EAD FWD Term Structure** that the Risk
team currently produces and inputs manually, removing the operational risk of the
manual process. The engine runs inside the TWIST projection.

---

## 1. Overview

The Risk team supplies three inputs in TWIST (Parameters > Projection):

1. **Scenario** (macro-variable projections)
2. **PARAMETRAGE** (computation parameters per perimeter/segment)
3. **INPUTS_RA** (monthly risk-adjustment series)

The engine reads them and produces the final **EAD FWD Term Structure** file: for each
*matrix* (perimeter × segment × frequency), each *scenario* and each *term*, a single
`EAD_RA_RATE` value.

---

## 2. Inputs

### 2.1 Scenario (macro variables)
CSV, `;`-delimited. One row per (date, scenario).

| Column | Meaning |
|--------|---------|
| `Date` | Quarter, e.g. `2025Q4` |
| `scenario` | `Central`, `Adverse`, `Optimistic`, `Extreme` (`Secto` not yet provided) |
| `IR_10Y_FR`, `IR_10Y_BE`, `IR_10Y_IT` | Macro variables (10Y interest rates per country) |

The shock for a scenario is read along the macro **path** over a configured window
(`shock_window_start..shock_window_end`, e.g. `2021Q1..2025Q4`).

### 2.2 PARAMETRAGE
Excel. One row per (perimeter, segment, rate type).

| Column | Meaning |
|--------|---------|
| `PERIMETER` | Entity (BCEF, BGL, BNL, FORTIS, …) |
| `SEGMENT` | Portfolio segment (MORTGAGE, CONSO, INVEST_PRO, …) |
| `RATE_TYPE` | Rate type (`TF`, …) |
| `AGGREGATION` | `YES` → segment is aggregated into `AGGREGATED_SEGMENT_NAME` |
| `AGGREGATED_SEGMENT_NAME` | Target segment name when aggregated (e.g. `INVEST`) |
| `FWL_TO_BE_APPLIED` | `YES`/`NO` — whether forward-looking (scenario) shock applies |
| `MACRO_VARIABLE` | Macro variable driving the shock (e.g. `IR_10Y_FR`) |
| `PROJECTION_HORIZON` | **Not used by the engine** (see §6) |

### 2.3 INPUTS_RA
Excel (sheet per perimeter, e.g. `RA_BCEF`). One row per (segment, rate type, FWL type, metric).

| Column | Meaning |
|--------|---------|
| `PERIMETER`, `SEGMENT`, `RATE_TYPE` | Keys |
| `FWL_TYPE` | `BASELINE`, `STRESS (+)`, `STRESS (-)` |
| `METRIC` | `CRD`, `RA STAT`, `RA FI`, `RE` |
| `M1` … `M361` | **Monthly** time series (M = month), 361 months ≈ 30 years |

---

## 3. Output

`TS_EAD_FWD_25Q4_v1_small.csv` — `;`-delimited, **decimal comma**.

| Column | Description |
|--------|-------------|
| `EAD_MATRIX_ID` | `PERIMETER_SEGMENT_RATETYPE_(Q\|Y)` (see §4.1) |
| `SCENARIO_ID` | `C`/`A`/`O`/`E` (see §4.2) |
| `TERM` | Term in years (see §4.3) |
| `EAD_RA_RATE` | Final factored survival value in `[0,1]` |

---

## 4. Business rules

### 4.1 EAD_MATRIX_ID
`PERIMETER + "_" + SEGMENT + "_" + RATE_TYPE + "_" + (Q|Y)`, e.g. `BCEF_CONSO_TF_Q`:
- `Q` when computation frequency = quarterly, `Y` when annual.
- **Both** Q and Y matrices are produced for every segment.
- `SEGMENT` is the `AGGREGATED_SEGMENT_NAME` when `AGGREGATION = YES` (so `INVEST_PRO` +
  `INVEST_CORP` → `INVEST`).
- `RATE_TYPE` **is included** (per spec). Distinct rate types (e.g. `TF`/`TV`) form separate
  matrices. NB: this differs from the sample target file, which dropped `RATE_TYPE`
  (see `OPEN_QUESTIONS.md` Q1).

### 4.2 Scenario_ID
`C` = Central, `A` = Adverse, `O` = Optimistic, `E` = Extreme, `S` = Secto.
Secto is **not** implemented (no Secto data in the scenario file).

### 4.3 Term structure

> **Recap (M = month, not metric).** `M1…M361` are the *monthly* time series; `METRIC`
> is the separate key column (`CRD`/`RA STAT`/`RA FI`/`RE`). All 361 months are used.
> From 361 months the engine builds:
> - **Quarterly:** 120 computed periods → terms `0, 0.25 … 29.75`; every term from
>   `29.75` onward (`30, 30.25 … 50.25` and `100`) repeats the **term-29.75 value** =
>   **203 rows**.
> - **Yearly:** 30 computed points → terms `0 … 29`; every term from `29` onward
>   (`30, 31 … 50` and `100`) repeats the **term-29 value** = **52 rows**.

The intent is to compute to term **30 years** then hold flat. With exactly 361 input
months the *last computable* term is **29.75** (quarterly) / **29** (yearly) — the
quarterly window for term 30 would need month 362 — so the plateau value is the
last-computed value, and all later grid terms (up to 50 and the single `100`) carry it.
A longer input vintage would simply shift the last-computed term to 30 without changing
the flat-tail behaviour.

### 4.4 The four computation cases
Per the specification, `EAD_RA_RATE` is replicated for four cases:

| Case | FWL_TO_BE_APPLIED | Frequency |
|------|-------------------|-----------|
| 1 | NO | Quarterly |
| 2 | NO | Yearly |
| 3 | YES | Quarterly |
| 4 | YES | Yearly |

For each case the value is computed for every
`PERIMETER × SEGMENT × RATE_TYPE × FWL_TYPE × METRIC × SCENARIO × TERM`.

### 4.5 Period aggregation (monthly → quarter / year)

**Quarterly** (term step 0.25):
- RA metrics (`RA STAT`, `RA FI`, `RE`) — half-weight overlapping window:
  - `Q1 = M1 + M2/2`
  - `Qn = M[3n-4]/2 + M[3n-3] + M[3n-2] + M[3n-1]/2` (n ≥ 2)
- `CRD` — block average: `Qi = mean(M[3i-2], M[3i-1], M[3i])`.

**Yearly** (term step 1):
- RA metrics — **sum** over the window (`Y1` = 6 months, `Yn` = 12 months).
- `CRD` — **mean** over the same window.

### 4.6 Core computation (Central, and all FWL = NO scenarios)
1. `RA_i = -(RA_STAT_i + RA_FI_i + RE_i) / CRD_i` (BASELINE values).
2. `VECTOR_i = 1 - RA_i`.
3. `EAD_RA_RATE = cumulative product of VECTOR` (factored vector).
4. Held flat from term 30.

**Run-off rule:** when `CRD_i = 0` (exposure fully run off) the period contributes
`RA_i = 0` (`VECTOR_i = 1`), so the curve flattens instead of dividing by zero.

For **FWL = NO**, all scenarios equal the Central value (no scenario differentiation).

### 4.7 FWL = YES — scenario shock (macro path)
For non-Central scenarios the shock is **term-varying**, read from the scenario macro path
over a window (`shock_window_start..shock_window_end`, default `2021Q1..2025Q4`):
term 0 = window start, step 1 quarter (yearly steps 1 year); past the window end the last
delta is held.
1. `delta_i(scenario) = MACRO[scenario][date_i] − MACRO[Central][date_i]` (per term `i`).
2. Stress leg = `STRESS (-)` if `delta_i < 0`, else `STRESS (+)`.
3. Per period:
   - `stat_detail = -RA_STAT_baseline / CRD`
   - `fire_base = -(RA_FI_baseline + RE_baseline) / CRD`
   - `fire_stress = -(RA_FI_stress + RE_stress) / CRD`
   - `weight = |delta| / ref_shock`
   - `fire_scen = fire_base + weight × (fire_stress − fire_base)`
   - `RA_i = stat_detail + fire_scen`
4. Then `VECTOR`, factored product, flat tail as in §4.6.

`ref_shock` is the magnitude the STRESS legs represent; it is a configurable
**calibration** parameter (see §7).

### 4.8 Aggregation (e.g. INVEST)
When several PARAMETRAGE rows share an `AGGREGATED_SEGMENT_NAME`, their monthly metric
series are **summed element-wise** before the formula is applied. The combined
`FWL_TO_BE_APPLIED` flag is **YES if any** constituent applies it.

---

## 5. Validation status

- **Central / FWL = NO** matches the reference target to ~`1e-5`
  (e.g. `BCEF_MORTGAGE_Q` term 1: computed `0.993422715` vs target `0.993416733`).
- The computation logic is therefore considered correct.

## 6. Decisions / assumptions

- `PROJECTION_HORIZON` is **not** used; the projection uses a fixed term horizon (30y)
  and reads the scenario shock along the macro path over the configured window
  (`shock_window_start..shock_window_end`).
- Computation always emits **both** Q and Y matrices.
- Scenario `S` (Secto) is omitted until data is available.

## 7. Open items (data-dependent)

Tracked in [`MISSING_INPUTS.md`](../MISSING_INPUTS.md):
1. **Scenario file** — current sample has Adverse and Extreme *identical*, so distinct
   A vs E cannot be produced; and the `ref_shock` calibration constant is needed.
2. **INPUTS_RA vintage** — current sample's exposure does not run off like the target,
   causing deep-tail deviation; the 25Q4-matching RA series is needed.

`PARAMETRAGE` has already been corrected (`PARAMETRAGE_corrected.xlsx`).
