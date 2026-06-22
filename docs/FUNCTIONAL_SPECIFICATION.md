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
Excel workbook with **one sheet per scenario** (`Central`, `Adverse`, `Optimistic`, `Extreme`); the
sheet name becomes the scenario and the sheets are unioned. One row per quarter.

| Column | Meaning |
|--------|---------|
| `Date` | Quarter, e.g. `2025Q4` |
| (scenario) | the **sheet name** — `Central`, `Adverse`, `Optimistic`, `Extreme` (`Secto` not yet provided) |
| `IR_10Y_FR`, `IR_10Y_BE`, `IR_10Y_IT` | Macro variables (10Y interest rates per country) |

The shock for a scenario is read along the macro **path** over a per-matrix window from
`as_of_date_quarter` to `as_of_date_quarter + PROJECTION_HORIZON` (the PARAMETRAGE column,
e.g. `2025Q4 + "3Y" = 2028Q4`; a blank cell falls back to `last_quarter_projection_horizon`).

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
| `PROJECTION_HORIZON` | Relative shock horizon (e.g. `3Y`) — sets the FWL=YES shock-window **end** per matrix (`as_of + horizon`); blank → config fallback (see §4.7, §6) |

### 2.3 INPUTS_RA
Excel (sheet per perimeter, e.g. `RA_BCEF`). One row per (perimeter, segment, rate type, FWL type,
metric) — the **perimeter is part of the key**, so a segment name shared across entities (e.g.
`MORTGAGE` in BCEF and BGL) does not collide.

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
| `EAD_CCF_RATE` | **Placeholder column, emitted empty** (CCF rate not computed by this engine yet; reserved so downstream consumers can rely on the column existing). |

Optionally, rows whose `EAD_RA_RATE >= 1` (the *full-exposure* terms — leading terms before any loss
accrues, plus any run-off backstop term) can be **excluded** from the file — see §4.9.

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

### 4.6 Core computation (Central scenario, and every scenario when FWL = NO)
No macro shock applies here, so all four scenarios share one curve. Per period (BASELINE values):
1. Loss rate:
   - **FWL = YES, Central:** `RA_i = -(RA_STAT_i + RA_FI_i + RE_i) / CRD_i`
   - **FWL = NO:** `RA_i = -(RA_STAT_i) / CRD_i` — `RA_FI` and `RE` are **excluded**.
2. `VECTOR_i = 1 - RA_i`.
3. `EAD_RA_RATE = cumulative product of VECTOR` (factored vector).
4. Held flat from term 30.

**Run-off rule:** when `CRD_i = 0` (exposure fully run off) the period contributes
`RA_i = 0` (`VECTOR_i = 1`), so the curve flattens instead of dividing by zero. (A second guard
freezes the curve if a per-period `RA_i ≥ 1` cliff appears in the deep run-off tail.)

For **FWL = NO**, all scenarios equal the Central value (no scenario differentiation).

### 4.7 FWL = YES — scenario shock

The forward-looking scenario adjustment uses **two different models by frequency** (they are separate
code paths and must evolve independently):
- **Quarterly** — a macro-path additive shock on FI+RE (§4.7.1).
- **Yearly** — the OAT-10Y sensitivity model reconstructed from the business workbook (§4.7.2),
  specified in full in [`YEAR_CALCULATION_SPECIFICATION.md`](YEAR_CALCULATION_SPECIFICATION.md).

#### 4.7.1 Quarterly (macro-path additive shock)
For non-Central scenarios the shock is **term-varying**, read from the scenario macro path
over the matrix's window (`as_of_date_quarter .. as_of_date_quarter + PROJECTION_HORIZON`):
term 0 = window start, step 1 quarter (yearly steps 1 year); past the window end (the projection
horizon) the last delta is held.
1. `delta_i = MACRO[scenario][q_i] − MACRO[Central][q_i]` — the raw macro delta (`Rate/100`), per term `i`.
2. Stress leg is fixed by **scenario**: **Adverse / Extreme → STRESS (-)**, **Optimistic → STRESS (+)**.
3. `RA_STAT` stays baseline; only FI + RE are shocked. Per period (`det(x,c) = -x/c`, `0` if `c = 0`):
   - `stat       = det(RA_STAT_base, CRD_base)`
   - `fire_base  = det(RA_FI_base, CRD_base) + det(RE_base, CRD_base)`
   - `shock_FI   = det(RA_FI_leg, CRD_leg) − det(RA_FI_base, CRD_base)`  (each leg uses its **own** CRD)
   - `shock_RE   = det(RE_leg, CRD_leg)   − det(RE_base, CRD_base)`
   - `fire_scen  = fire_base − (shock_FI + shock_RE) · delta_i`
   - `RA_i       = stat + fire_scen`
4. Then `VECTOR`, factored product, flat tail as in §4.6.

The shock is scaled by the macro `delta` (`Rate/100`) when `apply_rate_to_shock = true` (default), so
Adverse ≠ Extreme and the magnitude follows the macro path; with `false` it is applied full-size
(factor `1.0`), making Adverse = Extreme. There is **no `ref_shock` calibration knob**.

#### 4.7.2 Yearly (OAT-10Y sensitivity model)
The yearly path does **not** use the quarterly shock above. It implements the business workbook
(`EDB_EAD_FWD_BCEF_reconstruction.xlsx`): RA STAT and the CRD denominator stay at **baseline**; only
the FI/RE component is adjusted, by each stress leg's per-100bp sensitivity scaled by the **OAT-10Y
spread** (macro `IR_10Y_FR`), with the workbook's `×fire_base_det` factor. Per year:
```
stat_det      = -RA_STAT_base / CRD_base
fire_base_det = -(RA_FI_base + RE_base) / CRD_base
sensFI/sensRE = (-FI_leg/CRD_leg) - (FI_base/CRD_base)   (each leg's own CRD; leg by scenario)
ΔOAT          = (IR_10Y_FR_scen - IR_10Y_FR_central) · 10000     (point-sampled at the year anniversary)
fire_scen_det = fire_base_det · (1 - (sensFI + sensRE) · ΔOAT)
RA_i          = stat_det + fire_scen_det
```
The leg is fixed by scenario (Optimistic → STRESS(+), Adverse/Extreme → STRESS(−)); the **sign of
ΔOAT** gives the direction, so **Adverse ≠ Extreme** (distinct OAT spreads). This reproduces the
workbook to 8 dp (e.g. `BCEF_MORTGAGE_TF_Y;A;1 = 0.90785228`). Full derivation, scaling rationale and
open points: [`YEAR_CALCULATION_SPECIFICATION.md`](YEAR_CALCULATION_SPECIFICATION.md).

### 4.8 Aggregation (e.g. INVEST)
When several PARAMETRAGE rows share an `AGGREGATED_SEGMENT_NAME`, their monthly metric
series are **summed element-wise** before the formula is applied. The combined
`FWL_TO_BE_APPLIED` flag is **YES if any** constituent applies it.

### 4.9 Optional exclusion of full-exposure rows (`EAD_RA_RATE >= 1`)
The survival factor is capped at `1` (and a sub-zero running product is reported as `1`), so an
`EAD_RA_RATE` of exactly `1` marks a **full-exposure** term — a leading term before any loss has
accrued, or a run-off backstop term. When the config flag
`parameters.exclude_ead_ra_rate_ge_1 = true` these rows are **omitted from the output** (default
`false` → every term is emitted). The flag only filters output rows; it does not change any computed
value. See `TECHNICAL_SPECIFICATION.md` §5.

---

## 5. Validation status

- **Central / FWL = NO** matches the reference target to ~`1e-5`
  (e.g. `BCEF_MORTGAGE_Q` term 1: computed `0.993422715` vs target `0.993416733`).
- **Yearly FWL = YES (OAT-10Y model)** reproduces the business workbook
  (`EDB_EAD_FWD_BCEF_reconstruction.xlsx`) to **8 dp** — e.g. `BCEF_MORTGAGE_TF_Y;A;1 = 0.90785228`,
  with the scenario ordering Optimistic > Central > Extreme > Adverse at term 1.
- The computation logic is therefore considered correct. Remaining open items are **data-dependent**
  (quarterly, §8) and yearly **convention** confirmations (see
  [`YEAR_CALCULATION_SPECIFICATION.md`](YEAR_CALCULATION_SPECIFICATION.md) §7).

## 6. Decisions / assumptions

- `PROJECTION_HORIZON` (PARAMETRAGE, per matrix) sets the shock-window **end**: the macro
  shock path runs from `as_of_date_quarter` to `as_of + PROJECTION_HORIZON` and is held flat
  beyond it; a blank cell falls back to `last_quarter_projection_horizon`. (The term-structure
  output still spans the fixed 30y grid; only the shock path stops at the horizon.)
- Computation always emits **both** Q and Y matrices.
- Scenario `S` (Secto) is omitted until data is available.

## 7. Data control & auditability

- **Pre-calculation data control.** Before any computation the engine validates the parsed inputs
  (required columns, the monthly grid, label vocabulary, numeric integrity, FWL=YES stress legs,
  scenario coverage, the shock-window quarters). It logs a consolidated PASS/WARN/FAIL report and
  writes an auditable `DATA_CONTROL_<table>.csv`. When `validation.strict = true` (default) any FAIL
  **aborts the run** before calculation; otherwise it only warns.
- **Analysis generator.** A separate job (`Term0AnalysisDriver`) regenerates the worked computation
  breakdown per *(matrix, scenario, term)* — a Markdown narrative and a CSV — from the same inputs and
  the **same** formulas as production, optionally **reconciled** against the real output (`MATCH` /
  `DIFF` / `MISSING`). It is the auditable, machine-generated equivalent of the manual worked example.

## 8. Open items (data-dependent)

Tracked in [`MISSING_INPUTS.md`](../MISSING_INPUTS.md):
1. **Scenario file** — distinct Adverse vs Extreme now follow the macro path
   (`apply_rate_to_shock = true`); confirm the supplied macro values differentiate them as intended.
2. **INPUTS_RA vintage** — current sample's exposure does not run off like the target,
   causing deep-tail deviation; the 25Q4-matching RA series is needed.

`PARAMETRAGE` has already been corrected (`PARAMETRAGE_corrected.xlsx`).
