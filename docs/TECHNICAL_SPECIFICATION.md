# Technical Specification — EAD FWD Term Structure Engine

**Module:** `com.bnp.str.tseadfwd`
**Stack:** Scala 2.12.18, Apache Spark 3.5.4, Maven, Typesafe Config, spark-excel.

This document describes the implementation. For the business rules see
[`FUNCTIONAL_SPECIFICATION.md`](FUNCTIONAL_SPECIFICATION.md).

---

## 1. Design principle

The inputs are **small** reference series (a few dozen rows × 361 months). The heavy
numeric work is therefore done **on the driver** with plain Scala collections
([`PrimaryView`](#32-primaryview--computation-core)); Spark is used only for IO
(reading Excel/CSV, writing the output CSV). This keeps the formulas testable and
debuggable without distributed-execution noise.

---

## 2. Architecture / data flow

```
MainDriver
  ├─ SparkSessionManager.fetchSparkSession
  ├─ read application.conf (HDFS reader)
  ├─ PrimaryReader            → reads RA_BCEF (Excel), MACRO_VARIABLE (Excel, per-scenario sheets), PARAMETRAGE (Excel)
  ├─ PrimaryRunner            → wires reader → mapper
  │    └─ PrimaryMapper.getDataFrame
  │         ├─ parse PARAMETRAGE  → matrix definitions (aggregation, FWL flag, macro var)
  │         ├─ collect RA + scenario to driver maps
  │         ├─ per matrix × {Q,Y} × scenario:
  │         │     PrimaryView aggregation → RA → vector-factored → flat tail
  │         └─ build output DataFrame (decimal-comma strings)
  └─ PrimaryWriter / PrimaryUtilities.writeDataframe
        └─ write CSV (1 partition) + collapse part-file → single clean CSV
```

| File | Responsibility |
|------|----------------|
| `job/MainDriver.scala` | Entry point; builds session, reads config, runs pipeline, writes output |
| `sessionmanager/SparkSessionManager.scala` | SparkSession factory (local config) |
| `reader/PrimaryReader.scala` | Reads RA / scenario / PARAMETRAGE via Typesafe config |
| `common/RunnerProvider.scala`, `common/PrimaryRunner.scala` | Lazy inputs; invoke mapper |
| `common/MapperProvider.scala` | Mapper interface (`getMapping_tseadfwd`) |
| `mapping/PrimaryView.scala` | **Pure computation core** (no Spark) |
| `mapping/PrimaryMapper.scala` | Spark glue: parse, aggregate, compute, build output DF, debug trace |
| `writer/PrimaryWriter.scala`, `utility/PrimaryUtilities.scala` | Write + single-file collapse, Excel/CSV readers, HDFS helpers |
| `utility/PrimaryConstants.scala` | Column/value names, scenario codes, output columns |

---

## 3. Key components

### 3.1 Term & metric model

> **Recap (M = month, not metric).** `M1…M361` are the *monthly* columns; `METRIC` is
> the separate key column (`CRD`/`RA STAT`/`RA FI`/`RE`). `collectRa` reads **all 361
> months**; any "first N months" shown in debug is display-only. From 361 months:
> - **Quarterly:** 120 computed periods (terms `0 … 29.75`); terms `30 … 50.25` and `100`
>   all repeat the term-29.75 value → 203 rows.
> - **Yearly:** 30 computed points (terms `0 … 29`); terms `30 … 50` and `100` all repeat
>   the term-29 value → 52 rows.
>
> `computeRa` stops when the aggregation window exceeds the 361 available months (the
> quarterly window for term 30 needs month 362) or when term > 30, whichever comes first.
> The last computable term is therefore 29.75 (Q) / 29 (Y); `termSeries` holds that value
> flat for every later grid term.

### 3.2 `PrimaryView` — computation core

Pure functions, no Spark dependency:

| Function | Purpose |
|----------|---------|
| `Frequency` (`Quarterly`/`Yearly`) | suffix, term step, core max term |
| `termGrid(freq)` | output term grid (0..coreMax by step, then `100`) |
| `aggregate(m, period, freq, isCrd)` | monthly → period value; `Option` (None if window exceeds data) |
| `centralRa(crd, raStat, raFi, re, freq)` | per-period `RA = -(STAT+FI+RE)/CRD`; run-off guard `CRD==0 → 0` |
| `scenarioRa(... both stress legs, deltaAt, refShock)` | FWL=YES RA; `deltaAt(period)` selects leg + weight per term |
| `vectorFactored(ra)` | cumulative product of `1 - RA` |
| `termSeries(vf, freq)` | maps the output grid to `vf`, holding the last value flat |

Aggregation windows (1-based months):
- Quarterly RA metrics: `Q1 = M1 + M2/2`; `Qn = M[3n-4]/2 + M[3n-3] + M[3n-2] + M[3n-1]/2`.
- Quarterly CRD: `mean(M[3i-2], M[3i-1], M[3i])`.
- Yearly RA metrics: sum (Y1 = 6 months, Yn = 12); Yearly CRD: mean of same window.

`computeRa` stops a series when the window exceeds available months or term > 30; the
flat tail in `termSeries` covers the remaining grid points.

> The **complete arithmetic** — segment aggregation, period windows, the Central and
> FWL=YES `RA` formulas, the macro delta path, both run-off guards (`CRD==0` and the
> `RA≥1` cliff freeze), the survival product, the term grid, and number formatting — is
> consolidated in [§4 Arithmetic rules](#4-arithmetic-rules--complete-reference).

### 3.3 `PrimaryMapper` — orchestration

- `parseParametrage` — groups rows by `(perimeter, output-segment, rate-type)`; output
  segment = `AGGREGATED_SEGMENT_NAME` if aggregated else `SEGMENT`. Combined
  `fwlApplied = any(YES)`; `macroVar` = first non-`NONE`. Matrix id =
  `PERIMETER_SEGMENT_RATETYPE_(Q|Y)`.
- `collectRa` — `(SEGMENT, RATE_TYPE, FWL_TYPE, METRIC) → Array[Double]` of months.
- `aggregateSegments` — element-wise sum of constituent segments' monthly series.
- `collectScenario` — `(scenario, Date) → (macroVar → Double)`.
- `shockWindow` / `macroDeltaArray` / `deltaPath` — build the macro delta path
  (`scenario − Central`) over `shock_window_start..shock_window_end`, then map a 1-based
  projection period to it (term 0 = window start, step 1 quarter; yearly step = 4 quarters;
  held past the window end).
- `matrixRows` — selects `centralRa` (FWL=NO or Central) or `scenarioRa` (FWL=YES,
  non-Central), then `vectorFactored` + `termSeries`; emits output rows.
- `fmtNumber` — decimal-comma formatting (half-up, trailing zeros stripped; non-finite → `0`).

### 3.4 Number parsing — IMPORTANT
`spark-excel` (with `inferSchema=false`) returns **locale-formatted strings with
thousands separators** (e.g. `M1` = `"-8,128"`, not `-8128`). Inputs use `.` as the
decimal mark, so `toDouble` **strips commas** (thousands separators). Replacing comma
with dot would corrupt every large value ~1000× and blow up the cumulative product.

### 3.5 Output write + single-file collapse
`PrimaryUtilities.writeDataframe` writes the DataFrame as CSV (`;`, header,
`numPartition` partitions) to `$tmpPath/$tableName`, then — when `singleFile = true`
(default) — `collapseToSingleFile` moves the single `part-*` file to
`$tmpPath/$tableName.csv`, deletes the Spark directory and the `.crc` sidecar, leaving
one clean file. The decimal comma is safe because the field delimiter is `;`
(no quoting triggered).

---

## 4. Arithmetic rules — complete reference

This is the full set of computations the engine performs, in order. Notation: monthly
inputs `M1…M361` (1-based); period `p` (1-based); `term = (p-1)·step` with `step = 0.25`
(Quarterly) or `1.0` (Yearly). All inputs are read from the **BASELINE** leg unless a
**STRESS (+)/(-)** leg is named.

### 4.1 Segment aggregation (before period aggregation)
For an aggregated matrix (e.g. `INVEST = INVEST_PRO + INVEST_CORP`), each metric's monthly
series is the **element-wise sum** of the constituent segments' monthly series of the same
`RATE_TYPE`, computed per month **before** any period aggregation:
```
series_metric[m] = Σ_segment  RA[(segment, rateType, fwl, metric)][m]      (m = 1..361)
```

### 4.2 Period aggregation (monthly → period)  `aggregate(...)`
**Quarterly** (`step = 0.25`):
- RA metrics (`RA STAT`, `RA FI`, `RE`) — half-weight overlapping window:
  - `Q1 = M1 + M2/2`
  - `Qn (n≥2) = M[3n-4]/2 + M[3n-3] + M[3n-2] + M[3n-1]/2`
- `CRD` — block average of 3 months: `Qi = (M[3i-2] + M[3i-1] + M[3i]) / 3`

**Yearly** (`step = 1.0`):
- window: `Y1` = months `M1…M6` (6 months); `Yn (n≥2)` = 12 months starting `M[6+12(n-2)+1]`
- RA metrics = **sum** over the window; `CRD` = **mean** over the same window (÷6 for Y1, ÷12 for Yn)

**Window validity:** a period value exists only if every month index in its window is within
`1..361`; otherwise the period is `None` (the series stops — see §4.6).

### 4.3 Per-period loss rate `RA`
**Central scenario, and every scenario when FWL=NO** (`centralRa`):
```
RA_p = -(RA_STAT_p + RA_FI_p + RE_p) / CRD_p
```
**Non-Central scenario when FWL=YES** (`scenarioRa`) — only FI+RE are shocked, RA_STAT stays baseline:
```
delta_p    = deltaAt(p)                         (signed macro delta, §4.4)
leg        = STRESS(-) if delta_p < 0 else STRESS(+)
statDetail = -RA_STAT_base_p / CRD_p
fireBase   = -(RA_FI_base_p + RE_base_p) / CRD_p
fireStress = -(RA_FI_leg_p  + RE_leg_p ) / CRD_p
w          = |delta_p| / ref_shock              (0 if ref_shock = 0)
RA_p       = statDetail + fireBase + w·(fireStress - fireBase)
```

### 4.4 Macro delta path (FWL=YES)  `macroDeltaArray` / `deltaPath`
```
shockWindow      = ordered quarters [shock_window_start .. shock_window_end]
macroDeltaArray  = [ MACRO[scenario][q] - MACRO[Central][q]  for q in shockWindow ]   (missing → 0)
deltaPath(p)     = macroDeltaArray[ min((p-1)·s, len-1) ]      s = 1 (Q) or 4 (Y); empty → 0
```
Term 0 = window start; the last delta is **held flat** for periods past the window end.

### 4.5 Run-off guards  (when the exposure has amortized)
1. **Exact zero (spec):** `if CRD_p == 0 → RA_p = 0` (`VECTOR = 1`), avoiding `0/0`.
2. **Run-off cliff (added 2026-06-04, `RUNOFF_RA_CAP = 1.0`):** a per-period loss rate
   `RA_p ≥ 1` is non-physical — it only occurs when `|CRD|` collapses to ~0 in one quarter
   while the offset RA-metric window (§4.2) still includes pre-cliff months, so
   `RA = -(…)/CRD` explodes. When `RA_p ≥ 1` the series **stops** at `p-1` and the curve is
   **frozen** (held flat) for all later terms. (See OPEN_QUESTIONS Q26/Q30 — the *plateau level*
   still depends on whether `RA_FI`/`RE` are constant or decaying.)

### 4.6 Series prefix  `computeRa`
Iterate `p = 1, 2, …`; **stop** when `term > 30` (`COMPUTED_HORIZON_Y`) **or** the period is
`None` (window exceeds 361 months §4.2, or run-off cliff §4.5). Keep the valid prefix.

### 4.7 Survival factor  `vectorFactored`
```
VECTOR_p      = 1 - RA_p
EAD_RA_RATE_p = Π_{k=1..p} VECTOR_k         (cumulative product; acc starts at 1)
emitted value = min(1, max(0, EAD_RA_RATE_p))   (clamp backstop: an exposure factor ∈ [0,1])
```

### 4.8 Output term grid & flat tail  `termGrid` / `termSeries`
- **Quarterly grid:** `0, 0.25, …, 50.25`, then `100` → 203 points.
- **Yearly grid:** `0, 1, …, 50`, then `100` → 52 points.
- For each grid term `t`: `idx = min(round(t/step)+1, len) - 1` → reads the computed vector,
  **holding the last computed value flat** for every term beyond the last computed period
  (and for the `100` tail term).

### 4.9 Number formatting  `fmtNumber`
- `EAD_RA_RATE`: `BigDecimal` half-up to **9 dp**, trailing zeros stripped, decimal point → **comma**;
  non-finite (`NaN`/`Inf`) → `"0"`.
- `TERM`: same formatter at **2 dp** (e.g. `0`, `0,25`, `100`).

---

## 5. Configuration (`localRun/tseadfwd/application.conf`)

Under `tseadfwd_app`:

| Key | Meaning | Current value |
|-----|---------|---------------|
| `RA_BCEF.path` / `.sheetNames` | INPUTS_RA Excel path/sheet | `Inputs_RA_v2.xlsx` / `RA_BCEF` |
| `PARAMETRAGE.path` / `.sheetNames` | PARAMETRAGE Excel | `PARAMETRAGE_corrected.xlsx` / `PARAMETRAGE` |
| `MACRO_VARIABLE.path` / `.sheetNames` | scenario **Excel workbook, one sheet per scenario** (read + unioned by `readScenarioFromExcelSheets`; sheet name → `Scenario_ID`) | `Scenario_EAD_FWD.xlsx` / `["Central","Adverse","Optimistic","Extreme"]` |
| `shock_window_start` / `shock_window_end` | macro path window the FWL shock is read over (term 0 = start, step 1Q) | `"2025Q4"` / `"2028Q4"` |
| `ref_shock` | stress-leg magnitude for FWL=YES scaling (calibration) | `1.0` |
| `debug` | enable titled `show()` of inputs + per-term trace | `false` |
| `TS_EAD_FWD.{format,mode,numPartition,tmpPath,tableName,singleFile}` | output | csv / overwrite / 1 / … / true |

> **Note (scenario input).** Earlier vintages used a single scenario **CSV** (`header`/`delimiter`,
> one table with a `scenario` column). It is now a per-scenario **Excel workbook**: each sheet is one
> scenario, `date`→`Date` is normalized, `scenario = sheet name` is stamped, and the sheets are
> unioned by name. Only the `MACRO_VARIABLE` column referenced per matrix in PARAMETRAGE is used;
> extra macro columns are ignored. See OPEN_QUESTIONS Q25.

---

## 6. Debug / trace mode

When `debug = true` (`PrimaryMapper`), `logShow(title, df)` logs a title line before
each `show()`:
- `INPUT - PARAMETRAGE`
- `INPUT - MACRO_VARIABLE (scenario)` (filtered to `projection_date`)
- `INPUT - RA_BCEF (keys + first/last months; 361 monthly cols used in full)`
- `PARSED - matrix definitions`
- `TRACE - <matrixId> / <scenario>` — per-period table:
  `period, term, CRD, RA_STAT, RA_FI, RE, RA, VECTOR, EAD_RA_RATE`
  (FWL=NO matrices trace Central only; FWL=YES trace all scenarios).
- `OUTPUT - TS_EAD_FWD` (final, in `MainDriver`).

The TRACE is the row-by-row view of the 120-quarter / 31-year build described in §3.1.

---

## 7. Logging

`src/main/resources/log4j2.properties` quiets Spark/Hadoop to `WARN` and shows
`com.bnp.str.tseadfwd` at `INFO` (Start/End/write/collapse/trace), which Spark's
default config would otherwise filter out. App log lines use Scala string interpolation
(`logger.info(s"…")`) — not SLF4J `{}` parameterization, which is ambiguous against the
overloads under Scala.

---

## 8. Build & run

```bash
# build
mvn -o package                     # fat jar (assembly), mainClass = com.bnp.str.tseadfwd.job.MainDriver

# run (cluster / spark-submit)
spark-submit --class com.bnp.str.tseadfwd.job.MainDriver <jar> <path-to-application.conf>
```

**Local validation harness** — `src/test/.../EadFwdValidationApp.scala` runs the real
pipeline, writes the output, and diffs vs the target file. Run via the test classpath
(surefire cannot run offline):

```bash
mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
     com.bnp.str.tseadfwd.EadFwdValidationApp
```

---

## 9. Validation & open items

- Central / FWL=NO matches the target to ~`1e-5`.
- Deep-tail deviation and FWL=YES magnitude are **data-dependent** open items — see
  [`MISSING_INPUTS.md`](../MISSING_INPUTS.md) (scenario file where Adverse ≠ Extreme +
  `ref_shock` calibration; and the 25Q4-matching INPUTS_RA vintage). No logic change is
  expected once the corrected inputs arrive.
