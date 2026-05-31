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
  ├─ PrimaryReader            → reads RA_BCEF (Excel), MACRO_VARIABLE (CSV), PARAMETRAGE (Excel)
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
| `scenarioRa(... stress legs, rateDelta, refShock)` | FWL=YES parallel-shock RA |
| `vectorFactored(ra)` | cumulative product of `1 - RA` |
| `termSeries(vf, freq)` | maps the output grid to `vf`, holding the last value flat |

Aggregation windows (1-based months):
- Quarterly RA metrics: `Q1 = M1 + M2/2`; `Qn = M[3n-4]/2 + M[3n-3] + M[3n-2] + M[3n-1]/2`.
- Quarterly CRD: `mean(M[3i-2], M[3i-1], M[3i])`.
- Yearly RA metrics: sum (Y1 = 6 months, Yn = 12); Yearly CRD: mean of same window.

`computeRa` stops a series when the window exceeds available months or term > 30; the
flat tail in `termSeries` covers the remaining grid points.

### 3.3 `PrimaryMapper` — orchestration

- `parseParametrage` — groups rows by `(perimeter, output-segment)`; output segment =
  `AGGREGATED_SEGMENT_NAME` if aggregated else `SEGMENT`. Combined `fwlApplied = any(YES)`;
  `macroVar` = first non-`NONE`.
- `collectRa` — `(SEGMENT, FWL_TYPE, METRIC) → Array[Double]` of months.
- `aggregateSegments` — element-wise sum of constituent segments' monthly series.
- `collectScenario` — `(scenario, Date) → (macroVar → Double)`.
- `macroDelta` — `MACRO[scen][projection_date] − MACRO[Central][projection_date]`.
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

## 4. Configuration (`localRun/tseadfwd/application.conf`)

Under `tseadfwd_app`:

| Key | Meaning | Default |
|-----|---------|---------|
| `RA_BCEF.path` / `.sheetNames` | INPUTS_RA Excel path/sheet | — |
| `PARAMETRAGE.path` / `.sheetNames` | PARAMETRAGE Excel (points to `PARAMETRAGE_corrected.xlsx`) | — |
| `MACRO_VARIABLE.path` / `.header` / `.delimiter` | scenario CSV | — |
| `projection_date` | date the FWL shock is read at | `"2025Q4"` |
| `ref_shock` | stress-leg magnitude for FWL=YES scaling (calibration) | `1.0` |
| `debug` | enable titled `show()` of inputs + per-term trace | `false` |
| `TS_EAD_FWD.{format,mode,numPartition,tmpPath,tableName,singleFile}` | output | csv / overwrite / 1 / … / true |

---

## 5. Debug / trace mode

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

## 6. Logging

`src/main/resources/log4j2.properties` quiets Spark/Hadoop to `WARN` and shows
`com.bnp.str.tseadfwd` at `INFO` (Start/End/write/collapse/trace), which Spark's
default config would otherwise filter out. App log lines use Scala string interpolation
(`logger.info(s"…")`) — not SLF4J `{}` parameterization, which is ambiguous against the
overloads under Scala.

---

## 7. Build & run

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

## 8. Validation & open items

- Central / FWL=NO matches the target to ~`1e-5`.
- Deep-tail deviation and FWL=YES magnitude are **data-dependent** open items — see
  [`MISSING_INPUTS.md`](../MISSING_INPUTS.md) (scenario file where Adverse ≠ Extreme +
  `ref_shock` calibration; and the 25Q4-matching INPUTS_RA vintage). No logic change is
  expected once the corrected inputs arrive.
