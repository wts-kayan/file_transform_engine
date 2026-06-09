# file_transform_engine

A repository of **Spark/Scala data-transform engines**. Each engine ("package") reads reference
inputs, runs a validated computation core on the driver, and writes a tabular output — sharing the
same Spark session / IO plumbing. Everything builds into one fat jar; you pick which engine's job to
run with `spark-submit --class …`.

## Packages

| Package | `com.bnp.str.*` | Description | Status |
|---|---|---|---|
| [TS_EAD_FWD](#ts_ead_fwd) | `tseadfwd` | EAD FWD Term Structure — forward-looking EAD factor curve per *(matrix, scenario, term)* | ✅ active |
| _…_ | _…_ | _more engines to come_ | 🚧 planned |

Each engine lives under its own `com.bnp.str.<name>` package with its own jobs (objects with a
`main`) and config block, and reuses the shared `sessionmanager` / `reader` / `writer` / `utility`
plumbing. To add one: create the package + jobs, give it a config block under `tseadfwd_app` (or its
own root), add a row above, and a `## <NAME>` section below.

## Build (repo-wide)

```bash
mvn clean package -DskipTests      # -> target/wts-training-spark.jar (fat jar; Spark/Hadoop are 'provided')
```

Scala 2.12.18 · Spark 3.5.4 · Java 8 · Maven. Every job takes one argument — the path to an
`application.conf` — read via Hadoop FileSystem, so a local path, a `--files`-shipped conf, or an HDFS
path all work. On a cluster, drop `--master local[*]`: `spark-submit` provides the master and
`SparkSessionManager` adapts automatically.

> On Windows a harmless `Failed to delete temp dir` stack trace may print at JVM shutdown *after* the
> output is written; set `HADOOP_HOME` with `winutils.exe` to silence it.

---

## TS_EAD_FWD

Package `com.bnp.str.tseadfwd`. Computes the **EAD FWD Term Structure** (`TS_EAD_FWD`) — the
forward-looking Exposure-At-Default factor curve per *(matrix, scenario, term)* — from monthly RA
series, macro scenarios, and a PARAMETRAGE config. The numeric core is pure Scala on the driver;
Spark is used for IO. Output is a single `;`-delimited, decimal-comma CSV:
`EAD_MATRIX_ID;SCENARIO_ID;TERM;EAD_RA_RATE`.

### What it computes

For each **matrix** (PARAMETRAGE group: perimeter × output segment × rate type), **frequency**
(Quarterly `Q` / Yearly `Y`), and **scenario** (`Central/C`, `Adverse/A`, `Optimistic/O`, `Extreme/E`):

1. **Aggregate** monthly RA inputs → per-period values: `CRD = mean(window months)`;
   `RA metric = M1 + M2/2` (first period) / half-weight window thereafter.
2. **Loss rate** `RA`:
   - FWL=YES Central / FWL=NO: `RA = -(RA_STAT + RA_FI + RE) / CRD` (or `-(RA_STAT)/CRD` for FWL=NO).
   - FWL=YES non-Central: a stress-leg shock on FI+RE, scaled by the macro delta path (`scenario − Central`).
3. **Vector & factor:** `VECTOR = 1 − RA`; `EAD_RA_RATE` = cumulative product of `VECTOR`, computed to
   30y then held flat out to the long tail (with a run-off freeze when the book amortizes to ~0).

### Components

| Component | Role |
|---|---|
| `mapping/PrimaryView` | Pure formula core (no Spark): aggregation, RA, vector-factored product, term grid. |
| `mapping/PrimaryMapper` | Parses inputs and orchestrates `PrimaryView` into the output DataFrame. |
| `reader/PrimaryReader`, `writer/PrimaryWriter`, `utility/PrimaryUtilities` | Excel/CSV IO, single-file collapse, locale-tolerant parsing. |
| `validation/DataControlView` | Pre-calculation data-quality checks (labels, numeric integrity, stress legs, scenario coverage). |
| `sessionmanager/SparkSessionManager` | One factory for local **and** cluster (decided from the runtime, no flag). |

### Inputs

All paths/sheets come from the conf block `tseadfwd_app`:

- **RA_BCEF / RA_BGL / RA_BNL / RA_FORTIS / RA_LS** — monthly RA series per perimeter. Columns:
  `PERIMETER, SEGMENT, RATE_TYPE, FWL_TYPE, METRIC, M1..Mn`; `FWL_TYPE ∈ {BASELINE, STRESS (+),
  STRESS (-)}`, `METRIC ∈ {CRD, RA STAT, RA FI, RE}`. Present sheets are unioned; missing ones skipped
  with a warning. **The RA lookup key includes `PERIMETER`**, so the same segment name (e.g.
  `MORTGAGE`) across entities does not collide.
- **MACRO_VARIABLE** — scenario workbook, **one sheet per scenario** (`Central/Adverse/Optimistic/Extreme`),
  a `Date` column (e.g. `2025Q4`) and one column per macro variable (`IR_10Y_FR`, …).
- **PARAMETRAGE** — matrix definitions: `PERIMETER, SEGMENT, RATE_TYPE, AGGREGATION,
  AGGREGATED_SEGMENT_NAME, FWL_TO_BE_APPLIED, MACRO_VARIABLE, PROJECTION_HORIZON`.

### Jobs & run

| Class | Purpose |
|---|---|
| `job.MainDriver` | **Production pipeline** — read inputs → compute → write `TS_EAD_FWD`. (jar default main class) |
| `job.Term0AnalysisDriver` | Generates the per-*(matrix, scenario, term)* computation **breakdown** (Markdown + CSV), optionally **reconciled** against the real engine output. |
| `job.EadFwdCompare` | Compares an output CSV against a target CSV (per-key error report + CSV). |

```bash
spark-submit --class com.bnp.str.tseadfwd.job.MainDriver \
  --master "local[*]" target/wts-training-spark.jar localRun/tseadfwd/application.conf

spark-submit --class com.bnp.str.tseadfwd.job.Term0AnalysisDriver \
  --master "local[*]" target/wts-training-spark.jar localRun/tseadfwd/application.conf

spark-submit --class com.bnp.str.tseadfwd.job.EadFwdCompare \
  --master "local[*]" target/wts-training-spark.jar localRun/tseadfwd/application.conf
```

### Configuration (`tseadfwd_app`)

| Key | Meaning |
|---|---|
| `RA_*`, `MACRO_VARIABLE`, `PARAMETRAGE` `.path` / `.sheetNames` | input workbook paths/sheets |
| `as_of_date_quarter` | projection start = term 0; the FWL shock macro path is read from here (step 1Q) |
| `last_quarter_projection_horizon` | **fallback** shock-window end, used only when a matrix's `PROJECTION_HORIZON` is blank |
| `apply_rate_to_shock` | scale the FWL=YES shock by the macro `Rate/100` (true) or apply it full-size (false) |
| `debug` | log titled `show()` of inputs + a per-term trace |
| `validation.strict` | abort the run on a data-control FAIL (true) or only warn (false) |
| `TS_EAD_FWD.{format,mode,numPartition,tmpPath,tableName,singleFile}` | output settings |
| `COMPARE.{outputPath,targetPath,stripRateType,tol,comparePath}` | `EadFwdCompare` job |
| `TERM0_ANALYSIS.{enabled,terms,enginePath,tol,mdPath,csvPath}` | `Term0AnalysisDriver` job |

**Projection horizon** — the FWL=YES shock-window **end** is derived **per matrix** as
`as_of_date_quarter + PROJECTION_HORIZON` (PARAMETRAGE column, e.g. `2025Q4 + "3Y" = 2028Q4`); past it
the macro delta is held flat. A blank cell falls back to `last_quarter_projection_horizon`.

**Analysis generator (`Term0AnalysisDriver`)** — the worked computation breakdown per
*(matrix, scenario, term)* as Markdown + CSV, using the **same** validated parsing + `PrimaryView`
formulas as production. `terms` = the output terms to break down; `enginePath` = (optional) real
`TS_EAD_FWD` output to **reconcile** against (each `EAD` tagged `MATCH` / `DIFF` / `MISSING`, so a bad
input parse surfaces as a `DIFF`).

### Validation

`EadFwdValidationApp` runs the real reader → runner → mapper pipeline against the `localRun` sample and
prints per-matrix/scenario max-abs-error versus `target_output/TS_EAD_FWD_25Q4_v1_small.csv`:

```bash
mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
java -cp "target/classes;target/test-classes;$(cat cp.txt)" com.bnp.str.tseadfwd.EadFwdValidationApp
```

Full business + technical specs: `docs/FUNCTIONAL_SPECIFICATION.md`, `docs/TECHNICAL_SPECIFICATION.md`.
