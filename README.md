# IRIS — file_transform_engine

**IRIS** is a repository of **Spark/Scala data-transform modules**. Each module reads reference
inputs, runs a validated computation core, and writes a tabular output — sharing the same Spark
session / IO / audit plumbing and the same internal layering (the [module contract](#module-contract)
below). Everything builds into one fat jar; you pick which module's job to run with
`spark-submit --class …`.

## Modules

| Module | `com.bnp.str.*` | Config block | What it produces | Status |
|---|---|---|---|---|
| [TS_EAD_FWD](#ts_ead_fwd) | `tseadfwd` | `tseadfwd_app` | EAD FWD term structure — forward-looking EAD factor curve per *(matrix, scenario, term)* | ✅ active |
| [ADDONS](#addons) | `addons` | `addons_app` | IRBA add-on parameter table `crr_param_add_on_ste` | ✅ active |
| [AGEING](#ageing) | `ageing` | `ageing_macroeconomic_scenarios_app` | Macroeconomic scenarios aged onto a later as-of date | ✅ active |
| _…_ | _…_ | _…_ | _more modules to come_ | 🚧 planned |

Shared, module-agnostic code lives in `com.bnp.str.utilities` — currently the [run audit](#run-audit).

## Module contract

Every module implements the **same six-layer shape**. It is a convention enforced by review rather
than by a compiler-checked interface: the layers, their responsibilities and their file locations are
identical across modules, only the method names carry the module's own suffix. Learn one module and
you can read any of them — and a new module is a copy of the layout below with its own business logic.

```
com.bnp.str.<module>
├── job/MainDriver             entry point — spark-submit --class …  (one arg: application.conf)
├── sessionmanager/…           local/cluster SparkSession factory (decided at runtime, no flag)
├── reader/PrimaryReader       reads the declared inputs (lazy), path/sheet from the config block
├── common/RunnerProvider      abstract: exposes the inputs to the runner, declares run_<module>_runner
├── common/PrimaryRunner       orchestrates reader -> mapper, returns the output
├── common/MapperProvider      abstract: declares the mapping entry point
├── mapping/PrimaryMapper      turns inputs into the output (Spark)
├── mapping/PrimaryView        the computation core (pure/SQL — the part worth unit-testing)
├── writer/PrimaryWriter       writes the output, target path from the config block
├── audit/<Module>Audit        optional: records the run into `run_history`
└── utility/
    ├── PrimaryConstants       every literal: config keys, column names, business constants
    └── PrimaryUtilities       IO helpers (Excel/CSV read+write, single-file collapse)
```

**Data flow** — identical in all three:

```
application.conf ─▶ MainDriver ─▶ PrimaryReader ─▶ PrimaryRunner ─▶ PrimaryMapper ─▶ PrimaryView
                                                                                          │
                                            output ◀─ PrimaryWriter ◀───────────────────────
```

**Per-module names.** The same role, spelled with each module's suffix:

| Contract point | `tseadfwd` | `addons` | `ageing` |
|---|---|---|---|
| Runner entry | `run_tseadfwd_runner(): DataFrame` | `run_addons_runner(): DataFrame` | `run_ageing_runner(): Map[String, DataFrame]` |
| Mapper entry | `getMapping_tseadfwd` / `getDataFrame` | `getMapping_addons` / `getDataFrame_addons` | `getMapping_ageing` / `getDataFrame_ageing` |
| Writer entry | `write(df, tableName)` | `write(df, tableName)` | `write(df, scenario)` + `writeCsv(scenarios)` |
| Session factory | `SparkSessionManager` | `StrSparkSessionManager` | `StrSparkSessionManager` |
| Run audit | `TseadfwdAudit` | — (not wired) | `AgeingAudit` |
| Output shape | one DataFrame | one DataFrame | one DataFrame **per scenario** |

Note the one genuine shape difference: `ageing` produces a *map* of scenario → DataFrame (it writes
one Excel sheet per scenario plus a single combined CSV), where the other two produce a single table.

**Adding a module.** Create `com.bnp.str.<name>` with the layout above, give it a config block named
`<name>_app`, wire the run audit in `MainDriver` (two calls — see [Run audit](#run-audit)), then add a
row to the [Modules](#modules) table and a `## <NAME>` section below. No repo-wide change is needed:
the fat jar picks the module up from `--class`.

## Build (repo-wide)

```bash
mvn clean package -DskipTests      # -> target/wts-training-spark.jar (fat jar; Spark/Hadoop are 'provided')
```

Scala 2.12.18 · Spark 3.5.4 · Java 8 · Maven. Every job takes one argument — the path to an
`application.conf` — read via Hadoop FileSystem, so a local path, a `--files`-shipped conf, or an HDFS
path all work. On a cluster, drop `--master local[*]`: `spark-submit` provides the master and the
session manager adapts automatically. The jar's default main class is
`com.bnp.str.tseadfwd.job.MainDriver`.

> On Windows a harmless `Failed to delete temp dir` stack trace may print at JVM shutdown *after* the
> output is written; set `HADOOP_HOME` with `winutils.exe` to silence it.

## Run audit

Modules record each execution into the shared, module-agnostic `run_history` table (ORC, EXTERNAL,
partitioned by `(module_name, run_id)`) via `com.bnp.str.utilities.audit` — two calls in `MainDriver`:
`RunAudit.start(...)` then `succeeded()` / `failed(e)`. Configured under `<module>_app.audit`.
Currently wired in `tseadfwd` and `ageing`. Full details: `docs/RUN_AUDIT.md`.

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

Input blocks and output/job blocks live at the root; engine **run parameters** are grouped under
`parameters { … }`.

| Key | Meaning |
|---|---|
| `RA_*`, `MACRO_VARIABLE`, `PARAMETRAGE` `.path` / `.sheetNames` | input workbook paths/sheets |
| `parameters.as_of_date_quarter` | projection start = term 0; the FWL shock macro path is read from here (step 1Q) |
| `parameters.last_quarter_projection_horizon` | **fallback** shock-window end, used only when a matrix's `PROJECTION_HORIZON` is blank |
| `parameters.apply_rate_to_shock` | scale the FWL=YES shock by the macro `Rate/100` (true) or apply it full-size (false) |
| `parameters.debug` | log titled `show()` of inputs + a per-term trace |
| `parameters.validation.strict` | abort the run on a data-control FAIL (true) or only warn (false) |
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

Full business + technical specs: `docs/tseadfwd/FUNCTIONAL_SPECIFICATION.md`,
`docs/tseadfwd/TECHNICAL_SPECIFICATION.md`.

---

## ADDONS

Package `com.bnp.str.addons`. Builds the **IRBA add-on parameter table** `crr_param_add_on_ste` — the
file consumed downstream by another stress-testing engine. It reads three add-on reference sheets from
one Excel workbook and joins them into a flat, `;`-delimited CSV where each row is an
*(action × perimeter)* add-on rule with its operand / variable / factor and activation rank.

### What it computes

1. **Filter** `add_on_application` to the active rows (`ACTIVE_IND = "1"`) and `add_on_action` to the
   non-CCF rows (`VARIABLE <> "CCF"`).
2. **Register** the three inputs as temp views and run the **join SQL** — built in, or loaded from an
   external query file when `queryName` is set (`sql_queries.path`).
3. **Write** the 18-column result as one CSV, then **harmonize** the Spark part-file into the final
   `<path>/<tableName>.<format>` name (e.g. `crr_param_add_on_ste.csv`).

Everything after step 2 is skipped when `crr_param_add_on_ste.enable = false`.

### Inputs

All three sheets come from one workbook, read with crealytics spark-excel using `header=true` and
`inferSchema=false` (every cell as text — parsing is explicit, so a locale never changes a value).

| Input (config key) | Sheet | Key columns used |
|---|---|---|
| `add_on_application` | `r_add_on_application` | `ACTION_ID`, `PERIMETER_ID`, `IMPACT_RANK`, `ACTIVE_IND` |
| `add_on_action` | `r_add_on_action` | `ACTION_ID`, `OPERAND`, `VARIABLE`, `FACTOR` |
| `add_on_perimeter` | `r_add_on_perimeter` | `PERIMETER_ID` plus its 11 perimeter filter columns (`PD_MODEL_NAME`, `PRODUCT_CODE`, … — listed in the spec) |

`add_on_action.columnsToFix` lists the decimal columns normalized to a locale-independent form on read
(`'.'` decimal, no grouping), so a comma-decimal host (`-0,05`) yields the same value as `-0.05`.

### Run

```bash
spark-submit --class com.bnp.str.addons.job.MainDriver \
  --master "local[*]" target/wts-training-spark.jar localRun/addons/application.conf
```

### Configuration (`addons_app`)

| Key | Meaning |
|---|---|
| `add_on_application` / `add_on_action` / `add_on_perimeter` `.path` / `.sheetNames` | input workbook path + sheet per business object |
| `add_on_action.columnsToFix` | decimal columns to normalize on read (locale-independent) |
| `crr_param_add_on_ste.enable` | write the output at all — `false` skips write **and** harmonization |
| `crr_param_add_on_ste.{format,mode,delimiter,header,numPartition}` | output format settings |
| `crr_param_add_on_ste.{path,tmpPath,deleteTmpPath}` | final output dir, Spark staging dir, and whether to drop the staging dir afterwards |
| `crr_param_add_on_ste.tableName` | base name of the final file (also the temp sub-directory) |
| `crr_param_add_on_ste.queryName` | name of an external join query; empty = use the built-in SQL |
| `sql_queries.path` | file holding the external queries |

Full technical spec: `docs/addons/TECHNICAL_SPECIFICATION.md`.

---

## AGEING

Package `com.bnp.str.ageing`. **Ages macroeconomic scenarios** onto a later as-of date: it lifts each
scenario's deviation from Central observed at an *early* date and re-applies it at the *corresponding
later* date, leaving the Central path itself untouched. Used to re-date an existing scenario set
without re-running the macro models.

### What it computes

With `initial` = the as-of date the scenarios were built for and `ageing` = the as-of date they are
being moved to, over a fixed **12-quarter** horizon (`PrimaryConstants.PROJECTION_QUARTERS`):

```
dateForShocks = initial .. initial+12Q          shockToAge = dateForShocks.zip(dateToAge).toMap
dateToAge     = ageing  .. ageing +12Q

shock(d)              = scenario(d) − central(d)              for d in dateForShocks
aged(shockToAge(d))   = central(shockToAge(d)) + shock(d)
```

Dates with no mapped shock keep their Central value, so a scenario identical to Central ages to
Central. `Central` is passed through unchanged and **must** be present — the run fails fast, naming the
sheets it did find, if it is not. When the workbook stops short of the horizon, the last available
quarter is replicated forward to fill it.

### Inputs

One Excel workbook, **one sheet per scenario** (`Central`, `Adverse`, `Optimistic`, `Extreme`, …) — the
sheet names *are* the scenario names, discovered at runtime, so adding a scenario means adding a sheet.
Each sheet has a date column (`2025Q1`, …) plus one column per macro variable.

The date header is normalized to the canonical `Date` on read, so sheets that disagree on casing
(`date` vs `Date`) all behave identically and the output header is stable.

### Outputs

| Output | Config key | Layout |
|---|---|---|
| Excel workbook | `output.macroeconomic.path` | **one sheet per scenario**, mirroring the input layout (no `scenario` column — the sheet name carries it) |
| CSV | `output.macroeconomic.csv_path` | **all scenarios in one file**, tagged by a `scenario` column placed immediately after `Date`; ordered by date, then by workbook sheet order |

```
Date;scenario;IR_10Y_BE;IR_10Y_FR;…
2025Q1;Central;0.031485469;0.033225781;…
2025Q1;Adverse;0.031485469;0.033225781;…
```

The workbook is deleted before each run, and the CSV is overwritten in place, so re-running is
idempotent. `csv_path` is optional: when omitted the CSV is written next to the workbook under the same
base name.

### Run

```bash
spark-submit --class com.bnp.str.ageing.job.MainDriver \
  --master "local[*]" target/wts-training-spark.jar localRun/ageing/application.conf
```

### Configuration (`ageing_macroeconomic_scenarios_app`)

| Key | Meaning |
|---|---|
| `macroeconomic.path` | input scenario workbook (one sheet per scenario) |
| `as_of_date.initial` | as-of date the scenarios were built for — start of the shock window |
| `as_of_date.ageing` | as-of date to age them onto — start of the target window |
| `output.macroeconomic.path` | output Excel workbook (one sheet per scenario) |
| `output.macroeconomic.csv_path` | output CSV (all scenarios, `scenario` column). Optional — defaults to the workbook path with a `.csv` extension |
| `audit.{enabled,database,table,root,userLauncher,motor}` | run-audit settings — see `docs/RUN_AUDIT.md` |

### Tests

27 unit tests across four suites — quarter arithmetic, the ageing core, the CSV/Excel output, and the
writer's path resolution. They are **not** bound to `mvn test` (the pom declares no surefire/scalatest
plugin); run them with the ScalaTest runner:

```bash
mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
     org.scalatest.tools.Runner -o -s com.bnp.str.ageing.DateUtilsSpec \
     -s com.bnp.str.ageing.PrimaryViewSpec -s com.bnp.str.ageing.PrimaryUtilitiesSpec \
     -s com.bnp.str.ageing.PrimaryWriterSpec
```
