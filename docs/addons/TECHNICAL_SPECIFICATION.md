# Addons — Technical Specification

Module: **`com.bnp.str.addons`** (sibling of `com.bnp.str.tseadfwd` in the same fat jar).

## 1. Purpose

The addons module builds the **IRBA add-on parameter table** `crr_param_add_on_ste` — the input
file consumed by another stress-testing engine. It reads three add-on reference sheets from an
Excel workbook, joins them into one flat, `;`-delimited CSV where each row is an
*(action × perimeter)* add-on rule with its operand/variable/factor and activation rank.

## 2. Package layout

```
com.bnp.str.addons
├── job/MainDriver                  entry point (spark-submit --class …)
├── reader/PrimaryReader            reads the 3 input sheets by name (lazy)
├── common/RunnerProvider           abstract: exposes the 3 inputs to the runner
├── common/PrimaryRunner            orchestrates reader -> mapper
├── common/MapperProvider           abstract mapper contract
├── mapping/PrimaryMapper           filters inputs, registers temp views, runs the join query
├── mapping/PrimaryView             the join SQL (built-in) + loader for an external query
├── writer/PrimaryWriter            thin wrapper over PrimaryUtilities.writeDataframe
├── sessionmanager/StrSparkSessionManager   local/cluster SparkSession factory
└── utility/
    ├── PrimaryConstants            names: inputs, views, filter columns/values, output table
    └── PrimaryUtilities            Excel read, locale fix, CSV write, single-file harmonize
```

## 3. Data flow

```
application.conf ─▶ MainDriver
                     │  build Hive-enabled SparkSession (StrSparkSessionManager)
                     ▼
              PrimaryReader ── lazily reads 3 Excel sheets (crealytics)
                     ▼
              PrimaryRunner.run_addons_runner
                     ▼
              PrimaryMapper.getMapping_addons
                     │  filter add_on_application (ACTIVE_IND = "1")
                     │  filter add_on_action     (VARIABLE <> "CCF")
                     │  register 3 temp views
                     │  run the join SQL (built-in or loaded by queryName)
                     ▼
              DataFrame (crr_param_add_on_ste, 18 columns)
                     ▼  (only if crr_param_add_on_ste.enable = true)
              PrimaryWriter.write ─▶ PrimaryUtilities.writeDataframe  (CSV to tmpPath/<tableName>/)
                     ▼
              harmonizeFilesInDirectory  ─▶  <path>/<tableName>.<format>
```

## 4. Inputs

All three come from one Excel workbook (per-sheet path/sheet configured under `addons_app`), read
with crealytics spark-excel, `header=true`, `inferSchema=false` (every cell as text).

| Input (config key / constant) | Sheet | Key columns used |
|---|---|---|
| `add_on_application` | `r_add_on_application` | `ACTION_ID`, `PERIMETER_ID`, `IMPACT_RANK`, `ACTIVE_IND` |
| `add_on_action` | `r_add_on_action` | `ACTION_ID`, `OPERAND`, `VARIABLE`, `FACTOR` |
| `add_on_perimeter` | `r_add_on_perimeter` | `PERIMETER_ID`, `PD_MODEL_NAME`, `PD_MODEL_NAME_5`, `LGD_MODEL_NAME`, `EAD_MODEL_NAME`, `ORIGIN_FLG`, `UNPAID_FLG`, `ACCOUNTING_SITE_CODE_POST_ACC`, `PRODUCT_CODE`, `CONF_FLG`, `SECURIT_POS_FLG`, `PRODUCT_TYPE` |

Inputs are materialized lazily (only read when the runner asks for them) and dispatched by name in
`PrimaryReader.getMappingReader`.

## 5. Transformation

`PrimaryMapper.getDataFrame_addons`:

1. **Filter** `add_on_application` to active rows: `ACTIVE_IND = "1"`.
2. **Filter** `add_on_action` to non-CCF rules: `VARIABLE <> "CCF"`.
3. Register the three (filtered) frames as temp views: `add_on_application_view`,
   `add_on_action_view`, `add_on_perimeter_view` (names are `PrimaryConstants.VIEW_*`).
4. Run the join query — the built-in `PrimaryView.get_on_application_active_ind`, unless the output
   config sets a non-empty `queryName`, in which case the query of that name is loaded from the SQL
   queries file (`addons_app.sql_queries.path`) via `PrimaryView.loadQuery`.

**Join** (all `LEFT JOIN`, application is the driving table):

```
add_on_application_view app
  LEFT JOIN add_on_action_view    action    ON app.ACTION_ID    = action.ACTION_ID
  LEFT JOIN add_on_perimeter_view perimeter ON app.PERIMETER_ID = perimeter.PERIMETER_ID
ORDER BY app.ACTION_ID, app.PERIMETER_ID, perimeter.PD_MODEL_NAME
```

Because the joins are left, the row count equals the number of active application rows; unmatched
action/perimeter columns come out empty.

## 6. Output

One `;`-delimited CSV with a header. Final name is **`<tableName>.<format>`**
(e.g. `crr_param_add_on_ste.csv`) at `crr_param_add_on_ste.path`.

Columns, in order:

| # | Column | Source |
|---|---|---|
| 1 | `ADDON_ID` | `app.ACTION_ID` |
| 2 | `PERIMETER_ID` | `app.PERIMETER_ID` |
| 3 | `PD_MODEL_NAME` | perimeter |
| 4 | `PD_MODEL_NAME_5` | perimeter |
| 5 | `LGD_MODEL_NAME` | perimeter |
| 6 | `EAD_MODEL_NAME` | perimeter |
| 7 | `ORIGIN_FLG` | perimeter |
| 8 | `UNPAID_FLG` | perimeter |
| 9 | `ACCOUNTING_SITE_CODE_POST_ACC` | perimeter |
| 10 | `PRODUCT_CODE` | perimeter |
| 11 | `CONF_FLG` | perimeter |
| 12 | `SECURIT_POS_FLG` | perimeter |
| 13 | `PRODUCT_TYPE` | perimeter |
| 14 | `OPERAND` | `action.OPERAND` |
| 15 | `VARIABLE` | `action.VARIABLE` |
| 16 | `FACTOR` | `action.FACTOR` |
| 17 | `RANK` | `app.IMPACT_RANK` |
| 18 | `ACTIVATE` | `app.ACTIVE_IND` |

Empty cells are written as a bare field (`emptyValue=""`), not `""`.

### Harmonization (single clean file)

Spark writes a partitioned directory; `harmonizeFilesInDirectory` turns it into one file:

1. Create `path` (output folder) if it does not exist.
2. Rename the single `part-*` file from `tmpPath/<tableName>/` to `path/<tableName>.<format>`
   (`renameFilesInDirectory`): fails fast if there is **no** part-file or **more than one**
   (numPartition must be 1); creates the target dir; overwrites an existing output; deletes the
   `.crc` sidecar.
3. If `deleteTmpPath=true`, delete `tmpPath/<tableName>/` and the `tmpPath` parent if now empty.

## 7. Configuration (`addons_app`)

```hocon
addons_app {
  add_on_application { path = "...xlsx", sheetNames = r_add_on_application }
  add_on_action      { path = "...xlsx", sheetNames = r_add_on_action, columnsToFix = ["FACTOR"] }
  add_on_perimeter   { path = "...xlsx", sheetNames = r_add_on_perimeter }

  crr_param_add_on_ste {
    enable        = true          # gate: write + harmonize only when true
    format        = "csv"
    path          = "localRun/addons/output/"      # final output folder (created if missing)
    tmpPath       = "localRun/addons/output_tmp/"  # Spark write dir (cleaned up)
    tableName     = "crr_param_add_on_ste"          # -> crr_param_add_on_ste.csv
    numPartition  = 1              # must be 1 (harmonize expects a single part-file)
    mode          = "Overwrite"
    delimiter     = ";"
    header        = "true"
    deleteTmpPath = "true"
    queryName     = ""             # empty = built-in query; else load this query by name
  }

  sql_queries { path = "localRun/addons/sql/queries.sql.conf" }  # only read when queryName is set
}
```

| Key | Meaning |
|---|---|
| `<input>.path` / `.sheetNames` | input workbook path / sheet name |
| `<input>.columnsToFix` | decimal columns to normalize to locale-independent form on read (optional) |
| `crr_param_add_on_ste.enable` | when false, compute only — no write, no harmonize |
| `crr_param_add_on_ste.tableName`/`format` | drive the final file name `<tableName>.<format>` |
| `crr_param_add_on_ste.numPartition` | must be 1 |
| `crr_param_add_on_ste.queryName` | select an external SQL query instead of the built-in |
| `sql_queries.path` | HOCON file holding named SQL queries (read via Hadoop FS) |

## 8. Locale handling

Reads use `inferSchema=false`, so POI renders cells with the JVM default locale (a fr-FR host emits
comma decimals / grouping). `usePlainNumberFormat` is **deliberately not** enabled — it would render
whole numbers as `"1.0"`, breaking the `ACTIVE_IND = "1"` filter and changing integer output columns
(`RANK`, `ACTIVATE`). Instead, only the decimal columns listed in a sheet's `columnsToFix` are
normalized (`PrimaryUtilities.replaceCommaInColunms`): strip grouping (space / NBSP U+00A0 / narrow
NBSP U+202F) then convert the decimal comma to a dot — so `-0,05` and `1 234,56` become `-0.05` and
`1234.56` while integers/text are untouched. For addons, `add_on_action.columnsToFix = ["FACTOR"]`.

## 9. Session, logging, run

- **Session:** `StrSparkSessionManager.fetchSparkSession` builds one Hive-enabled session that adapts
  to local (embedded Derby metastore, `local[*]`) vs cluster (spark-submit-provided master) from the
  runtime, no flag.
- **Logging:** log4j2 (`src/main/resources/log4j2.properties`); `com.bnp.str` at INFO, Spark/Hadoop
  at WARN.
- **Run:**
  ```bash
  spark-submit --class com.bnp.str.addons.job.MainDriver \
    --master "local[*]" target/wts-training-spark.jar localRun/addons/application.conf
  ```
  The single CLI argument is the path to `application.conf` (local or HDFS, read via Hadoop FS).

## 10. Error handling / edge cases

- `MainDriver` requires the config-path argument and wraps the run in try/catch (logs + rethrows).
- `enable=false` → compute only; write and harmonize are skipped (no crash on a missing part-file).
- `renameFilesInDirectory` fails fast with a clear message on 0 or >1 part-files.
- Unknown reader input / unset schema surfaces as an `IllegalArgumentException` naming the expected
  values.

## 11. Extensibility

- **Custom join** — set `crr_param_add_on_ste.queryName` and add the query to the `sql_queries` file
  to override the built-in join without code changes.
- **New solutions** — additional solutions are expected under the IRIS engine, each in its own
  package, reusing the shared session/read/write/harmonize plumbing. Note the temp-view names are
  currently session-global; namespace them per solution before running several in one session.
