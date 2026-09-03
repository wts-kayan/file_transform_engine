# Configuration reference — which block feeds which job

Maps every block of `localRun/tseadfwd/application.conf` to the code that reads it.

Written to answer two questions that keep coming back: *what does `MainDriver` actually need?*
and *is anything in this file dead?* The short answers are the table below, and **no** — every
key in the file is read **and** its value consumed. See [Audit](#audit--nothing-in-the-file-is-unused).

## Block → driver matrix

| Block | `MainDriver` | `ConsistencyCheckDriver` | `Term0AnalysisDriver` | `YearAnalysisDriver` | `EadFwdCompare` |
|-------|:---:|:---:|:---:|:---:|:---:|
| `RA` (or the legacy `RA_*` blocks) | yes | – | yes | yes | – |
| `PARAMETRAGE` | yes | – | yes | yes | – |
| `MACRO_VARIABLE` | yes | – | yes | yes | – |
| `parameters` | yes | yes | yes | yes | – |
| `audit` | yes | – | – | – | – |
| `TS_EAD_FWD` | yes | yes | yes | yes | – |
| `CONSISTENCY_CHECK` | yes | yes | – | – | – |
| `TERM0_ANALYSIS` | – | – | yes | – | – |
| `YEAR_ANALYSIS` | – | – | – | yes | – |
| `COMPARE` | – | – | – | – | yes |

`MainDriver` uses **7 of the 10 blocks**. `TERM0_ANALYSIS`, `YEAR_ANALYSIS` and `COMPARE` belong
to the three standalone analysis/comparison entry points and are never read by a production run.

## `MainDriver` — what is read, in execution order

| Step | `MainDriver` | Reads |
|------|--------------|-------|
| 1. Reader constructed | `:40` | nothing yet — the frames are `lazy` |
| 2. Run audit starts | `:49` → `TseadfwdAudit.start` → `RunAudit.start` (`RunAudit.scala:113`) | `audit.*`, plus `parameters.as_of_date_quarter`, `MACRO_VARIABLE.sheetNames`, `TS_EAD_FWD.tableName` |
| 3. Inputs read | `:54` → `PrimaryReader` | `RA` (`raInput`, `PrimaryReader.scala:71`), `PARAMETRAGE` (`:40`), `MACRO_VARIABLE` (`:34`) |
| 4. Calculation + data control | `:54` → `PrimaryMapper` | `parameters.*` (`PrimaryMapper.scala:41-141`), `TS_EAD_FWD.tmpPath`/`tableName` for the `DATA_CONTROL_*.csv` (`:575-578`) |
| 5. Consistency check configured | `:62` → `CheckConfig.from` (`ConsistencyCheckMapper.scala:57`) | `CONSISTENCY_CHECK.*`, `TS_EAD_FWD.format`/`singleFile`/`tmpPath`/`tableName`, `parameters.exclude_ead_ra_rate_ge_1` |
| 6. Output written | `:80` → `PrimaryUtilities.writeDataframe` (`:310`) | `TS_EAD_FWD.*` |

## Keys per block

### `RA` — read by `RaSheetConfig.from` (`RaSheetDiscovery.scala:61`)

| Key | Notes |
|-----|-------|
| `paths` | required; ordered — a sheet name in two workbooks is loaded from the **first** listed |
| `sheetPattern` | gate 1, on the sheet NAME |
| `requireColumns` | gate 2, on the sheet CONTENT |
| `includeSheets` / `excludeSheets` | escape hatches; `exclude` is checked first |

Absent entirely, `PrimaryReader.raInput` (`:72`) falls back to the legacy per-entity blocks
`RA_BCEF` / `RA_BGL` / `RA_BNL` / `RA_FORTIS` / `RA_LS` (`PrimaryReader.scala:125`). Those blocks
are no longer present in `application.conf`; the fallback exists so an unconverted config still runs.

### `PARAMETRAGE` / `MACRO_VARIABLE`

| Key | Notes |
|-----|-------|
| `path` | workbook |
| `sheetNames` | a single name for `PARAMETRAGE`; a **list** (one sheet per scenario) for `MACRO_VARIABLE` |

`MACRO_VARIABLE.sheetNames` is read twice: by the reader, and by the audit as the `scenarios` column.

### `parameters` — read by `PrimaryMapper` (`:41-141`)

| Key | Default | Configured |
|-----|---------|-----------|
| `as_of_date_quarter` | `2021Q1` | `2025Q4` |
| `last_quarter_projection_horizon` | `2025Q4` | `2028Q4` — fallback only, when a matrix's `PROJECTION_HORIZON` is blank |
| `apply_rate_to_shock` | `true` | `true` |
| `macro_delta_scale` | `1.0` | **`100`** |
| `exclude_ead_ra_rate_ge_1` | `false` | **`true`** — read by `CheckConfig`, not the mapper |
| `allow_negative_ead_ra_rate` | `false` | `false` |
| `debug` | `false` | `false` |
| `validation.strict` | `true` | `true` |

### `audit` — read by `RunAudit`

`enabled`, `database`, `table`, `root`, `userLauncher`, `motor`, plus optional `runId` and
`usedJar`. The last two are also injectable per run via `-Drun.*` / `RUN_*` env vars, which
override the config values.

### `TS_EAD_FWD`

`format`, `mode`, `numPartition`, `tmpPath`, `tableName`, `singleFile`, `alsoExcel`, `sheetName`
— all consumed by `writeDataframe` (`PrimaryUtilities.scala:310`) and `writeDataframeToExcel` (`:373`).

### `CONSISTENCY_CHECK` — read by `CheckConfig.from`

`enabled`, `htmlPath`, `sourcePath`, and the eight rule keys
(`rules.all_terms_equal_one.{enabled,remove,tolerance}`,
`rules.some_terms_equal_one.{enabled,maxRowsInReport}`,
`rules.negative_ead_ra_rate.{enabled,includeZero,replaceWith}`).

`maxRowsInReport` used to live on `negative_ead_ra_rate` and is still read from there as a fallback:
CR02 now reports a summary and has nothing left to cap, so the key moved to `some_terms_equal_one`,
the only rule that can still overflow it.

The block is also honoured under its former name `DATA_QUALITY` (`ConsistencyCheckMapper.scala:48`),
so a config written before the rename still applies rather than silently defaulting.

## Two things worth knowing

### `TS_EAD_FWD` is the most load-bearing block

Four components read it, not just the writer:

- `writeDataframe` — writes `$tmpPath/$tableName.csv` (and `.xlsx` when `alsoExcel`)
- `PrimaryMapper:575` — places `DATA_CONTROL_$tableName.csv`
- `CheckConfig:78` — re-derives the output filename from `format` + `singleFile` so the consistency
  report names the file it judges; a name that does not match what lands in the directory is worse
  than no name at all
- `TseadfwdAudit` — records `tableName` as `base_folder_name`

Changing `tableName` therefore moves **four** artefacts, not one.

### `CONSISTENCY_CHECK.sourcePath` is not used by `MainDriver`

`CheckConfig.from` populates it (`:106`), but `MainDriver` passes `outputFile = checks.outputFile`
explicitly (`:71`) — it already holds the frame in memory and knows where it is about to write it.
Only the standalone `ConsistencyCheckDriver` consumes `sourcePath` (`:52,57,63`), to locate an
already-written CSV.

So it is the one key inside a `MainDriver` block that a production run reads but never uses. If
this file is ever trimmed to a `MainDriver`-only config, `sourcePath` goes with the three unused
blocks.

## Audit — nothing in the file is unused

All 58 keys are read, and every value is consumed (no read-then-ignored keys: each `TS_EAD_FWD`
key reaches the write call, and `macro_delta_scale` is applied at `PrimaryMapper.scala:464`).

Two are easy to miss with a naive `grep` for the key name:

| Key | Why grep misses it |
|-----|--------------------|
| `validation.strict` | read as one dotted path — `paramsConf.getBoolean("validation.strict")` (`PrimaryMapper.scala:141`); searching for `"strict"` finds nothing |
| `mdPath` / `csvPath` | read through a local `strOr` helper, not `getString` (`Term0AnalysisDriver.scala:86-87`) |

### Keys that restate the code default

These 19 are no-ops — deleting them changes nothing at runtime. They are kept deliberately, as
the file doubles as the documentation of what is tunable:

`RA.sheetPattern`, `RA.requireColumns`, `RA.includeSheets`, `RA.excludeSheets`,
`parameters.apply_rate_to_shock`, `parameters.allow_negative_ead_ra_rate`, `parameters.debug`,
`parameters.validation.strict`, `TS_EAD_FWD.singleFile`, `CONSISTENCY_CHECK.enabled`,
`CONSISTENCY_CHECK.sourcePath`, six of the seven `rules.*` keys (all but `includeZero`), and
`TERM0_ANALYSIS.tol` / `YEAR_ANALYSIS.tol`.

Note `includeSheets` / `excludeSheets` read as removable, but they are the escape hatches the
dynamic RA-sheet discovery feature added — see [DYNAMIC_RA_SHEETS_ANALYSIS.md](DYNAMIC_RA_SHEETS_ANALYSIS.md).
Dropping them removes the documented way to force or skip a sheet.

### Keys carrying a non-default value

These are the ones actually steering a run:

| Key | Value | Default |
|-----|-------|---------|
| `parameters.macro_delta_scale` | `100` | `1.0` |
| `parameters.exclude_ead_ra_rate_ge_1` | `true` | `false` |
| `TS_EAD_FWD.alsoExcel` | `true` | `false` |
| `TS_EAD_FWD.sheetName` | `TS_EAD_FWD` | the table name |
| `CONSISTENCY_CHECK.htmlPath` | `CR_TS_EAD_FWD.html` | `CR_TS_EAD_FWD_25Q4_v1_small.html` |
| `rules.negative_ead_ra_rate.includeZero` | `true` | `false` |

Everything else either has no default (the input paths, `as_of_date_quarter`) or is a required
output setting (`format`, `mode`, `numPartition`, `tmpPath`, `tableName`).
