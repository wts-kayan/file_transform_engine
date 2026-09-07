# Run Audit — `run_history`

A **shared, module-agnostic** execution audit for every module packaged in the jar. It lives in
`com.bnp.str.utilities.audit` (a sibling of `com.bnp.str.tseadfwd`) so any current or future
module — addons / climatetables / excelor / tseadfwd — records its runs to the same `run_history`
table with two calls.

## Storage — ORC external table, partitioned by (module_name, run_id)

- Records are written as **ORC** files laid out in Hive-style partitions:

  ```
  <root>/module_name=<module_name>/run_id=<run_id>/part-*.orc
  ```

- Exposed as an **EXTERNAL** Hive table `run_history`, `PARTITIONED BY (module_name, run_id)`,
  `STORED AS ORC`, `LOCATION <root>`. Being EXTERNAL — and pinned with
  `TBLPROPERTIES ('external.table.purge'='FALSE')` — dropping the table never deletes the data.
- **Location resolution.** If `audit.database` is set, the `LOCATION` is derived dynamically from
  that Hive database's own warehouse directory — `spark.catalog.getDatabase(database).locationUri +
  "/" + table.toLowerCase` — and the table is registered as `database.table`. Otherwise the location
  falls back to the config `root`/`location` (normalized to an absolute local path) with an
  unqualified table name. The same fallback applies if the database lookup fails (e.g. no metastore).
- **One partition per run.** Writing a run overwrites only its own `module_name=…/run_id=…`
  partition (dynamic partition overwrite), so the row written at start (with `end_date`/`duration`
  null) is replaced in place by the finalized row, and concurrent runs never contend.

### Two-phase persistence (why it's robust)

| Phase | Mechanism | Needs metastore? |
|---|---|---|
| **Data write** | Spark ORC datasource, `partitionBy("module_name","run_id")`, dynamic overwrite | **No** — works locally and on the cluster |
| **Table registration** | `CREATE EXTERNAL TABLE IF NOT EXISTS … STORED AS ORC` + `ADD PARTITION` | Yes — **best-effort & guarded** |

If the metastore is unavailable (e.g. a local box without Hadoop native IO / winutils), the ORC
data is still written and the run still audited; only the catalog entry is skipped with a warning.
And the audit **never breaks the job** — all audit IO is guarded (warn-only on failure).

## Schema (`run_history`)

Partition columns are **`module_name`** and **`run_id`**; the rest are regular columns.

| Column | Type | Part? | Null? | Meaning | Example |
|---|---|---|---|---|---|
| `run_id` | string | **P** | no | Unique run id — generated (UUID) or given in the conf | `5afc5010-3e62-46f3-a5ea-3856f01dcf0d` |
| `application_id` | string | | no | Spark application id | `application_1773889567248_10449` |
| `module_name` | string | **P** | no | Module that ran | `addons` / `climatetables` / `excelor` / `tseadfwd` |
| `used_jar` | string | | no | File name of the jar the run was launched from (never a path) | `str-file-transform-engine-1.0-RELEASE-Climate-Tables.jar` |
| `used_conf` | string | | no | Path of the `application.conf` used | `/Projects/…/application_climate_tables_run_2.conf` |
| `user_launcher` | string | | no | User who launched the run (from the conf) | `j03627` |
| `status` | string | | no | Spark run state: `RUNNING` \| `SUCCESS` \| `FAILED` | `SUCCESS` |
| `creation_date` | timestamp | | no | Run start | `2026-03-25 09:34:53.828` |
| `end_date` | timestamp | | yes | Run end (null while running) | `2026-03-25 09:55:21.22` |
| `duration` | string | | yes | Human-readable elapsed time (null while running) | `0h 20mn 27s` |
| `motor` | string | | no | Compute motor/engine | `iris` |
| `projection_dates` | string | | yes | Projection years (module-specific) | `[2030, 2040, 2050]` |
| `scenarios` | string | | yes | Scenarios (module-specific) | `["FW", "NZ50", "DT", "NDC"]` |
| `base_folder_name` | string | | yes | Run base folder (module-specific) | `ICAAP_TR_2025Q1_251016_v1` |

A row is written at start with `status = RUNNING` (`end_date`/`duration` null), then **overwritten**
in place at the end with `status = SUCCESS` or `FAILED` plus `end_date` + `duration`. A row left in
`RUNNING` (null `end_date`) = a run whose JVM was killed / crashed.

### Table DDL

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS run_history (
  application_id STRING, used_jar STRING, used_conf STRING, user_launcher STRING,
  status STRING, creation_date TIMESTAMP, end_date TIMESTAMP, duration STRING, motor STRING,
  projection_dates STRING, scenarios STRING, base_folder_name STRING
)
PARTITIONED BY (module_name STRING, run_id STRING)
STORED AS ORC
LOCATION 'hdfs:///…/run_history';
```

`RunAuditStore.createTableDDL(table, location)` returns exactly this string;
`RunAuditStore.schema` is the equivalent Spark `StructType`. Right after creating the table,
`RunAuditStore.alterProps(table)` runs `ALTER TABLE … SET TBLPROPERTIES ('external.table.purge'='FALSE')`
so a later `DROP TABLE` keeps the ORC data.

## Launcher-supplied metadata

Resolved per field, first non-blank wins:

`explicit override` → `-D<sysprop>` → `<ENV_VAR>` → `config` → default

| Field | System property | Env var | Config key | Default |
|---|---|---|---|---|
| `run_id` | — | — | `audit.runId` | generated UUID |
| `user_launcher` | `run.userLauncher` | `RUN_USER_LAUNCHER` | `audit.userLauncher` | JVM `user.name` |
| `motor` | `run.motor` | `RUN_MOTOR` | `audit.motor` | `UNKNOWN` |
| `used_jar` | `run.usedJar` | `RUN_USED_JAR` | `audit.usedJar` | auto-detected jar file name, else `UNKNOWN` |

> `used_jar` is always the jar's **file name**, never a path. Detection tries, in order:
>
> 1. the classloader code source, **following the `__app__.jar` symlink** when that is what it
>    reports — covers an IDE, a plain `java -jar`, a YARN **client** run, and YARN **cluster** mode;
> 2. that same symlink read straight from the container working directory, for when the code source
>    is unavailable or reports something else;
> 3. the application jar on YARN's distributed cache, `spark.yarn.cache.filenames`;
> 4. `spark.jars` / `spark.yarn.dist.jars`.
>
> **The symlink is what makes cluster mode work.** YARN localizes the application jar into the
> NodeManager cache under its real name and puts a link beside it:
>
> ```
> __app__.jar -> /hadoop/yarn/nm/usercache/<user>/filecache/<id>/str-…-RELEASE.jar
> ```
>
> so following it recovers the name the placeholder hides. Everything else comes up empty there:
> the code source is only the placeholder; `ApplicationMaster` **deletes** the `spark.yarn.cache.*`
> keys from the SparkConf as soon as it has built the executor resources, so level 3 finds nothing
> by the time user code runs; and Spark does not always copy the primary application resource into
> `spark.jars`, leaving level 4 empty too. That combination is what produced `UNKNOWN`.
>
> The link is followed **only** for `__app__.jar`. Everywhere else the code source is already the
> real file, and resolving it would rename a perfectly good jar wherever a deployment symlinks its
> jars. Level 3 is likewise strict — only the `#__app__.jar` cache entry counts, because falling back
> to "the first jar in the list" would report a `--jars` dependency as the application jar, which is
> worse than `UNKNOWN`: a wrong build name reads as fact, a missing one reads as missing.
>
> The path is dropped at every level on purpose. On YARN the classloader reports the container-local
> copy (`/hadoop/yarn/nm/usercache/<user>/filecache/<id>/app.jar`), whose directory is per-node and
> per-run — the NodeManager cache slot gets reused — so it resolves nowhere afterwards. The name
> still says which **build** ran.
>
> If all three come up empty, `used_jar` is `UNKNOWN` **and the driver log says why** — one WARN
> line naming the code source and every conf key that was read:
>
> ```
> [audit] used_jar not detected, recording UNKNOWN. codeSource='…', codeSourceResolved='…',
> appJarSymlink='…', cwd='…', spark.yarn.cache.filenames=<absent>, spark.jars=<absent>,
> spark.yarn.dist.jars=<absent>, spark.submit.deployMode='cluster', spark.master='yarn'.
> Set audit.usedJar (or -Drun.usedJar / RUN_USED_JAR) to record it explicitly.
> ```
>
> Grep the driver log for `used_jar not detected` when a run shows `UNKNOWN`; that line is the whole
> diagnosis.
>
> **The guaranteed answer is the override.** `-Drun.usedJar`, `RUN_USED_JAR` or `audit.usedJar` is
> applied *before* detection and works in every mode, so a submit script that sets it never depends
> on detection at all:
>
> ```bash
> spark-submit --conf spark.driver.extraJavaOptions=-Drun.usedJar=str-file-transform-engine-1.4.2-RELEASE.jar …
> ```

`application_id` is read from `spark.sparkContext.applicationId`. `projection_dates`, `scenarios`
and `base_folder_name` are module-specific and passed by the driver (null when not applicable).

## Config block

```hocon
audit {
  enabled = true
  table   = "run_history"            # external Hive table name (unqualified; see `database`)
  # database = "my_hive_db"          # optional: put the table in this Hive db and derive LOCATION
  #                                  #   from its warehouse dir (<db.locationUri>/<table>).
  root    = "localRun/run_history"   # table LOCATION when `database` is unset; hdfs://… on the cluster
  # runId        = "..."             # optional: pin the run_id (else a UUID is generated)
  userLauncher = "j03627"
  motor        = "tseadfwd"
  # usedJar     = "str-file-transform-engine-1.0-RELEASE.jar"
}
```

## Usage in a driver

```scala
import com.bnp.str.utilities.audit.RunAudit

implicit val spark: SparkSession = ...   // Hive-enabled on the cluster
val audit = RunAudit.start(
  moduleName = "tseadfwd",
  auditConfig = config.getConfig("tseadfwd_app.audit"),
  usedConf    = absoluteConfigPath
  // climatetables also passes: projectionDates=…, scenarios=…, baseFolderName=…
)
try {
  run()
  audit.succeeded()
} catch {
  case e: Throwable => audit.failed(e); throw e
}
```

Each new module calls the same `RunAudit.start(moduleName = "<its-module>", …)`.

## Reading the history back

```scala
import com.bnp.str.utilities.audit.RunAuditStore
// via the catalog (needs the external table registered):
val hist = RunAuditStore.read("run_history")
// or straight from ORC files (no metastore needed):
val hist = RunAuditStore.readFiles("hdfs:///…/run_history")
hist.groupBy("module_name").count().show()
```

## Notes

- **`run_id` as a partition** means one partition per run — convenient for point lookups and
  in-place updates, but the partition count grows with the number of runs. If that becomes large,
  periodically compact/expire old `run_id` partitions or add a coarser partition upstream.
- Local Windows dev without Hadoop native IO (winutils) writes the ORC data fine but skips the
  metastore registration (logged as a warning); use `RunAuditStore.readFiles(root)` there.
