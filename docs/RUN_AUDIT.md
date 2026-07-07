# Run Audit — `run_history`

A **shared, job-agnostic** execution audit for every job packaged in the jar. It lives in
`com.bnp.str.utilities.audit` (a sibling of `com.bnp.str.tseadfwd`) so any current or future
driver — launched by any IHM — records its runs to the same `run_history` table with two calls.

## Storage — ORC external table, partitioned by (module, runid)

- Records are written as **ORC** files laid out in Hive-style partitions:

  ```
  <root>/module=<module>/runid=<runid>/part-*.orc
  ```

- Exposed as an **EXTERNAL** Hive table `run_history`, `PARTITIONED BY (module, runid)`,
  `STORED AS ORC`, `LOCATION <root>`. Being EXTERNAL, dropping the table never deletes the data,
  and a fresh metastore can be re-pointed at the same location.
- **One partition per run.** Writing a run overwrites only its own `module=…/runid=…` partition
  (dynamic partition overwrite), so the `RUNNING` row written at start is replaced in place by the
  final `SUCCESS`/`FAILED` row, and concurrent runs/IHM never touch each other's partition.

### Two-phase persistence (why it's robust)

| Phase | Mechanism | Needs metastore? |
|---|---|---|
| **Data write** | Spark ORC datasource, `partitionBy("module","runid")`, dynamic overwrite | **No** — works locally and on the cluster |
| **Table registration** | `CREATE EXTERNAL TABLE IF NOT EXISTS … STORED AS ORC` + `ADD PARTITION` | Yes — **best-effort & guarded** |

If the metastore is unavailable (e.g. a local box without Hadoop native IO / winutils), the ORC
data is still written and the run still audited; only the catalog entry is skipped with a warning.
On a Hive-enabled cluster the table and partitions register automatically. And the audit **never
breaks the job** — all audit IO is guarded (warn-only on failure).

## Schema (`run_history`)

Partition columns are **`module`** and **`runid`**; the rest are regular columns.

| Column | Type | Part? | Null? | Meaning |
|---|---|---|---|---|
| `module` | string | **P** | no | Logical module/package that owns the job, e.g. `tseadfwd` |
| `runid` | string (UUID) | **P** | no | Unique id of the run (primary key) |
| `job_name` | string | | no | Logical job, e.g. `TS_EAD_FWD` |
| `job_class` | string | | no | Fully-qualified driver class that ran |
| `app_version` | string | | no | Application / jar version |
| `status` | string | | no | `RUNNING` \| `SUCCESS` \| `FAILED` |
| `triggered_by` | string | | no | User / service account that launched the run |
| `launch_channel` | string | | no | Which IHM / entry point (e.g. `IHM_RISK`, `SCHEDULER`, `CLI`) |
| `environment` | string | | no | `DEV` \| `UAT` \| `PROD` \| `LOCAL` |
| `hostname` | string | | no | Machine that executed the driver |
| `config_path` | string | | yes | Path of the `application.conf` used |
| `params` | string (JSON) | | no | Snapshot of key run parameters (free-form per job) |
| `input_count` | bigint | | yes | Input records/files read (null if not measured) |
| `output_count` | bigint | | yes | Output rows written (null if not measured) |
| `output_path` | string | | yes | Where the job wrote its output |
| `start_ts` | string (ISO-8601 UTC) | | no | Run start instant |
| `end_ts` | string (ISO-8601 UTC) | | yes | Run end (null while `RUNNING`) |
| `duration_ms` | bigint | | yes | Wall-clock duration (null while `RUNNING`) |
| `error_type` | string | | yes | Exception class on failure |
| `error_message` | string | | yes | Exception message on failure |
| `error_stacktrace` | string | | yes | Truncated stack trace (≤ 8000 chars) on failure |
| `run_date` | string (date) | | no | `yyyy-MM-dd` of start (handy for filtering) |

A `RUNNING` row is written at start, then **overwritten** in place with the final `SUCCESS` /
`FAILED` row. A row left in `RUNNING` state = a run whose JVM was killed / crashed.

### Table DDL

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS run_history (
  job_name STRING, job_class STRING, app_version STRING, status STRING,
  triggered_by STRING, launch_channel STRING, environment STRING, hostname STRING,
  config_path STRING, params STRING, input_count BIGINT, output_count BIGINT,
  output_path STRING, start_ts STRING, end_ts STRING, duration_ms BIGINT,
  error_type STRING, error_message STRING, error_stacktrace STRING, run_date STRING
)
PARTITIONED BY (module STRING, runid STRING)
STORED AS ORC
LOCATION 'hdfs:///…/run_history';
```

`RunAuditStore.createTableDDL(table, location)` returns exactly this string;
`RunAuditStore.schema` is the equivalent Spark `StructType`.

## Identity — who / which IHM launched the run

Resolved per field, first non-blank wins:

`explicit override` → `-D<sysprop>` → `<ENV_VAR>` → `config` → default

| Field | System property | Env var | Config key | Default |
|---|---|---|---|---|
| `triggered_by` | `run.triggeredBy` | `RUN_TRIGGERED_BY` | `audit.triggeredBy` | JVM `user.name` |
| `launch_channel` | `run.launchChannel` | `RUN_LAUNCH_CHANNEL` | `audit.launchChannel` | `UNKNOWN` |
| `environment` | `run.env` | `RUN_ENV` | `audit.environment` | `UNKNOWN` |

So each IHM stamps its identity **without code changes** — e.g.:

```
spark-submit -Drun.triggeredBy=alice -Drun.launchChannel=IHM_RISK ...
# or
RUN_TRIGGERED_BY=svc_batch RUN_LAUNCH_CHANNEL=SCHEDULER spark-submit ...
```

## Config block

```hocon
audit {
  enabled     = true
  table       = "run_history"            # external Hive table name (optionally db.table)
  root        = "localRun/run_history"   # table LOCATION; hdfs://… on the cluster
  environment = "LOCAL"
  appVersion  = "1.0.0"
  # triggeredBy   = "batch"   # optional fallback if the IHM sets neither -D nor env
  # launchChannel = "CLI"     # optional fallback
}
```

## Usage in a driver

```scala
import com.bnp.str.utilities.audit.{AuditJson, RunAudit}

implicit val spark: SparkSession = ...   // Hive-enabled on the cluster
val audit = RunAudit.start(
  module      = "tseadfwd",
  jobName     = "TS_EAD_FWD",
  jobClass    = this.getClass.getName,
  auditConfig = config.getConfig("tseadfwd_app.audit"),
  configPath  = absoluteConfigPath,
  paramsJson  = AuditJson.obj("as_of_date_quarter" -> "2025Q4", "macro_delta_scale" -> 100)
)
try {
  val df = run()
  audit.succeeded(outputCount = Some(df.count()), outputPath = out)
} catch {
  case e: Throwable => audit.failed(e); throw e
}
```

Each new module in the jar calls the same `RunAudit.start(module = "<its-module>", …)`.

## Reading the history back

```scala
import com.bnp.str.utilities.audit.RunAuditStore
// via the catalog (needs the external table registered):
val hist = RunAuditStore.read("run_history")
// or straight from ORC files (no metastore needed):
val hist = RunAuditStore.readFiles("hdfs:///…/run_history")
hist.groupBy("module", "status").count().show()
```

## Notes

- **`runid` as a partition** means one partition per run — convenient for point lookups and
  in-place status updates, but the partition count grows with the number of runs. If that becomes
  large, periodically compact/expire old `runid` partitions (they are plain ORC dirs) or add a
  coarser partition (e.g. `run_date`) upstream of `runid`.
- Local Windows dev without Hadoop native IO (winutils) writes the ORC data fine but skips the
  metastore registration (logged as a warning); use `RunAuditStore.readFiles(root)` there.
