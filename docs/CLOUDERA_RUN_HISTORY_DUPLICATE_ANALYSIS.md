# Analysis — Duplicate `run_history` rows on the Cloudera cluster (one `RUNNING` + one `SUCCESS` for the same `run_id`)

**Module:** `ageing` (`AgeingMacroeconomicScenarios`) · **Table:** `dbiris.run_history`
**Symptom:** two rows share the same `run_id` — one `status = RUNNING` (null `end_date`/`duration`)
and one `status = SUCCESS` (populated `end_date`/`duration`).
**Scope:** reproduces on the Cloudera integration cluster; **not** reproducible on production.

---

## 1. TL;DR (root cause)

The audit writer relies on **Spark dynamic partition overwrite** to *replace in place* the
`RUNNING` row with the final `SUCCESS` row inside the same
`module_name=ageing/run_id=<id>/` partition directory (see `RunAuditStore.write`, `RUN_AUDIT.md`
§ "One partition per run"). "Overwrite" here means: **delete the existing files in that partition,
then write the new file.**

On the integration cluster the job runs with the **S3A / cloud commit protocol**
`org.apache.spark.internal.io.cloud.PathOutputCommitProtocol` (visible in the captured `SparkConf`),
instead of the default `SQLHadoopMapReduceCommitProtocol` used in production. That commit protocol
does **not** honour the "delete the target partition before committing" step that dynamic partition
overwrite depends on. The consequence:

1. `RunAudit.start()` writes `part-…-<uuidA>.orc` (status `RUNNING`) into the run's partition.
2. `RunAudit.succeeded()` writes `part-…-<uuidB>.orc` (status `SUCCESS`) into the **same** partition
   — but the `RUNNING` file is **not removed**.
3. The partition now contains **two ORC files**, so scanning the table returns **two rows** for the
   one `run_id`.

Production uses the standard `FileOutputCommitter`-based protocol, which *does* delete the partition
on a dynamic overwrite — so there the `RUNNING` file is replaced and only the final row remains.

> **In one line:** the replace-in-place contract of `run_history` breaks under the cloud commit
> protocol (`PathOutputCommitProtocol`), which appends rather than replaces the run's partition.

---

## 2. How the audit is *supposed* to work

Each run writes to `dbiris.run_history` exactly twice, from the **same JVM**, with the **same
`run_id`** (`src/main/scala/com.bnp.str.ageing/job/MainDriver.scala`):

```scala
val audit = AgeingAudit.start(config, usedConf = absoluteConfigPath, primaryReader.scenarioNames)
// ... work ...
audit.succeeded()          // or audit.failed(e) on error
```

- `RunAudit.start` builds the record with `status = RUNNING`, `endDate = None`, then calls
  `persist()` → `RunAuditStore.write(...)`.
- `RunAudit.succeeded` copies the record to `status = SUCCESS` with `endDate`/`duration` set, then
  calls `persist()` → `RunAuditStore.write(...)` **again**.

The physical write (`RunAuditStore.write`, lines 71–84) is a **DataSource-V1 file write to a path**:

```scala
df.write.mode("overwrite").format("orc").partitionBy("module_name", "run_id").save(location)
```

and dynamic partition overwrite is turned on for the session
(`StrSparkSessionManager`, line 17):

```scala
.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

**Intended effect:** the second `save(...)` overwrites *only* the `run_id=<id>` partition — deleting
the `RUNNING` file and leaving a single `SUCCESS` file. This is the exact promise documented in
`docs/RUN_AUDIT.md`:

> *"Writing a run overwrites only its own `module_name=…/run_id=…` partition (dynamic partition
> overwrite), so the row written at start … is replaced in place by the finalized row."*

That promise holds **only if the active commit protocol implements dynamic-overwrite by deleting the
target partition directory.** That is the assumption that fails on this cluster.

---

## 3. Why two rows appear — the commit-protocol mechanism

`.save(mode = "overwrite")` is executed by Spark's `InsertIntoHadoopFsRelationCommand` /
`FileFormatWriter`, whose commit behaviour is governed by
**`spark.sql.sources.commitProtocolClass`**.

| | Commit protocol | Dynamic-overwrite behaviour | Result on 2nd write |
|---|---|---|---|
| **Production** | `SQLHadoopMapReduceCommitProtocol` (default, `FileOutputCommitter`) | Stages output, then **deletes the touched partition dir** and moves the new file in | `RUNNING` file removed → **1 row** ✅ |
| **Cloudera integ.** | `org.apache.spark.internal.io.cloud.PathOutputCommitProtocol` (S3A/cloud committers) | Commits task files **directly** into the final partition with unique names; the classic "delete partition, then rename" step is bypassed | `RUNNING` file **survives**, `SUCCESS` file added next to it → **2 rows** ❌ |

The cloud committers (`directory`/`magic`/manifest) exist precisely to *avoid* the rename-and-list
dance the `FileOutputCommitter` performs on object stores. A documented limitation of that family is
that **dynamic partition overwrite is not supported / not equivalent** to the classic committer: the
pre-existing partition data is not cleaned. Applied to `run_history`, "not cleaned" means the
`RUNNING` row from `start()` is left behind when `succeeded()` writes the final row.

### Config evidence (from the captured `SparkConf` dump)

The provided YARN container log shows this cluster runs with the cloud committer and S3A committer
wiring — the settings that flip the behaviour away from production:

```
spark.sql.sources.partitionOverwriteMode   = dynamic
spark.sql.sources.commitProtocolClass      = org.apache.spark.internal.io.cloud.PathOutputCommitProtocol
mapreduce.fileoutputcommitter.algorithm.version = 1
spark.hadoop.fs.s3a.committer.name         = directory
```

- `commitProtocolClass = PathOutputCommitProtocol` is the smoking gun — this is **not** the Spark
  default, and it is almost certainly injected at the cluster / Oozie / Spark-defaults level on this
  environment, not by the application (the app never sets it).
- `fs.s3a.committer.name = directory` + `PathOutputCommitProtocol` are the S3A "zero-rename"
  committer stack. They are being applied even though the warehouse is HDFS
  (`spark.sql.warehouse.dir = hdfs://hahdfsnameservice/...`), which is itself a misconfiguration for
  an HDFS target and changes commit semantics vs. production.
- `partitionOverwriteMode = dynamic` is present on **both** clusters (the app forces it in
  `StrSparkSessionManager`), so the overwrite *mode* is **not** the differentiator — the **commit
  protocol** is.

---

## 4. Alternatives considered and ruled out

- **Oozie / YARN relaunch (a second attempt re-ran the job).**
  The two rows share the **same `run_id`**. `run_id` is a **per-JVM UUID** — the `ageing`
  `application.conf` does **not** pin `audit.runId`, and `RunAudit.start` generates
  `UUID.randomUUID()` when it is absent. A relaunch is a new JVM → a **new** `run_id` → a **different
  partition**, which could never collide on the same `run_id`. So the two rows must come from the
  **two writes of a single JVM** (`start` + `succeeded`), which is exactly the replace-in-place path.
  → **Ruled out.**

- **The write threw and was retried.**
  If `PathOutputCommitProtocol` had *rejected* dynamic overwrite outright (older builds `require`
  against it), the write would throw. `persist()` guards writes in try/catch, so a throw at `start()`
  would mean **no `RUNNING` row at all**. But a `RUNNING` row *is* present → the write did **not**
  throw; it silently appended. → Consistent with §3, **rules out the "hard error" variant.**

- **Static vs dynamic overwrite mismatch.**
  Under *static* overwrite the second write would wipe the whole table location (all runs) and leave
  one row — the failure mode would be **data loss of other runs**, not a duplicate of the same
  `run_id`. The observed symptom (duplicate, other runs intact) matches **dynamic + non-deleting
  committer**, not static. → **Ruled out.**

- **Metastore / `ADD PARTITION` doubling.**
  Reads go through the partition's files (`spark.table` → the ORC files under
  `run_id=<id>/`). Registering a partition twice does not duplicate rows; **two files in one
  partition** does. The partition registration (`ADD IF NOT EXISTS PARTITION`) is idempotent. →
  **Not the cause.**

---

## 5. How to confirm on the cluster (quick checks)

1. **Count physical files in the partition** — the definitive check:
   ```bash
   hdfs dfs -ls '/warehouse/tablespace/managed/hive/dbiris.db/run_history/module_name=ageing/run_id=<id>/'
   ```
   Two `part-*.orc` files ⇒ the `SUCCESS` write appended instead of replacing.

2. **Confirm the active commit protocol** at runtime (Spark UI → Environment, or in the driver log
   `SparkConfLogger` dump — the same one captured):
   ```
   spark.sql.sources.commitProtocolClass = ...PathOutputCommitProtocol   ← integ (broken)
   spark.sql.sources.commitProtocolClass = ...SQLHadoopMapReduceCommitProtocol  ← prod (ok)
   ```

3. **Read the two rows and compare `creation_date`** — both carry the *same* `run_id`; the older
   `creation_date` with null `end_date` is the leftover `start()` file:
   ```sql
   SELECT run_id, status, creation_date, end_date
   FROM dbiris.run_history
   WHERE module_name = 'ageing' AND run_id = '<id>'
   ORDER BY creation_date;
   ```

4. **Diff the two clusters' Spark defaults** (`spark-defaults.conf`, Oozie `spark-opts`, CDS/parcel
   config) for `spark.sql.sources.commitProtocolClass`, `spark.hadoop.fs.s3a.committer.name`, and
   `mapreduce.outputcommitter.factory.scheme.hdfs`.

---

## 6. Fix options (ranked)

The real problem is that **replace-in-place depends on a committer behaviour that the environment can
silently change.** Options, from most robust to quickest:

1. **Make the audit writer independent of the committer's dynamic-overwrite delete (recommended).**
   Before writing the final row, explicitly delete the run's partition directory, then write:
   ```scala
   // in RunAuditStore.write, before df.write...:
   val partPath = new Path(s"$location/module_name=${record.moduleName}/run_id=${record.runId}")
   val fs = partPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
   if (fs.exists(partPath)) fs.delete(partPath, true)
   df.write.mode("overwrite").format("orc").partitionBy("module_name", "run_id").save(location)
   ```
   This guarantees a single file per run regardless of the active commit protocol. (Keep it guarded,
   like the rest of the audit IO.)

2. **Pin the standard commit protocol for the audit write**, so the environment's cloud committer
   cannot alter replace-in-place semantics:
   ```
   spark.sql.sources.commitProtocolClass = org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
   spark.hadoop.mapreduce.outputcommitter.factory.scheme.hdfs = \
       org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory
   ```
   (Cluster-side change; verify it does not conflict with other jobs on the cluster that *need* the
   cloud committer.)

3. **Register the audit table as a real Hive-managed partitioned table and use
   `INSERT OVERWRITE … PARTITION (module_name=…, run_id=…)`** through the metastore instead of a raw
   `.save(path)`. The metastore-driven overwrite deletes the partition regardless of the file
   committer.

4. **Make the read tolerant (defensive, not a real fix).** When querying `run_history`, keep only the
   latest row per `run_id` (e.g. the one with the newest `creation_date`, or `SUCCESS`/`FAILED` over
   `RUNNING`). This hides the symptom but leaves duplicate ORC files accumulating on disk.

**Recommendation:** implement **(1)** in `RunAuditStore.write` (portable, self-contained, fixes both
integ and any future cluster with a non-standard committer) and, if the cluster team agrees, also
apply **(2)** so the audit table's storage stays clean. Option (4) can be layered on reads as a
belt-and-braces guard while (1) rolls out.

---

## 7. Why production is unaffected

Production does not inject `PathOutputCommitProtocol` — it uses Spark's default
`SQLHadoopMapReduceCommitProtocol` with the classic `FileOutputCommitter`. There, a dynamic partition
overwrite deletes the `run_id=<id>` partition before committing the final file, so the `RUNNING` file
is replaced and exactly one row survives. Same application code, same `partitionOverwriteMode =
dynamic`; the **only** behavioural difference is the environment-supplied commit protocol.

---

### References (code)

- `src/main/scala/com.bnp.str.utilities/audit/RunAuditStore.scala` — `write` (lines 71–84), the
  `.mode("overwrite").partitionBy(...).save(location)` path.
- `src/main/scala/com.bnp.str.utilities/audit/RunAudit.scala` — `start` / `succeeded` / `finish` /
  `persist` (the guarded two-phase write).
- `src/main/scala/com.bnp.str.ageing/job/MainDriver.scala` — `start()` … `succeeded()` call sites.
- `src/main/scala/com.bnp.str.ageing/sessionmanager/StrSparkSessionManager.scala` — line 17 forces
  `partitionOverwriteMode = dynamic`.
- `docs/RUN_AUDIT.md` — documents the replace-in-place contract that this environment breaks.
