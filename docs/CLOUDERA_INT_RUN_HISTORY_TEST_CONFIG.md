# INT test config — stop `run_history` duplicating on the Cloudera integration cluster

**Goal:** make the `ageing` run write **one** row per `run_id` (the `RUNNING` row replaced in place by
the final `SUCCESS`/`FAILED` row) on the integration cluster, without touching the application code.

**Why this works:** the duplicate is caused by the INT cluster injecting the cloud commit protocol
`org.apache.spark.internal.io.cloud.PathOutputCommitProtocol`, which does **not** delete the run's
partition on a dynamic overwrite. Production runs with `commitProtocolClass = <unset>` (the default
`SQLHadoopMapReduceCommitProtocol`) and does not duplicate. The settings below **override the INT
cluster back to the production/default committer for this job only** — nothing else changes. See
`docs/CLOUDERA_RUN_HISTORY_DUPLICATE_ANALYSIS.md` for the full root-cause.

> The app never sets `spark.sql.sources.commitProtocolClass`, so a job-level `--conf` cleanly wins.
> Keep `spark.sql.sources.partitionOverwriteMode = dynamic` (the app already forces it) — do **not**
> remove it.

---

## A. Minimal — try this first (usually enough)

Force the standard commit protocol and the standard HDFS output committer for this run. Add these
three `--conf` flags:

```
--conf spark.sql.sources.commitProtocolClass=org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
--conf spark.hadoop.mapreduce.outputcommitter.factory.scheme.hdfs=org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory
--conf spark.sql.sources.partitionOverwriteMode=dynamic
```

| `--conf` | Why |
|---|---|
| `spark.sql.sources.commitProtocolClass=…SQLHadoopMapReduceCommitProtocol` | Restore the default Spark commit protocol that **deletes the target partition** on a dynamic overwrite (the one production uses). |
| `spark.hadoop.mapreduce.outputcommitter.factory.scheme.hdfs=…FileOutputCommitterFactory` | Ensure HDFS uses the classic `FileOutputCommitter`, not a cloud/S3A `PathOutputCommitter` binding. |
| `spark.sql.sources.partitionOverwriteMode=dynamic` | Keep dynamic overwrite ON (the app already forces it — listed only to be explicit). Do **not** remove it. |

### How to inject it

**`spark-submit`** (manual test run):

```bash
spark-submit \
  --conf spark.sql.sources.commitProtocolClass=org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol \
  --conf spark.hadoop.mapreduce.outputcommitter.factory.scheme.hdfs=org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  --class com.bnpparibas.itg.fresh.str.ageing.job.MainDriver \
  <app>.jar  <path-to-application.conf>
```

**Oozie `spark3`/`spark` action** — same flags, space-separated inside `<spark-opts>` in `workflow.xml`:

```xml
<spark-opts>--conf spark.sql.sources.commitProtocolClass=org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol --conf spark.hadoop.mapreduce.outputcommitter.factory.scheme.hdfs=org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory --conf spark.sql.sources.partitionOverwriteMode=dynamic</spark-opts>
```

---

## B. If A still duplicates — also disable the S3A committer binding

The INT dump also carried `spark.hadoop.fs.s3a.committer.name = directory` and Iceberg wiring. If those
are still steering the commit, add these `--conf` flags on top of block A:

```
--conf spark.hadoop.fs.s3a.committer.name=file
--conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=1
--conf spark.hadoop.mapreduce.outputcommitter.class=org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
```

Full `spark-submit` (A + B):

```bash
spark-submit \
  --conf spark.sql.sources.commitProtocolClass=org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol \
  --conf spark.hadoop.mapreduce.outputcommitter.factory.scheme.hdfs=org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory \
  --conf spark.hadoop.fs.s3a.committer.name=file \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=1 \
  --conf spark.hadoop.mapreduce.outputcommitter.class=org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  --class com.bnpparibas.itg.fresh.str.ageing.job.MainDriver \
  <app>.jar  <path-to-application.conf>
```

Full `<spark-opts>` (A + B):

```xml
<spark-opts>--conf spark.sql.sources.commitProtocolClass=org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol --conf spark.hadoop.mapreduce.outputcommitter.factory.scheme.hdfs=org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory --conf spark.hadoop.fs.s3a.committer.name=file --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=1 --conf spark.hadoop.mapreduce.outputcommitter.class=org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter --conf spark.sql.sources.partitionOverwriteMode=dynamic</spark-opts>
```

---

## C. Verify the test run

1. **Committer took effect** — in the driver log, `SparkConfLogger` should now print:
   ```
   [conf] spark.sql.sources.commitProtocolClass = org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
   ```
   (production shows `<unset>`, which resolves to the same default — either is fine.)

2. **One physical ORC file per run** — after a full run:
   ```bash
   hdfs dfs -ls '/warehouse/tablespace/external/hive/dbiris.db/run_history/module_name=ageing/run_id=<id>/'
   ```
   Expect a **single** `part-*.orc`. (Adjust the path to your `dbiris` warehouse dir; it is derived
   from `spark.catalog.getDatabase("dbiris").locationUri` — see `RunAudit.resolveTableAndLocation`.)

3. **One row per run** — in Hive/Spark SQL:
   ```sql
   SELECT run_id, COUNT(*) AS n
   FROM dbiris.run_history
   WHERE module_name = 'ageing'
   GROUP BY run_id
   HAVING COUNT(*) > 1;   -- should return 0 rows for runs made after the config change
   ```

---

## Notes & caveats

- **Scope it to the job.** Set these via the Oozie action `<spark-opts>` / `spark-submit --conf`, not
  in cluster-wide `spark-defaults.conf`, so other INT jobs that legitimately need the cloud committer
  are unaffected.
- **Existing duplicate rows are not cleaned** by a config change — it only prevents *new* duplicates.
  To clear old ones, delete the extra ORC file in the affected `run_id=…` partitions (or drop/rewrite
  those partitions).
- **This is an environment workaround, not the durable fix.** Because any cluster can silently swap the
  committer, the robust fix is code-side: have `RunAuditStore.write` explicitly delete the run's
  partition directory before `.save(...)` (option 1 in the analysis doc). Once that ships, the audit no
  longer depends on the committer's dynamic-overwrite delete and this INT config becomes unnecessary.
- Warehouse path is HDFS (`hdfs://hahdfsnameservice/...`) on both clusters, so the classic
  `FileOutputCommitter` is the correct committer here anyway; the S3A committer stack on INT was a
  mismatch for an HDFS target.
