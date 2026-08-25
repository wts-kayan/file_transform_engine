# Business data quality on `TS_EAD_FWD`

What the branch `feature/tseadfwd-data-quality` added (commit `072ac18`): two business rules on the
**output** term structure, the row removal they imply, and a self-contained HTML report handed to the
business team.

This is **post-calculation, business** control on what the engine produced. It is distinct from
`validation/DataControlView`, which is the **pre-calculation, technical** control on the *inputs* and
can abort the run — nothing here ever aborts anything.

---

## The two rules

| Rule | What it checks | What happens to the line |
|---|---|---|
| **R01** — all terms equal to 1 | Lines are grouped **by `EAD_MATRIX_ID` *and* `SCENARIO_ID`** — one group is one curve. The group is flagged when **every** one of its terms carries `EAD_RA_RATE = 1` (within `tolerance`): exposure is full at every term, no loss ever accrues, so the line carries no information. A single term below 1 keeps the whole group. `EAD_MATRIX_ID` ends in `_Q` / `_Y`, so a matrix's quarterly and yearly curves are **two separate groups**. | **Removed** from the output and listed in the report. `remove = false` keeps the lines and only reports them. |
| **R02** — negative `EAD_RA_RATE` | The rate is strictly negative, which is not a possible exposure factor (it must lie in `[0, 1]`). Zero does not fire; a blank/unparseable cell is treated as *not* a number, never as 1. | **Line and value kept as computed**, and reported. A `replaceWith` token can mask the value for a consumer that cannot take a negative. |

Rule identity, wording and the report's value model live in `dataquality/DqModel.scala`
(`DqRule`, `DqFinding`, `DqRuleResult`, `DqReport`) — pure data, no Spark, no IO.

### Statuses

`PASS` (nothing found) · `SKIPPED` (rule disabled in the conf — reported as skipped, never as a pass)
· `REMOVED` (found and dropped) · `REPORTED` (found and kept). The report's *Action* column resolves
the "nothing found" case first, so a clean run never reads as though its removal had been switched off.

---

## The split: the rules never write

Enforced structurally, because it is what the business asked for:

- `dataquality/DataQualityMapper` **evaluates** the rules and **names** the offending keys. It never
  writes a row.
- `job/MainDriver` performs the removal — **once**, on the frame it is about to write — then writes
  the report next to the output.
- `job/DataQualityDriver` re-runs the *same* rules against an **already produced CSV** and rewrites
  the report. Report only; it changes nothing.

Two entry points on the mapper reflect that:

```scala
def reportOnly(df, source, runId): DqReport   // evaluate, touch nothing (rowsOut == rowsIn)
def apply(df, source, runId): DqOutcome       // evaluate + return the cleaned frame for the writer
```

Order inside `apply`, which matters:

1. R01 → anti-join the flagged `(EAD_MATRIX_ID, SCENARIO_ID)` keys out (broadcast `left_anti`).
2. `exclude_ead_ra_rate_ge_1` → keep the terms where loss has started to accrue. A **null** rate is
   kept: an unparseable value is a finding to look at, not a row to silently drop.
3. R02's marker **last** — it turns the cell into a non-numeric token, so every numeric predicate
   above has to have run already.

`EAD_RA_RATE` and `TERM` arrive as decimal-comma **strings** (`PrimaryMapper.fmtNumber`), so every
comparison goes through a `numeric()` column helper rather than a raw string test.

---

## Two engine changes the rules required

### 1. `exclude_ead_ra_rate_ge_1` moved out of `PrimaryMapper`

It used to delete every `EAD_RA_RATE >= 1` row inside `PrimaryMapper` — which is exactly what R01
needs to see: an all-ones curve had already vanished before any rule could group it. `PrimaryMapper`
now emits **every** computed term, and the exclusion is applied by the data-quality mapper afterwards.

The conf key keeps its name and its place (`parameters.exclude_ead_ra_rate_ge_1`), so existing
configuration files run unchanged, and a local run reproduces the previous CSV byte for byte.

### 2. `parameters.allow_negative_ead_ra_rate` (new, default `false`)

A negative `EAD_RA_RATE` could not occur at all, for two independent reasons:

- the **run-off freeze** (`PrimaryView.RUNOFF_RA_CAP = 1.0`) truncates the RA series at the first
  `RA >= 1`, so every `(1 - RA)` factor stays positive and the cumulative product never goes negative;
- `vectorFactored` reported a sub-zero product as `1` anyway.

Relaxing only one of the two changes nothing, which is why they share a single flag. With
`allow_negative_ead_ra_rate = true`, the cap becomes `PrimaryView.NO_RUNOFF_CAP` (`+Infinity`) and
`vectorFactored(ra, emitNegative = true)` emits the true running product — so the real number reaches
the output and **R02 has something to report**.

Mechanically: every RA function (`PrimaryView.centralRa` / `statOnlyRa` / `scenarioRa` and their
`PrimaryViewYearly` counterparts) gained a `raCap` parameter defaulting to `RUNOFF_RA_CAP`;
`PrimaryMapper` resolves the cap once per run and threads it through.

> **Warning.** With the freeze off, a run-off matrix keeps computing past the cliff instead of holding
> its last good value flat, so its **whole tail changes**. Default `false` is today's validated
> behaviour — the one the target output was reconciled against.

---

## The HTML report

One **self-contained** HTML document (`dataquality/DqHtmlView`, pure `String` in / `String` out):
no external stylesheet, script, image or web font, and plain ASCII wording throughout. It is mailed
around and opened from a share, and a character mangled by a gateway reads as a defect in the numbers
next to it. Cells are HTML-escaped.

It carries:

- header verdict (`PASS` / `FINDINGS`) and the counters — lines examined, kept, removed, findings;
- the **output file the rules were run on**, named twice: the file *name* next to the title and in the
  browser tab (`Data quality: TS_EAD_FWD_25Q4_v1_small.csv`), where a reader checks it first because
  several vintages of the same table live side by side in one output directory, and the full *path*
  in the metadata block, which identifies it without ambiguity. It is resolved from the `TS_EAD_FWD`
  block exactly as `PrimaryUtilities.writeDataframe` resolves it — `.csv` when the part files are
  collapsed, `.xlsx` for an Excel output, and the directory name marked `(part-file directory)` when
  `singleFile = false`. A standalone run names the CSV it actually read instead. The file name also
  opens the run log's `DATA QUALITY on <file> - <verdict> ...` line;
- **source** (the dataset, `TS_EAD_FWD`), **run id** (the `run_history` run id, so a report ties back
  to its execution) and generation timestamp;
- a summary table, one row per rule (id, title, status, findings, action);
- one section per rule: the rule in the business team's own terms, what was done about it, then the
  findings — group-level for R01 (matrix, scenario, value), row-level for R02 (matrix, scenario, term,
  value). The R02 listing is capped by `maxRowsInReport`; the **count is always complete**, and a
  truncated listing says so.

`DqWriter.writeHtml` goes through Hadoop's `FileSystem`, so `htmlPath` may be local or HDFS with no
change.

---

## Configuration — `tseadfwd_app.DATA_QUALITY`

Every key has a default, so a conf predating this feature still runs (data quality on, both rules on,
R01 removing, report written next to `TS_EAD_FWD`).

```hocon
DATA_QUALITY {
  enabled    = true                                              # false: no rules, no report, every row written
  htmlPath   = "localRun/tseadfwd/output/DQ_TS_EAD_FWD.html"     # default: <tmpPath>/DQ_<tableName>.html
  sourcePath = "localRun/tseadfwd/output/TS_EAD_FWD_...csv"      # DataQualityDriver only; default: the output CSV

  rules {
    all_terms_equal_one {
      enabled   = true
      remove    = true      # false: report the line, keep it
      tolerance = 1e-9      # |value - 1| <= tolerance counts as "equal to 1"
    }
    negative_ead_ra_rate {
      enabled         = true
      replaceWith     = ""  # empty: write the value AS COMPUTED. A token (e.g. "NV") only for a
                            # consumer that cannot take a negative — it makes the cell non-numeric
      maxRowsInReport = 500 # cap on rows LISTED; the count is always complete
    }
  }
}
```

Related `parameters` keys: `exclude_ead_ra_rate_ge_1` (unchanged name, new place of effect) and
`allow_negative_ead_ra_rate` (new).

---

## Running it

```bash
# report produced automatically by every production run
spark-submit --class com.bnp.str.tseadfwd.job.MainDriver \
  --master "local[*]" target/wts-training-spark.jar localRun/tseadfwd/application.conf

# regenerate the report from an already produced CSV (report only)
spark-submit --class com.bnp.str.tseadfwd.job.DataQualityDriver \
  --master "local[*]" target/wts-training-spark.jar localRun/tseadfwd/application.conf
```

**Caveat.** `DataQualityDriver` can only report what is still in the file. When the main run already
removed the R01 lines — the default — a standalone pass over its output correctly finds nothing; the
report that *names* them is the one `MainDriver` wrote.

---

## Latent crash fixed along the way

`job/EadFwdCompare` parsed `EAD_RA_RATE` with `java.lang.Double.valueOf` and would have thrown on any
non-numeric cell — including an R02 `replaceWith` token. It now yields `null`, so the key is reported
as a **difference** instead of killing the job.

---

## Tests and demonstration

- `DataQualityMapperSpec` — 26 tests: R01 grouping (including `_Q` vs `_Y` as separate groups), the
  single-deviating-term boundary, tolerance, blank values, `remove = false`, disabled rules; R02
  reporting, the marker and its ordering after the numeric filters, the empty-`replaceWith` default,
  zero not firing, the report cap; `exclude_ead_ra_rate_ge_1` running *after* the rules; `reportOnly`
  removing nothing; a clean output producing `PASS`; and the output file the report names — carried
  through from the caller, defaulted to the file a standalone run read, and resolved from the conf for
  a collapsed CSV, a part-file directory and an Excel workbook.
- `DqHtmlViewSpec` — 12 tests: self-containment (no external stylesheet, script, image or web font),
  plain-ASCII wording, HTML escaping, `PASS` vs `SKIPPED`, truncation notice, verdict, and the output
  file — named as a path and as a name, escaped, and leaving no blank line when unknown.
- `SparkTestSession` — one local `SparkSession` shared by every tseadfwd suite (a session per suite
  multiplied the run time and made two live sessions fight over the temp dirs).
- `DqSimulationApp` (test sources, next to `EadFwdValidationApp`) — the production inputs never
  produce a full-exposure curve, so the rules report nothing on a normal run. This app reads the
  produced `TS_EAD_FWD` CSV, **adds four simulated curves** to it and runs the real mapper over the
  result — same code path as `MainDriver`, removal included:

  | Simulated curve | Expected |
  |---|---|
  | `SIMU_ALLONES_TF_Q` / `C` — every term = 1 | R01: flagged, removed |
  | `SIMU_ALLONES_TF_Q` / `A` — every term = 1 except the last | R01: **not** flagged (the boundary case) |
  | `SIMU_ALLONES_TF_Y` / `C` — every term = 1 | R01: flagged, removed as a **separate** group |
  | `SIMU_NEGATIVE_TF_Q` / `C` — two sub-zero terms | R02: line kept, value written as computed (or as the marker) |

  It writes `TS_EAD_FWD_SIMULATED.csv` (before), `TS_EAD_FWD_SIMULATED_CLEANED.csv` (after) and
  `DQ_SIMULATION.html` under the gitignored `output/` directory. The real CSV is read, never rewritten.

Verified on the branch: the whole ScalaTest run is green (**90 tests**, every suite in the repo — it
was 52 at the feature commit, before the output-file additions), and a production run still writes the
`localRun` CSV byte for byte.

Surefire cannot run offline here (`surefire-junit4` is not in the local repository), so the suites are
run through the ScalaTest runner directly:

```bash
mvn -o -q test-compile
java -cp "target/classes;target/test-classes;$(cat cp.txt)" org.scalatest.tools.Runner -o -R target/test-classes
```

---

## Files

| File | Role |
|---|---|
| `dataquality/DqModel.scala` | Rules, findings, per-rule result, report — pure data |
| `dataquality/DataQualityMapper.scala` | `DqConfig` + rule evaluation, removal, marker |
| `dataquality/DqHtmlView.scala` | Self-contained HTML rendering |
| `dataquality/DqWriter.scala` | Persists the report through Hadoop's `FileSystem` |
| `job/DataQualityDriver.scala` | Standalone report job |
| `job/MainDriver.scala` | Calls the mapper, writes the cleaned frame and the report |
| `mapping/PrimaryMapper.scala` | Emits every term; resolves `raCap` / `allowNegativeEadRaRate` |
| `mapping/PrimaryView.scala`, `mapping/PrimaryViewYearly.scala` | `raCap` parameter, `NO_RUNOFF_CAP`, `vectorFactored(emitNegative)` |
| `job/EadFwdCompare.scala` | Non-numeric cell → `null` instead of a crash |
| `README.md` | "Data quality" section, new job and conf keys |
