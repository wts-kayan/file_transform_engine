# Dynamic RA sheet loading — feasibility analysis

> **Status: implemented.** Option **C** below, with the safety rules of §5. What was actually built,
> and the one thing the analysis did not foresee, are in [§9](#9-as-implemented) at the end. The rest
> of this document is the reasoning that led there, kept as written.

**Question.** The RA input workbook holds one sheet per entity (`RA_BCEF`, `RA_BGL`, `RA_BNL`, …).
Today each one is named in `application.conf` *and* in the code. When the business adds an entity,
can the engine pick the new sheet up on its own, with no code change?

**Verdict: yes, and the change is small.** Everything downstream of the reader is already driven by
the *data*, not by the sheet name. The only thing that hardcodes the entity list is
`reader/PrimaryReader` — one file. Sheet names can be listed at runtime with an API the project
already depends on, so no new dependency is needed. The work is not in the reading; it is in
deciding **which sheets count** and in failing loudly when a newly added entity is incomplete.

---

## 1. What the code does today

```
application.conf                     PrimaryReader                     downstream
─────────────────────────────────    ──────────────────────────────    ──────────────────────────
RA_BCEF   { path, sheetNames }  ──▶  lazy val ra_bcef                  raInput  (unionByName)
RA_BGL    { path, sheetNames }  ──▶  lazy val ra_bgl                        │
RA_BNL    { path, sheetNames }  ──▶  lazy val ra_bnl              PrimaryMapper.getDataFrame
RA_FORTIS { path, sheetNames }  ──▶  lazy val ra_fortis                     │
RA_LS     { path, sheetNames }  ──▶  lazy val ra_ls               perimeters = raInput.PERIMETER.distinct
                                          │                       matrices   = PARAMETRAGE ∩ perimeters
                                     raInput: 5 entries, hardcoded
```

Five conf blocks, five constants (`PrimaryConstants.RA_*`), five `lazy val`s, and a five-entry `Seq`
in `raInput`. Adding `RA_XYZ` today means editing all four places, recompiling and redeploying the
jar.

**Blast radius of a change.** The `RA_*` constants are referenced **only inside
`tseadfwd/reader/PrimaryReader.scala`**. Every caller outside it uses `raInput` (the union), or the
two non-RA inputs by name:

| Caller | Uses |
|---|---|
| `common/RunnerProvider`, `job/Term0AnalysisDriver` | `raInput`, `MACRO_VARIABLE`, `PARAMETRAGE` |
| `mapping/PrimaryMapper` | the unioned `raInput` frame only |
| `getMappingReader("RA_BCEF" \| …)` | **nothing** — those cases are dead for external callers |

So a dynamic reader changes one file plus the conf. No mapper, runner, writer or job is affected.

---

## 2. Why the sheet name can be dropped from the code

Three properties, each verified against the real workbooks:

1. **The entity is in the data, not in the sheet name.** Every RA sheet carries the same header, and
   `PERIMETER` is the first column:

   ```
   Inputs_RA.xlsx  'RA_BCEF'   rows=49  cols=366  | PERIMETER ; SEGMENT ; RATE_TYPE ; FWL_TYPE ; METRIC ; M1 ; M2 ; M3 …
   Inputs_RA.xlsx  'RA_BGL'    rows=49  cols=366  | PERIMETER ; SEGMENT ; RATE_TYPE ; FWL_TYPE ; METRIC ; M1 ; M2 ; M3 …
   ```

   `PrimaryMapper.collectRa` keys every series by `(PERIMETER, SEGMENT, RATE_TYPE, FWL_TYPE, METRIC)`.
   The sheet name is never read — it is only the address the reader loads from.

2. **The union already tolerates differences.** `raInput` reduces with
   `unionByName(allowMissingColumns = true)`, so a new sheet with an extra or a missing column joins
   without a code change.

3. **The matrix set is data-driven.** `parseParametrage(df, perimeters)` keeps the PARAMETRAGE rows
   whose `PERIMETER` is present in RA. A perimeter that appears in RA but not in PARAMETRAGE produces
   nothing; a PARAMETRAGE row whose perimeter is absent from RA is skipped. So the *computation*
   already adapts to whatever entities the inputs carry.

The consequence: the reader does not need to know entity names. It needs a **list of sheets to read**.

---

## 3. Can the sheets be listed at runtime? Yes — verified

`spark-excel` (already a dependency: `com.crealytics:spark-excel_2.12:3.3.1_0.18.5`) ships a workbook
reader with exactly this method:

```scala
com.crealytics.spark.excel.WorkbookReader(Map("path" -> p, "location" -> p), hadoopConf).sheetNames
```

- It opens the file through the **Hadoop `Configuration`** (an `FSDataInputStream`), so the same call
  resolves a local path and an HDFS path — identical to how every other input is read today.
- It is **stream-based**, so it cannot write back to the workbook. (Opening a workbook with POI's
  `WorkbookFactory.create(File, …)` is *not* stream-based and can rewrite the file in place — avoid
  it on inputs.)
- No new dependency, no POI code of our own.

Probed against the actual workbooks:

```
localRun/tseadfwd/input/Inputs_RA.xlsx      -> Inputs RA -> | RA_BCEF | RA_BGL | RA_BNL
localRun/tseadfwd/input/Inputs_RA_v3.xlsx   -> RA_BCEF
localRun/tseadfwd/input/Scenario_EAD_FWD.xlsx -> _NOTES | Central | Adverse | Optimistic | Extreme
```

Two things that output settles immediately:

- **A filter is mandatory.** Real workbooks carry non-data sheets — a divider tab literally named
  `Inputs RA ->`, and `_NOTES` in the scenario workbook. "Read every sheet" would break the run.
- **The same sheet name exists in two workbooks.** `RA_BCEF` is in both `Inputs_RA.xlsx` and
  `Inputs_RA_v3.xlsx`; today the conf resolves that by pinning BCEF to `_v3`. Discovery over both
  files must not load it twice (see §5).

---

## 4. Options

| | Approach | New entity costs | Notes |
|---|---|---|---|
| **A** | One `RA` block with an explicit `sheetNames = [...]` list | one conf line, no rebuild | Simplest; still an edit, and the conf is deployed with the job |
| **B** | **Discover the sheets and filter by pattern** | nothing | What was asked for; needs the safety rules below |
| **C** | **B + explicit `include` / `exclude` overrides** | nothing (override available) | **Recommended** — discovery by default, an escape hatch when a workbook misbehaves |

Recommended shape (C), with the legacy blocks still honoured so nothing breaks on the first deploy:

```hocon
RA {
  # One or more workbooks. Sheets are discovered in each, in order; first file wins on a duplicate.
  paths = [
    "localRun/tseadfwd/input/Inputs_RA_v3.xlsx",
    "localRun/tseadfwd/input/Inputs_RA.xlsx"
  ]
  # A sheet is loaded when its NAME matches this (case-insensitive) …
  sheetPattern = "^RA[_ ].*"
  # … and its columns carry the RA key layout. Belt and braces: the pattern keeps out `Inputs RA ->`,
  # the column check keeps out anything that matches the name but is not an RA table.
  requireColumns = ["PERIMETER", "SEGMENT", "RATE_TYPE", "FWL_TYPE", "METRIC"]
  # Escape hatches, both optional and both empty by default.
  include = []          # force these sheets in, whatever the pattern says
  exclude = []          # keep these out (e.g. "RA_OLD_2024")
}
```

Behaviour: discover → filter → read each kept sheet with today's locale-safe options → union by name
→ log one summary line naming every sheet **loaded** and every sheet **skipped with its reason**. The
existing "missing input is skipped with a warning" behaviour is preserved, not replaced.

---

## 5. Risks, and what each one needs

| # | Risk | Handling |
|---|---|---|
| 1 | Non-data sheets (`Inputs RA ->`, `_NOTES`, a pivot tab) | name pattern **and** required-column check; both configurable |
| 2 | **Same sheet in two workbooks** (`RA_BCEF` in `Inputs_RA.xlsx` and `_v3`) | ordered `paths`, first occurrence wins, WARN on the duplicate. Without this the perimeter is read twice |
| 3 | Duplicate rows from a double-load collapse **silently** — `collectRa` builds a `Map`, so a repeated key keeps one series and drops the other | add a data-control check: duplicate `(PERIMETER, SEGMENT, RATE_TYPE, FWL_TYPE, METRIC)` keys → FAIL |
| 4 | Sheet-name casing is inconsistent in the real files (`RA_BCEF` vs `ra_ls`) | case-insensitive matching |
| 5 | A new sheet has a broken layout | required-column check skips it with a WARN instead of failing the run; the existing `RA.columns` / `RA.months` controls still guard the union |
| 6 | **A new entity is loaded but has no PARAMETRAGE row** → it silently produces no output | new data-control check: perimeter present in RA, absent from PARAMETRAGE → WARN. This gap exists today and matters more once sheets appear on their own |
| 7 | An entity is FWL=YES but its stress legs / macro variable are missing | already covered — `RA.stressLegs` (FAIL) and `SCENARIO.macroVars` (FAIL) |
| 8 | Cost: `spark-excel` re-parses the whole workbook for **each** sheet read | fine at today's sizes (~0.1–0.2 MB, 49×366); measure on the production workbook before enabling discovery over a large file, and use `maxRowsInMemory` (streaming) if needed |
| 9 | A conf without the new block | keep reading the legacy `RA_BCEF … RA_LS` blocks as a fallback, exactly as `CONSISTENCY_CHECK` falls back to `DATA_QUALITY` |

---

## 6. What still has to happen when the business adds an entity

Dynamic loading removes the **code** change. These remain, and they are all data the business owns:

1. the new sheet follows the RA layout (`PERIMETER … METRIC`, `M1..M361`) — a copy of an existing tab;
2. `PERIMETER` inside the sheet carries the entity code, and it matches what PARAMETRAGE uses;
3. PARAMETRAGE has rows for that perimeter — otherwise nothing is produced (risk 6 makes this visible);
4. if any of its matrices is FWL=YES: both stress legs in RA, and its `MACRO_VARIABLE` present in the
   scenario workbook.

Point 3 is the one that will bite in practice, which is why the WARN is part of the recommendation
rather than a nice-to-have.

---

## 7. Implementation sketch

One new private helper in `PrimaryReader` (or a small `RaSheetDiscovery` object next to it):

```scala
def discover(paths: Seq[String], pattern: Regex, include: Set[String], exclude: Set[String])
    : Seq[(String, String)]        // (workbook path, sheet name), duplicates removed, order preserved
```

then `raInput` becomes: discover → `readDataFrameFromExcelSheet(path, sheet)` per entry (the existing
reader options, lifted to take an explicit sheet instead of a conf key) → filter on `requireColumns`
→ `unionByName(allowMissingColumns = true)` → one summary log line.

**Tests** (no Spark needed for the first two):

- discovery filters: pattern, exclusions, duplicate across paths, case-insensitivity — pure function
  over a sheet-name list;
- a workbook fixture with `RA_BCEF` + `RA_BGL` + a junk sheet: only the two RA sheets load, the union
  carries both perimeters;
- the legacy conf (five `RA_*` blocks, no `RA` block) still produces the identical frame;
- the new data-control checks (duplicate keys FAIL, orphan perimeter WARN).

**Effort:** roughly half a day including tests and the doc — the reader change is contained, and most
of the work is the safety rules in §5.

---

## 8. Out of scope, but the same trick applies

`MACRO_VARIABLE` reads a fixed list of scenario sheets (`Central`, `Adverse`, `Optimistic`,
`Extreme`) from a workbook that also contains `_NOTES`. The same discovery would let the business add
a scenario without a conf edit. It is deliberately **not** part of this proposal: scenario names carry
meaning in the engine (`Central` is the baseline every shock is measured against, and the output
`SCENARIO_ID` codes are fixed), so discovering them needs its own analysis.

---

## 9. As implemented

### The shape

```hocon
RA {
  paths          = ["…/Inputs_RA_v3.xlsx", "…/Inputs_RA.xlsx"]   # in order; first wins a duplicate
  sheetPattern   = "^RA[_ ].*"                                    # gate 1: the NAME
  requireColumns = ["PERIMETER","SEGMENT","RATE_TYPE","FWL_TYPE","METRIC"]   # gate 2: the CONTENT
  includeSheets  = []
  excludeSheets  = []
}
```

| File | What it does |
|---|---|
| `reader/RaSheetDiscovery.scala` | `RaSheetConfig` (the block, `None` when absent), `sheetNames` (one workbook, via spark-excel), `select` (**pure**: filter, exclusions, duplicates), `missingColumns` (gate 2) |
| `reader/PrimaryReader.scala` | `raInput` picks `raInputDiscovered` or the unchanged `raInputConfigured` |
| `utility/PrimaryUtilities.scala` | `readExcelSheet(path, sheet, label)` — the locale-safe options, split out so a sheet can be read with no conf key naming it |
| `mapping/PrimaryMapper.scala` | the two new controls: `RA.duplicateKeys` (FAIL), `RA.perimeters` (WARN) |

`select` being pure is what makes the awkward cases cheap to test — a divider tab, an entity spelled
lowercase, the same sheet in two workbooks — without a single Excel file.

### The thing the analysis missed

`include` is a **HOCON keyword**. A block containing `include = []` does not merely ignore the key —
the *whole configuration file* fails to parse:

```
ConfigException$Parse: Reader: 32: include keyword is not followed by a quoted string, but by: '='
```

Hence `includeSheets` / `excludeSheets` in the conf (the Scala fields keep the short names).

### Verified

- 31 new tests (`RaSheetDiscoverySpec` 16, `RaSheetReaderSpec` 10, `RaDataControlSpec` 5); the whole
  repo suite is green at **121**. `RaSheetReaderSpec` writes real `.xlsx` fixtures reproducing the
  production oddities — the `Inputs RA ->` divider, a lowercase `ra_bgl`, a tab that matches the
  pattern but is not an RA table, and `RA_BCEF` present in two workbooks.
- A production run on the real conf: identical output, **byte for byte** (`md5 99bd7eff…` before and
  after), with the reader now reporting
  `RA sheets selected: RA_BCEF; skipped: RA_BCEF (already loaded from …/Inputs_RA_v3.xlsx)`.
- The end-to-end story, on the real pipeline, with a new entity sheet (`RA_DEMO`, built from the real
  BCEF sheet) added to a workbook and nothing else touched:

  ```
  RA sheets selected: RA_BCEF, RA_DEMO; skipped: RA_BCEF (already loaded from …/Inputs_RA_v3.xlsx)
  RA sheet 'RA_DEMO' loaded from …/Inputs_RA_DEMO.xlsx
  DATA CONTROL - WARN (14 checks: 0 FAIL, 1 WARN)
    [WARN] RA.perimeters - perimeter(s) loaded from RA but absent from PARAMETRAGE,
                           so they produce NO output: DEMO
  ```

  The sheet was found and read with no code and no conf key naming it, and the run said plainly why
  the new entity produced nothing — which is the whole point of the WARN.

### Not done

`maxRowsInMemory` streaming (§5 risk 8) is not wired: at the sizes measured here (0.1–0.2 MB, 49×366)
each sheet read costs a fraction of a second. Measure on the production workbook before enabling
discovery over a large file — spark-excel re-parses the workbook once per sheet.
