# RA input comparison report — treatment design

> **Status: design + layout reference.** This branch delivers (a) the design of the treatment and
> (b) the **workbook it must produce**, generated from the repo's own inputs:
> [`Compare_RA_reference_model.xlsx`](Compare_RA_reference_model.xlsx), built by
> `tools/ra_compare/build_ra_compare_workbook.py`. No engine code is written yet — §7 says where it
> goes and §10 what has to be confirmed first.

The US asks for a **downloadable comparison report** (Excel / HTML) between **two selected inputs**,
carrying the `%change` of the four metrics for every perimeter and segment the two inputs share,
plus **graphs superimposing the two curves**. The Risk team produces that report by hand today
(`Compare_RA_BCEF_KO_Prod 26Q2.xlsx`); this document specifies how the engine produces it instead.

---

## 1. What the US asks

| Rule | Source wording |
|---|---|
| Scope | *"For all perimeters and their segments common between the 2 selected files"* |
| Scenarios | *"For all 3 scenarios (`FWL_TYPE` in `BASELINE`, `STRESS (-)`, `STRESS (+)`)"* |
| Horizon | *"and all months (M1 to M361)"* |
| Metrics | *"for all 4 metrics (CRD, RA STAT, RA FI and RE)"* |
| Formula | *"%change of metric = (metric_new − metric_old) / metric_old"* |
| Graphs | *"graphs comparing the curves between the 2 input files for all 4 metrics"* |
| Output | *"a new excel/HTML report output available from TWIST in a new tab on EAD FWL TS tool"* |
| Acceptance | *"Test the generated reports against the manual calculations for all perimeters & segments … check the expected formats of the tables and graphs"* |

**The two "selected inputs" are two `INPUTS_RA` workbooks** — the file the Risk team is about to use
(*Updated*) against the one used at the previous reference (*Previous*). That is what the manual file
compares, and it is the only input carrying `FWL_TYPE`, the four metrics and 361 monthly columns.
→ [Q1](#q1) asks the business to confirm it, because "the 2 inputs selected for the comparison" is
also readable as two engine *outputs*.

---

## 2. What the manual workbook does today

`Compare_RA_BCEF_KO_Prod 26Q2.xlsx` — one sheet per *(perimeter × stress leg)* (`BCEF -100`,
`BCEF +100`, …), each sheet three stacked blocks of four segment tables, then a grid of charts:

```
row  1   Updated        (yellow)          <- the new INPUTS_RA file, as loaded
row  3   M0  M1  M2 …                     <- month index row (the chart categories)
row  4   Immos à TF     01/04/2026 …      <- segment table header + month dates
rows 5-10   CRD · RA · RA STAT · RA FI · RE · %RA
rows 12-18  Invest pro à TF …             <- next segment, 8 rows lower
rows 20-26  Invest corp à TF …
rows 28-34  Conso à TF …

row 38   Previous       (yellow)          <- the old INPUTS_RA file, same four tables
rows 41-71  … same geometry, 37 rows lower

row 75   Evol           (yellow)          <- the %change block
rows 78-108 … same geometry, 37 rows lower again;  every cell is  =(C5-C42)/C42

rows 112+   charts: one per (metric × segment), two curves — Updated vs Previous
```

Three things the US text does not say but the manual file does, and the engine must reproduce:

1. **Two derived rows** sit next to the four metrics in every table:
   `RA = RA STAT + RA FI + RE` and `%RA = RA × 12 / −CRD` (the annualised RA rate over the
   outstanding). Checked against the file: Immos M0 `RA STAT 373 + RA FI 10 + RE 6 = 389`,
   `389 × 12 / 93 172 = 5.01 %` against the `5.02 %` displayed. The `Evol` block carries the
   `%change` of those two rows too (`RA/RE` and `%RA`).
2. **The two files are aligned by month INDEX, not by date.** In the manual file *Updated* starts at
   `01/04/2026` and *Previous* at `01/01/2026` — a quarter apart — and the Evol formula still reads
   `=(C5-C42)/C42`, i.e. column C against column C. Dates are **labels**, not join keys
   ([Q3](#q3)).
3. **The first column is labelled `M0`** in the manual file while the `INPUTS_RA` sheets call it
   `M1`. Same month, two names ([Q2](#q2)); the reference model keeps the input's own `M1…M361`.

---

## 3. Input contract

Every `INPUTS_RA` sheet is a wide table — five key columns then one column per month:

| PERIMETER | SEGMENT | RATE_TYPE | FWL_TYPE | METRIC | M1 | M2 | … | M361 |
|---|---|---|---|---|---|---|---|---|
| BCEF | MORTGAGE | TF | BASELINE | CRD | −92 925 | −92 368 | … | … |

* **Key** = `PERIMETER × SEGMENT × RATE_TYPE × FWL_TYPE × METRIC` (48 rows per perimeter today:
  4 segments × 3 FWL types × 4 metrics).
* **Sheet discovery** reuses the engine's existing two gates
  ([`RaSheetDiscovery`](../../src/main/scala/com.bnp.str.tseadfwd/reader/RaSheetDiscovery.scala)):
  the sheet NAME matches `^RA[_ ].*`, and its CONTENT carries the five key columns. A divider tab
  such as `Inputs RA ->` is skipped, with the reason reported — the comparison job must not invent
  its own rule here.
* **Values are already in M€** and `CRD` is **negative** (exposure). Neither is rescaled: the report
  shows the input as it is, exactly like the manual file.

---

## 4. Scope: what is compared, what is dropped

Compare the **intersection** of the two files on the full key. Everything outside it is **excluded
from the tables and named in the report**, never silently dropped:

| Case | Behaviour |
|---|---|
| Key in both files | compared |
| Perimeter / segment only in one file | excluded; listed on the `COMPARE INFO` sheet |
| `FWL_TYPE` only in one file | that sheet is not produced; listed |
| Metric only in one file | its rows stay empty; listed |
| One file has fewer month columns | compare `min(n_new, n_old)` months; the truncation is stated |

Worked example — `Inputs_RA_v4.xlsx` (6 perimeters) against `Inputs_RA.xlsx` (BCEF only):
48 keys compared, **240 keys dropped** (BGL, BNL, FORTIS, LS, OLA), one divider tab skipped.

---

## 5. Computation

For every compared key and every month `i` of the common horizon:

```
pctChange(metric, i) = (new(metric, i) − old(metric, i)) / old(metric, i)
```

and, per segment table, the two derived rows:

```
RA(i)   = RA STAT(i) + RA FI(i) + RE(i)
%RA(i)  = RA(i) × 12 / −CRD(i)            (annualised rate on the outstanding)
```

**Degenerate cases** — the manual file leaves `#DIV/0!` in the cells; the engine must decide
explicitly ([Q5](#q5)). Proposed defaults:

| `old` | `new` | Result |
|---|---|---|
| `0` | `0` | empty cell (no change to report) |
| `0` | `≠ 0` | empty cell + one line in the report's *not computable* list |
| `≠ 0` | missing | empty cell + listed |
| negative (e.g. `CRD`) | any | computed as written — the ratio of two negatives is the right relative change; **no `abs()`** |

**Precision.** Everything is computed in `double` at full precision; rounding is a *display* format
only (`#,##0` for CRD, `#,##0.00` for the RA rows, `0.00%` for `%RA` and for every `Evol` cell). The
manual file's own numbers are display-rounded, which is why a cell-by-cell diff against it shows
~`1e-5` relative noise — the acceptance test in §9 states the tolerance rather than chasing it.

---

## 6. Output

### 6.1 Excel workbook (primary)

One workbook per run, named `Compare_RA_<PERIMETERS>_<reference>.xlsx`. Sheets:

| Sheet | Content |
|---|---|
| `COMPARE INFO` | the two source paths, the RA sheets read on each side, months compared, keys compared, keys dropped on each side, the `%change` / `RA` / `%RA` definitions |
| `<PERIMETER> BASELINE` | the three blocks + charts, `FWL_TYPE = BASELINE` |
| `<PERIMETER> -100` | idem, `FWL_TYPE = STRESS (-)` |
| `<PERIMETER> +100` | idem, `FWL_TYPE = STRESS (+)` |

Sheet geometry is **fixed** (the manual file's own anchors, so a reader's eye and any downstream
macro keep working):

| Constant | Value |
|---|---|
| Title rows (`Updated` / `Previous` / `Evol`) | 1 / 38 / 75 |
| Month index row of each block | title row + 2 |
| First segment table header | 4 / 41 / 78 |
| Table pitch | 8 rows (header + 6 metric rows + 1 blank) |
| First data column | `C` |
| Metric row order | `CRD`, `RA`, `RA STAT`, `RA FI`, `RE`, `%RA` |
| Segment order | `MORTGAGE`, `INVEST_PRO`, `INVEST_CORP`, `CONSO`, then anything else, alphabetically |
| Segment labels | `Immos`, `Invest pro`, `Invest corp`, `Conso` — header reads `<label> à <RATE_TYPE>` |

`RA`, `%RA` and the whole `Evol` block are written as **live Excel formulas** (`=(C5-C42)/C42`, as in
the manual file), so a reviewer can audit any cell by clicking it. Wrapped in `IFERROR(…,"")` by
default; `--raw-div` reproduces the bare formula and its `#DIV/0!`.

**Charts.** One line chart per *(metric × segment)* — 16 per sheet — titled `<METRIC> - <segment>`,
two series named `Updated` and `Previous`, categories = the month index row (text labels, one tick
every 12 months), placed in a 4 × 4 grid under the `Evol` block.

### 6.2 HTML report (secondary)

Same tables and the same curves as a **self-contained** page, following the pattern already used for
the coherence checks
([`coherence/CheckHtmlView`](../../src/main/scala/com.bnp.str.tseadfwd/coherence/CheckHtmlView.scala)):
one HTML string, inline CSS, no external asset, written through Hadoop's `FileSystem` so the path may
be local or HDFS. Curves as inline SVG polylines (361 points, two per chart) — no chart library.
Whether TWIST needs it in addition to the workbook is [Q8](#q8).

### 6.3 Flat CSV (proposed, for reconciliation)

`;`-delimited, decimal comma, one row per compared cell:

```
PERIMETER;SEGMENT;RATE_TYPE;FWL_TYPE;METRIC;MONTH;NEW;OLD;PCT_CHANGE;STATUS
```

`STATUS ∈ {OK, NOT_COMPUTABLE, ONLY_NEW, ONLY_OLD}`. Not asked for by the US, but it is what makes
the acceptance test of §9 a diff instead of a reading exercise, and it matches what `EadFwdCompare`
already writes for the output side.

---

## 7. Where it lives in the code

A **new job in the `tseadfwd` module**, not a new module: it reads the same `INPUTS_RA` workbook with
the same discovery rules, ships in the same jar and records the same run audit. It touches nothing in
the production pipeline — `MainDriver` is unchanged.

```
com.bnp.str.tseadfwd
├── job/RaCompareDriver           entry point (one arg: application.conf), reads COMPARE_RA
├── reader/RaCompareReader        loads the TWO workbooks via RaSheetDiscovery, returns two frames
├── mapping/RaCompareView         PURE core: intersection, %change, RA, %RA, dropped-key report
├── writer/RaCompareExcelWriter   POI (XSSFWorkbook + XDDF charts) — §6.1 geometry
├── writer/RaCompareHtmlView      self-contained HTML — §6.2
└── utility/PrimaryConstants      + the new literals (block anchors, row order, segment labels)
```

Follows the [module contract](../../README.md#module-contract): the computation core is a pure,
unit-testable object; IO stays in reader/writer; every literal lands in `PrimaryConstants`.

**Volume.** Two files × 6 perimeters × 48 rows × 361 months ≈ 200 k cells — small. The core runs
**driver-side on collected rows**, like `EadFwdCompare` does; Spark is used to read the sheets
(`spark-excel`), not to shuffle. POI writes the workbook directly, which `spark-excel` cannot do
(it has no chart support and no multi-block sheet layout).

### Configuration sketch (`tseadfwd_app.COMPARE_RA`)

```hocon
COMPARE_RA {
  enabled   = true
  newPath   = "localRun/tseadfwd/input/Inputs_RA_v2.xlsx"   # "Updated"
  oldPath   = "localRun/tseadfwd/input/Inputs_RA.xlsx"      # "Previous"
  newLabel  = "Updated"
  oldLabel  = "Previous"
  newStart  = "2026-01"        # first projection month — column DATE labels only, never a join key
  oldStart  = "2025-10"
  perimeters = []              # empty = every perimeter common to both files
  xlsxPath  = "localRun/tseadfwd/output/Compare_RA.xlsx"
  htmlPath  = "localRun/tseadfwd/output/Compare_RA.html"
  csvPath   = "localRun/tseadfwd/output/Compare_RA.csv"     # §6.3, optional
  chartStep = 12               # x-axis label every N months
  safeDiv   = true             # false reproduces the manual file's #DIV/0!
}
```

---

## 8. The reference model in this branch

`tools/ra_compare/build_ra_compare_workbook.py` builds the workbook described in §6.1 from any two
`INPUTS_RA` files. It is the **layout reference and the acceptance fixture** for the Scala job — a
runnable specification of the geometry, so §6.1 cannot drift from what the business validates.

```bash
python3 tools/ra_compare/build_ra_compare_workbook.py \
  --new localRun/tseadfwd/input/Inputs_RA_v2.xlsx \
  --old localRun/tseadfwd/input/Inputs_RA.xlsx \
  --out docs/tseadfwd/Compare_RA_reference_model.xlsx \
  --new-start 2026-01 --old-start 2025-10
```

The committed [`Compare_RA_reference_model.xlsx`](Compare_RA_reference_model.xlsx) is that command's
output: 4 sheets, 361 months, 48 compared keys, 48 charts. It was opened and **recalculated in
LibreOffice Calc** — every formula resolves, `IFERROR` blanks the `CONSO` `RA FI`/`RE` rows that are
zero on both sides, and the 4 × 4 chart grid renders with both curves.

Two caveats on this particular pair of files, so nobody reads a business signal into it: they are the
**same vintage at different rounding** (`Inputs_RA.xlsx` is rounded to the unit, `_v2` is not), so
the `Evol` block is rounding noise (~`1e-5`), and both start dates were **supplied on the command
line** — an `INPUTS_RA` sheet carries no as-of date ([Q4](#q4)).

---

## 9. Acceptance tests

Mapping the US's *"test the generated reports against the manual calculations"* onto testable steps:

| # | Test | How |
|---|---|---|
| T1 | `%change` formula | unit test on `RaCompareView`: `(new−old)/old` per metric/month, including negative `CRD` |
| T2 | Degenerate cases | unit test: `old = 0`, both `0`, missing key, missing month → §5 table |
| T3 | Derived rows | unit test: `RA = STAT+FI+RE`, `%RA = RA×12/−CRD` |
| T4 | Scope | unit test: only common keys compared; every dropped key reported (v4-vs-v1 case of §4) |
| T5 | Geometry | golden test: the job's workbook vs `Compare_RA_reference_model.xlsx` — same sheets, same anchors, same row labels, same chart titles/series/ranges |
| T6 | Numbers | the §6.3 CSV against the manual `Compare_RA_BCEF_KO_Prod 26Q2.xlsx`, per perimeter × segment × metric × month, **tolerance `1e-4` relative** (the manual file's cells are display-rounded — see §5) |
| T7 | Opens clean | the produced workbook loads in Excel with no repair prompt and all formulas resolve (the reference model is checked this way in §8) |

T6 needs the manual workbook **and the two `INPUTS_RA` files it was built from** — see [Q7](#q7).

---

## 10. Open questions

Each has a **default** chosen so the build can start; every one of them changes the output if the
business answers otherwise.

<a id="q1"></a>**Q1 — Which two inputs?** Default: two `INPUTS_RA` workbooks (what the manual file
compares). Could also mean two engine *outputs* (`TS_EAD_FWD` term structures), which is a different
report — `EadFwdCompare` already covers that side.

<a id="q2"></a>**Q2 — `M0` or `M1` for the first column?** The `INPUTS_RA` sheets say `M1`, the manual
report says `M0`, the US says *"M1 to M361"*. Default: keep the input's `M1…M361` labels.

<a id="q3"></a>**Q3 — Alignment when the two files have different as-of dates.** Default: **by month
index** (the manual file's `=(C5-C42)/C42` across a one-quarter gap). The alternative — align by
calendar month, comparing overlapping dates only — is a different report and would drop the first
quarter of one file.

<a id="q4"></a>**Q4 — Where do the column dates come from?** An `INPUTS_RA` sheet carries no as-of
date. Default: a config parameter per side (`newStart` / `oldStart`). Alternative: reuse
`parameters.as_of_date_quarter` for the new file and require the old one explicitly.

<a id="q5"></a>**Q5 — `old = 0`.** Default: empty cell + a *not computable* list. The manual file
shows `#DIV/0!`; `safeDiv = false` reproduces that if the business prefers it.

<a id="q6"></a>**Q6 — Is `%RA` annualised by 12?** `RA × 12 / −CRD` reproduces the manual file's
percentages to the displayed decimal. Confirm the factor is 12 (monthly RA → annual rate) and not a
period convention that changes on the yearly grid.

<a id="q7"></a>**Q7 — T6 fixtures.** To reconcile against the manual calculations we need
`Compare_RA_BCEF_KO_Prod 26Q2.xlsx` **and** the two `INPUTS_RA` files it was produced from. Without
them, T6 can only run on the pair in this repo, whose `Evol` is rounding noise (§8).

<a id="q8"></a>**Q8 — Excel, HTML, or both from TWIST?** The US says *"excel/HTML etc."*. Default:
Excel is the deliverable, HTML is the in-browser preview. Confirm what the new TWIST tab serves.

<a id="q9"></a>**Q9 — One workbook for every perimeter, or one file per perimeter?** Default: one
workbook, three sheets per perimeter (the manual file is per perimeter because it is written by
hand). With 6 perimeters that is 18 sheets and ~288 charts — acceptable, but say so if TWIST
prefers one file per perimeter.

<a id="q10"></a>**Q10 — Should a material `%change` be highlighted?** Nothing in the US asks for it.
A conditional format above a configurable threshold (e.g. `|%change| > 5 %`) would turn the `Evol`
block into something a reviewer can scan. Off by default.
