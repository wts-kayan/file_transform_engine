# Ticket 977 — RA input comparison report: treatment design

> **Status: design + layout reference, business-answered 2026-08-27.** This branch delivers (a) the
> design of the treatment and (b) the **workbook it must produce**, generated from the repo's own
> inputs: [`Compare_RA_reference_model.xlsx`](Compare_RA_reference_model.xlsx), built by
> `tools/ra_compare/build_ra_compare_workbook.py`. No engine code is written yet — §7 says where it
> goes.
>
> **All sixteen open questions are now answered** ([`OPEN_QUESTIONS_977.csv`](OPEN_QUESTIONS_977.csv),
> also as a workbook). Three answers change this document materially and are folded into it below:
> the two **derived rows are dropped** (§2, §5, §6.1), the report carries **no calendar dates**
> (§6.1), and segment tables are headed with the **input's own segment names** (§6.1). §10 records
> every answer verbatim.

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

That is the manual file. What the engine produces is §6.1: the derived rows are gone, the month
index row and the second label column went with the business review of 2026-08-27 (§6.1.1), and the
anchors are 1 / 29 / 57 with the data starting at column `B`.

Three things the US text does not say but the manual file does, and the engine must reproduce:

1. ~~**Two derived rows** sit next to the four metrics in every table~~ — **not reproduced.** The
   manual file adds `RA = RA STAT + RA FI + RE` and `%RA = RA × 12 / −CRD` (the annualised RA rate
   over the outstanding). Neither is a column of `INPUTS_RA` and neither appears in the US wording;
   the `%RA` factor of 12 was an inference, and recomputing the manual file gave `5.01 %` against
   the `5.02 %` displayed. The business dropped **both** on 2026-08-27 ([Q6](#q6), [Q11](#q11)): the
   report works only from the metrics present in the input. This is the decision that makes a
   segment table four rows instead of six.
2. **The two files are aligned by month INDEX, not by date.** In the manual file *Updated* starts at
   `01/04/2026` and *Previous* at `01/01/2026` — a quarter apart — and the Evol formula still reads
   `=(C5-C42)/C42`, i.e. column C against column C. Confirmed ([Q3](#q3)) — and the dates are gone
   from the report altogether ([Q4](#q4)), so the question can no longer arise: every column is
   `M1…M361` on all three blocks.
3. **The first column is labelled `M0`** in the manual file while the `INPUTS_RA` sheets call it
   `M1`. Same month, two names; the business chose the input's own `M1…M361` ([Q2](#q2)).

---

## 3. Input contract

Every `INPUTS_RA` sheet is a wide table — five key columns then one column per month:

| PERIMETER | SEGMENT | RATE_TYPE | FWL_TYPE | METRIC | M1 | M2 | … | M361 |
|---|---|---|---|---|---|---|---|---|
| BCEF | MORTGAGE | TF | BASELINE | CRD | −92 925 | −92 368 | … | … |

* **Key** = `PERIMETER × SEGMENT × RATE_TYPE × FWL_TYPE × METRIC` (48 rows per perimeter today:
  4 segments × 3 FWL types × 4 metrics).
* **Sheet discovery** reuses the engine's existing two gates
  ([`RaSheetDiscovery`](../../../src/main/scala/com.bnp.str.tseadfwd/reader/RaSheetDiscovery.scala)):
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

for each of the four metrics `CRD`, `RA STAT`, `RA FI`, `RE` — and nothing else. The manual file's
derived `RA` and `%RA` rows are **not computed** ([Q6](#q6), [Q11](#q11)), so no `Evol` cell is ever
applied to a rate and the percentage-point ambiguity of [Q12](#q12) does not arise.

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
| `COMPARE INFO` | the two source paths, months compared, keys compared, keys dropped on each side, the `%change` definition, what the metrics/months/values conventions are, and the engine (`IRIS`) |
| `<PERIMETER> BASELINE` | the three blocks + charts, `FWL_TYPE = BASELINE` |
| `<PERIMETER> -100` | idem, `FWL_TYPE = STRESS (-)` |
| `<PERIMETER> +100` | idem, `FWL_TYPE = STRESS (+)` |

Sheet geometry is **fixed** (the manual file's own anchors, so a reader's eye and any downstream
macro keep working):

| Constant | Value |
|---|---|
| Title rows (`Updated` / `Previous` / `Evol`) | 1 / 29 / 57 |
| First segment table header | 3 / 31 / 59 — title row + 2 |
| Table pitch | 6 rows (header + 4 metric rows + 1 blank) |
| First data column | `B` |
| Row label column | `A` — the metric name, and nothing else |
| Metric row order | `CRD`, `RA STAT`, `RA FI`, `RE` |
| Segment order | configuration; default `MORTGAGE`, `INVEST_PRO`, `INVEST_CORP`, `CONSO`, then anything else alphabetically |
| Segment labels | the input's own `SEGMENT` — header reads `<SEGMENT> a <RATE_TYPE>` |
| Column headers | `M1 … M361` on every table header row; **no calendar dates anywhere** |
| `CRD` sign | `−1 ×` the input, so an outstanding reads **positive** |

> The manual file's anchors were 1 / 38 / 75 at a pitch of 8. Dropping the two derived rows
> ([Q6](#q6), [Q11](#q11)) takes a table to 6 rows, so each block is 4 × 6 = 24 rows of tables
> instead of 32. The **business review of 2026-08-27** (§6.1.1) then took one more row off the top
> of every block, bringing the anchors to 1 / 29 / 57. The generator **derives** these anchors from
> the row list rather than hard-coding them, so they stay correct if the list changes again.

#### 6.1.1 Business review of 2026-08-27

Three changes to the first release, asked for on the produced workbook and applied to both the job
and the reference model:

1. **The duplicated label bands are gone.** Each block carried a standalone month index row above
   its first table, repeating the `M1 … M361` the table headers already carry; and column A carried
   the manual file's business wording for each metric (`Outstanding`, `Amount PPstat`,
   `Amount PPfin`, `Amount RE`) next to column B's `CRD`, `RA STAT`, `RA FI`, `RE`. Both said the
   same thing twice. The metric name moves into column A, the data starts at **B**, and the charts
   take their categories from **the table header row of the table they chart** — the same labels,
   on the row the chart is about.
2. **`CRD` is written as `−1 ×` the input**, so an outstanding reads positive. The input carries it
   negative (it is an exposure), which put every curve below the axis and made the `%change` read
   against a negative base. The flip is **presentation only and changes no arithmetic**:
   `(−n − −o) / −o` is exactly `(n − o) / o`, so every percentage in the report is the number it
   was. A zero is normalised to `+0`, since negating one gives IEEE `−0.0` and Excel renders that
   as `-0`. `COMPARE INFO` states the convention, and the same convention is applied to the §6.3
   CSV so a reconciliation row matches the cell it describes.
3. **No freeze pane.** It froze column A/B and the rows above the first table — which pinned a
   header belonging to the `Updated` block over the `Previous` and `Evol` blocks further down.

The whole `Evol` block is written as **live Excel formulas** (`=IFERROR((B4-B32)/B32,"")`, the manual
file's own shape re-anchored to the new geometry), so a reviewer can audit any cell by clicking it.
`--raw-div` reproduces the bare formula and its `#DIV/0!` instead of the `IFERROR` blank.

**Presentation.** Distinct from the geometry above, and deliberately not pinned by T5:

* Block titles are coloured by side: **`Updated` blue** (`BDD7EE`), **`Previous` orange**
  (`F8CBAD`), `Evol` keeping the manual workbook's yellow. Three stacked blocks of identical tables
  are easy to lose your place in, and the colour says which side you are reading before the word
  does. Light tints, since they sit behind black text.
* Sheet tabs are coloured by stress leg — slate for `BASELINE`, orange for `STRESS (-)`, blue for
  `STRESS (+)`. One workbook holds three sheets per perimeter ([Q9](#q9)), which is eighteen
  near-identically named tabs at six perimeters. Blue and orange rather than red and green, since
  colour is doing real work here.

  > Note the two schemes reuse the same pair: on a **tab**, blue/orange means the stress leg; on a
  > **block title**, it means which of the two files. They are read in different places and never
  > side by side, but it is worth knowing they are not the same axis.
* `COMPARE INFO` row heights follow their content. The cells wrap, but a wrapped cell at the default
  height shows one line, and the dropped-key list runs to several hundred characters — so the report
  was naming excluded keys (§4) and then hiding them.
* No conditional formatting: the business declined highlighting ([Q10](#q10)).
* No print setup. Printing 363 columns needs landscape, fit-to-width and repeated label columns;
  TWIST serves the workbook for viewing ([Q8](#q8)), so this was left out rather than guessed at.

**Charts.** One line chart per *(metric × segment)* — 16 per sheet — titled `<METRIC> - <SEGMENT>`,
two series named `Updated` and `Previous`, categories = the charted table's own header row (text
labels, one tick every 12 months), placed in a 4 × 4 grid under the `Evol` block.

### 6.2 HTML report (secondary)

Same tables and the same curves as a **self-contained** page, following the pattern already used for
the coherence checks
([`coherence/CheckHtmlView`](../../../src/main/scala/com.bnp.str.tseadfwd/coherence/CheckHtmlView.scala)):
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
the same discovery rules and ships in the same jar. It touches nothing in the production pipeline —
`MainDriver` is unchanged, and the job only ever READS the input workbooks.

**Built 2026-08-27:**

```
com.bnp.str.tseadfwd
├── job/CompareAndReportDriver     entry point (one arg: application.conf), reads COMPARE_AND_REPORT
│                                  + CompareAndReportConfig, and the §6.3 flat CSV
├── reader/RaCompareReader         loads ONE workbook via RaSheetDiscovery into the series map
├── mapping/RaCompareView          PURE core: keys, intersection, %change, layout rules
└── writer/RaCompareExcelWriter    POI (XSSFWorkbook + XDDF charts) — the §6.1 geometry
```

`writer/RaCompareHtmlView` (§6.2) is **not built** — Excel is the deliverable and the HTML page is
deferred ([Q8](#q8)).

The driver is named for the config block the business asked for (`COMPARE_AND_REPORT`), not
`RaCompareDriver` as this section first sketched. `RaCompareView` holds the layout rules — metric
rows, sheet naming, segment ordering — next to the arithmetic, so the writer reads them rather than
redeclaring them; nothing new was needed in `PrimaryConstants`, since the RA column and label names
were already there.

Follows the [module contract](../../../README.md#module-contract): the computation core is a pure,
unit-testable object; IO stays in reader/writer; every literal lands in `PrimaryConstants`.

**Volume.** Two files × 6 perimeters × 48 rows × 361 months ≈ 200 k cells — small. The core runs
**driver-side on collected rows**, like `EadFwdCompare` does; Spark is used to read the sheets
(`spark-excel`), not to shuffle. POI writes the workbook directly, which `spark-excel` cannot do
(it has no chart support and no multi-block sheet layout).

### Configuration sketch (`tseadfwd_app.COMPARE_RA`)

Named `COMPARE_AND_REPORT` per [Q1](#q1) — the answer wrote `compare_and_report`, spelled here in
the `UPPER_SNAKE` every other block of `application.conf` uses, since HOCON is case-sensitive.

```hocon
COMPARE_AND_REPORT {
  enabled   = true
  newPath   = "localRun/tseadfwd/input/Inputs_RA_v4.xlsx"   # "Updated"
  oldPath   = "localRun/tseadfwd/input/Inputs_RA_v3.xlsx"   # "Previous"
  newLabel  = "Updated"
  oldLabel  = "Previous"
  perimeters   = []            # empty = every perimeter common to both files (Q15)
  segmentOrder = ["MORTGAGE", "INVEST_PRO", "INVEST_CORP", "CONSO"]   # Q14; unlisted follow, A-Z
  xlsxPath  = "localRun/tseadfwd/output/Compare_RA.xlsx"
  csvPath   = "localRun/tseadfwd/output/Compare_RA.csv"     # §6.3, optional
  safeDiv   = true             # false reproduces the manual file's #DIV/0! (Q5)
}
```

No `chartStep` either: `XDDFCategoryAxis` exposes no tick-label skip and the `CTSkip` type needed to
reach the underlying axis is absent from `poi-ooxml-lite`, so the Python reference's `tickLblSkip` has
no POI equivalent worth a new schema dependency — Excel thins a 361-point axis by itself.

No `newStart` / `oldStart`: the report carries no dates ([Q4](#q4)). No `htmlPath` for now — Excel
is the deliverable and the HTML page is deferred ([Q8](#q8)).

---

## 8. The reference model in this branch

`tools/ra_compare/build_ra_compare_workbook.py` builds the workbook described in §6.1 from any two
`INPUTS_RA` files. It is the **layout reference and the acceptance fixture** for the Scala job — a
runnable specification of the geometry, so §6.1 cannot drift from what the business validates.

```bash
python tools/ra_compare/build_ra_compare_workbook.py \
  --new localRun/tseadfwd/input/Inputs_RA_v2.xlsx \
  --old localRun/tseadfwd/input/Inputs_RA.xlsx \
  --out docs/tseadfwd/977/Compare_RA_reference_model.xlsx
```

The committed [`Compare_RA_reference_model.xlsx`](Compare_RA_reference_model.xlsx) is that command's
output: 4 sheets, 361 months, 48 compared keys, 48 charts. **Regenerated on 2026-08-27** against
the answers of §10 — four metric rows per table, blocks on 1 / 30 / 59, `M1…M361` column headers and
the input's own segment names.

Two caveats on this particular pair of files, so nobody reads a business signal into it: they are the
**same vintage at different rounding** (`Inputs_RA.xlsx` is rounded to the unit, `_v2` is not), so
the `Evol` block is rounding noise (~`1e-5`). For a run where the comparison does real work — and
where the 240 dropped keys of §4 appear on `COMPARE INFO` — use `Inputs_RA_v4.xlsx` against
`Inputs_RA_v3.xlsx`.

Note the workbook is **not byte-reproducible**: `docProps/core.xml` carries a creation timestamp, so
a regenerated file always shows a binary diff even when every cell is identical.

**The model pins the layout, not the styling.** Since 2026-08-27 the Scala writer carries
presentation the generator does not — coloured tabs, content-driven `COMPARE INFO` row heights — so
the two workbooks no longer look identical, by choice. Everything T5 compares (sheets, anchors, row
labels, formulas, values, chart titles) still matches exactly; what has diverged is only what a
reader sees, not what the report says. Read the model as the specification of *where things go*, and
the job's own output as the specification of *how it looks*.

---

## 9. Acceptance tests

Mapping the US's *"test the generated reports against the manual calculations"* onto testable steps:

| # | Test | How |
|---|---|---|
| T1 | `%change` formula | unit test on `RaCompareView`: `(new−old)/old` per metric/month, including negative `CRD` |
| T2 | Degenerate cases | unit test: `old = 0`, both `0`, missing key, missing month → §5 table |
| T3 | ~~Derived rows~~ | **dropped** with the rows themselves ([Q6](#q6), [Q11](#q11)) — nothing is derived any more |
| T4 | Scope | unit test: only common keys compared; every dropped key reported (v4-vs-v1 case of §4) |
| T5 | Geometry | golden test in `RaCompareGoldenSpec`: the job's workbook vs `Compare_RA_reference_model.xlsx` — same sheets, same column-B layout, same table headers, every `Evol` formula, the values, the chart titles |
| T6 | Numbers *(deferred)* | the §6.3 CSV against the manual `Compare_RA_BCEF_KO_Prod 26Q2.xlsx`, per perimeter × segment × metric × month, **tolerance `1e-4` relative** (the manual file's cells are display-rounded — see §5). Deferred by [Q7](#q7) |
| T7 | Opens clean | the produced workbook loads in Excel with no repair prompt and all formulas resolve (the reference model is checked this way in §8) |

T6 needs the manual workbook **and the two `INPUTS_RA` files it was built from**, which the business
deferred ([Q7](#q7)) — so the suite to build now is T1, T2, T4, T5, T7.

---

## 10. Questions and answers

Answered by the business on **2026-08-27**. Their wording is quoted verbatim; the full record, with
what the engine did before each answer, is [`OPEN_QUESTIONS_977.csv`](OPEN_QUESTIONS_977.csv) (and
the same as a workbook). Six of these were not in the original list of ten — they came out of
reading the reference model against the manual file, and four of them concern the derived rows.

**The three that changed this document:** Q6 and Q11 drop both derived rows, Q3/Q4 drop the dates,
Q14 replaces the French labels with the input's own segment names.

<a id="q1"></a>**Q1 — Which two inputs?**
> *"two INPUTS_RA workbooks as input, should be given in configuration file in new block, name it
> compare_and_report"*

Two `INPUTS_RA` workbooks, named in a new block. Spelled `COMPARE_AND_REPORT` in §7 to match the
`UPPER_SNAKE` of every other block in `application.conf`; HOCON is case-sensitive, and this would
otherwise be the only lowercase block in the file.

<a id="q2"></a>**Q2 — `M0` or `M1`?**
> *"M1 as we have in the input_ra file"*

`M1…M361`. The manual file's `M0` is not reproduced.

<a id="q3"></a>**Q3 — Alignment when the as-of dates differ?**
> *"don't care about date replace them by M1,M2,M3 …."*

By month index — and since the dates leave the report entirely (Q4), the question of a differing
as-of date no longer arises.

<a id="q4"></a>**Q4 — Where do the column dates come from?**
> *"skip the date replace them by M1,M2,M3 …."*

They do not. Every block's table header carries `M1…M361`, so no `newStart` / `oldStart` setting is
needed and an `INPUTS_RA` file never has to be told its own as-of date.

<a id="q5"></a>**Q5 — `old = 0`.**
> *"Proposed defaults: old 0 and new 0 -> blank; old 0 and new non-zero -> blank plus listed; old
> present and new missing -> blank plus listed."*

The §5 defaults stand. The manual file's visible `#DIV/0!` is not reproduced; `safeDiv = false`
still gets it back.

<a id="q6"></a>**Q6 — Where does `%RA` come from, and is the ×12 right?**
> *"work only using CRD,RA STAT,RA FI,RE in the RA input file, the input will be for example
> Inputs_RA_v4.xlsx"*

`%RA` is **dropped**. The report works only from the four metrics present in the input, so the
inferred ×12 annualisation and its denominator both fall away with it.

<a id="q7"></a>**Q7 — T6 fixtures.**
> *"no need for now, will be tested later"*

Acceptance test T6 (reconciliation against the manual workbook) is **deferred**; T1–T5 and T7 stand.

<a id="q8"></a>**Q8 — Excel, HTML, or both?**
> *"go first for excel file as devivrable, will see if html is needed"*

Excel is the deliverable. §6.2's HTML page is **not built for now**.

<a id="q9"></a>**Q9 — One workbook or one per perimeter?**
> *"one workbook."*

One workbook for every perimeter.

<a id="q10"></a>**Q10 — Highlight a material `%change`?**
> *"Off"*

No highlighting.

<a id="q11"></a>**Q11 — Is `RA = STAT+FI+RE`, and why is the Evol row `RA/RE`?**
> *"no need %RA"*

The answer named only `%RA`, while the question covered both derived rows; confirmed on 2026-08-27
that the `RA` subtotal goes too, consistent with Q6's *"work only using CRD, RA STAT, RA FI, RE"*.
A segment table is therefore **four rows**, and the `RA/RE` labelling problem disappears with the
row that carried it.

<a id="q12"></a>**Q12 — `%RA` in `Evol`: relative change or percentage points?**
> *"skip %RA"*

Moot, and usefully so: with `%RA` gone, every `Evol` cell is `(new − old) / old` over an **amount**,
which is the unambiguous case. Had `%RA` stayed, a move from 5.00 % to 4.00 % would have been
reported as −20 % where a risk reader expects −1.00 point.

<a id="q13"></a>**Q13 — Which CRD does the `%RA` denominator use?**
> *"yes Same-month CRD assumed"*

Moot — the denominator only existed for `%RA`. Recorded here should `%RA` ever be reinstated.

<a id="q14"></a>**Q14 — Are the French segment labels canonical?**
> *"Labels reproduced from the input RA file, put the order in configuration file"*

Use the input's own `SEGMENT` values (`MORTGAGE`, `INVEST_PRO`, `INVEST_CORP`, `CONSO`), **not** the
manual file's `Immos` / `Invest pro` / `Invest corp` / `Conso`. The display **order** is
configuration (`segmentOrder`); a segment the order does not name still appears, after the named
ones and alphabetically, so a new segment in `INPUTS_RA` is never silently dropped.

<a id="q15"></a>**Q15 — Which perimeters and rate types?**
> *"every perimeter common to both files."*

The intersection rule of §4 stands unchanged.

<a id="q16"></a>**Q16 — Always in millions of euros?**
> *"Like in input RA file like we did for RA calcultation"*

Values are used at the scale they appear in `INPUTS_RA`, with no rescaling — the convention the RA
calculation already follows. The `En M EUR` block header is an assertion about the input, not a
conversion. Only `CRD`'s **sign** is changed, on the business review of 2026-08-27 (§6.1.1), and
`COMPARE INFO` says so on the face of the report.

---

## 11. State of play

**Nothing is waiting on the business.** The job of §7 is built, and T1, T2, T4 and T5 are in the
suite:

| Test | Where |
|---|---|
| T1, T2, T4 | `RaCompareViewSpec` — 25 unit tests on the pure core, no Spark and no workbook, including the `CRD` display sign and its invariance on the `%change` |
| **T5** | `RaCompareGoldenSpec` — 10 assertions running the real pipeline over the two inputs the reference model was generated from, diffing sheets, the whole column-A layout, the table header row, **every** `Evol` formula, the values and the chart titles, plus two that pin the 2026-08-27 review: the dropped label bands stay dropped and `CRD` is written positive on both sides |
| T7 | checked structurally — zip integrity, every XML part parses, 48 charts |

T5 is a *golden* test, so it is worth exactly what it catches. It was checked against a deliberate
one-row anchor change (`BlockGap` 2 → 3): three of its assertions failed and named the drift
precisely — a shifted block row and a re-anchored formula — while the sheet, header and chart assertions correctly
stayed green. It reads the two real workbooks rather than a miniature fixture, and costs about 25
seconds; the whole tseadfwd suite is 126 tests in about 45 seconds.

Deferred, in the order it makes sense to pick them up:

| Item | State |
|---|---|
| **T6** — reconcile against the manual calculation | deferred by [Q7](#q7); needs `Compare_RA_BCEF_KO_Prod 26Q2.xlsx` and the two `INPUTS_RA` files it was built from |
| **HTML report** (§6.2) | deferred by [Q8](#q8); designed, not built, pending what the TWIST tab serves |
| **TWIST wiring** | outside this repo — the job writes a file, TWIST serves it |

The caveat to carry forward: the reference model and the Scala job agree because both were written
from this document, so T5 proves they have not drifted **from each other**. It does not prove either
matches what the Risk team produces by hand. That is what T6 is for, which is why deferring T6
leaves a real gap rather than a formality.
