#!/usr/bin/env python3
"""
Build the RA input comparison workbook (`Compare_RA_<PERIMETER>_...xlsx`).

The Risk team produces this comparison by hand today (`Compare_RA_BCEF_KO_Prod 26Q2.xlsx`).
This script rebuilds the SAME workbook from two `Inputs_RA` files, so the layout the engine
must produce is pinned by something runnable rather than by a screenshot.

It is the LAYOUT REFERENCE for the treatment specified in
`docs/tseadfwd/977/RA_COMPARISON_REPORT_DESIGN.md` — the production job is Scala/POI inside the
`tseadfwd` module; this generator stays the acceptance fixture the job is compared against.

    python3 tools/ra_compare/build_ra_compare_workbook.py \
        --new localRun/tseadfwd/input/Inputs_RA_v2.xlsx \
        --old localRun/tseadfwd/input/Inputs_RA.xlsx \
        --out docs/tseadfwd/977/Compare_RA_reference_model.xlsx

One sheet per (PERIMETER x FWL_TYPE) common to both files, each holding three stacked blocks
(Updated / Previous / Evol) of four segment tables, then one chart per (metric x segment).
"""

import argparse
import re
from datetime import date

from openpyxl import Workbook, load_workbook
from openpyxl.chart import LineChart, Reference
from openpyxl.chart.data_source import AxDataSource, StrRef
from openpyxl.chart.series import SeriesLabel
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

# ---------------------------------------------------------------- input model

KEY_COLUMNS = ["PERIMETER", "SEGMENT", "RATE_TYPE", "FWL_TYPE", "METRIC"]
SHEET_PATTERN = re.compile(r"^RA[_ ].*", re.IGNORECASE)

# Metric rows of one segment table, in the order the manual file lists them.
# (row label in column A, row label in column B, kind)
BLOCK_ROWS = [
    ("Outstanding", "CRD", "metric"),      # CRD, as loaded (negative = exposure)
    ("", "RA", "ra_total"),                # RA STAT + RA FI + RE
    ("Amount PPstat", "RA STAT", "metric"),
    ("Amount PPfin", "RA FI", "metric"),
    ("Amount RE", "RE", "metric"),
    ("", "%RA", "ra_pct"),                 # annualised RA rate: RA * 12 / -CRD
]
METRICS = ["CRD", "RA STAT", "RA FI", "RE"]          # the four metrics of the ticket
CHART_METRICS = ["CRD", "RA STAT", "RA FI", "RE"]

# The manual file names its segment tables in French business wording.
SEGMENT_LABELS = {
    "MORTGAGE": "Immos",
    "INVEST_PRO": "Invest pro",
    "INVEST_CORP": "Invest corp",
    "CONSO": "Conso",
}
SEGMENT_ORDER = ["MORTGAGE", "INVEST_PRO", "INVEST_CORP", "CONSO"]

# The manual file names its sheets after the stress leg (+/- 100 bp).
FWL_SHEET_SUFFIX = {"BASELINE": "BASELINE", "STRESS (-)": "-100", "STRESS (+)": "+100"}
FWL_ORDER = ["BASELINE", "STRESS (-)", "STRESS (+)"]

# ------------------------------------------------------------ sheet geometry
# Same anchors as the manual workbook: titles on 1 / 38 / 75, first table header on
# 4 / 41 / 78, four tables of 8 rows each (header + 6 metric rows + 1 blank).
BLOCK_PITCH = 8
UPDATED_TITLE_ROW, PREVIOUS_TITLE_ROW, EVOL_TITLE_ROW = 1, 38, 75
UPDATED_FIRST_HEADER, PREVIOUS_FIRST_HEADER, EVOL_FIRST_HEADER = 4, 41, 78
FIRST_DATA_COL = 3  # column C

YELLOW = PatternFill("solid", fgColor="FFFF00")
THIN = Side(style="thin", color="808080")
BOX = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

FMT_CRD = "#,##0"
FMT_AMOUNT = "#,##0.00"
FMT_PCT = "0.00%"


def read_ra_file(path):
    """(keys -> monthly series) plus the month column labels, from every RA sheet of a workbook.

    Applies the engine's own two gates: the sheet NAME matches `^RA[_ ].*`, and its CONTENT
    carries the five key columns. Anything else in the workbook is ignored.
    """
    wb = load_workbook(path, read_only=True, data_only=True)
    series, months, sheets, skipped = {}, None, [], []
    for name in wb.sheetnames:
        if not SHEET_PATTERN.match(name):
            skipped.append((name, "name does not match ^RA[_ ].*"))
            continue
        ws = wb[name]
        rows = ws.iter_rows(values_only=True)
        header = next(rows, None)
        if header is None:
            skipped.append((name, "empty sheet"))
            continue
        header = [str(h).strip() if h is not None else "" for h in header]
        if header[:5] != KEY_COLUMNS:
            skipped.append((name, "not an RA table: missing %s" % ", ".join(
                c for c in KEY_COLUMNS if c not in header)))
            continue
        sheet_months = [h for h in header[5:] if h]
        if months is None:
            months = sheet_months
        for row in rows:
            if row is None or row[0] is None:
                continue
            key = tuple(str(v).strip() for v in row[:5])
            series[key] = [(v if isinstance(v, (int, float)) else 0.0)
                           for v in row[5:5 + len(sheet_months)]]
        sheets.append(name)
    wb.close()
    if not series:
        raise SystemExit("no RA sheet could be read from %s" % path)
    return series, months, sheets, skipped


def month_dates(start, count):
    """`count` month starts from `start` (a date), as dates — the column headers of a block."""
    out = []
    y, m = start.year, start.month
    for _ in range(count):
        out.append(date(y, m, 1))
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def perimeter_segments(series, perimeter):
    """(SEGMENT, RATE_TYPE) pairs of a perimeter, in the manual file's order."""
    pairs = {(k[1], k[2]) for k in series if k[0] == perimeter}
    known = [(s, r) for s in SEGMENT_ORDER for (sg, r) in sorted(pairs) if sg == s]
    rest = sorted(p for p in pairs if p[0] not in SEGMENT_ORDER)
    return known + rest


def write_block(ws, first_header_row, title_row, title, series, perimeter, fwl,
                segments, months, start_date, n_months):
    """One `Updated` / `Previous` block: the title, the month index row, four segment tables."""
    ws.cell(title_row, 2, title).fill = YELLOW
    ws.cell(title_row, 2).font = Font(bold=True)
    ws.cell(title_row + 1, 2, "En M€").font = Font(italic=True)

    index_row = first_header_row - 1
    for i, label in enumerate(months[:n_months]):
        c = ws.cell(index_row, FIRST_DATA_COL + i, label)
        c.font = Font(size=8, color="808080")
        c.alignment = Alignment(horizontal="center")

    dates = month_dates(start_date, n_months)
    for b, (segment, rate_type) in enumerate(segments):
        header = first_header_row + b * BLOCK_PITCH
        label = SEGMENT_LABELS.get(segment, segment)
        h = ws.cell(header, 2, "%s à %s" % (label, rate_type))
        h.font, h.border = Font(bold=True), BOX
        for i, d in enumerate(dates):
            c = ws.cell(header, FIRST_DATA_COL + i, d)
            c.number_format = "DD/MM/YYYY"
            c.font, c.border, c.alignment = Font(bold=True, size=9), BOX, Alignment(horizontal="center")

        for j, (label_a, label_b, kind) in enumerate(BLOCK_ROWS):
            row = header + 1 + j
            if label_a:
                ws.cell(row, 1, label_a).font = Font(italic=True, size=9)
            ws.cell(row, 2, label_b).font = Font(size=9)
            ws.cell(row, 2).border = BOX
            for i in range(n_months):
                col = FIRST_DATA_COL + i
                cell = ws.cell(row, col)
                letter = get_column_letter(col)
                if kind == "metric":
                    values = series.get((perimeter, segment, rate_type, fwl, label_b))
                    cell.value = values[i] if values else None
                    cell.number_format = FMT_CRD if label_b == "CRD" else FMT_AMOUNT
                elif kind == "ra_total":
                    # RA = RA STAT + RA FI + RE, kept as a formula so the reader can audit it
                    stat, fi, re_ = (header + 3, header + 4, header + 5)
                    cell.value = "=%s%d+%s%d+%s%d" % (letter, stat, letter, fi, letter, re_)
                    cell.number_format = FMT_AMOUNT
                else:  # ra_pct — annualised RA rate over the outstanding
                    ra, crd = header + 2, header + 1
                    cell.value = "=IFERROR(%s%d*12/-%s%d,\"\")" % (letter, ra, letter, crd)
                    cell.number_format = FMT_PCT
                cell.font = Font(size=9)


def write_evol_block(ws, segments, n_months, safe_div):
    """The `Evol` block: (Updated - Previous) / Previous, cell by cell, as live formulas."""
    ws.cell(EVOL_TITLE_ROW, 2, "Evol").fill = YELLOW
    ws.cell(EVOL_TITLE_ROW, 2).font = Font(bold=True)
    ws.cell(EVOL_TITLE_ROW + 1, 2, "En M€").font = Font(italic=True)

    for i in range(n_months):
        src = ws.cell(UPDATED_FIRST_HEADER - 1, FIRST_DATA_COL + i).value
        c = ws.cell(EVOL_FIRST_HEADER - 1, FIRST_DATA_COL + i, src)
        c.font = Font(size=8, color="808080")
        c.alignment = Alignment(horizontal="center")

    for b, (segment, rate_type) in enumerate(segments):
        header = EVOL_FIRST_HEADER + b * BLOCK_PITCH
        up_header = UPDATED_FIRST_HEADER + b * BLOCK_PITCH
        prev_header = PREVIOUS_FIRST_HEADER + b * BLOCK_PITCH
        label = SEGMENT_LABELS.get(segment, segment)
        h = ws.cell(header, 2, "%s à %s" % (label, rate_type))
        h.font, h.border = Font(bold=True), BOX
        for i in range(n_months):
            c = ws.cell(header, FIRST_DATA_COL + i,
                        ws.cell(up_header, FIRST_DATA_COL + i).value)
            c.number_format = "DD/MM/YYYY"
            c.font, c.border, c.alignment = Font(bold=True, size=9), BOX, Alignment(horizontal="center")

        for j, (label_a, label_b, _) in enumerate(BLOCK_ROWS):
            row = header + 1 + j
            if label_a:
                ws.cell(row, 1, label_a).font = Font(italic=True, size=9)
            ws.cell(row, 2, "RA/RE" if label_b == "RA" else label_b).font = Font(size=9)
            ws.cell(row, 2).border = BOX
            for i in range(n_months):
                col = FIRST_DATA_COL + i
                letter = get_column_letter(col)
                new_ref = "%s%d" % (letter, up_header + 1 + j)
                old_ref = "%s%d" % (letter, prev_header + 1 + j)
                # The manual file's own formula: =(C5-C42)/C42
                body = "(%s-%s)/%s" % (new_ref, old_ref, old_ref)
                cell = ws.cell(row, col)
                cell.value = "=IFERROR(%s,\"\")" % body if safe_div else "=" + body
                cell.number_format = FMT_PCT
                cell.font = Font(size=9)


def add_charts(ws, segments, n_months, chart_step):
    """One line chart per (metric x segment): the Updated and the Previous curve, superimposed."""
    first_chart_row = EVOL_FIRST_HEADER + len(segments) * BLOCK_PITCH + 3
    max_col = FIRST_DATA_COL + n_months - 1
    index_row = UPDATED_FIRST_HEADER - 1
    for m, metric in enumerate(CHART_METRICS):
        offset = [r[1] for r in BLOCK_ROWS].index(metric)
        for b, (segment, rate_type) in enumerate(segments):
            chart = LineChart()
            chart.title = "%s - %s" % (metric, SEGMENT_LABELS.get(segment, segment))
            chart.style = 2
            chart.height, chart.width = 7.5, 12.5
            chart.y_axis.numFmt = FMT_CRD if metric == "CRD" else FMT_AMOUNT
            for block_first, name in ((UPDATED_FIRST_HEADER, "Updated"),
                                      (PREVIOUS_FIRST_HEADER, "Previous")):
                row = block_first + b * BLOCK_PITCH + 1 + offset
                data = Reference(ws, min_col=FIRST_DATA_COL, max_col=max_col,
                                 min_row=row, max_row=row)
                chart.add_data(data, titles_from_data=False, from_rows=True)
                series = chart.series[-1]
                series.tx = SeriesLabel(v=name)
                series.smooth = False
            # month labels are TEXT (M1, M2, …): a strRef keeps Excel from numbering the axis 1..n
            categories = AxDataSource(strRef=StrRef("'%s'!$%s$%d:$%s$%d" % (
                ws.title, get_column_letter(FIRST_DATA_COL), index_row,
                get_column_letter(max_col), index_row)))
            for series in chart.series:
                series.cat = categories
            chart.x_axis.tickLblSkip = chart_step
            chart.x_axis.tickMarkSkip = chart_step
            anchor = "%s%d" % (get_column_letter(2 + b * 9), first_chart_row + m * 16)
            ws.add_chart(chart, anchor)


def write_info_sheet(wb, args, new_meta, old_meta, common, only_new, only_old, n_months):
    ws = wb.create_sheet("COMPARE INFO", 0)
    ws.column_dimensions["A"].width = 28
    ws.column_dimensions["B"].width = 90
    rows = [
        ("RA input comparison", ""),
        ("", ""),
        ("Updated (new) file", args.new),
        ("Previous (old) file", args.old),
        ("Updated RA sheets", ", ".join(new_meta["sheets"])),
        ("Previous RA sheets", ", ".join(old_meta["sheets"])),
        ("Months compared", "%d (%s .. %s), aligned by month INDEX" %
            (n_months, new_meta["months"][0], new_meta["months"][n_months - 1])),
        ("Updated first month", args.new_start),
        ("Previous first month", args.old_start),
        ("", ""),
        ("Compared keys", "%d perimeter x segment x rate type x FWL type x metric" % len(common)),
        ("Only in Updated (dropped)", ", ".join(sorted({"/".join(k[:3]) for k in only_new})) or "none"),
        ("Only in Previous (dropped)", ", ".join(sorted({"/".join(k[:3]) for k in only_old})) or "none"),
        ("", ""),
        ("%change", "(Updated - Previous) / Previous, per metric, per month, per key"),
        ("RA", "RA STAT + RA FI + RE"),
        ("%RA", "RA x 12 / -CRD (annualised rate over the outstanding)"),
        ("", ""),
        ("Generated by", "tools/ra_compare/build_ra_compare_workbook.py"),
        ("Design", "docs/tseadfwd/977/RA_COMPARISON_REPORT_DESIGN.md"),
    ]
    for i, (a, b) in enumerate(rows, start=1):
        ws.cell(i, 1, a).font = Font(bold=(b == "" and a != ""))
        ws.cell(i, 2, b)
    ws["A1"].font = Font(bold=True, size=14)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--new", required=True, help="Updated Inputs_RA workbook")
    p.add_argument("--old", required=True, help="Previous Inputs_RA workbook")
    p.add_argument("--out", required=True, help="workbook to write")
    p.add_argument("--new-start", default="2026-01", help="first projection month of the new file (YYYY-MM)")
    p.add_argument("--old-start", default="2026-01", help="first projection month of the old file (YYYY-MM)")
    p.add_argument("--perimeters", default="", help="comma-separated subset; default = all common ones")
    p.add_argument("--chart-step", type=int, default=12, help="x-axis label step of the charts")
    p.add_argument("--raw-div", action="store_true",
                   help="write the manual file's bare =(C5-C42)/C42 (shows #DIV/0!) instead of IFERROR")
    args = p.parse_args()

    def start_of(s):
        y, m = s.split("-")
        return date(int(y), int(m), 1)

    new_series, new_months, new_sheets, new_skipped = read_ra_file(args.new)
    old_series, old_months, old_sheets, old_skipped = read_ra_file(args.old)

    common = sorted(set(new_series) & set(old_series))
    only_new = sorted(set(new_series) - set(old_series))
    only_old = sorted(set(old_series) - set(new_series))
    if not common:
        raise SystemExit("the two files share no (perimeter, segment, rate type, FWL type, metric) key")
    n_months = min(len(new_months), len(old_months),
                   min(len(v) for v in new_series.values()),
                   min(len(v) for v in old_series.values()))

    perimeters = sorted({k[0] for k in common})
    if args.perimeters:
        wanted = {s.strip().upper() for s in args.perimeters.split(",")}
        perimeters = [p for p in perimeters if p in wanted]

    wb = Workbook()
    wb.remove(wb.active)
    for perimeter in perimeters:
        segments = [sr for sr in perimeter_segments(new_series, perimeter)
                    if any(k[:3] == (perimeter, sr[0], sr[1]) for k in common)]
        for fwl in FWL_ORDER:
            if not any(k[0] == perimeter and k[3] == fwl for k in common):
                continue
            ws = wb.create_sheet("%s %s" % (perimeter, FWL_SHEET_SUFFIX.get(fwl, fwl)))
            ws.sheet_view.showGridLines = False
            ws.freeze_panes = "C4"
            ws.column_dimensions["A"].width = 16
            ws.column_dimensions["B"].width = 14
            for i in range(n_months):
                ws.column_dimensions[get_column_letter(FIRST_DATA_COL + i)].width = 11

            write_block(ws, UPDATED_FIRST_HEADER, UPDATED_TITLE_ROW, "Updated", new_series,
                        perimeter, fwl, segments, new_months, start_of(args.new_start), n_months)
            write_block(ws, PREVIOUS_FIRST_HEADER, PREVIOUS_TITLE_ROW, "Previous", old_series,
                        perimeter, fwl, segments, old_months, start_of(args.old_start), n_months)
            write_evol_block(ws, segments, n_months, safe_div=not args.raw_div)
            add_charts(ws, segments, n_months, args.chart_step)

    write_info_sheet(wb, args,
                     {"sheets": new_sheets, "months": new_months},
                     {"sheets": old_sheets, "months": old_months},
                     common, only_new, only_old, n_months)
    wb.save(args.out)

    print("wrote %s" % args.out)
    print("  sheets      : %s" % ", ".join(s.title for s in wb.worksheets))
    print("  months      : %d" % n_months)
    print("  common keys : %d   only-new %d   only-previous %d"
          % (len(common), len(only_new), len(only_old)))
    for name, reason in new_skipped + old_skipped:
        print("  skipped     : %s (%s)" % (name, reason))


if __name__ == "__main__":
    main()
