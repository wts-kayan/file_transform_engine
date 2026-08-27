package com.bnp.str.tseadfwd.writer

import com.bnp.str.tseadfwd.mapping.RaCompareView
import com.bnp.str.tseadfwd.mapping.RaCompareView._
import com.bnp.str.tseadfwd.utility.PrimaryConstants._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.ss.usermodel.{CellStyle, IndexedColors, Workbook}
import org.apache.poi.ss.util.{CellRangeAddress, CellReference}
import org.apache.poi.xddf.usermodel.chart._
import org.apache.poi.xssf.usermodel.{XSSFColor, XSSFSheet, XSSFWorkbook}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Writes the comparison workbook: the geometry of design §6.1, which is pinned by the reference
 * model `docs/tseadfwd/977/Compare_RA_reference_model.xlsx`.
 *
 * POI directly, not `spark-excel`: the connector has no chart support and cannot lay out three
 * stacked blocks on one sheet. Volume is small (design §7), so the workbook is built in memory on
 * the driver and written once through Hadoop's `FileSystem`, which keeps a local path and an HDFS
 * path behaving identically — the same rule every other output in this module follows.
 *
 * Sheet layout, per (perimeter x stress leg):
 * {{{
 *   row  1   Updated                       <- the new INPUTS_RA file
 *   row  3   M1 M2 M3 …                    <- month index row (the chart categories)
 *   row  4   MORTGAGE a TF                 <- segment table header, then M1..Mn labels
 *   rows 5-8    CRD · RA STAT · RA FI · RE
 *   rows 10-14  INVEST_PRO a TF …          <- next table, 6 rows lower
 *   row  30  Previous                      <- same geometry, 29 rows lower
 *   row  59  Evol                          <- every cell =IFERROR((C5-C34)/C34,"")
 *   rows 84+ charts: one per (metric x segment), Updated vs Previous
 * }}}
 *
 * The anchors are DERIVED from the row list rather than written down, so dropping or adding a
 * metric row moves every block correctly instead of silently overlapping the next one.
 */
object RaCompareExcelWriter {

  private val log = LoggerFactory.getLogger(this.getClass)

  /** Table header + one row per metric + one blank. */
  val BlockPitch: Int = RaCompareView.MetricRows.size + 2
  /** Title, the "En M EUR" note, the month index row, then the first table header. */
  val TitleToFirstHeader = 3
  val BlockGap = 2
  /** Column C — A holds the business row label, B the metric name. */
  val FirstDataCol = 2 // 0-based

  private def blockHeight(tables: Int): Int = TitleToFirstHeader + tables * BlockPitch + BlockGap

  /**
   * @param label   what the block is called in the report
   * @param titleRow 0-based row of the block title
   */
  private final case class Block(label: String, titleRow: Int) {
    def firstHeader: Int = titleRow + TitleToFirstHeader
    def monthIndexRow: Int = titleRow + 2
    def headerOf(table: Int): Int = firstHeader + table * BlockPitch
    /** 1-based Excel row of one metric inside one table — what a formula has to reference. */
    def excelMetricRow(table: Int, metric: Int): Int = headerOf(table) + 1 + metric + 1
  }

  /**
   * Tab colour per stress leg, so a leg is findable among the tabs.
   *
   * One workbook holds three sheets per perimeter (Q9), which is eighteen near-identically named
   * tabs at six perimeters. Blue and orange rather than red and green: the pair stays distinguishable
   * for the commonest forms of colour blindness, and this is the only thing on the sheet carrying
   * meaning in colour alone.
   */
  private val TabColour: Map[String, Array[Byte]] = Map(
    FWL_BASELINE -> rgb(0x5B, 0x64, 0x70),        // slate — the unshocked reference
    FWL_STRESS_MINUS -> rgb(0xC2, 0x70, 0x3D),    // orange
    FWL_STRESS_PLUS -> rgb(0x3D, 0x7E, 0xA6))     // blue

  private def rgb(r: Int, g: Int, b: Int): Array[Byte] =
    Array(r.toByte, g.toByte, b.toByte)

  /** Row labels of column A, as the manual workbook words them. */
  private val RowLabelA: Map[String, String] = Map(
    METRIC_CRD -> "Outstanding",
    METRIC_RA_STAT -> "Amount PPstat",
    METRIC_RA_FI -> "Amount PPfin",
    METRIC_RE -> "Amount RE")

  final case class WriteOptions(newLabel: String = "Updated",
                                oldLabel: String = "Previous",
                                segmentOrder: Seq[String] = Seq.empty,
                                safeDiv: Boolean = true,
                                sourceNew: String = "",
                                sourceOld: String = "")

  /**
   * Build the workbook and write it to `path` (local or HDFS).
   *
   * @return the sheets written, in order
   */
  def write(path: String, result: RaCompareResult, newSeries: Series, oldSeries: Series,
            opts: WriteOptions)(implicit spark: SparkSession): Seq[String] = {
    val wb = new XSSFWorkbook()
    val styles = Styles(wb)
    val written = Vector.newBuilder[String]

    writeInfoSheet(wb, styles, result, opts)
    written += "COMPARE INFO"

    result.perimeters.foreach { perimeter =>
      val tables = RaCompareView.segmentTables(result.compared, perimeter, opts.segmentOrder)
      RaCompareView.FwlOrder.foreach { fwl =>
        val present = result.compared.exists(k => k.perimeter == perimeter && k.fwlType == fwl)
        if (present) {
          val name = RaCompareView.sheetName(perimeter, fwl)
          val sheet = wb.createSheet(name)
          TabColour.get(fwl).foreach(c => sheet.setTabColor(new XSSFColor(c, null)))
          writeSheet(sheet, styles, perimeter, fwl, tables, result, newSeries, oldSeries, opts)
          written += name
        }
      }
    }

    save(wb, path)
    val names = written.result()
    log.info(s"RA comparison workbook written to $path (${names.size} sheet(s))")
    names
  }

  // ---- one (perimeter x leg) sheet ------------------------------------------

  private def writeSheet(sheet: XSSFSheet, st: Styles, perimeter: String, fwl: String,
                         tables: Seq[(String, String)], result: RaCompareResult,
                         newSeries: Series, oldSeries: Series, opts: WriteOptions): Unit = {
    val months = result.months
    val h = blockHeight(tables.size)
    val updated = Block(opts.newLabel, 0)
    val previous = Block(opts.oldLabel, h)
    val evol = Block("Evol", 2 * h)

    sheet.setDisplayGridlines(false)
    sheet.createFreezePane(FirstDataCol, updated.firstHeader)
    sheet.setColumnWidth(0, 16 * 256)
    sheet.setColumnWidth(1, 14 * 256)

    writeValueBlock(sheet, st, updated, perimeter, fwl, tables, months, newSeries)
    writeValueBlock(sheet, st, previous, perimeter, fwl, tables, months, oldSeries)
    writeEvolBlock(sheet, st, evol, updated, previous, tables, months, opts.safeDiv)
    addCharts(sheet, evol, updated, previous, tables, months, opts)
  }

  /** An `Updated` or `Previous` block: title, month index row, one table per segment. */
  private def writeValueBlock(sheet: XSSFSheet, st: Styles, block: Block, perimeter: String,
                              fwl: String, tables: Seq[(String, String)], months: Int,
                              series: Series): Unit = {
    title(sheet, st, block)

    val idx = row(sheet, block.monthIndexRow)
    RaCompareView.monthLabels(months).zipWithIndex.foreach { case (label, i) =>
      val c = idx.createCell(FirstDataCol + i); c.setCellValue(label); c.setCellStyle(st.monthIndex)
    }

    tables.zipWithIndex.foreach { case ((segment, rateType), t) =>
      val header = block.headerOf(t)
      tableHeader(sheet, st, header, segment, rateType, months)

      RaCompareView.MetricRows.zipWithIndex.foreach { case (metric, m) =>
        val r = row(sheet, header + 1 + m)
        val a = r.createCell(0); a.setCellValue(RowLabelA.getOrElse(metric, "")); a.setCellStyle(st.rowLabelA)
        val b = r.createCell(1); b.setCellValue(metric); b.setCellStyle(st.rowLabelB)

        val key = RaKey(perimeter, segment, rateType, fwl, metric)
        val style = if (metric == METRIC_CRD) st.crd else st.amount
        for (i <- 0 until months) {
          val cell = r.createCell(FirstDataCol + i)
          RaCompareView.valueAt(series, key, i) match {
            case Some(v) => cell.setCellValue(v)
            case None    => // left blank: the series has no value here, and 0 would be a lie
          }
          cell.setCellStyle(style)
        }
      }
    }
  }

  /** The `Evol` block: `(Updated - Previous) / Previous`, as live formulas a reviewer can click. */
  private def writeEvolBlock(sheet: XSSFSheet, st: Styles, block: Block, updated: Block,
                             previous: Block, tables: Seq[(String, String)], months: Int,
                             safeDiv: Boolean): Unit = {
    title(sheet, st, block)

    val idx = row(sheet, block.monthIndexRow)
    RaCompareView.monthLabels(months).zipWithIndex.foreach { case (label, i) =>
      val c = idx.createCell(FirstDataCol + i); c.setCellValue(label); c.setCellStyle(st.monthIndex)
    }

    tables.zipWithIndex.foreach { case ((segment, rateType), t) =>
      tableHeader(sheet, st, block.headerOf(t), segment, rateType, months)

      RaCompareView.MetricRows.zipWithIndex.foreach { case (metric, m) =>
        val r = row(sheet, block.headerOf(t) + 1 + m)
        val a = r.createCell(0); a.setCellValue(RowLabelA.getOrElse(metric, "")); a.setCellStyle(st.rowLabelA)
        val b = r.createCell(1); b.setCellValue(metric); b.setCellStyle(st.rowLabelB)

        val upRow = updated.excelMetricRow(t, m)
        val prevRow = previous.excelMetricRow(t, m)
        for (i <- 0 until months) {
          val col = CellReference.convertNumToColString(FirstDataCol + i)
          val body = s"($col$upRow-$col$prevRow)/$col$prevRow"
          val cell = r.createCell(FirstDataCol + i)
          // IFERROR blanks the division by zero the manual file shows as #DIV/0! (Q5); safeDiv =
          // false writes the bare formula for a reader who wants that error visible.
          cell.setCellFormula(if (safeDiv) s"""IFERROR($body,"")""" else body)
          cell.setCellStyle(st.pct)
        }
      }
    }
  }

  private def title(sheet: XSSFSheet, st: Styles, block: Block): Unit = {
    val t = row(sheet, block.titleRow).createCell(1)
    t.setCellValue(block.label); t.setCellStyle(st.title)
    // An assertion about the input, not a conversion: values are written exactly as the workbook
    // holds them (Q16).
    val u = row(sheet, block.titleRow + 1).createCell(1)
    u.setCellValue("En M EUR"); u.setCellStyle(st.unit)
  }

  private def tableHeader(sheet: XSSFSheet, st: Styles, rowIdx: Int, segment: String,
                          rateType: String, months: Int): Unit = {
    val r = row(sheet, rowIdx)
    val h = r.createCell(1)
    // The input's own segment name, not the manual file's French label (Q14).
    h.setCellValue(s"$segment a $rateType"); h.setCellStyle(st.tableHeader)
    // Month labels, not calendar dates: an INPUTS_RA sheet carries no as-of date (Q3/Q4).
    RaCompareView.monthLabels(months).zipWithIndex.foreach { case (label, i) =>
      val c = r.createCell(FirstDataCol + i); c.setCellValue(label); c.setCellStyle(st.tableHeader)
    }
  }

  // ---- charts ---------------------------------------------------------------

  /** One line chart per (metric x segment): the two curves superimposed. */
  private def addCharts(sheet: XSSFSheet, evol: Block, updated: Block, previous: Block,
                        tables: Seq[(String, String)], months: Int, opts: WriteOptions): Unit = {
    if (months <= 0 || tables.isEmpty) return
    val drawing = sheet.createDrawingPatriarch()
    val firstChartRow = evol.headerOf(tables.size) + 2
    val perRow = 4
    var n = 0

    RaCompareView.MetricRows.zipWithIndex.foreach { case (metric, m) =>
      tables.zipWithIndex.foreach { case ((segment, _), t) =>
        val gridRow = n / perRow
        val gridCol = n % perRow
        n += 1
        val anchor = drawing.createAnchor(0, 0, 0, 0,
          1 + gridCol * 8, firstChartRow + gridRow * 20,
          9 + gridCol * 8, firstChartRow + gridRow * 20 + 20)

        val chart = drawing.createChart(anchor)
        chart.setTitleText(s"$metric - $segment")
        chart.setTitleOverlay(false)
        chart.getOrAddLegend.setPosition(LegendPosition.BOTTOM)

        val cats = XDDFDataSourcesFactory.fromStringCellRange(sheet,
          new CellRangeAddress(updated.monthIndexRow, updated.monthIndexRow,
            FirstDataCol, FirstDataCol + months - 1))
        def rowSource(block: Block) = {
          val r = block.excelMetricRow(t, m) - 1 // back to 0-based for CellRangeAddress
          XDDFDataSourcesFactory.fromNumericCellRange(sheet,
            new CellRangeAddress(r, r, FirstDataCol, FirstDataCol + months - 1))
        }

        val bottom = chart.createCategoryAxis(AxisPosition.BOTTOM)
        val left = chart.createValueAxis(AxisPosition.LEFT)
        left.setCrosses(AxisCrosses.AUTO_ZERO)

        // No tick-label skip: XDDFCategoryAxis exposes none, and the CTSkip type that would let
        // us reach the underlying CTCatAx is absent from poi-ooxml-lite. Excel thins the labels of
        // a 361-point axis by itself, so this costs nothing worth a new schema dependency.
        val data = chart.createData(ChartTypes.LINE, bottom, left).asInstanceOf[XDDFLineChartData]
        Seq(opts.newLabel -> updated, opts.oldLabel -> previous).foreach { case (label, block) =>
          val s = data.addSeries(cats, rowSource(block)).asInstanceOf[XDDFLineChartData#Series]
          s.setTitle(label, null)
          s.setSmooth(false)
          // 361 points per series: markers would be a solid band of shapes, not a curve.
          s.setMarkerStyle(MarkerStyle.NONE)
        }
        chart.plot(data)
      }
    }
  }

  // ---- COMPARE INFO ---------------------------------------------------------

  /** What was compared, what was dropped, and the definitions — so the file explains itself. */
  private def writeInfoSheet(wb: Workbook, st: Styles, result: RaCompareResult,
                             opts: WriteOptions): Unit = {
    val sheet = wb.createSheet("COMPARE INFO")
    sheet.setDisplayGridlines(false)
    sheet.setColumnWidth(0, 30 * 256)
    sheet.setColumnWidth(1, 100 * 256)

    def dropped(keys: Seq[RaKey]) =
      if (keys.isEmpty) "none" else keys.map(_.tableId).distinct.sorted.mkString(", ")

    val rows = Seq(
      "RA input comparison" -> "",
      "" -> "",
      s"${opts.newLabel} (new) file" -> opts.sourceNew,
      s"${opts.oldLabel} (old) file" -> opts.sourceOld,
      "" -> "",
      "Months compared" -> s"${result.months} (M1 .. M${result.months}), aligned by month INDEX",
      "Compared keys" -> s"${result.comparedCount} perimeter x segment x rate type x FWL type x metric",
      s"Only in ${opts.newLabel} (dropped)" -> dropped(result.onlyNew),
      s"Only in ${opts.oldLabel} (dropped)" -> dropped(result.onlyOld),
      "" -> "",
      "%change" -> s"(${opts.newLabel} - ${opts.oldLabel}) / ${opts.oldLabel}, per metric, per month, per key",
      "Metrics" -> ("CRD, RA STAT, RA FI, RE - as they appear in the input. The manual workbook's " +
        "derived RA and %RA rows are not reproduced (977 Q6/Q11)."),
      "Months" -> "identified as M1..Mn; the report carries no calendar dates (977 Q3/Q4).",
      "Values" -> "used exactly as they appear in INPUTS_RA, with no rescaling (977 Q16).",
      "" -> "",
      "Generated by" -> "com.bnp.str.tseadfwd.job.CompareAndReportDriver",
      "Design" -> "docs/tseadfwd/977/RA_COMPARISON_REPORT_DESIGN.md")

    rows.zipWithIndex.foreach { case ((k, v), i) =>
      val r = sheet.createRow(i)
      val a = r.createCell(0); a.setCellValue(k)
      a.setCellStyle(if (i == 0) st.title else st.rowLabelB)
      val b = r.createCell(1); b.setCellValue(v); b.setCellStyle(st.info)
      r.setHeightInPoints(infoRowHeight(v))
    }

    if (result.newTruncated || result.oldTruncated) {
      val note =
        s"the two files carry a different number of month columns; the comparison was truncated " +
          s"to the shorter horizon (${result.months} months)"
      val r = sheet.createRow(rows.size + 1)
      r.createCell(0).setCellValue("NOTE")
      val c = r.createCell(1); c.setCellValue(note); c.setCellStyle(st.info)
      r.setHeightInPoints(infoRowHeight(note))
    }
  }

  /**
   * Height of one COMPARE INFO row, from the length of its text.
   *
   * The cells wrap, but a wrapped cell at the default height shows ONE line — so the dropped-key
   * list, which can run to several hundred characters, was being written and then hidden. The
   * report's whole promise is that an excluded key is named rather than silently dropped, so the
   * height has to follow the content.
   */
  private def infoRowHeight(text: String): Float = {
    val charsPerLine = 98              // column B is 100 characters wide
    val lines = math.max(1, math.ceil(text.length.toDouble / charsPerLine).toInt)
    math.min(lines * 14.0f, 409.0f)    // 409pt is Excel's per-row maximum
  }

  // ---- plumbing -------------------------------------------------------------

  private def row(sheet: XSSFSheet, i: Int) =
    Option(sheet.getRow(i)).getOrElse(sheet.createRow(i))

  /** Write through Hadoop's FileSystem so the path may be local or HDFS. */
  private def save(wb: Workbook, path: String)(implicit spark: SparkSession): Unit = {
    val p = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val out = fs.create(p, true)
    try wb.write(out) finally { out.close(); wb.close() }
  }

  /** Cell styles, created once per workbook — POI caps a workbook's style count. */
  private final case class Styles(wb: Workbook) {
    private def fmt(f: String): Short = wb.createDataFormat().getFormat(f)
    private def bold = {
      val f = wb.createFont(); f.setBold(true); f
    }

    val title: CellStyle = {
      val s = wb.createCellStyle()
      s.setFont(bold)
      s.setFillForegroundColor(IndexedColors.YELLOW.getIndex)
      s.setFillPattern(org.apache.poi.ss.usermodel.FillPatternType.SOLID_FOREGROUND)
      s
    }
    val unit: CellStyle = {
      val s = wb.createCellStyle()
      val f = wb.createFont(); f.setItalic(true); s.setFont(f); s
    }
    val tableHeader: CellStyle = {
      val s = wb.createCellStyle()
      val f = wb.createFont(); f.setBold(true); f.setFontHeightInPoints(9.toShort); s.setFont(f)
      s.setAlignment(org.apache.poi.ss.usermodel.HorizontalAlignment.CENTER)
      s
    }
    val monthIndex: CellStyle = {
      val s = wb.createCellStyle()
      val f = wb.createFont(); f.setFontHeightInPoints(8.toShort); s.setFont(f)
      s.setAlignment(org.apache.poi.ss.usermodel.HorizontalAlignment.CENTER)
      s
    }
    private def small = {
      val f = wb.createFont(); f.setFontHeightInPoints(9.toShort); f
    }
    val rowLabelA: CellStyle = {
      val s = wb.createCellStyle()
      val f = wb.createFont(); f.setItalic(true); f.setFontHeightInPoints(9.toShort); s.setFont(f); s
    }
    val rowLabelB: CellStyle = { val s = wb.createCellStyle(); s.setFont(small); s }
    val crd: CellStyle = { val s = wb.createCellStyle(); s.setFont(small); s.setDataFormat(fmt("#,##0")); s }
    val amount: CellStyle = { val s = wb.createCellStyle(); s.setFont(small); s.setDataFormat(fmt("#,##0.00")); s }
    val pct: CellStyle = { val s = wb.createCellStyle(); s.setFont(small); s.setDataFormat(fmt("0.00%")); s }
    val info: CellStyle = { val s = wb.createCellStyle(); s.setWrapText(true); s }
  }
}
