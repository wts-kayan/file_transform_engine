package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.mapping.RaCompareView
import com.bnp.str.tseadfwd.reader.{RaCompareReader, RaSheetConfig}
import com.bnp.str.tseadfwd.writer.RaCompareExcelWriter
import com.bnp.str.tseadfwd.writer.RaCompareExcelWriter.WriteOptions
import org.apache.poi.ss.usermodel.{Cell, CellType}
import org.apache.poi.xssf.usermodel.{XSSFSheet, XSSFWorkbook}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/**
 * Acceptance test T5 of the RA comparison design (docs/tseadfwd/977, §9): the job's workbook
 * against the committed reference model.
 *
 * The reference model is built by `tools/ra_compare/build_ra_compare_workbook.py` and is the
 * runnable specification of the §6.1 geometry — the artefact the business looks at. This suite runs
 * the real pipeline (reader -> view -> writer) over the SAME two inputs the model was generated
 * from, and diffs the result: sheets, the whole column-B layout, the table headers, every `Evol`
 * formula, the values, and the charts.
 *
 * What it does and does not prove. It catches DRIFT — a changed anchor, a renamed row, a formula
 * pointing at the wrong block, a lost chart — which is what will actually go wrong as this code is
 * edited. It does not prove either side matches what the Risk team produces by hand, because both
 * were written from the same document; that is acceptance test T6, deferred by Q7 for want of the
 * manual workbook.
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.tseadfwd.RaCompareGoldenSpec
 */
class RaCompareGoldenSpec extends AnyFunSuite with Matchers with SparkTestSession {

  /** The committed layout reference, and the exact pair of inputs it was generated from. */
  private val ReferenceModel = "docs/tseadfwd/977/Compare_RA_reference_model.xlsx"
  private val NewInput = "localRun/tseadfwd/input/Inputs_RA_v2.xlsx"
  private val OldInput = "localRun/tseadfwd/input/Inputs_RA.xlsx"

  private val OutDir = "target/test-ra-compare"
  private val Produced = s"$OutDir/Compare_RA_produced.xlsx"

  /** Every fixture this suite needs, so a missing one is named rather than surfacing as a NPE. */
  private val fixtures = Seq(ReferenceModel, NewInput, OldInput)

  /**
   * Produced once for the whole suite: the pipeline reads two real workbooks, which is the slow
   * part. Each test then asserts one facet, so a failure names WHAT drifted rather than just
   * "the workbooks differ".
   */
  private lazy val produced: XSSFWorkbook = {
    Files.createDirectories(Paths.get(OutDir))
    val cfg = RaSheetConfig(paths = Vector.empty, sheetPattern = RaSheetConfig.DEFAULT_PATTERN,
      requireColumns = RaSheetConfig.DEFAULT_REQUIRED, include = Vector.empty, exclude = Vector.empty)

    val loadedNew = RaCompareReader.load(NewInput, cfg)
    val loadedOld = RaCompareReader.load(OldInput, cfg)
    val result = RaCompareView.compare(loadedNew.series, loadedOld.series,
      loadedNew.monthCount, loadedOld.monthCount)

    // The reference model is generated with the tool's defaults, so the writer gets them too.
    RaCompareExcelWriter.write(Produced, result, loadedNew.series, loadedOld.series,
      WriteOptions(segmentOrder = Seq("MORTGAGE", "INVEST_PRO", "INVEST_CORP", "CONSO"),
        sourceNew = NewInput, sourceOld = OldInput))

    open(Produced)
  }

  private lazy val reference: XSSFWorkbook = open(ReferenceModel)

  /** Read through a stream: POI's file-based open can write back to what it opened. */
  private def open(path: String): XSSFWorkbook = {
    val in = new FileInputStream(new File(path))
    try new XSSFWorkbook(in) finally in.close()
  }

  /** Sheets carrying the report proper; COMPARE INFO is prose and is checked separately. */
  private def dataSheets(wb: XSSFWorkbook): Seq[String] =
    wb.sheetIterator().asScala.map(_.getSheetName).filter(_ != "COMPARE INFO").toVector

  private def cellAt(sheet: XSSFSheet, r: Int, c: Int): Option[Cell] =
    Option(sheet.getRow(r)).flatMap(row => Option(row.getCell(c)))

  /** A cell's text, with a missing cell and an empty one deliberately indistinguishable. */
  private def text(sheet: XSSFSheet, r: Int, c: Int): String =
    cellAt(sheet, r, c).map { cell =>
      cell.getCellType match {
        case CellType.STRING => cell.getStringCellValue
        case CellType.NUMERIC => cell.getNumericCellValue.toString
        case CellType.FORMULA => "=" + cell.getCellFormula
        case CellType.BLANK => ""
        case CellType.BOOLEAN => cell.getBooleanCellValue.toString
        case _ => ""
      }
    }.getOrElse("")

  private def numeric(sheet: XSSFSheet, r: Int, c: Int): Option[Double] =
    cellAt(sheet, r, c).filter(_.getCellType == CellType.NUMERIC).map(_.getNumericCellValue)

  private def formula(sheet: XSSFSheet, r: Int, c: Int): Option[String] =
    cellAt(sheet, r, c).filter(_.getCellType == CellType.FORMULA).map(_.getCellFormula)

  /** Last row either workbook uses — the layout scan must not stop before the shorter one ends. */
  private def lastRow(a: XSSFSheet, b: XSSFSheet): Int = math.max(a.getLastRowNum, b.getLastRowNum)

  private def chartTitles(sheet: XSSFSheet): Seq[String] =
    Option(sheet.getDrawingPatriarch).toSeq.flatMap(_.getCharts.asScala).map { ch =>
      Option(ch.getTitleText).map(_.getString).getOrElse("")
    }

  // ---- fixtures --------------------------------------------------------------

  test("T5 the reference model and both inputs are present") {
    // Named explicitly: without this the rest fails as a file-not-found halfway through a
    // 20-second pipeline, which says nothing about what is wrong.
    fixtures.foreach(f => withClue(s"missing fixture $f ") { new File(f).isFile shouldBe true })
  }

  // ---- T5 ---------------------------------------------------------------------

  test("T5 the same sheets, in the same order") {
    dataSheets(produced) shouldBe dataSheets(reference)
    produced.getSheetIndex("COMPARE INFO") should be >= 0
  }

  test("T5 the same column-B layout: block titles, table headers, metric rows") {
    // Column B carries the whole vertical geometry - if an anchor moved or a row was renamed,
    // this is where it shows, with the row number that drifted.
    dataSheets(reference).foreach { name =>
      val ref = reference.getSheet(name)
      val got = produced.getSheet(name)
      def labels(s: XSSFSheet) =
        (0 to lastRow(ref, got)).map(r => r -> text(s, r, 1)).filter(_._2.nonEmpty)

      withClue(s"sheet '$name' column B ") { labels(got) shouldBe labels(ref) }
    }
  }

  test("T5 the same month index row and table headers") {
    dataSheets(reference).foreach { name =>
      val ref = reference.getSheet(name)
      val got = produced.getSheet(name)
      // Row 2 is the month index; row 3 the first table header. Both must read M1..Mn, and the
      // last column pins the horizon.
      Seq(2, 3).foreach { r =>
        withClue(s"sheet '$name' row $r ") {
          text(got, r, 2) shouldBe text(ref, r, 2)
          text(got, r, 362) shouldBe text(ref, r, 362)
        }
      }
    }
  }

  test("T5 every Evol formula points at the same two blocks") {
    // The formulas are the part most likely to break silently: an anchor change moves the block
    // but leaves a formula that still resolves - to the wrong rows.
    dataSheets(reference).foreach { name =>
      val ref = reference.getSheet(name)
      val got = produced.getSheet(name)
      def formulas(s: XSSFSheet) =
        for {
          r <- 0 to lastRow(ref, got)
          c <- 2 to 362
          f <- formula(s, r, c)
        } yield (r, c, f)

      val refF = formulas(ref)
      val gotF = formulas(got)
      withClue(s"sheet '$name': formula count ") { gotF.size shouldBe refF.size }
      val firstDiff = refF.zip(gotF).find { case (a, b) => a != b }
      withClue(s"sheet '$name': first differing formula ") { firstDiff shouldBe None }
    }
  }

  test("T5 the same values in the Updated and Previous blocks") {
    dataSheets(reference).foreach { name =>
      val ref = reference.getSheet(name)
      val got = produced.getSheet(name)
      val diffs =
        for {
          r <- 0 to lastRow(ref, got)
          c <- 2 to 362
          a = numeric(ref, r, c)
          b = numeric(got, r, c)
          if !sameNumber(a, b)
        } yield s"r$r c$c: reference $a, produced $b"

      // Report the count and the first few, never the whole vector: a one-row anchor drift makes
      // every cell below it differ, and printing 5000 of them buries the one fact that matters.
      if (diffs.nonEmpty)
        fail(s"sheet '$name': ${diffs.size} differing value(s); first: ${diffs.take(3).mkString("; ")}")
    }
  }

  test("T5 the same charts, with the same titles") {
    dataSheets(reference).foreach { name =>
      val ref = reference.getSheet(name)
      val got = produced.getSheet(name)
      withClue(s"sheet '$name' chart titles ") {
        chartTitles(got) should contain theSameElementsAs chartTitles(ref)
      }
      withClue(s"sheet '$name' chart count ") {
        chartTitles(got).size shouldBe chartTitles(ref).size
      }
    }
  }

  test("T5 the produced workbook names its own provenance on COMPARE INFO") {
    // Not part of the geometry, but the report has to say which files it judged - a report found
    // on a share is useless if it cannot name its inputs.
    val info = produced.getSheet("COMPARE INFO")
    val all = (0 to info.getLastRowNum).map(r => text(info, r, 0) + " " + text(info, r, 1)).mkString("\n")
    all should include(NewInput)
    all should include(OldInput)
    all should include("aligned by month INDEX")
  }

  /** Numeric equality that treats a blank and an absent cell alike, with FP tolerance. */
  private def sameNumber(a: Option[Double], b: Option[Double]): Boolean = (a, b) match {
    case (None, None) => true
    case (Some(x), Some(y)) =>
      val scale = math.max(math.abs(x), math.abs(y))
      math.abs(x - y) <= 1e-9 * math.max(scale, 1.0)
    case _ => false
  }
}
