package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.reader.PrimaryReader
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.FileOutputStream
import java.nio.file.{Files, Paths}

/**
 * End-to-end test of the dynamic RA reader against REAL workbooks written for the test.
 *
 * The fixtures reproduce what the production files actually look like: a divider tab named
 * `Inputs RA ->`, an entity sheet spelled lowercase, a tab whose name matches the pattern but which
 * is not an RA table, and the same entity present in two workbooks.
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.tseadfwd.RaSheetReaderSpec
 */
class RaSheetReaderSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val dir = "target/test-ra"
  private val header = Seq("PERIMETER", "SEGMENT", "RATE_TYPE", "FWL_TYPE", "METRIC", "M1", "M2", "M3")

  /** Write a workbook: sheet name -> rows (the first row given is the header). */
  private def workbook(name: String, sheets: Seq[(String, Seq[Seq[String]])]): String = {
    Files.createDirectories(Paths.get(dir))
    val path = s"$dir/$name"
    val wb = new XSSFWorkbook()
    sheets.foreach { case (sheetName, rows) =>
      val sheet = wb.createSheet(sheetName)
      rows.zipWithIndex.foreach { case (cells, r) =>
        val row = sheet.createRow(r)
        cells.zipWithIndex.foreach { case (v, c) => row.createCell(c).setCellValue(v) }
      }
    }
    val out = new FileOutputStream(path)
    try wb.write(out) finally { out.close(); wb.close() }
    path
  }

  private def raRow(perimeter: String, metric: String, m1: String): Seq[String] =
    Seq(perimeter, "MORTGAGE", "TF", "BASELINE", metric, m1, m1, m1)

  /** A workbook shaped like the production one: divider tab, two entities, one non-RA tab. */
  private lazy val mixed: String = workbook("ra_mixed.xlsx", Seq(
    // the divider tab the real Inputs_RA.xlsx carries, with a blank first row
    "Inputs RA ->" -> Seq(Seq("", "", "")),
    "RA_BCEF" -> Seq(header, raRow("BCEF", "CRD", "-90"), raRow("BCEF", "RA STAT", "9")),
    // lowercase, as `ra_ls` is in the production conf
    "ra_bgl" -> Seq(header, raRow("BGL", "CRD", "-50"), raRow("BGL", "RA STAT", "5")),
    // matches the name pattern, is NOT an RA table
    "RA_NOTES" -> Seq(Seq("Comment", "Author"), Seq("rebased in June", "risk"))))

  /** A second workbook that also carries RA_BCEF — with different numbers, so precedence is visible. */
  private lazy val older: String = workbook("ra_older.xlsx", Seq(
    "RA_BCEF" -> Seq(header, raRow("BCEF", "CRD", "-11"), raRow("BCEF", "RA STAT", "1")),
    "RA_BNL" -> Seq(header, raRow("BNL", "CRD", "-70"), raRow("BNL", "RA STAT", "7"))))

  private def conf(body: String): Config = ConfigFactory.parseString(s"tseadfwd_app { $body }")

  private def perimetersOf(paths: String*): Seq[String] = {
    implicit val c: Config = conf(s"""RA { paths = [${paths.map(p => "\"" + p + "\"").mkString(", ")}] }""")
    new PrimaryReader().raInput.select("PERIMETER").distinct().collect().map(_.getString(0)).sorted.toSeq
  }

  test("the entity sheets are read from the workbook without being named in the configuration") {
    perimetersOf(mixed) shouldBe Seq("BCEF", "BGL")
  }

  test("a tab that is not an RA table is skipped instead of failing the run") {
    // RA_NOTES matches the name pattern; only the column check keeps it out — and the run goes on.
    implicit val c: Config = conf(s"""RA { paths = ["$mixed"] }""")
    val df = new PrimaryReader().raInput

    df.columns should contain allOf("PERIMETER", "METRIC", "M1")
    df.columns should not contain "Comment"
    df.count() shouldBe 4 // two entities x (CRD + RA STAT)
  }

  test("a new entity sheet is picked up with no code and no configuration change") {
    val grown = workbook("ra_grown.xlsx", Seq(
      "RA_BCEF" -> Seq(header, raRow("BCEF", "CRD", "-90")),
      "RA_XYZ" -> Seq(header, raRow("XYZ", "CRD", "-30")))) // the entity the business just added

    perimetersOf(grown) should contain("XYZ")
  }

  test("the same entity in two workbooks is read once, from the first path listed") {
    implicit val c: Config = conf(s"""RA { paths = ["$mixed", "$older"] }""")
    val df = new PrimaryReader().raInput

    df.select("PERIMETER").distinct().collect().map(_.getString(0)).sorted shouldBe Array("BCEF", "BGL", "BNL")
    // BCEF comes from `mixed` (M1 = -90), not from `older` (M1 = -11) — and exactly once.
    val bcefCrd = df.where("PERIMETER = 'BCEF' AND METRIC = 'CRD'").collect()
    bcefCrd.length shouldBe 1
    bcefCrd.head.getAs[String]("M1") shouldBe "-90"
  }

  test("excludeSheets keeps an entity out of the run") {
    implicit val c: Config = conf(s"""RA { paths = ["$mixed"], excludeSheets = ["ra_bgl"] }""")

    new PrimaryReader().raInput.select("PERIMETER").distinct().collect()
      .map(_.getString(0)) shouldBe Array("BCEF")
  }

  test("includeSheets loads a sheet the pattern would have left out") {
    // The keys are `includeSheets` / `excludeSheets` and NOT `include` / `exclude`, because
    // `include` is a HOCON keyword: a bare `include = [...]` fails the whole file at parse time.
    implicit val c: Config = conf(
      s"""RA { paths = ["$mixed"], sheetPattern = "^NOTHING$$", includeSheets = ["RA_BCEF"] }""")

    new PrimaryReader().raInput.select("PERIMETER").distinct().collect()
      .map(_.getString(0)) shouldBe Array("BCEF")
  }

  test("a narrower pattern loads only what it names") {
    implicit val c: Config = conf(s"""RA { paths = ["$mixed"], sheetPattern = "^RA_BCEF$$" }""")

    new PrimaryReader().raInput.select("PERIMETER").distinct().collect()
      .map(_.getString(0)) shouldBe Array("BCEF")
  }

  test("a missing workbook among several is skipped, the others still load") {
    perimetersOf(s"$dir/does_not_exist.xlsx", mixed) shouldBe Seq("BCEF", "BGL")
  }

  test("when nothing can be read the error names the workbooks and what was seen") {
    implicit val c: Config = conf(s"""RA { paths = ["$mixed"], sheetPattern = "^NOTHING_MATCHES$$" }""")

    val ex = intercept[IllegalStateException](new PrimaryReader().raInput)
    ex.getMessage should include(mixed)
    ex.getMessage should include("RA_BCEF")
  }

  test("without the RA block the per-entity blocks still drive the read") {
    // The legacy shape: one block per entity, each naming its own sheet.
    implicit val c: Config = conf(
      s"""RA_BCEF   { path = "$mixed", sheetNames = RA_BCEF }
         |RA_BGL    { path = "$mixed", sheetNames = ra_bgl }
         |RA_BNL    { path = "$older", sheetNames = RA_BNL }
         |RA_FORTIS { path = "$older", sheetNames = RA_FORTIS }
         |RA_LS     { path = "$older", sheetNames = RA_LS }""".stripMargin)

    // FORTIS / LS sheets do not exist in that workbook: skipped with a warning, as before.
    new PrimaryReader().raInput.select("PERIMETER").distinct().collect()
      .map(_.getString(0)).sorted shouldBe Array("BCEF", "BGL", "BNL")
  }
}
