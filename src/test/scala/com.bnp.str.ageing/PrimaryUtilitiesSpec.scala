package com.bnp.str.ageing

import com.bnp.str.ageing.utility.PrimaryConstants._
import com.bnp.str.ageing.utility.PrimaryUtilities
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}

/**
 * Unit tests for the output side of the module:
 *  - `writeScenariosCsv` — every scenario in ONE csv, tagged by a `scenario` column sitting
 *    immediately after the date column, ordered by date then by the caller's scenario order,
 *    landing as a single clean file (no part-*, no .crc, no leftover temp dir).
 *  - `readScenario` — the date header is normalised to the canonical casing, because the
 *    scenario sheets are hand-authored and disagree ("Date" vs "date").
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.ageing.PrimaryUtilitiesSpec
 */
class PrimaryUtilitiesSpec extends AnyFunSuite with Matchers with SparkTestSession with BeforeAndAfterAll {

  import spark.implicits._

  private val workDir = Paths.get("target", "test-out", "ageing").toAbsolutePath

  override def beforeAll(): Unit = {
    super.beforeAll()
    fs.delete(new Path(workDir.toString.replace('\\', '/')), true)
    Files.createDirectories(workDir)
  }

  private def fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  private def path(name: String): String =
    workDir.resolve(name).toString.replace('\\', '/')

  /** Reads a written csv back as (header, dataRows) of already-split cells. */
  private def readCsv(target: String): (Seq[String], Seq[Seq[String]]) = {
    val source = scala.io.Source.fromFile(target, "UTF-8")
    try {
      val lines = source.getLines().toVector
      (lines.head.split(CSV_DELIMITER, -1).toSeq, lines.tail.map(_.split(CSV_DELIMITER, -1).toSeq))
    } finally source.close()
  }

  private def centralDf: DataFrame = Seq(
    ("2025Q2", 1.0), ("2025Q1", 2.0)
  ).toDF(COL_DATE, "X")

  private def adverseDf: DataFrame = Seq(
    ("2025Q2", 3.0), ("2025Q1", 4.0)
  ).toDF(COL_DATE, "X")

  // ---- writeScenariosCsv: layout -----------------------------------------------------------

  test("the scenario column sits immediately after the date column") {
    val target = path("layout.csv")
    PrimaryUtilities.writeScenariosCsv(Seq("Central" -> centralDf, "Adverse" -> adverseDf), target)

    val (header, _) = readCsv(target)
    header shouldBe Seq(COL_DATE, COL_SCENARIO, "X")
    header(1) shouldBe COL_SCENARIO
  }

  test("all the scenarios land in one file, each row tagged with its own scenario") {
    val target = path("combined.csv")
    PrimaryUtilities.writeScenariosCsv(Seq("Central" -> centralDf, "Adverse" -> adverseDf), target)

    val (_, rows) = readCsv(target)
    rows should have size 4 // 2 dates x 2 scenarios
    rows.groupBy(_(1)).map { case (scn, r) => scn -> r.size } shouldBe Map("Central" -> 2, "Adverse" -> 2)

    // the values follow their own scenario, they are not cross-contaminated
    rows.filter(_(1) == "Central").map(r => r.head -> r(2)).toMap shouldBe
      Map("2025Q1" -> "2.0", "2025Q2" -> "1.0")
    rows.filter(_(1) == "Adverse").map(r => r.head -> r(2)).toMap shouldBe
      Map("2025Q1" -> "4.0", "2025Q2" -> "3.0")
  }

  test("rows are ordered by date, then by the scenario order the caller passed in") {
    val target = path("ordering.csv")
    PrimaryUtilities.writeScenariosCsv(Seq("Central" -> centralDf, "Adverse" -> adverseDf), target)

    val (_, rows) = readCsv(target)
    rows.map(r => (r.head, r(1))) shouldBe Seq(
      ("2025Q1", "Central"), ("2025Q1", "Adverse"),
      ("2025Q2", "Central"), ("2025Q2", "Adverse"))
  }

  test("the caller's scenario order wins over the alphabetical one") {
    val target = path("ordering-reversed.csv")
    PrimaryUtilities.writeScenariosCsv(Seq("Adverse" -> adverseDf, "Central" -> centralDf), target)

    val (_, rows) = readCsv(target)
    rows.map(_(1)) shouldBe Seq("Adverse", "Central", "Adverse", "Central")
  }

  // ---- writeScenariosCsv: the file itself --------------------------------------------------

  test("the output is a single clean file — no part-*, no .crc, no leftover temp directory") {
    val target = path("single-file.csv")
    PrimaryUtilities.writeScenariosCsv(Seq("Central" -> centralDf), target)

    val out = new Path(target)
    fs.exists(out) shouldBe true
    fs.getFileStatus(out).isFile shouldBe true
    fs.exists(new Path(s"$target.__tmp")) shouldBe false
    fs.exists(new Path(out.getParent, s".${out.getName}.crc")) shouldBe false
    fs.globStatus(new Path(s"${workDir.toString.replace('\\', '/')}/part-*")) shouldBe empty
  }

  test("writing twice overwrites in place rather than failing or appending") {
    val target = path("rerun.csv")
    PrimaryUtilities.writeScenariosCsv(Seq("Central" -> centralDf), target)
    PrimaryUtilities.writeScenariosCsv(Seq("Central" -> centralDf), target)

    val (_, rows) = readCsv(target)
    rows should have size 2 // not 4
  }

  test("writeScenariosCsv rejects an empty scenario list instead of writing an empty file") {
    an[IllegalArgumentException] should be thrownBy
      PrimaryUtilities.writeScenariosCsv(Seq.empty, path("never-written.csv"))
  }

  // ---- date-header casing ------------------------------------------------------------------

  test("the scenario column still lands second when a frame carries a differently-cased date header") {
    val oddCasing = Seq(("2025Q1", 1.0)).toDF("dAtE", "X")
    val target    = path("odd-casing.csv")
    PrimaryUtilities.writeScenariosCsv(Seq("Central" -> oddCasing), target)

    val (header, _) = readCsv(target)
    header(1) shouldBe COL_SCENARIO
    header(0).toLowerCase shouldBe COL_DATE.toLowerCase
  }

  test("readScenario normalises the date header to the canonical casing") {
    // round-trip through the real Excel reader/writer: write a sheet whose date header is
    // lower-cased, read it back, and expect the canonical name.
    val target = path("casing.xlsx")
    val sheet  = Seq(("2025Q1", 1.0), ("2025Q2", 2.0)).toDF("date", "X")
    PrimaryUtilities.writeScenario(sheet, target, "Central")

    val read = PrimaryUtilities.readScenario(target, "Central")
    read.columns.head shouldBe COL_DATE
    read.columns shouldBe Array(COL_DATE, "X")
    read.count() shouldBe 2
  }
}
