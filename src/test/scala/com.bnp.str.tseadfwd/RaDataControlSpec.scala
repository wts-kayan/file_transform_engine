package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.mapping.PrimaryMapper
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/**
 * The two pre-calculation controls that dynamic RA sheets made necessary.
 *
 * With the sheets discovered rather than named one by one, two things can happen that no one typed:
 * the same entity is read twice (its series would be silently halved by the map), and an entity is
 * read that PARAMETRAGE never asks for (it produces nothing, silently). Both are now reported.
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.tseadfwd.RaDataControlSpec
 */
class RaDataControlSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val outDir = "target/test-ra-control"

  /**
   * A label-complete RA block for one perimeter: every FWL_TYPE x METRIC the engine looks for, so
   * the unrelated `RA.labels` control passes and only the check under test can speak.
   */
  private def rowsFor(perimeter: String, crd: String): Seq[(String, String, String, String, String, String, String, String)] =
    for {
      fwl <- Seq("BASELINE", "STRESS (+)", "STRESS (-)")
      metric <- Seq("CRD", "RA STAT", "RA FI", "RE")
    } yield {
      val v = if (metric == "CRD") crd else "1"
      (perimeter, "MORTGAGE", "TF", fwl, metric, v, v, v)
    }

  private def ra(rows: Seq[(String, String, String, String, String, String, String, String)]): DataFrame = {
    import spark.implicits._
    rows.toDF("PERIMETER", "SEGMENT", "RATE_TYPE", "FWL_TYPE", "METRIC", "M1", "M2", "M3")
  }

  private def parametrage(perimeters: String*): DataFrame = {
    import spark.implicits._
    perimeters.map(p => (p, "MORTGAGE", "TF", "NO", "", "NO", "", "3Y"))
      .toDF("PERIMETER", "SEGMENT", "RATE_TYPE", "AGGREGATION", "AGGREGATED_SEGMENT_NAME",
        "FWL_TO_BE_APPLIED", "MACRO_VARIABLE", "PROJECTION_HORIZON")
  }

  private def scenario: DataFrame = {
    import spark.implicits._
    Seq(("2025Q4", "Central")).toDF("Date", "scenario")
  }

  private def conf(strict: Boolean = true): Config = ConfigFactory.parseString(
    s"""tseadfwd_app {
       |  parameters { as_of_date_quarter = "2025Q4", validation { strict = $strict } }
       |  TS_EAD_FWD { tmpPath = "$outDir", tableName = "T" }
       |}""".stripMargin)

  /** The DATA CONTROL report the mapper writes next to the output. */
  private def controlReport: String =
    new String(Files.readAllBytes(Paths.get(outDir, "DATA_CONTROL_T.csv")), StandardCharsets.UTF_8)

  private def run(raDf: DataFrame, paramDf: DataFrame, strict: Boolean = true): DataFrame = {
    implicit val c: Config = conf(strict)
    new PrimaryMapper(raDf, scenario, paramDf, "TS_EAD_FWD").getDataFrame
  }

  test("the same series read twice FAILS the run instead of being halved in silence") {
    // What a double-loaded sheet looks like once unioned: the identical keys, twice, with different
    // numbers. `collectRa` builds a Map, so one of each pair would simply disappear — and the
    // survivor is whichever came last.
    val duplicated = ra(rowsFor("BCEF", "-90") ++ rowsFor("BCEF", "-11"))

    val ex = intercept[IllegalStateException](run(duplicated, parametrage("BCEF")))

    ex.getMessage should include("RA.duplicateKeys")
    ex.getMessage should include("SILENTLY dropped")
  }

  test("the duplicate report names the offending key") {
    intercept[IllegalStateException](
      run(ra(rowsFor("BCEF", "-90") ++ rowsFor("BCEF", "-11")), parametrage("BCEF")))

    controlReport should include("RA.duplicateKeys")
    controlReport should include("BCEF|MORTGAGE|TF|BASELINE|CRD x2")
  }

  test("a clean input passes the duplicate control") {
    run(ra(rowsFor("BCEF", "-90")), parametrage("BCEF"))

    controlReport should include("RA.duplicateKeys")
    controlReport should include("no RA series appears twice")
  }

  // ---- blank lines in the sheet ------------------------------------------------

  /** A row blank across every key column - what Excel leaves below a table. */
  private def blankRow(months: String = "") =
    ("", "", "", "", "", months, months, months)

  test("blank rows do not FAIL the run as if an entity had been loaded twice") {
    // Before the guard, every blank row canon()ed to the same empty key, so N of them looked like
    // one series read N times and RA.duplicateKeys aborted the run - pointing at a double-loaded
    // sheet, which is not what is wrong.
    val withBlanks = ra(rowsFor("BCEF", "-90") ++ Seq.fill(6)(blankRow()))

    val df = run(withBlanks, parametrage("BCEF"))

    df.count() should be > 0L
    controlReport should include("no RA series appears twice")
  }

  test("the dropped blank rows are reported, not silently swallowed") {
    run(ra(rowsFor("BCEF", "-90") ++ Seq.fill(6)(blankRow())), parametrage("BCEF"))

    controlReport should include("RA.blankRows")
    controlReport should include("6 row(s) carry no key at all")
  }

  test("blank rows are not counted as parsed series") {
    run(ra(rowsFor("BCEF", "-90") ++ Seq.fill(6)(blankRow())), parametrage("BCEF"))

    // 3 FWL types x 4 metrics, and not one more
    controlReport should include("12 RA series parsed")
  }

  test("a row carrying only strippable punctuation counts as blank") {
    // trim() would call this row non-empty; canon() reduces it to "" and it would then collide with
    // every other key-less row. The guard has to use the same normalisation the key does.
    val punctuation = (".", "/", "()", "", "", "1", "1", "1")
    run(ra(rowsFor("BCEF", "-90") :+ punctuation), parametrage("BCEF"))

    controlReport should include("1 row(s) carry no key at all")
    controlReport should include("no RA series appears twice")
  }

  test("a bare '-' is a key, not a blank — canon keeps the sign") {
    // canon() strips everything except letters, digits, '+' and '-', because the signs are what
    // separate STRESS (+) from STRESS (-). So a key column holding only '-' canons to "-", which is
    // a real (if odd) key: the row is malformed, not empty, and must not be silently dropped.
    run(ra(rowsFor("BCEF", "-90") :+ (("-", "-", "-", "-", "-", "1", "1", "1"))), parametrage("BCEF"))

    controlReport should include("no key-less rows in the RA input")
    controlReport should include("13 RA series parsed")
  }

  test("a clean input says so rather than staying silent") {
    run(ra(rowsFor("BCEF", "-90")), parametrage("BCEF"))
    controlReport should include("no key-less rows in the RA input")
  }

  test("a partially blank key still counts as a key, and is named readably when duplicated") {
    // Only SOME key columns empty: the row is not blank, it is malformed, and two of them still
    // collide. The report has to make the gaps visible - "BCEF||TF||CRD" reads as nothing.
    val partial = ("BCEF", "", "TF", "", "CRD", "1", "1", "1")
    intercept[IllegalStateException](
      run(ra(rowsFor("BCEF", "-90") ++ Seq(partial, partial)), parametrage("BCEF")))

    controlReport should include("BCEF|<blank>|TF|<blank>|CRD x2")
  }

  test("an entity nobody configured is reported, and does not stop the run") {
    // The likely shape of "the business added a sheet and the run ignored it": XYZ loads fine, but
    // PARAMETRAGE has no row for it, so it produces nothing. A WARN — the run is still valid.
    val df = run(ra(rowsFor("BCEF", "-90") ++ rowsFor("XYZ", "-30")), parametrage("BCEF"))

    df.count() should be > 0L
    controlReport should include("RA.perimeters")
    controlReport should include("absent from PARAMETRAGE")
    controlReport should include("XYZ")
    // and the output really does carry only the configured entity
    df.select("EAD_MATRIX_ID").distinct().collect().map(_.getString(0))
      .foreach(_ should startWith("BCEF"))
  }

  test("every RA perimeter configured — the control passes and says so") {
    run(ra(rowsFor("BCEF", "-90")), parametrage("BCEF"))

    controlReport should include("every RA perimeter is referenced by PARAMETRAGE")
  }
}
