package com.bnp.str.ageing

import com.bnp.str.ageing.mapping.PrimaryView.{applyAgeing, buildShock}
import com.bnp.str.ageing.utility.PrimaryConstants.COL_DATE
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for the ageing core `PrimaryView`.
 *
 * The module ages a scenario by lifting its deviation from Central at an EARLY date and
 * re-applying it at a LATER one:
 *   shock(d)      = scenario(d) - central(d)              for d in dateForShocks
 *   aged(shockToAge(d)) = central(shockToAge(d)) + shock(d)
 * Dates with no mapped shock keep their Central value.
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.ageing.PrimaryViewSpec
 */
class PrimaryViewSpec extends AnyFunSuite with Matchers with SparkTestSession {

  import spark.implicits._

  private val tol = 1e-9

  /** Shock at 2025Qn is re-applied two years later, at 2027Qn. */
  private val dateForShocks = Seq("2025Q1", "2025Q2")
  private val shockToAge    = Map("2025Q1" -> "2027Q1", "2025Q2" -> "2027Q2")

  private def central: DataFrame = Seq(
    ("2025Q1", 1.0, 10.0),
    ("2025Q2", 2.0, 20.0),
    ("2027Q1", 3.0, 30.0),
    ("2027Q2", 4.0, 40.0)
  ).toDF(COL_DATE, "X", "Y")

  /** Deviates from Central by (+0.5, +2.0) at 2025Q1 and (+0.5, +5.0) at 2025Q2. */
  private def scenario: DataFrame = Seq(
    ("2025Q1", 1.5, 12.0),
    ("2025Q2", 2.5, 25.0),
    ("2027Q1", 9.9, 99.0), // outside dateForShocks — must be ignored
    ("2027Q2", 9.9, 99.0)
  ).toDF(COL_DATE, "X", "Y")

  /** date -> (X, Y), collected to the driver for assertions. */
  private def byDate(df: DataFrame): Map[String, (Double, Double)] =
    df.select(COL_DATE, "X", "Y").collect()
      .map(r => r.getString(0) -> (r.getDouble(1), r.getDouble(2))).toMap

  private def age(scn: DataFrame): DataFrame =
    applyAgeing(central, buildShock(scn, central, dateForShocks, shockToAge))

  // ---- buildShock --------------------------------------------------------------------------

  test("buildShock is the scenario-minus-Central difference, keyed by the date it applies to") {
    val shock = buildShock(scenario, central, dateForShocks, shockToAge)

    shock.columns should contain theSameElementsAs
      Seq("X", "Y", "date_where_the_stress_should_be_applied")

    val rows = shock.collect()
      .map(r => r.getAs[String]("date_where_the_stress_should_be_applied") ->
        (r.getAs[Double]("X"), r.getAs[Double]("Y"))).toMap

    rows.keySet shouldBe Set("2027Q1", "2027Q2")
    rows("2027Q1")._1 shouldBe (0.5 +- tol) // 1.5 - 1.0
    rows("2027Q1")._2 shouldBe (2.0 +- tol) // 12.0 - 10.0
    rows("2027Q2")._1 shouldBe (0.5 +- tol) // 2.5 - 2.0
    rows("2027Q2")._2 shouldBe (5.0 +- tol) // 25.0 - 20.0
  }

  test("buildShock drops dates outside dateForShocks — the late scenario values never leak in") {
    val shock = buildShock(scenario, central, dateForShocks, shockToAge)
    shock.count() shouldBe 2 // 2025Q1 and 2025Q2 only; the 9.9 rows at 2027Qn are filtered out
  }

  // ---- applyAgeing -------------------------------------------------------------------------

  test("applyAgeing adds the shock to Central on the mapped date, per macro variable") {
    val aged = byDate(age(scenario))

    aged("2027Q1")._1 shouldBe (3.5 +- tol) // 3.0 + 0.5
    aged("2027Q1")._2 shouldBe (32.0 +- tol) // 30.0 + 2.0
    aged("2027Q2")._1 shouldBe (4.5 +- tol) // 4.0 + 0.5
    aged("2027Q2")._2 shouldBe (45.0 +- tol) // 40.0 + 5.0
  }

  test("applyAgeing leaves dates with no mapped shock at their Central value") {
    val aged = byDate(age(scenario))

    aged("2025Q1") shouldBe ((1.0, 10.0))
    aged("2025Q2") shouldBe ((2.0, 20.0))
  }

  test("applyAgeing preserves the date grid and the column layout") {
    val aged = age(scenario)

    aged.columns shouldBe Array(COL_DATE, "X", "Y")
    aged.count() shouldBe 4
    aged.select(COL_DATE).as[String].collect().toSet shouldBe
      Set("2025Q1", "2025Q2", "2027Q1", "2027Q2")
  }

  test("a scenario identical to Central ages to Central (zero shock is a no-op)") {
    val aged = byDate(age(central))

    byDate(central).foreach { case (date, (x, y)) =>
      aged(date)._1 shouldBe (x +- tol)
      aged(date)._2 shouldBe (y +- tol)
    }
  }

  test("a negative deviation ages downwards — the shock keeps its sign") {
    val downside = Seq(
      ("2025Q1", 0.4, 7.0), // -0.6, -3.0 vs Central
      ("2025Q2", 2.0, 20.0),
      ("2027Q1", 0.0, 0.0),
      ("2027Q2", 0.0, 0.0)
    ).toDF(COL_DATE, "X", "Y")

    val aged = byDate(age(downside))
    aged("2027Q1")._1 shouldBe (2.4 +- tol)  // 3.0 - 0.6
    aged("2027Q1")._2 shouldBe (27.0 +- tol) // 30.0 - 3.0
    aged("2027Q2") shouldBe ((4.0, 40.0))    // no deviation at 2025Q2 -> Central
  }
}
