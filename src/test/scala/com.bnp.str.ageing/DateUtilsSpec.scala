package com.bnp.str.ageing

import com.bnp.str.ageing.utility.DateUtils._
import com.bnp.str.ageing.utility.PrimaryConstants.PROJECTION_QUARTERS
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for the quarter arithmetic driving the projection window (no Spark).
 * `PrimaryRunner` builds the whole shock/ageing date mapping from these three functions:
 *   dateForShocks = getDatesBetween(initial, addQuarters(initial, 12))
 *   dateToAge     = getDatesBetween(ageing,  addQuarters(ageing,  12))
 *   shockToAge    = dateForShocks.zip(dateToAge).toMap
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.ageing.DateUtilsSpec
 */
class DateUtilsSpec extends AnyFunSuite with Matchers {

  // ---- addQuarters -------------------------------------------------------------------------

  test("addQuarters steps inside the same year") {
    addQuarters("2025Q1", 1) shouldBe "2025Q2"
    addQuarters("2025Q1", 3) shouldBe "2025Q4"
    addQuarters("2025Q2", 0) shouldBe "2025Q2"
  }

  test("addQuarters rolls the year over") {
    addQuarters("2025Q4", 1) shouldBe "2026Q1"
    addQuarters("2025Q2", 3) shouldBe "2026Q1"
  }

  test("addQuarters over the 12-quarter horizon lands on the same quarter, three years on") {
    addQuarters("2025Q2", PROJECTION_QUARTERS) shouldBe "2028Q2"
    addQuarters("2027Q4", PROJECTION_QUARTERS) shouldBe "2030Q4"
  }

  // ---- getDatesBetween ---------------------------------------------------------------------

  test("getDatesBetween is inclusive at both ends and contiguous") {
    getDatesBetween("2025Q3", "2026Q2") shouldBe Seq("2025Q3", "2025Q4", "2026Q1", "2026Q2")
  }

  test("getDatesBetween over the projection horizon yields 13 quarters (12 steps, both ends)") {
    val dates = getDatesBetween("2025Q2", addQuarters("2025Q2", PROJECTION_QUARTERS))
    dates.length shouldBe PROJECTION_QUARTERS + 1
    dates.head shouldBe "2025Q2"
    dates.last shouldBe "2028Q2"
  }

  test("the shock->ageing mapping is quarter-aligned and offset by a constant number of quarters") {
    val dateForShocks = getDatesBetween("2025Q2", addQuarters("2025Q2", PROJECTION_QUARTERS))
    val dateToAge     = getDatesBetween("2027Q4", addQuarters("2027Q4", PROJECTION_QUARTERS))
    val shockToAge    = dateForShocks.zip(dateToAge).toMap

    shockToAge should have size (PROJECTION_QUARTERS + 1)
    shockToAge("2025Q2") shouldBe "2027Q4"
    shockToAge("2028Q2") shouldBe "2030Q4"
    // every pair is separated by the same number of quarters (2027Q4 - 2025Q2 = 10)
    shockToAge.foreach { case (shock, aged) => quarterDiff(aged, shock) shouldBe 10 }
  }

  // ---- quarterDiff -------------------------------------------------------------------------

  test("quarterDiff counts quarters and is signed (date1 - date2)") {
    quarterDiff("2025Q3", "2025Q1") shouldBe 2
    quarterDiff("2026Q1", "2025Q1") shouldBe 4
    quarterDiff("2025Q1", "2026Q1") shouldBe -4
    quarterDiff("2025Q1", "2025Q1") shouldBe 0
  }

  test("quarterDiff drives the 'extend the input' guard: negative means the data stops short") {
    // PrimaryRunner extends the scenarios when quarterDiff(lastAvailable, lastDate) < 0
    quarterDiff("2030Q2", "2030Q4") should be < 0 // input ends before the horizon -> extend
    quarterDiff("2030Q4", "2030Q4") shouldBe 0    // exactly covers the horizon    -> no-op
  }
}
