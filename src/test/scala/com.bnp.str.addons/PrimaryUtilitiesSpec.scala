package com.bnp.str.addons

import com.bnp.str.addons.utility.PrimaryUtilities
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for `dropRowsWithoutKeyInformation` — the last mapping step before the output file is
 * generated: a row that is null/empty/blank on PERIMETER_ID, ACTION_ID (or its ADDON_ID alias) and
 * OPERAND all at once is dropped; anything keeping a value in one of them survives untouched.
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.addons.PrimaryUtilitiesSpec
 */
class PrimaryUtilitiesSpec extends AnyFunSuite with Matchers with SparkTestSession {

  import spark.implicits._

  private def keyRows(df: org.apache.spark.sql.DataFrame): Seq[(String, String, String)] =
    df.select("ADDON_ID", "PERIMETER_ID", "OPERAND")
      .collect()
      .map(r => (r.getString(0), r.getString(1), r.getString(2)))
      .toSeq

  test("drops only the rows empty on every key column") {
    val df = Seq(
      ("A1", "P1", "*", "keep: full row"),
      (null, "P2", null, "keep: perimeter only"),
      ("A3", null, null, "keep: addon only"),
      (null, null, "+", "keep: operand only"),
      (null, null, null, "drop: all null"),
      ("", "", "", "drop: all empty"),
      ("  ", "", null, "drop: blank / empty / null")
    ).toDF("ADDON_ID", "PERIMETER_ID", "OPERAND", "COMMENT")

    val cleaned = PrimaryUtilities.dropRowsWithoutKeyInformation(df)

    cleaned.count() shouldBe 4
    keyRows(cleaned) should contain theSameElementsAs Seq(
      ("A1", "P1", "*"),
      (null, "P2", null),
      ("A3", null, null),
      (null, null, "+")
    )
  }

  test("keeps every column and value of the surviving rows") {
    val df = Seq(
      ("A1", "P1", "*", "2.5"),
      (null, null, null, "9.9")
    ).toDF("ADDON_ID", "PERIMETER_ID", "OPERAND", "FACTOR")

    val cleaned = PrimaryUtilities.dropRowsWithoutKeyInformation(df)

    cleaned.columns should contain theSameElementsInOrderAs df.columns
    cleaned.collect().map(_.mkString("|")) shouldBe Array("A1|P1|*|2.5")
  }

  test("uses ACTION_ID when the query does not alias it to ADDON_ID") {
    val df = Seq(
      ("A1", null, null),
      (null, null, null)
    ).toDF("ACTION_ID", "PERIMETER_ID", "OPERAND")

    PrimaryUtilities.dropRowsWithoutKeyInformation(df).count() shouldBe 1
  }

  test("handles non-string key columns") {
    val df = Seq(
      (Some(1), "P1"),
      (None, null)
    ).toDF("ACTION_ID", "PERIMETER_ID")

    PrimaryUtilities.dropRowsWithoutKeyInformation(df).count() shouldBe 1
  }

  test("is a no-op when none of the key columns is present") {
    val df = Seq(("x", "y")).toDF("FACTOR", "VARIABLE")

    PrimaryUtilities.dropRowsWithoutKeyInformation(df).count() shouldBe 1
  }
}
