package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.mapping.{PrimaryMapper, PrimaryView}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

/**
 * Reproduction guard for the PERIMETER-key collision: two perimeters (BCEF, BGL) each carry a
 * MORTGAGE/TF/BASELINE row with DIFFERENT CRD. Before the fix the (SEGMENT,RATE_TYPE,FWL,METRIC)
 * key collided and `.toMap` kept the last one, so BCEF_MORTGAGE used BGL's CRD. With PERIMETER in
 * the key, each matrix must read its own perimeter's CRD.
 */
object PerimeterKeyCheck {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("perimeter-key-check").master("local[2]")
      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // raInput: two perimeters, same MORTGAGE segment, DIFFERENT baseline CRD / RA STAT.
    val ra = Seq(
      ("BCEF", "MORTGAGE", "TF", "BASELINE", "CRD",     "-90", "-90", "-90"),
      ("BCEF", "MORTGAGE", "TF", "BASELINE", "RA STAT", "9",   "9",   "9"),
      ("BGL",  "MORTGAGE", "TF", "BASELINE", "CRD",     "-50", "-50", "-50"),
      ("BGL",  "MORTGAGE", "TF", "BASELINE", "RA STAT", "5",   "5",   "5")
    ).toDF("PERIMETER", "SEGMENT", "RATE_TYPE", "FWL_TYPE", "METRIC", "M1", "M2", "M3")

    // PARAMETRAGE: a MORTGAGE matrix per perimeter, FWL=NO (stat-only, no shock/scenario needed).
    val parametrage = Seq(
      ("BCEF", "MORTGAGE", "TF", "NO", "", "NO", ""),
      ("BGL",  "MORTGAGE", "TF", "NO", "", "NO", "")
    ).toDF("PERIMETER", "SEGMENT", "RATE_TYPE", "AGGREGATION", "AGGREGATED_SEGMENT_NAME",
      "FWL_TO_BE_APPLIED", "MACRO_VARIABLE")

    val scenario = Seq(("2025Q4", "Central")).toDF("Date", "scenario")

    implicit val config: Config = ConfigFactory.parseString("tseadfwd_app{}")
    val mapper = new PrimaryMapper(ra, scenario, parametrage, "TS_EAD_FWD")
    val rows = mapper.term0AnalysisRows(Seq(0.0), PrimaryView.Quarterly)

    def crdOf(mid: String): Double =
      rows.find(r => r.matrixId == mid && r.scenarioCode == "C").map(_.crdAgg).getOrElse(Double.NaN)

    val bcef = crdOf("BCEF_MORTGAGE_TF_Q") // CRD_Q1 = mean(-90,-90,-90) = -90  (its own)
    val bgl  = crdOf("BGL_MORTGAGE_TF_Q")  // CRD_Q1 = mean(-50,-50,-50) = -50  (its own)
    println(f"BCEF_MORTGAGE CRD_Q1 = $bcef%.3f  (expect -90.000)")
    println(f"BGL_MORTGAGE  CRD_Q1 = $bgl%.3f  (expect -50.000)")
    val ok = math.abs(bcef + 90.0) < 1e-9 && math.abs(bgl + 50.0) < 1e-9
    println(if (ok) ">>> PASS: each perimeter reads its OWN MORTGAGE row (no key collision)."
            else    ">>> FAIL: perimeter key collision — a matrix read another entity's MORTGAGE row.")
    spark.stop()
    if (!ok) sys.exit(1)
  }
}
