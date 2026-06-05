package com.bnp.str.tseadfwd.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Spark job that compares an EAD FWD output CSV against the target CSV and reports the
 * differences (row coverage, error stats, per-matrix max error, worst mismatches).
 *
 * Both files are `;`-delimited with a decimal comma; the join key is
 * (EAD_MATRIX_ID, SCENARIO_ID, TERM) and EAD_RA_RATE is compared numerically.
 *
 * Args (all optional):
 *   0 outputPath   default localRun/tseadfwd/output/TS_EAD_FWD_25Q4_v1_small.csv
 *   1 targetPath   default localRun/tseadfwd/target_output/TS_EAD_FWD_25Q4_v1_small.csv
 *   2 stripRateType  default true  — drop the RATE_TYPE token from the output id
 *                                    (PERIMETER_SEGMENT_TF_Q -> PERIMETER_SEGMENT_Q) so it
 *                                    aligns with a target that omits RATE_TYPE
 *   3 tol          default 1e-6   — abs-error threshold counted as "matching"
 *   4 comparePath  default localRun/tseadfwd/output/COMPARE_TS_EAD_FWD.csv — per-key result CSV
 *
 * Besides the console report, writes a single `;`-delimited, decimal-comma CSV of every joined key
 * (EAD_MATRIX_ID;SCENARIO_ID;TERM;OUTPUT;TARGET;ABS_ERROR;STATUS) where STATUS ∈
 * {MATCH, DIFF, ONLY_OUTPUT, ONLY_TARGET}.
 */
object EadFwdCompare {

  def main(args: Array[String]): Unit = {
    val outputPath = args.lift(0).getOrElse("localRun/tseadfwd/output/TS_EAD_FWD_25Q4_v1_small.csv")
    val targetPath = args.lift(1).getOrElse("localRun/tseadfwd/target_output/TS_EAD_FWD_25Q4_v1_small.csv")
    val stripRateType = args.lift(2).forall(_.toBoolean) // default true
    val tol = args.lift(3).map(_.toDouble).getOrElse(1e-6)
    val comparePath = args.lift(4).getOrElse("localRun/tseadfwd/output/COMPARE_TS_EAD_FWD.csv")

    val spark = SparkSession.builder()
      .appName("ead-fwd-compare").master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    compare(spark, outputPath, targetPath, stripRateType, tol, comparePath)
    spark.stop()
  }

  def compare(spark: SparkSession, outputPath: String, targetPath: String,
              stripRateType: Boolean, tol: Double, comparePath: String): Unit = {
    import spark.implicits._

    // decimal-comma string -> Double (null-safe; returns java.lang.Double so null is allowed)
    val toNum = udf((s: String) =>
      if (s == null || s.trim.isEmpty) null.asInstanceOf[java.lang.Double]
      else java.lang.Double.valueOf(s.trim.replace(",", ".")))
    // PERIMETER_SEGMENT_RATETYPE_(Q|Y) -> PERIMETER_SEGMENT_(Q|Y)  (drop the rate-type token)
    val stripRt = udf((id: String) => {
      val p = id.split("_")
      if (p.length >= 4) (p.dropRight(2) :+ p.last).mkString("_") else id
    })

    def load(path: String, normalize: Boolean, rateCol: String): DataFrame = {
      val raw = spark.read.option("header", "true").option("delimiter", ";").csv(path)
      val mid = if (normalize) stripRt(col("EAD_MATRIX_ID")) else col("EAD_MATRIX_ID")
      raw.select(
        mid.as("mid"),
        col("SCENARIO_ID").as("scen"),
        col("TERM").as("term"),
        toNum(col("EAD_RA_RATE")).as(rateCol))
    }

    val out = load(outputPath, stripRateType, "outRate")
    val tgt = load(targetPath, normalize = false, "tgtRate")

    val joined = out.join(tgt, Seq("mid", "scen", "term"), "full_outer")
      .withColumn("absErr", abs($"outRate" - $"tgtRate"))
      .cache()

    val outRows = out.count()
    val tgtRows = tgt.count()
    val onlyOut = joined.filter($"tgtRate".isNull).count()
    val onlyTgt = joined.filter($"outRate".isNull).count()
    val matched = joined.filter($"outRate".isNotNull && $"tgtRate".isNotNull)
    val nMatched = matched.count()
    val within = matched.filter($"absErr" <= tol).count()
    val agg = matched.agg(max("absErr").as("mx"), avg("absErr").as("mean")).first()
    val median = if (nMatched > 0) matched.stat.approxQuantile("absErr", Array(0.5), 1e-4)(0) else 0.0

    println("\n================ EAD FWD output vs target ================")
    println(f"output rows = $outRows%d   target rows = $tgtRows%d   stripRateType = $stripRateType")
    println(f"matched keys = $nMatched%d   only-in-output = $onlyOut%d   only-in-target = $onlyTgt%d")
    if (nMatched > 0) {
      println(f"abs error: max = ${agg.getAs[Double]("mx")}%.3e   mean = ${agg.getAs[Double]("mean")}%.3e   median = $median%.3e")
      println(f"within tol ($tol): ${100.0 * within / nMatched}%.1f%%  ($within / $nMatched)")
    }

    println("\n--- max abs error per matrix x scenario ---")
    matched.groupBy("mid", "scen")
      .agg(max("absErr").as("maxErr"), count("*").as("n"))
      .orderBy("mid", "scen")
      .show(100, truncate = false)

    println("--- 15 worst mismatches ---")
    matched.orderBy(desc("absErr"))
      .select("mid", "scen", "term", "outRate", "tgtRate", "absErr")
      .show(15, truncate = false)

    if (onlyOut > 0 || onlyTgt > 0) {
      println("--- sample keys present on only one side ---")
      joined.filter($"outRate".isNull || $"tgtRate".isNull)
        .select("mid", "scen", "term", "outRate", "tgtRate").show(10, truncate = false)
    }

    // ---- write the full per-key comparison to a single clean CSV (driver-side; data is small) ----
    val rows = joined.select(
      $"mid", $"scen", $"term", $"outRate", $"tgtRate", $"absErr",
      when($"outRate".isNull, lit("ONLY_TARGET"))
        .when($"tgtRate".isNull, lit("ONLY_OUTPUT"))
        .when($"absErr" <= tol, lit("MATCH"))
        .otherwise(lit("DIFF")).as("status"))
      .collect()

    // decimal-comma formatting (9 dp for rates is plenty; null/NaN -> empty)
    def num(v: Any): String = v match {
      case null                                  => ""
      case d: Double if d.isNaN || d.isInfinite  => ""
      case d: Double =>
        BigDecimal(d).setScale(12, BigDecimal.RoundingMode.HALF_UP)
          .bigDecimal.stripTrailingZeros.toPlainString.replace(".", ",")
      case o => o.toString
    }
    def termNum(s: String): Double =
      try s.replace(",", ".").toDouble catch { case _: Throwable => Double.MaxValue }

    val sorted = rows.sortBy(r => (r.getString(0), r.getString(1), termNum(r.getString(2))))
    val f = new java.io.File(comparePath)
    Option(f.getParentFile).foreach(_.mkdirs())
    val pw = new java.io.PrintWriter(f, "UTF-8")
    try {
      pw.println("EAD_MATRIX_ID;SCENARIO_ID;TERM;OUTPUT;TARGET;ABS_ERROR;STATUS")
      sorted.foreach { r =>
        pw.println(Seq(r.getString(0), r.getString(1), r.getString(2),
          num(r.get(3)), num(r.get(4)), num(r.get(5)), r.getString(6)).mkString(";"))
      }
    } finally pw.close()

    val nDiff = sorted.count(r => r.getString(6) == "DIFF")
    val nOnly = sorted.count(r => r.getString(6).startsWith("ONLY"))
    println(f"\n>>> comparison written to $comparePath (${sorted.length}%d rows: $nDiff%d DIFF, $nOnly%d only-one-side)")

    joined.unpersist()
  }
}
