package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.dataquality.{DataQualityMapper, DqConfig, DqHtmlView, DqRule}
import com.bnp.str.tseadfwd.utility.PrimaryUtilities
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Simulation of data-quality rule **R01** (all terms equal to 1, by EAD_MATRIX_ID and SCENARIO_ID)
 * on the REAL engine output.
 *
 * The production inputs never produce a full-exposure curve, so the rule reports nothing on a normal
 * run and there is nothing to look at. This app takes the produced `TS_EAD_FWD` CSV, ADDS four
 * simulated curves to it, and runs the real [[DataQualityMapper]] over the result — same code path
 * as `MainDriver`, removal included:
 *
 *   SIMU_ALLONES_TF_Q / C — every term = 1                  -> R01: flagged, removed
 *   SIMU_ALLONES_TF_Q / A — every term = 1 except the last  -> R01: NOT flagged (the boundary case)
 *   SIMU_ALLONES_TF_Y / C — every term = 1                  -> R01: flagged, removed as a SEPARATE
 *                                                              group (the _Y curve of the matrix)
 *   SIMU_NEGATIVE_TF_Q / C — a normal curve with two sub-zero terms
 *                                                           -> R02: line kept, value written as
 *                                                              computed (or as the configured
 *                                                              `replaceWith` marker)
 *
 * It writes three files next to the output (all under the gitignored `output/` directory):
 *   TS_EAD_FWD_SIMULATED.csv          the real output + the simulated lines (the "before")
 *   TS_EAD_FWD_SIMULATED_CLEANED.csv  what the main job would write (the "after")
 *   DQ_SIMULATION.html                the report naming what was removed
 *
 * Nothing production is touched: the real `TS_EAD_FWD` CSV is read, never rewritten.
 *
 * Run:
 *   mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        com.bnp.str.tseadfwd.DqSimulationApp [localRun/tseadfwd/application.conf]
 */
object DqSimulationApp {

  private val COLUMNS = Seq("EAD_MATRIX_ID", "SCENARIO_ID", "TERM", "EAD_RA_RATE", "EAD_CCF_RATE")

  def main(args: Array[String]): Unit = {
    val confPath = args.lift(0).getOrElse("localRun/tseadfwd/application.conf")

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("dq-simulation").master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val config: Config = {
      val reader = PrimaryUtilities.getHdfsReader(confPath)(spark.sparkContext)
      try ConfigFactory.parseReader(reader) finally reader.close()
    }
    val dq = DqConfig.from(config)

    val real = spark.read.option("header", "true").option("delimiter", ";").csv(dq.sourcePath)
    val outDir = dq.sourcePath.substring(0, dq.sourcePath.lastIndexOf('/'))

    // Reuse a real curve's term grid, so the simulated lines look exactly like engine output.
    val quarterlyTerms = termsOf(real, suffix = "_Q")
    val yearlyTerms    = termsOf(real, suffix = "_Y")

    val simulated: Seq[Row] =
      curve("SIMU_ALLONES_TF_Q", "C", quarterlyTerms, _ => "1") ++
        // one term below 1 — the whole group must survive
        curve("SIMU_ALLONES_TF_Q", "A", quarterlyTerms,
          i => if (i == quarterlyTerms.size - 1) "0,98" else "1") ++
        curve("SIMU_ALLONES_TF_Y", "C", yearlyTerms, _ => "1") ++
        // a plausible decaying curve that dips below zero twice: R02 territory
        curve("SIMU_NEGATIVE_TF_Q", "C", quarterlyTerms,
          i => if (i == 2) "-0,15" else if (i == 5) "-2,4" else f"0,${99 - math.min(i, 90)}%02d")

    val schema = StructType(COLUMNS.map(StructField(_, StringType)))
    val simDf = spark.createDataFrame(spark.sparkContext.parallelize(simulated, 1), schema)
    val before = real.select(COLUMNS.map(col): _*).unionByName(simDf)

    writeCsv(s"$outDir/TS_EAD_FWD_SIMULATED.csv", before)

    // The real thing: same mapper, same settings, removal included. `outputFile` names the CLEANED
    // simulation file, not the production output — the report must not claim to judge the real one.
    val outcome = new DataQualityMapper(dq)(spark)
      .apply(before, source = s"$outDir/TS_EAD_FWD_SIMULATED.csv", runId = "simulation",
        outputFile = s"$outDir/TS_EAD_FWD_SIMULATED_CLEANED.csv")

    writeCsv(s"$outDir/TS_EAD_FWD_SIMULATED_CLEANED.csv", outcome.cleaned)
    PrimaryUtilities.writeStringToHdfs(s"$outDir/DQ_SIMULATION.html",
      DqHtmlView.render(outcome.report))(spark.sparkContext)

    // ---- what happened ----
    val r01 = outcome.report.results.find(_.rule == DqRule.AllTermsEqualOne).get
    val r02 = outcome.report.results.find(_.rule == DqRule.NegativeEadRaRate).get
    println(s"""
       |=== R01 / R02 simulation ===================================================
       |real output          : ${dq.sourcePath} (${real.count()} lines)
       |simulated lines added: ${simulated.size} over 4 curves
       |
       |R01 status           : ${r01.status}, ${r01.total} group(s) flagged
       |${r01.findings.map(f => f"  ${f.matrixId}%-22s ${f.scenarioId}  ${f.detail}").mkString("\n")}
       |R01 action           : ${r01.action}
       |
       |R02 status           : ${r02.status}, ${r02.total} value(s) flagged
       |${r02.findings.map(f => f"  ${f.matrixId}%-22s ${f.scenarioId}  term ${f.term}%-6s = ${f.value}").mkString("\n")}
       |R02 action           : ${r02.action}
       |
       |lines in             : ${outcome.report.rowsIn}
       |lines out            : ${outcome.report.rowsOut}
       |
       |still present after cleaning (the boundary case, and the R02 curve):
       |${survivors(outcome.cleaned).mkString("\n")}
       |
       |R02 rows as written to the output CSV${if (dq.negativeMarker.isEmpty) " (value as computed)" else s" (marker '${dq.negativeMarker}')"}:
       |${markedRows(outcome.cleaned, dq.negativeMarker).mkString("\n")}
       |
       |files written:
       |  $outDir/TS_EAD_FWD_SIMULATED.csv          (before)
       |  $outDir/TS_EAD_FWD_SIMULATED_CLEANED.csv  (after)
       |  $outDir/DQ_SIMULATION.html                (report)
       |============================================================================
       |""".stripMargin)

    spark.stop()
  }

  /** The term grid of the first matrix carrying `suffix`, in output order. */
  private def termsOf(df: DataFrame, suffix: String): Seq[String] = {
    val matrixId = df.where(col("EAD_MATRIX_ID").endsWith(suffix))
      .select("EAD_MATRIX_ID").head().getString(0)
    df.where(col("EAD_MATRIX_ID") === matrixId && col("SCENARIO_ID") === "C")
      .select("TERM").collect().map(_.getString(0)).toSeq
  }

  /** One simulated curve: a row per term, its rate given by `rateAt(index)`. */
  private def curve(matrixId: String, scenario: String, terms: Seq[String],
                    rateAt: Int => String): Seq[Row] =
    terms.zipWithIndex.map { case (t, i) => Row(matrixId, scenario, t, rateAt(i), "") }

  /**
   * The R02 rows exactly as they land in the CSV: the configured marker when one is set, otherwise
   * the negative value as computed (the default).
   */
  private def markedRows(df: DataFrame, marker: String): Seq[String] = {
    val rate = regexp_replace(col("EAD_RA_RATE"), ",", ".").cast("double")
    val hits = if (marker.nonEmpty) df.where(col("EAD_RA_RATE") === marker) else df.where(rate < 0.0)
    hits.orderBy("EAD_MATRIX_ID", "SCENARIO_ID")
      .collect()
      .map(r => "  " + COLUMNS.indices.map(i => Option(r.getString(i)).getOrElse("")).mkString(";"))
      .toSeq
  }

  /** The simulated lines left in a frame, as `MATRIX/SCENARIO xN` summaries. */
  private def survivors(df: DataFrame): Seq[String] =
    df.where(col("EAD_MATRIX_ID").startsWith("SIMU_"))
      .groupBy("EAD_MATRIX_ID", "SCENARIO_ID").count()
      .orderBy("EAD_MATRIX_ID", "SCENARIO_ID")
      .collect()
      .map(r => f"  ${r.getString(0)}%-22s ${r.getString(1)}  ${r.getLong(2)} line(s)")
      .toSeq

  /**
   * Write the frame as ONE `;`-delimited CSV with a header. The data is a few thousand rows, so it
   * is rendered on the driver — that keeps the file byte-comparable with the engine's own output
   * instead of a Spark part-file directory.
   */
  private def writeCsv(path: String, df: DataFrame)(implicit spark: SparkSession): Unit = {
    val body = df.select(COLUMNS.map(col): _*).collect()
      .map(r => COLUMNS.indices.map(i => Option(r.getString(i)).getOrElse("")).mkString(";"))
    PrimaryUtilities.writeStringToHdfs(path,
      (COLUMNS.mkString(";") +: body).mkString("\n") + "\n")(spark.sparkContext)
  }
}
