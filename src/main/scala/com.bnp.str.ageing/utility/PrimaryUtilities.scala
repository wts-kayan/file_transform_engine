package com.bnp.str.ageing.utility

import com.crealytics.spark.excel.WorkbookReader
import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, InputStreamReader, Reader}

object PrimaryUtilities {

  private val log = LoggerFactory.getLogger(this.getClass)

  def getHdfsReader(filePath: String)(sc: SparkContext): Reader = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(filePath)
    new BufferedReader(new InputStreamReader(fs.open(path)))
  }

  def sheetNames(path: String)(implicit sparkSession: SparkSession): Seq[String] = {
    log.info(s"Reading workbook sheet names from $path")
    WorkbookReader(Map("path" -> path), sparkSession.sparkContext.hadoopConfiguration).sheetNames
  }

  def readScenario(path: String, scenario: String)(implicit sparkSession: SparkSession): DataFrame = {
    log.info(s"Reading scenario '$scenario' from $path")
    val df = sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("header", value = true)
      .option("inferSchema", value = true)
      .option("dataAddress", s"$scenario!A1")
      .option("maxRowsInMemory", 30000)
      .load(path)
    normalizeDateColumn(df, scenario)
  }

  /**
   * Sheets are authored by hand, so the date header comes in various casings ("date", "DATE", …).
   * Renaming it to the canonical `Date` keeps the ageing logic and the output headers stable.
   */
  private def normalizeDateColumn(df: DataFrame, scenario: String): DataFrame =
    df.columns.find(c => c.equalsIgnoreCase(PrimaryConstants.COL_DATE) && c != PrimaryConstants.COL_DATE) match {
      case Some(actual) =>
        log.info(s"Sheet '$scenario': renaming column '$actual' to '${PrimaryConstants.COL_DATE}'")
        df.withColumnRenamed(actual, PrimaryConstants.COL_DATE)
      case None => df
    }

  def writeScenario(df: DataFrame, path: String, scenario: String)(implicit sparkSession: SparkSession): Unit = {
    log.info(s"Writing scenario '$scenario' to $path")
    df
      .orderBy(PrimaryConstants.COL_DATE)
      .write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", s"$scenario!A1")
      .option("header", "true")
      .mode("append")
      .save(path)
  }

  /**
   * Writes every scenario into a single CSV, tagged by a `scenario` column placed right after `date`.
   * Rows are ordered by date, then by the scenario order given by the caller.
   */
  def writeScenariosCsv(scenarios: Seq[(String, DataFrame)], targetFile: String)
                       (implicit sparkSession: SparkSession): Unit = {
    require(scenarios.nonEmpty, s"No scenario to write to '$targetFile'")

    val scenarioRank = "__scenario_rank"
    val combined = scenarios.zipWithIndex
      .map { case ((scenario, df), rank) => withScenarioColumn(df, scenario).withColumn(scenarioRank, lit(rank)) }
      .reduce(_ unionByName _)

    writeSingleCsv(
      combined.orderBy(PrimaryConstants.COL_DATE, scenarioRank).drop(scenarioRank),
      targetFile)
  }

  /** Inserts a constant `scenario` column just after the `date` column (first column if `date` is absent). */
  private def withScenarioColumn(df: DataFrame, scenario: String): DataFrame = {
    val scenarioCol = lit(scenario).as(PrimaryConstants.COL_SCENARIO)
    val columns = df.columns.toSeq
    val dateIndex = columns.indexWhere(_.equalsIgnoreCase(PrimaryConstants.COL_DATE))
    val selected =
      if (dateIndex < 0) scenarioCol +: columns.map(df.col)
      else columns.take(dateIndex + 1).map(df.col) ++ Seq(scenarioCol) ++ columns.drop(dateIndex + 1).map(df.col)
    df.select(selected: _*)
  }

  private def writeSingleCsv(df: DataFrame, targetFile: String)(implicit sparkSession: SparkSession): Unit = {
    log.info(s"Writing CSV $targetFile")
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val tmpDir = s"$targetFile.__tmp"

    df
      .coalesce(1)
      .write
      .format(PrimaryConstants.CSV_EXTENSION)
      .option("header", "true")
      .option("delimiter", PrimaryConstants.CSV_DELIMITER)
      .option("emptyValue", "")
      .mode("overwrite")
      .save(tmpDir)

    val parts = Option(fs.globStatus(new Path(s"$tmpDir/part-*"))).getOrElse(Array.empty)
    if (parts.isEmpty)
      throw new IllegalStateException(s"No part-* file produced under '$tmpDir' for '$targetFile'")

    val target = new Path(targetFile)
    Option(target.getParent).foreach(p => fs.mkdirs(p))
    if (fs.exists(target)) fs.delete(target, false)
    fs.rename(parts(0).getPath, target)

    fs.delete(new Path(tmpDir), true)
    val crc = new Path(target.getParent, s".${target.getName}.crc")
    if (fs.exists(crc)) fs.delete(crc, false)
  }
}
