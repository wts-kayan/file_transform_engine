package com.bnp.str.ageing.utility

import com.crealytics.spark.excel.WorkbookReader
import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, InputStreamReader, Reader}

/**
 * Shared helpers for the ageing module: a Hadoop-FS config reader plus the crealytics
 * spark-excel read/write of a single scenario sheet and the workbook sheet-name lookup.
 */
object PrimaryUtilities {

  private val log = LoggerFactory.getLogger(this.getClass)

  /** Read a config/text file through the Hadoop FileSystem so local and HDFS paths both work. */
  def getHdfsReader(filePath: String)(sc: SparkContext): Reader = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(filePath)
    new BufferedReader(new InputStreamReader(fs.open(path)))
  }

  /** List the sheet names (scenarios) of the macroeconomic workbook at `path`. */
  def sheetNames(path: String)(implicit sparkSession: SparkSession): Seq[String] = {
    log.info(s"Reading workbook sheet names from $path")
    WorkbookReader(Map("path" -> path), sparkSession.sparkContext.hadoopConfiguration).sheetNames
  }

  /** Read one scenario sheet of the macroeconomic workbook into a DataFrame. */
  def readScenario(path: String, scenario: String)(implicit sparkSession: SparkSession): DataFrame = {
    log.info(s"Reading scenario '$scenario' from $path")
    sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("header", value = true)
      .option("inferSchema", value = true)
      .option("dataAddress", s"$scenario!A1")
      .option("maxRowsInMemory", 30000)
      .load(path)
  }

  /** Append one scenario DataFrame (ordered by Date) to the output workbook sheet `scenario`. */
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
}
