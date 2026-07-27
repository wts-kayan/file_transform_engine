package com.bnp.str.ageing.utility

import com.crealytics.spark.excel.WorkbookReader
import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
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
    sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("header", value = true)
      .option("inferSchema", value = true)
      .option("dataAddress", s"$scenario!A1")
      .option("maxRowsInMemory", 30000)
      .load(path)
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

  def writeScenarioCsv(df: DataFrame, targetFile: String)(implicit sparkSession: SparkSession): Unit = {
    log.info(s"Writing CSV $targetFile")
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val tmpDir = s"$targetFile.__tmp"

    df
      .orderBy(PrimaryConstants.COL_DATE)
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
