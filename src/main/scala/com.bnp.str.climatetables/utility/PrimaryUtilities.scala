package com.bnp.str.climatetables.utility

import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, lit, regexp_replace}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, InputStreamReader, Reader}


object PrimaryUtilities extends SchemaSelector {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Reads a DataFrame from an Excel file.
   *
   * @param path          The path to the Excel file.
   * @param sheetName     The name of the sheet to read.
   * @param schema        The schema to apply to the DataFrame.
   * @param sparkSession  The SparkSession to use.
   * @return The DataFrame read from the Excel file.
   */
  def readDataFrameFromExcel(path: String,
                             sheetName: String,
                             schema: StructType)(implicit sparkSession: SparkSession): DataFrame = {


    log.info(s"Reading Excel file from path: $path, sheet: $sheetName")

    val rawdf = sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", sheetName) // Required
      .option("path", path)
      .option("location", path)
      .option("header", "true") // Required
      .option("setErrorCellsToFallbackValues", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      .option("addColorColumns", "false")
      //.schema(schema)
      .option("maxRowsInMemory", 200)
      // Optional, default None. If set, uses a streaming reader which can help with big files
      .load()

    //TODO ?
    rawdf.withColumn("sheetName", lit(sheetName))

  }

  def readDataFrameFromExcel(fileName: String, sheetName: String)(implicit sparkSession: SparkSession, config: Config): DataFrame = {

    val inputConfig = config.getConfig(s"${PrimaryConstants.APP_CONF}.$fileName")
    val path = inputConfig.getString("path")
    val sheetNamesConfig = inputConfig.getConfig("sheetNames")
    val sheetNameValue = sheetNamesConfig.getString(sheetName)

    log.info(s"Reading $fileName Excel file from path: $path, sheet: $sheetNameValue")

    val rawDf = sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("dataAddress", s"'$sheetNameValue'!A1")
      .option("path", path)
      .option("location", path)
      .option("header", "true")
      .option("setErrorCellsToFallbackValues", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      .option("addColorColumns", "false")
      //.schema(schema)
      .option("maxRowsInMemory", 200)
      // Optional, default None. If set, uses a streaming reader which can help with big files
      .load()

    //rawdf.show()
    //log.info(s"Reading count ${rawdf.count()}")
    //rawdf.printSchema()
    rawDf
  }

  def readDataFrameFromExcel(fileName: String)(implicit sparkSession: SparkSession, config: Config): DataFrame = {

    val inputConfig = config.getConfig(s"${PrimaryConstants.APP_CONF}.$fileName")
    val path = inputConfig.getString("path")

    log.info(s"Reading $fileName Excel file from path: $path")

    val rawDf = sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("path", path)
      .option("location", path)
      .option("header", "true")
      .option("setErrorCellsToFallbackValues", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      .option("addColorColumns", "false")
      //.schema(schema)
      //.option("maxRowsInMemory", 200) // Optional, default None. If set, uses a streaming reader which can help with big files
      .load()

    //rawdf.show()
    //log.info(s"Reading count ${rawdf.count()}")
    //rawdf.printSchema()
    rawDf
  }

  def readAndExtractColumns(fileName: String)(implicit sparkSession: SparkSession, config: Config): (Seq[String], Seq[String]) = {
    import sparkSession.implicits._

    val inputConfig = config.getConfig(s"${PrimaryConstants.APP_CONF}.$fileName")
    val path = inputConfig.getString("path")

    log.info(s"Reading $fileName txt file from path: $path")
    val df = sparkSession.read
      .text(path)
      .withColumnRenamed("value", "column_name")

    // Separate columns into fem and tiers based on prefix
    val femColumns = df.filter(!col("column_name").startsWith("tiers_").startsWith("terms_"))
      .select("column_name")
      .as[String]
      .collect()
      .toSeq

    val tiersColumns = df.filter(col("column_name").startsWith("tiers_"))
      .select(regexp_replace(col("column_name"), "^tiers_", "").as("column_name"))
      .as[String]
      .collect()
      .toSeq

    (femColumns, tiersColumns)
  }

  def readDataFrameFromFolder(folderName: String)(implicit sparkSession: SparkSession, config: Config): DataFrame = {

    val inputConfig = config.getConfig(s"${PrimaryConstants.APP_CONF}.$folderName")
    val path = inputConfig.getString("path")

    log.info(s"Reading $folderName folder from path: $path")

    val rawdf = sparkSession.read.format("orc").load(path)

    rawdf
  }


  def readDataFrameFromCsv(fileName: String)(implicit sparkSession: SparkSession, config: Config): DataFrame = {

    val inputConfig = config.getConfig(s"${PrimaryConstants.APP_CONF}.$fileName")
    val path = inputConfig.getString("path")
    val format = "csv"
    val delimiter = ";"

    log.info(s"Reading $fileName CSV file from path: $path")

    val df = sparkSession
      .read
      .format(format)
      //.schema(schema)
      .option("header", "true")
      .option("delimiter", delimiter)
      .load(path)

    //df.printSchema()

    df
  }

  def readDataFrame(tableName: String)(implicit sparkSession: SparkSession, conf: Config): DataFrame = {

    val path = conf.getString(s"${PrimaryConstants.APP_CONF}.${tableName.toLowerCase}.path")
    val format = conf.getString(s"${PrimaryConstants.APP_CONF}.${tableName.toLowerCase}.format")
    val schema = getSchema(tableName)

    val df = sparkSession
      .read
      .format(format)
      .schema(schema)
      .load(path)
      .selectExpr(ColumnSelector.getColumnSequence(tableName): _*)
    //df.printSchema()
    df
  }

  def selectColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
    val existingColumns = columns.filter(col => df.columns.contains(col))
    val nonExistentColumns = columns.diff(existingColumns)

    log.info(s"Columns not found in DataFrame and will be ignored: ${nonExistentColumns.mkString(", ")}")

    var selectDf = df.select(existingColumns.map(col): _*)

    selectDf = selectDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    selectDf.count()

    selectDf
  }

  def selectColumnsDebug(df: DataFrame, columns: Seq[String]): DataFrame = {
    val existingColumns = columns.filter(col => df.columns.contains(col))
    val nonExistentColumns = columns.diff(existingColumns)

    log.info(s"Columns not found in DataFrame and will be ignored: ${nonExistentColumns.mkString(", ")}")

    var selectDf = df.select(existingColumns.map(col): _*).filter(col("id_technique") === "1005022358652_35_Unknown")

    selectDf = selectDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    selectDf.count()

    selectDf
  }

  def getHdfsReader(filePath: String)(sc: SparkContext): Reader = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(filePath)
    new BufferedReader(new InputStreamReader(fs.open(path)))
  }

  def applySchema(df: DataFrame, schema: StructType): DataFrame = {

    val renamedDF = df.toDF(schema.fieldNames: _*)
    renamedDF.selectExpr(schema.fields.map(f => s"cast(${f.name} as ${f.dataType.sql}) as ${f.name}"): _*)
  }

  def replaceCommaInColunms(df: DataFrame,
                            columns: Seq[String]): DataFrame = {

    columns.foldLeft(df) { case (acc, colName) =>
      acc.withColumn(colName, regexp_replace(col(colName), ",", "."))
    }

  }


  def writeDataframe(dataframe: DataFrame,
                     directory: String,
                     tableName: String)(implicit sparkSession: SparkSession, conf: Config): Unit = {

    val outConfig = conf.getConfig(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.OUTPUT_TABLES}")
    val format = outConfig.getString("format")
    val mode = outConfig.getString("mode")
    val path = s"${directory}/${tableName}"


    log.info(s"Writing all files to $path (format: $format) (overwrite: $mode)")

    val dfToWrite = if (format.equalsIgnoreCase("csv")) {
      // Find columns whose datatype is ArrayType(StringType)
      val arrayStringCols = dataframe.schema.fields.collect {
        case f if f.dataType.isInstanceOf[org.apache.spark.sql.types.ArrayType] &&
          f.dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType
            .isInstanceOf[org.apache.spark.sql.types.StringType] => f.name
      }

      // Cast each of those columns to String
      arrayStringCols.foldLeft(dataframe) { (df, colName) =>
        df.withColumn(colName, col(colName).cast("string"))
      }
    } else {
      dataframe
    }

    dfToWrite
      .write
      .format(format)
      .option("compression", "snappy")
      .option("header", "true")
      .option("delimiter", ";")
      .mode(mode)
      .save(path)
  }

  def copyFilesWithOverwrite(
                              sourcePath: String,
                              targetPath: String,
                              overwrite: Boolean = true
                            )(sc: SparkContext): Unit = {
    log.info(s"Copying all files from $sourcePath to $targetPath (overwrite: $overwrite)")
    // Get Hadoop filesystem
    val fs = FileSystem.get(sc.hadoopConfiguration)

    // Construct full source and target paths
    val sourceDir = new Path(sourcePath)
    val targetDir = new Path(targetPath)

    // Check if source directory exists
    if (!fs.exists(sourceDir)) {
      log.error(s"Source directory does not exist: $sourceDir")
      throw new IllegalArgumentException(s"Source directory does not exist: $sourceDir")
    }

    // Create target directory if it doesn't exist
    if (!fs.exists(targetDir)) {
      log.info(s"Creating target directory: $targetPath")
      fs.mkdirs(targetDir)
    }

    // List all files in source directory
    val files = fs.listStatus(sourceDir)
      .filter(_.isFile)
      .map(_.getPath)

    // Process each file
    files.foreach { sourceFile =>
      val targetFile = new Path(targetDir, sourceFile.getName)

      // Delete target file if it exists and overwrite is true
      if (overwrite && fs.exists(targetFile)) {
        log.info(s"Deleting existing target file: $targetFile")
        fs.delete(targetFile, false)
      }

      // Copy file
      log.info(s"Copying file from $sourceFile to $targetFile")
      org.apache.hadoop.fs.FileUtil.copy(
        sourceFile.getFileSystem(sc.hadoopConfiguration),
        sourceFile,
        targetFile.getFileSystem(sc.hadoopConfiguration),
        targetFile,
        false,
        sc.hadoopConfiguration
      )

      log.info(s"Successfully copied file to $targetFile")
    }
  }
}
