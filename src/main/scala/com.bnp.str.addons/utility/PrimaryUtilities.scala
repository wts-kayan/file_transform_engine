package com.bnp.str.addons.utility

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, lit, regexp_replace}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.slf4j.LoggerFactory
import com.typesafe.config.Config
import java.io.{BufferedReader, InputStreamReader, Reader}


object PrimaryUtilities extends SchemaSelector {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * A crealytics spark-excel reader pre-configured with the options shared by every addons read.
   * The caller still adds the cell addressing (`sheetName` or `dataAddress`) before `.load()`.
   *
   * Locale note: with `inferSchema=false` POI's `DataFormatter` renders each cell using the JVM
   * default locale, so on a non-English host decimals come back with a comma (`-0,05`) and large
   * numbers with grouping. The obvious lever, `usePlainNumberFormat=true`, is deliberately NOT set
   * here: it renders EVERY number via BigDecimal.toPlainString, so whole numbers become `"1.0"`
   * instead of `"1"` — which breaks the downstream string filter `ACTIVE_IND === "1"` and would
   * change integer output columns (RANK/ACTIVATE) to `5.0`/`1.0`. Locale-dependent DECIMAL columns
   * are instead normalized explicitly with [[replaceCommaInColunms]] (see the 5-arg reader), which
   * fixes the comma without touching integer rendering.
   */
  private def excelReader(path: String)(implicit sparkSession: SparkSession): DataFrameReader =
    sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("path", path)
      .option("location", path)
      .option("header", "true")
      .option("setErrorCellsToFallbackValues", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      .option("addColorColumns", "false")

  /**
   * Reads a DataFrame from an Excel file.
   *
   * @param path The path to the Excel file.
   * @param sheetName The name of the sheet to read.
   * @param schema The schema to apply to the DataFrame.
   * @param sparkSession The SparkSession to use.
   * @return The DataFrame read from the Excel file.
   */
  def readDataFrameFromExcel(path: String,
                             sheetName: String,
                             schema: StructType,
                             columnsToFix: Seq[String],
                             isColumnsToFix: Boolean)(implicit sparkSession: SparkSession): DataFrame = {

    log.info(s"Reading Excel file from path: $path, sheet: $sheetName")

    val rawdf = excelReader(path)
      .option("sheetName", sheetName)
      .load()

    log.info(s"DataFrame with added sheet name column: $sheetName")
    if (isColumnsToFix) {
      val df = PrimaryUtilities.replaceCommaInColunms(rawdf, columnsToFix)
      applySchema(df, schema).withColumn("sheetName", lit(sheetName))
    } else {
      rawdf.withColumn("sheetName", lit(sheetName))
    }
  }

  def readDataFrameFromExcel(tableName: String)(implicit sparkSession: SparkSession, config: Config): DataFrame = {
    import scala.collection.JavaConverters._

    val addonInputConfig = config.getConfig(s"${PrimaryConstants.APP_CONF}.${tableName}")
    val path = addonInputConfig.getString("path")
    val sheetName = addonInputConfig.getString("sheetNames")
    log.info(s"Reading $tableName Excel file from path: $path, sheet: $sheetName")

    val df = excelReader(path)
      .option("dataAddress", s"'$sheetName'!A1")
      .load()

    // Locale-safe decimals: normalize the configured numeric columns (comma decimal + grouping ->
    // canonical '.' with no grouping) so the same workbook yields identical values on en-US and
    // fr-FR hosts. Only listed columns are touched, so integer/text columns are left untouched.
    val columnsToFix =
      if (addonInputConfig.hasPath("columnsToFix"))
        addonInputConfig.getStringList("columnsToFix").asScala.toSeq
      else Seq.empty[String]

    if (columnsToFix.nonEmpty) replaceCommaInColunms(df, columnsToFix) else df
  }

  def readDataFrameFromCsv(tableName: String)(implicit sparkSession: SparkSession, conf: Config) : DataFrame = {

    val tablenameConfig = conf.getConfig(s"${PrimaryConstants.APP_CONF}.$tableName")


    val path = tablenameConfig.getString(s"path")
    val format = tablenameConfig.getString(s"format")
    val header = tablenameConfig.getString(s"header")
    val delimiter = tablenameConfig.getString(s"delimiter")
    val schema = getSchema(tableName)

    sparkSession
      .read
      .format(format)
      .schema(schema)
      .option("header", header)
      .option("delimiter", delimiter)
      .load(path)
  }

  def readDataFrame(tableName: String)(implicit sparkSession: SparkSession, conf: Config) : DataFrame = {

    val path = conf.getString(s"${PrimaryConstants.APP_CONF}.${tableName.toLowerCase}.path")
    val format = conf.getString(s"${PrimaryConstants.APP_CONF}.${tableName.toLowerCase}.format")
    val schema = getSchema(tableName)

    sparkSession
      .read
      .format(format)
      .schema(schema)
      .load(path)
      .selectExpr(ColumnSelector.getColumnSequence(tableName):_*)
  }

  def getHdfsReader(filePath: String)(sc: SparkContext): Reader = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(filePath)
    new BufferedReader(new InputStreamReader(fs.open(path)))
  }

  def applySchema(df: DataFrame, schema: StructType): DataFrame = {

    val renamedDF = df.toDF(schema.fieldNames:_*)
    renamedDF.selectExpr(schema.fields.map(f => s"cast(${f.name} as ${f.dataType.sql}) as ${f.name}"): _*)
  }

  /**
   * Normalize locale-formatted numeric text in the given columns to a canonical form: strip
   * grouping separators (space / non-breaking space U+00A0 / narrow NBSP U+202F) then turn the
   * decimal comma into a dot — so "-0,05" and "1 234,56" become "-0.05" and "1234.56". Columns not
   * listed are untouched, so integer and text columns keep their original rendering. A no-op on
   * already-canonical values (e.g. en-US "-0.05"), so it is safe to apply unconditionally.
   */
  def replaceCommaInColunms(df: DataFrame,
                            columns: Seq[String]): DataFrame = {

    columns.foldLeft(df) { case (acc, colName) =>
      val degrouped = regexp_replace(col(colName), "[\\s\\u00A0\\u202F]", "")
      acc.withColumn(colName, regexp_replace(degrouped, ",", "."))
    }

  }


  def writeDataframe(dataframe: DataFrame,
                     partitionColumns: String,
                     tableName: String,
                     mode: String,
                     format:String="parquet",
                     numPartition: Int,
                     path: String): Unit = {


    // Split the partitionColumns string into a sequence of columns
    val partitionColumnSeq: Seq[String] = if (partitionColumns.trim.nonEmpty) {
      partitionColumns.split(",").map(_.trim)
    } else {
      Seq.empty[String]
    }

    dataframe
      .coalesce(numPartition)
      .write
      .format(format)
      .partitionBy(partitionColumnSeq: _*)
      .mode(mode)
      .save(s"$path/$tableName")

  }

  def copyFilesWithOverwrite(
                              sourcePath: String,
                              targetPath: String,
                              fileName: String,
                              overwrite: Boolean = true
                            )(sc: SparkContext): Unit = {
    log.info(s"Copying file $fileName from $sourcePath to $targetPath (overwrite: $overwrite)")
    // Get Hadoop filesystem
    val fs = FileSystem.get(sc.hadoopConfiguration)

    // Construct full source and target paths
    val sourceFile = new Path(sourcePath, fileName)
    val targetFile = new Path(targetPath, fileName)

    // Check if source file exists
    if (!fs.exists(sourceFile)) {
      log.error(s"Source file does not exist: $sourceFile")
      throw new IllegalArgumentException(s"Source file does not exist: $sourceFile")
    }

    // Create target directory if it doesn't exist
    if (!fs.exists(new Path(targetPath))) {
      log.info(s"Creating target directory: $targetPath")
      fs.mkdirs(new Path(targetPath))
    }

    // Delete target file if it exists and overwrite is true
    if (overwrite && fs.exists(targetFile)) {
      log.info(s"Deleting existing target file: $targetFile")
      fs.delete(targetFile, false)
    }

    // Copy file
    log.info(s"Copying file from $sourceFile to $targetFile")
    fs.copyFromLocalFile(false, sourceFile, targetFile)
    log.info(s"Successfully copied file to $targetFile")
  }

  /**
   * Collapse a Spark output directory into a single file `$targetPath/$newFileName` by renaming its
   * lone `part-*` file. Fails fast with a clear message when there is no part-file, or more than one
   * (which would silently drop data — write with numPartition = 1). Creates the target directory,
   * overwrites any existing output, and removes the checksum sidecar so the folder stays clean.
   */
  def renameFilesInDirectory(directoryPath: String,
                             targetPath: String,
                             newFileName: String)(implicit sparkSession: SparkSession): Unit = {
    log.info(s"Renaming the part-file in $directoryPath to $targetPath/$newFileName")
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

    val parts = Option(fs.globStatus(new Path(s"$directoryPath/part-*"))).getOrElse(Array.empty)
    if (parts.isEmpty)
      throw new IllegalStateException(s"No part-* file found under '$directoryPath' to rename")
    if (parts.length > 1)
      throw new IllegalStateException(
        s"Expected exactly one part-* file under '$directoryPath' but found ${parts.length}; " +
          "write with numPartition = 1 so the output collapses to a single file")

    val partFile = parts(0).getPath
    val target = new Path(targetPath, newFileName)
    Option(target.getParent).foreach(p => fs.mkdirs(p)) // ensure the target directory exists
    if (fs.exists(target)) fs.delete(target, false)     // overwrite an existing output
    fs.rename(partFile, target)

    // drop the checksum sidecar (.<name>.crc) so the output folder stays clean
    val crc = new Path(target.getParent, s".${target.getName}.crc")
    if (fs.exists(crc)) fs.delete(crc, false)

    log.info(s"Renamed ${partFile.getName} -> $target")
  }

  def harmonizeFilesInDirectory(tableName: String,
                                sourcePath: String,
                                targetPath: String,
                                deleteTmpPath: Boolean = true,
                                overwrite: Boolean = true)(implicit sparkSession: SparkSession, conf: Config): Unit = {


    log.info(s"Starting harmonization for table: $tableName from $sourcePath to $targetPath")

    val newFileName = getNewFileName(tableName)(sparkSession, conf)
    log.info(s"Generated new file name: $newFileName")

    renameFilesInDirectory(s"$sourcePath/$tableName", targetPath, newFileName)(sparkSession)
    log.info(s"File renaming completed")

    if (deleteTmpPath)
    {
      val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      log.info(s"Deleting source directory: $sourcePath")
      fs.delete(new Path(s"$sourcePath/$tableName"), true)
    }

    log.info(s"Harmonization completed successfully for table: $tableName")


  }

  /**
   * Final output file name = the configured `tableName` plus the format extension, e.g.
   * `crr_param_add_on_ste.csv`. (No quarter/date suffix.)
   */
  def getNewFileName(tableName: String)(implicit sparkSession: SparkSession, conf: Config): String = {
    val outConfig = conf.getConfig(s"${PrimaryConstants.APP_CONF}.${tableName}")
    val outTableName = outConfig.getString("tableName")
    val format = outConfig.getString("format")
    s"$outTableName.$format"
  }

  def writeDataframe(dataframe: DataFrame,
                     tableName: String)(implicit sparkSession: SparkSession, conf: Config): Unit = {

    val outConfig = conf.getConfig(s"${PrimaryConstants.APP_CONF}.${tableName}")
    val format = outConfig.getString("format")
    val mode = outConfig.getString("mode")
    val numPartition = outConfig.getInt("numPartition")
    val path = outConfig.getString("tmpPath")
    val outTableName = outConfig.getString("tableName")
    val delimiter = if (outConfig.hasPath("delimiter")) outConfig.getString("delimiter") else ";"
    val header = if (outConfig.hasPath("header")) outConfig.getString("header") else "true"

    val tmpOutput = s"$path/$outTableName"
    log.info(s"Writing $tableName ($format, mode=$mode, numPartition=$numPartition) to $tmpOutput")

    dataframe
      .coalesce(numPartition)
      .write
      .format(format)
      .option("header", header)
      .option("delimiter", delimiter)
      // write empty cells as a bare field (not the default quoted empty string "")
      .option("emptyValue", "")
      .mode(mode)
      .save(tmpOutput)
  }

}
