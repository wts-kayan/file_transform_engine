package com.bnp.str.tseadfwd.utility

import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter, Reader, Writer}
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.Date

/**
 * Utility object providing methods for interacting with HDFS, reading and writing DataFrames,
 * and handling date conversions, tailored for Spark applications. These utilities facilitate
 * common tasks such as reading data, finding the most recent data partitions, and writing data,
 * thereby simplifying data management tasks.
 *
 * @note Use these utilities to enhance code reusability and maintain clean, efficient operations within Spark jobs.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more information on Spark applications and data processing.</a>
 * @see <a href="https://www.linkedin.com/in/mtajmouati">Mehdi TAJMOUATI's LinkedIn profile</a>
 */
object PrimaryUtilities {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Fetches the most recent partition based on a date column from a specified HDFS path.
   * This method is useful for incremental data loading scenarios.
   *
   * @param path The HDFS directory to scan.
   * @param columnPartitioned The partition column, defaulted to 'date'.
   * @param spark The Spark session.
   * @return The most recent partition date as a string, or a far-future date if no partitions exist.
   * @throws ArrayIndexOutOfBoundsException if there is an error reading the partition data.
   */
  def getMaxPartition(path: String, columnPartitioned: String = "date")(
    spark: SparkSession): String = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    try {
      val listOfInsertDates: Array[String] = fs
        .listStatus(new Path(s"$path"))
        .filter(_.isDirectory)
        .map(_.getPath)
        .map(_.toString)
      val filterPartitionFolders =
        listOfInsertDates.filter(_.contains(s"$columnPartitioned="))
      val insertDateStr = filterPartitionFolders.map(_.split(s"$columnPartitioned=")(1))

      if (insertDateStr.length > 1) {
        val dateFormatted =
          insertDateStr.map(d => convertStringToDate(d, "yyyy-MM-dd"))
        val maxDate = dateToStrNdReformat(dateFormatted.max, "yyyy-MM-dd")
        log.info(s"\n**** max $columnPartitioned $maxDate ****\n")
        maxDate
      } else if (insertDateStr.length == 0) {
        log.info(s"\n**** there are no partitions by $columnPartitioned in $path ****\n")
        "2999-01-01"
      } else {
        log.info(s"\n**** max $columnPartitioned ${insertDateStr(0)} ****\n")
        insertDateStr(0)
      }
    } catch {
      case _: Throwable =>
        log.error("Fatal Exception: Check Src-View Data")
        throw new ArrayIndexOutOfBoundsException
    }
  }

  /**
   * Reads a DataFrame from a specified source path using a predefined schema, supporting data partitioning.
   *
   * @param sourceName The identifier for the data source to load.
   * @param schema The schema to apply to the DataFrame.
   * @param isCondition Boolean indicating if a condition should be applied.
   * @param condition The conditional filter to apply, if any.
   * @param sparkSession Implicit SparkSession to handle DataFrame operations.
   * @param env Implicit environment used for building the data path.
   * @param config Implicit Config object for additional settings.
   * @return DataFrame loaded from the specified path.
   */
    /*
  def readDataFrame(sourceName: String,
                    schema: StructType,
                    isCondition: Boolean = false,
                    condition: Column = null,
                    isLastPartition: Boolean = false)
                   (implicit sparkSession: SparkSession, env: String): DataFrame = {

    log.info(s"\n**** Reading file to create DataFrame ****\n")

    // Generate the effective condition
    val effectiveCondition: Column = if (isCondition) condition else lit(true)

    var inputPath: String = ""
    var tableName = ""
    var PartitionedValue = ""

    sourceName match {
      case PrimaryConstants.CLIENTS =>
        inputPath = "src/main/resources/data/"
        tableName = "clients"
      case PrimaryConstants.ORDERS =>
        inputPath = "src/main/resources/data/"
        tableName = "orders"
        PartitionedValue = getMaxPartition(s"$inputPath${tableName.toLowerCase}/")(sparkSession)
        tableName = s"orders/date=$PartitionedValue"
    }

    log.info(s"\n Loading $sourceName from $inputPath${tableName.toLowerCase} ***\n")



    val dataFrame: DataFrame = sparkSession.read
      .schema(schema)
      .option("header", "true")       // treat first line as header
      .option("delimiter", ",")       // use “;” as the field separator
      .csv(s"$inputPath${tableName.toLowerCase}/")
      .selectExpr(ColumnSelector.getColumnSequence(sourceName): _*)
      .where(effectiveCondition)

    if(isLastPartition) {
      log.info(s"\n Loading with partition ***\n")
      return dataFrame.withColumn("date", lit(PartitionedValue))
    }

    dataFrame
  }
*/
  /**
   * Opens a file in HDFS and returns a BufferedReader to read the file's contents.
   *
   * @param filePath The full path to the file in HDFS.
   * @param sc The SparkContext to access Hadoop configurations.
   * @return A BufferedReader that can be used to read the file.
   */
  def getHdfsReader(filePath: String)(sc: SparkContext): Reader = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(filePath)
    new BufferedReader(new InputStreamReader(fs.open(path)))
  }

  /**
   * Opens a file for writing through Hadoop's FileSystem and returns a UTF-8 BufferedWriter.
   * The same call resolves a local path, a `--files`-shipped basename, or an `hdfs://` path —
   * mirroring [[getHdfsReader]] — so analysis/report outputs land on the cluster (or locally in
   * a local Spark session) without the caller knowing which. Overwrites any existing file and
   * creates parent directories as needed.
   *
   * @param filePath The full path to the file to write (local or HDFS).
   * @param sc The SparkContext to access Hadoop configurations.
   * @return A Writer the caller is responsible for closing.
   */
  def getHdfsWriter(filePath: String)(sc: SparkContext): Writer = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(filePath)
    Option(path.getParent).foreach(p => fs.mkdirs(p))
    new BufferedWriter(new OutputStreamWriter(fs.create(path, true), StandardCharsets.UTF_8))
  }

  /** Convenience: write an entire string to a (local or HDFS) path through [[getHdfsWriter]]. */
  def writeStringToHdfs(filePath: String, content: String)(sc: SparkContext): Unit = {
    val w = getHdfsWriter(filePath)(sc)
    try w.write(content) finally w.close()
  }

  /**
   * Converts a string to a Date object using the specified date format.
   *
   * @param s The date string to convert.
   * @param formatType The format of the date string.
   * @return A Date object representing the parsed date string.
   */
  def convertStringToDate(s: String, formatType: String): Date = {
    val format = new SimpleDateFormat(formatType)
    format.parse(s)
  }

  /**
   * Reformats a Date object to a string using the specified date format.
   *
   * @param date The Date object to reformat.
   * @param format The desired format of the date string.
   * @return A string representing the formatted date.
   */
  def dateToStrNdReformat(date: Date, format: String): String = {
    val df = new SimpleDateFormat(format)
    df.format(date)
  }

  /**
   * Writes a DataFrame to a specified path with a given mode and number of partitions.
   *
   * @param dataFrame The DataFrame to write.
   * @param mode The save mode (e.g., "overwrite", "append").
   * @param numPartition The number of partitions to use when writing the DataFrame.
   * @param env Implicit environment string for the write operation.
   */
  def writeDataFrame(dataFrame: DataFrame,
                     mode: String,
                     numPartition: Int)(implicit env: String): Unit = {

    log.info(s"\n *** Write started (mode: $mode, numPartition: $numPartition) ... ***\n")

    dataFrame
      .coalesce(numPartition)
      .write
      .format("parquet")
      .partitionBy("location")
      .mode(mode)
      .save(s"/src/main/resources/data/datalake/clients_orders/")

    log.info(s"\n *** Write Completed ... *** \n")
  }

  def readDataFrameFromExcel(tableName: String)
                            (implicit sparkSession: SparkSession,
                             config: Config): DataFrame = {

    val addonInputConfig =
      config.getConfig(s"${PrimaryConstants.APP_CONF}.${tableName}")

    val path = addonInputConfig.getString("path")
    val sheetName = addonInputConfig.getString("sheetNames")

    log.info(
      s"Reading $tableName Excel file from path: $path, sheet: $sheetName"
    )

    val rawdf = sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("path", path)
      .option("location", path)
      .option("dataAddress", s"'$sheetName'!A1")
      .option("header", "true")
      .option("setErrorCellsToFallbackValues", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      // With inferSchema=false, POI's DataFormatter renders cells using the JVM default
      // locale, so large numbers pick up locale-specific grouping (e.g. a French work PC
      // emits a non-breaking space "-8 128" and a comma decimal). usePlainNumberFormat
      // forces a canonical, locale-independent rendering: '.' decimal, NO grouping, no
      // scientific notation — so the same Excel parses identically on en-US and fr-FR hosts.
      .option("usePlainNumberFormat", "true")
      .option("addColorColumns", "false")
      .load()

    rawdf
  }

  def readDataFrameFromCsv(tableName: String)
                          (implicit sparkSession: SparkSession,
                           conf: Config): DataFrame = {

    val config =
      conf.getConfig(s"${PrimaryConstants.APP_CONF}.$tableName")

    sparkSession.read
      .option("header", config.getString("header"))
      .option("delimiter", config.getString("delimiter"))
      .csv(config.getString("path"))
  }

  /**
   * Read a scenario workbook that holds ONE SHEET PER SCENARIO (e.g. Central / Adverse /
   * Optimistic / Extreme) and reshape it into the single CSV-style frame the mapper expects:
   * a `scenario` column (= sheet name) + a `Date` column + one column per macro variable.
   *
   * `sheetNames` in the config is a LIST here (unlike the single-sheet readDataFrameFromExcel).
   * Each sheet is read with the same locale-safe options (usePlainNumberFormat etc.); the date
   * header — which the source writes lowercase ("date") — is normalized to the expected "Date",
   * a `scenario` literal is stamped on every row, and the sheets are unioned BY NAME with
   * allowMissingColumns=true (sheets may carry different macro columns, so absent ones become
   * null). The result matches the old single-table scenario CSV, so collectScenario is unchanged.
   */
  def readScenarioFromExcelSheets(tableName: String)
                                 (implicit sparkSession: SparkSession,
                                  config: Config): DataFrame = {
    import scala.collection.JavaConverters._

    val cfg    = config.getConfig(s"${PrimaryConstants.APP_CONF}.$tableName")
    val path   = cfg.getString("path")
    val sheets = cfg.getStringList("sheetNames").asScala.toVector

    val perSheet = sheets.map { sheet =>
      log.info(s"Reading $tableName scenario sheet '$sheet' from path: $path")
      val raw = sparkSession.read
        .format("com.crealytics.spark.excel")
        .option("path", path)
        .option("location", path)
        .option("dataAddress", s"'$sheet'!A1")
        .option("header", "true")
        .option("setErrorCellsToFallbackValues", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "false")
        .option("usePlainNumberFormat", "true")
        .option("addColorColumns", "false")
        .load()

      // Normalize the date header to "Date" (source uses lowercase "date") and tag the rows
      // with their scenario (= sheet name), so the union mirrors the old CSV shape.
      val dated = raw.columns
        .find(c => c.equalsIgnoreCase(PrimaryConstants.COL_SCEN_DATE) && c != PrimaryConstants.COL_SCEN_DATE)
        .foldLeft(raw)((df, c) => df.withColumnRenamed(c, PrimaryConstants.COL_SCEN_DATE))

      dated.withColumn(PrimaryConstants.COL_SCEN_NAME, lit(sheet))
    }

    perSheet.reduce((a, b) => a.unionByName(b, allowMissingColumns = true))
  }

  def writeDataframe(
                      dataframe: DataFrame,
                      tableName: String
                    )(implicit sparkSession: SparkSession, conf: Config): Unit = {

    val outConfig =
      conf.getConfig(s"${PrimaryConstants.APP_CONF}.${tableName}")

    // val partitionColumns = outConfig.getString("partitionnedColumns")

    val format = outConfig.getString("format")
    val mode = outConfig.getString("mode")
    val numPartition = outConfig.getInt("numPartition")
    val path = outConfig.getString("tmpPath")
    val outTableName = outConfig.getString("tableName")

    val tmp_output = s"$path/$outTableName"

    dataframe
      .coalesce(numPartition)
      .write
      .format(format)
      .option("header", "true")
      .option("delimiter", ";")
      .mode(mode)
      .save(tmp_output)

    val singleFile =
      if (outConfig.hasPath("singleFile")) outConfig.getBoolean("singleFile") else true
    if (singleFile) collapseToSingleFile(path, outTableName, format)
  }

  /**
   * Collapse a Spark output directory (which holds a single `part-*` file plus `_SUCCESS`
   * and checksum sidecars) into one clean file `$path/$tableName.$ext`, removing the
   * directory. Requires the DataFrame to have been written with one partition.
   */
  private def collapseToSingleFile(path: String, tableName: String, format: String)
                                  (implicit sparkSession: SparkSession): Unit = {
    val ext = if (format.equalsIgnoreCase("csv")) "csv" else format
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val dir = new Path(s"$path/$tableName")
    val parts = Option(fs.globStatus(new Path(dir, "part-*"))).getOrElse(Array.empty)

    if (parts.length != 1) {
      log.warn(
        s"\n*** Cannot collapse: expected 1 part file in $dir but found ${parts.length}; left as a directory ***\n")
    } else {
      val finalPath = new Path(s"$path/$tableName.$ext")
      if (fs.exists(finalPath)) fs.delete(finalPath, false)
      fs.rename(parts(0).getPath, finalPath)
      fs.delete(dir, true)
      // drop the local checksum sidecar (.<name>.crc) so the output folder stays clean
      val crc = new Path(finalPath.getParent, s".${finalPath.getName}.crc")
      if (fs.exists(crc)) fs.delete(crc, false)
      log.info(s"\n*** Collapsed output to $finalPath ***\n")
    }
  }

}
