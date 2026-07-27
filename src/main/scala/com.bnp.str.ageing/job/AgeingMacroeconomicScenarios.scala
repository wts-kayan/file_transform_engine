package com.bnp.str.ageing.job

import com.bnp.str.ageing.utility.PrimaryConstants
import com.crealytics.spark.excel.WorkbookReader
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import com.bnp.str.ageing.sessionmanager.StrSparkSessionManager.fetchSparkSession
import com.bnp.str.ageing.utility.DateUtils
import com.bnp.str.ageing.utility.PrimaryConstants.APPLICATION_NAME

object AgeingMacroeconomicScenarios {
  private val log = LoggerFactory.getLogger(this.getClass.getName)

  def getScenario(path: String, scenario: String) = {
    val spark = SparkSession.builder().getOrCreate()

    spark
      .read.format("com.crealytics.spark.excel")
      .option("header", true)
      .option("inferSchema", true)
      .option("dataAddress", s"$scenario!A1")
      .option("maxRowsInMemory", 30000)
      .load(path)
  }

  def writeScenario(df: DataFrame, path: String, scenario: String) = {
    df
      .orderBy("Date")
      .write
      .format("com.crealytics.spark.excel") // Or .format("excel") for V2 implementation
      .option("dataAddress", s"$scenario!A1")
      .option("header", "true")
      .mode("append") // Optional, default: overwrite.
      .save(path)
  }

  def ageing(properties: AgeingMacroeconomicScenariosProperties, dfCentral: DataFrame, df: DataFrame) = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val varMacros = df.schema.fields.map(_.name).filter(_ != "Date")

    // Create the shock dataframe
    val shock = {
      var shock = df
        .join(dfCentral, Seq("Date"))
        .drop(dfCentral.col("Date"))
        .filter($"Date".isin(properties.dateForShocks: _*))

      val selectRequest: Array[Column] = $"Date" +: varMacros.map(varMacro => {
        //(col(s"$df_name.$varMacro") - col(s"$dfCentral_name.$varMacro")).as(s"${stressPrefix}_$varMacro")
        (df.col(varMacro) - dfCentral(varMacro)).as(varMacro)
      })

      shock = shock.select(selectRequest: _*)


      // the shock have to be applied to a different date
      val dateMappingDf = properties.shockToAge.toSeq.toDF("date_used_for_computing_stress", "date_where_the_stress_should_be_applied")
      shock = shock
        .join(broadcast(dateMappingDf), $"Date" === $"date_used_for_computing_stress")
        .drop("Date", "date_used_for_computing_stress")

      shock = shock.persist()
      shock.count

      shock
    }

    var res = dfCentral
      .join(broadcast(shock), $"Date" === $"date_where_the_stress_should_be_applied", "left")

    val selectRequest: Array[Column] = $"Date" +: varMacros.map(varMacro => {
      when(shock.col(varMacro).isNotNull,
        dfCentral.col(varMacro) + shock.col(varMacro)
      ).otherwise(
        dfCentral.col(varMacro)
      )
        .as(varMacro)
    })

    res = res.select(selectRequest: _*)

    res = res.persist()
    res.count

    res
  }

  def main(args: Array[String]): Unit = {
    log.info(" Start %s ".format(APPLICATION_NAME))

    if (args.size != 1) throw new IllegalArgumentException("We expect one and only one argument")
    val properties = AgeingMacroeconomicScenariosProperties.create(args.head)

    val spark = fetchSparkSession(PrimaryConstants.APPLICATION_NAME)
    import spark.implicits._

    // check output path availability
    val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(properties.outputPath))) {
      throw new Exception(s"the file ${properties.outputPath} already exists")
    }

    val scenarioNames = WorkbookReader( Map("path" -> properties.macroInputPath), spark.sparkContext.hadoopConfiguration).sheetNames

    if (!scenarioNames.contains(AgeingMacroeconomicScenariosProperties.central)) {
      throw new Exception(
        s"""The sheet with name "${AgeingMacroeconomicScenariosProperties.central}"
           |is not found in file ${properties.macroInputPath}, the sheets are ${scenarioNames.map("\""+_+"\"").mkString(", ")}""".stripMargin)
    }

    // read the var macro file
    log.info("Reading scenarios")
    var input: Map[String, DataFrame] = scenarioNames.map(scenario => {
      val res = getScenario(properties.macroInputPath, scenario).persist
      res.count
      scenario -> res
    }).toMap

    // if the ageingDate > the last date
    // we fill the missing dates by duplicating the last available date
    val lastDate = input(AgeingMacroeconomicScenariosProperties.central).select("Date").as[String].collect().max
    if (DateUtils.quarterDiff(lastDate, properties.lastDate) < 0) {
      log.info("Adding missing date")
      val datesToAdd = DateUtils.getDatesBetween(lastDate, properties.lastDate).tail

      input = input.mapValues(df => {
        val newDate = df
          .filter($"Date" === lastDate)
          .withColumn("Date", explode(array(datesToAdd.map(lit): _*)))

        val res = df.union(newDate).persist
        res.count()
        res
      })
    }

    // Age all scenario but central
    log.info("Ageing scenarios")
    val res = input.map(values => {
      val scenario = values._1
      val df = values._2

      if (scenario == AgeingMacroeconomicScenariosProperties.central) scenario -> input(AgeingMacroeconomicScenariosProperties.central)
      else scenario -> ageing(properties, input(AgeingMacroeconomicScenariosProperties.central), df)
    })

    log.info("writing aged scenarios")
    scenarioNames.foreach(scenario => {
      writeScenario(res(scenario), properties.outputPath, scenario)
    })


  }
}
