package com.bnp.str.ageing.job

import com.bnp.str.ageing.utility.DateUtils.{addQuarters, getDatesBetween}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedReader, InputStreamReader, Serializable}


class AgeingMacroeconomicScenariosProperties(props: Config) extends Serializable {

  private var errors: Seq[String] = Seq()

  private def get(key: String): String = {
    if (props.hasPath(key)) {
      props.getString(key)
    } else {
      errors = errors :+ key
      null
    }
  }

  private def getOrDefault(key: String, default: => String): String = {
    if (props.hasPath(key)) props.getString(key)
    else default
  }

  val macroInputPath = get("ageing_macroeconomic_scenarios_app.macroeconomic.path")
  val initialAsOfDate = get("ageing_macroeconomic_scenarios_app.as_of_date.initial")
  val ageingAsOfDate = get("ageing_macroeconomic_scenarios_app.as_of_date.ageing")

  val oneQuaterBeforeInitialAsOfDate = addQuarters(initialAsOfDate, -1)

  // the date used to compute the shock
  val dateForShocks = getDatesBetween(initialAsOfDate, addQuarters(initialAsOfDate, 12))

  // the date where the shock should de apply
  val dateToAge  = getDatesBetween(ageingAsOfDate, addQuarters(ageingAsOfDate, 12))
  val lastDate = addQuarters(ageingAsOfDate, 12)

  // map the date used to compute the shock to the date where the shock should be applied
  val shockToAge = dateForShocks.zip(dateToAge).toMap

  val outputPath = get("ageing_macroeconomic_scenarios_app.output.macroeconomic.path")


  val isLocal: Boolean = getOrDefault("ageing_macroeconomic_scenarios_app.technical.is_local", "false").toBoolean


}

object AgeingMacroeconomicScenariosProperties {

  val central = "Central"

  def create(propertiesPath: String): AgeingMacroeconomicScenariosProperties = {
    //val properties: Properties = new Properties()

    val fs = FileSystem.get(new org.apache.hadoop.conf.Configuration())
    val path = new Path(propertiesPath)
    val reader = new BufferedReader(new InputStreamReader(fs.open(path)))
    val config: Config = ConfigFactory.parseReader(reader)

    val res = new AgeingMacroeconomicScenariosProperties(config)

    if (res.errors.nonEmpty) {
      throw new IllegalArgumentException("Missing require arguments: \n\n\t\t"+res.errors.mkString("\t\t\n")+"\n")
    }

    res
  }

}
