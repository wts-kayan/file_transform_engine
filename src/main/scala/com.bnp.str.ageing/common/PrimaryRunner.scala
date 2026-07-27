package com.bnp.str.ageing.common

import com.bnp.str.ageing.mapping.PrimaryMapper
import com.bnp.str.ageing.reader.PrimaryReader
import com.bnp.str.ageing.utility.DateUtils.{addQuarters, getDatesBetween, quarterDiff}
import com.bnp.str.ageing.utility.PrimaryConstants
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Reads every scenario sheet, extends them if the ageing horizon runs past the last available date,
 * then ages every non-central scenario (central is passed through unchanged) via [[PrimaryMapper]].
 * The shock / ageing date windows are derived from the `initial` and `ageing` as-of dates in config.
 */
class PrimaryRunner(primaryReader: PrimaryReader)(implicit sparkSession: SparkSession, conf: Config)
  extends RunnerProvider(primaryReader) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val appConf = conf.getConfig(PrimaryConstants.APP_CONF)
  private val initialAsOfDate = appConf.getString(PrimaryConstants.INITIAL_AS_OF_DATE)
  private val ageingAsOfDate = appConf.getString(PrimaryConstants.AGEING_AS_OF_DATE)

  private val q = PrimaryConstants.PROJECTION_QUARTERS
  // the dates used to compute the shock, and the dates where the shock should be applied
  private val dateForShocks = getDatesBetween(initialAsOfDate, addQuarters(initialAsOfDate, q))
  private val dateToAge = getDatesBetween(ageingAsOfDate, addQuarters(ageingAsOfDate, q))
  private val shockToAge = dateForShocks.zip(dateToAge).toMap
  private val lastDate = addQuarters(ageingAsOfDate, q)

  override def run_ageing_runner(): Map[String, DataFrame] = {
    import sparkSession.implicits._
    log.info("run_ageing_runner")

    if (!scenarioNames.contains(PrimaryConstants.CENTRAL)) {
      throw new IllegalStateException(
        s"""The sheet "${PrimaryConstants.CENTRAL}" is not found in ${primaryReader.macroInputPath}; """ +
          s"""sheets are ${scenarioNames.map("\"" + _ + "\"").mkString(", ")}""")
    }

    // read every scenario sheet
    log.info("Reading scenarios")
    var input: Map[String, DataFrame] = scenarioNames.map { scenario =>
      val df = readScenario(scenario).persist()
      df.count()
      scenario -> df
    }.toMap

    // if the ageing horizon runs past the last available date, extend by duplicating the last row
    val lastAvailable =
      input(PrimaryConstants.CENTRAL).select(PrimaryConstants.COL_DATE).as[String].collect().max
    if (quarterDiff(lastAvailable, lastDate) < 0) {
      log.info("Adding missing dates")
      val datesToAdd = getDatesBetween(lastAvailable, lastDate).tail
      input = input.map { case (scenario, df) =>
        val newDates = df
          .filter(col(PrimaryConstants.COL_DATE) === lastAvailable)
          .withColumn(PrimaryConstants.COL_DATE, explode(array(datesToAdd.map(lit): _*)))
        val extended = df.union(newDates).persist()
        extended.count()
        scenario -> extended
      }
    }

    // age every non-central scenario; keep central unchanged
    log.info("Ageing scenarios")
    val dfCentral = input(PrimaryConstants.CENTRAL)
    input.map { case (scenario, df) =>
      if (scenario == PrimaryConstants.CENTRAL) scenario -> dfCentral
      else scenario -> new PrimaryMapper(dfCentral, df, dateForShocks, shockToAge).getMapping_ageing
    }
  }
}
