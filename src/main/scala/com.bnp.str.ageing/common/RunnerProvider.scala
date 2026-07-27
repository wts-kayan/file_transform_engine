package com.bnp.str.ageing.common

import com.bnp.str.ageing.reader.PrimaryReader
import org.apache.spark.sql.DataFrame

abstract class RunnerProvider(primaryReader: PrimaryReader) extends Serializable {

  private[common] lazy val scenarioNames: Seq[String] = primaryReader.scenarioNames

  private[common] def readScenario(scenario: String): DataFrame =
    primaryReader.read(scenario)

  def run_ageing_runner(): Map[String, DataFrame]

}
