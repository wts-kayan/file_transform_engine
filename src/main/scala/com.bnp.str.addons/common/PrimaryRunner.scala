package com.bnp.str.addons.common

import com.bnp.str.addons.mapping.PrimaryMapper
import com.bnp.str.addons.reader.PrimaryReader
import com.bnp.str.addons.utility.PrimaryConstants
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class PrimaryRunner(primaryReader: PrimaryReader, outputTableName: String)(implicit sparkSession: SparkSession, conf: Config)
  extends RunnerProvider(primaryReader) {

  private val log = LoggerFactory.getLogger(this.getClass)


  override def run_addons_runner(): DataFrame = {

    log.info(s" run_addons_runner", this.getClass.getName)

    val df = new PrimaryMapper(
      add_on_application,
      add_on_action,
      add_on_perimeter,
      outputTableName
    ).getMapping_addons

    df
  }

}
