package com.bnp.str.tseadfwd.common

import com.bnp.str.tseadfwd.mapping.PrimaryMapper
import com.bnp.str.tseadfwd.reader.PrimaryReader
import com.bnp.str.tseadfwd.utility.PrimaryConstants
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import com.typesafe.config.Config

class PrimaryRunner(
                     primaryReader: PrimaryReader,
                     outputTableName: String
                   )(implicit sparkSession: SparkSession, conf: Config)
  extends RunnerProvider(primaryReader) {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def run_tseadfwd_runner(): DataFrame = {

    log.info(
      s" run_tseadfwd_runner started",
      this.getClass.getName
    )

    new PrimaryMapper(
      ra_all, // BCEF + any of BGL/BNL/FORTIS/LS present in the workbook (unioned; missing skipped)
      macro_variable,
      parametrage,
      outputTableName
    ).getMapping_tseadfwd

  }
}