package com.bnp.str.tseadfwd.common

import com.bnp.str.tseadfwd.reader.PrimaryReader
import com.bnp.str.tseadfwd.utility.PrimaryConstants
import org.apache.spark.sql.DataFrame


abstract class RunnerProvider(primaryReader: PrimaryReader) extends Serializable {

  /** All RA perimeters unioned (BCEF + any of BGL/BNL/FORTIS/LS present); missing sheets skipped. */
  private[common] lazy val ra_all: DataFrame =
    primaryReader.raInput

  private[common] lazy val macro_variable: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.MACRO_VARIABLE)

  private[common] lazy val parametrage: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.PARAMETRAGE)

  def run_tseadfwd_runner(): DataFrame

}
