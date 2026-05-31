package com.bnp.str.tseadfwd.common

import com.bnp.str.tseadfwd.reader.PrimaryReader
import com.bnp.str.tseadfwd.utility.PrimaryConstants
import org.apache.spark.sql.DataFrame


abstract class RunnerProvider(primaryReader: PrimaryReader) extends Serializable {

  private[common] lazy val ra_bcef: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.RA_BCEF)
/*
  private[common] lazy val ra_bgl: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.RA_BGL)

  private[common] lazy val ra_bnl: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.RA_BNL)

  private[common] lazy val ra_fortis: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.RA_FORTIS)

  private[common] lazy val ra_ls: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.RA_LS)
*/
  private[common] lazy val macro_variable: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.MACRO_VARIABLE)

  private[common] lazy val parametrage: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.PARAMETRAGE)

  def run_tseadfwd_runner(): DataFrame

}
