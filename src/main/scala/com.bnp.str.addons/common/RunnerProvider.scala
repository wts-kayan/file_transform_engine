package com.bnp.str.addons.common

import com.bnp.str.addons.reader.PrimaryReader
import com.bnp.str.addons.utility.PrimaryConstants
import org.apache.spark.sql.DataFrame

abstract class RunnerProvider(primaryReader: PrimaryReader) extends Serializable {

  private[common] lazy val add_on_application: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.ADD_ON_APPLICATION)

  private[common] lazy val add_on_action: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.ADD_ON_ACTION)

  private[common] lazy val add_on_perimeter: DataFrame =
    primaryReader.getMappingReader(PrimaryConstants.ADD_ON_PERIMETER)


  def run_addons_runner(): DataFrame

}
