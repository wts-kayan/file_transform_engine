package com.bnp.str.tseadfwd.reader

import com.typesafe.config.Config
import com.bnp.str.tseadfwd.utility.{PrimaryConstants, SchemaSelector}
import com.bnp.str.tseadfwd.utility.PrimaryUtilities._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class PrimaryReader()(implicit sparkSession: SparkSession, conf: Config)
  extends SchemaSelector {

  private val log = LoggerFactory.getLogger(this.getClass)

  val addonInputConfig = conf.getConfig(s"${PrimaryConstants.APP_CONF}")

  private lazy val ra_bcef: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.RA_BCEF)(sparkSession, conf)

  private lazy val ra_bgl: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.RA_BGL)(sparkSession, conf)

  private lazy val ra_bnl: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.RA_BNL)(sparkSession, conf)

  private lazy val ra_fortis: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.RA_FORTIS)(sparkSession, conf)

  private lazy val ra_ls: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.RA_LS)(sparkSession, conf)

  private lazy val macro_variable: DataFrame =
    readScenarioFromExcelSheets(PrimaryConstants.MACRO_VARIABLE)(sparkSession, conf)

  private lazy val parametrage: DataFrame =
    readDataFrameFromExcel(PrimaryConstants.PARAMETRAGE)(sparkSession, conf)

  def getMappingReader(input: String): DataFrame = {
    input.toUpperCase match {
      case "RA_BCEF"        => ra_bcef
      case "RA_BGL"         => ra_bgl
      case "RA_BNL"         => ra_bnl
      case "RA_FORTIS"      => ra_fortis
      case "RA_LS"          => ra_ls
      case "MACRO_VARIABLE" => macro_variable
      case "PARAMETRAGE"    => parametrage
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid input $input. expected ..."
        )
    }
  }

  /**
   * All RA perimeter inputs unioned into one frame. Each entity is read independently and any
   * whose sheet is **absent or unreadable is skipped with a warning** — the sample workbook only
   * carries `RA_BCEF`, whereas a full workbook also has BGL/BNL/FORTIS/LS. The mapper keys every
   * row by `PERIMETER`, so a `unionByName` is all that is needed to bring the extra entities into
   * the computation (PARAMETRAGE then drives which of those perimeters actually produce matrices).
   */
  def raInput: DataFrame = {
    val sources: Seq[(String, () => DataFrame)] = Seq(
      PrimaryConstants.RA_BCEF   -> (() => ra_bcef),
      PrimaryConstants.RA_BGL    -> (() => ra_bgl),
      PrimaryConstants.RA_BNL    -> (() => ra_bnl),
      PrimaryConstants.RA_FORTIS -> (() => ra_fortis),
      PrimaryConstants.RA_LS     -> (() => ra_ls)
    )
    val frames = sources.flatMap { case (name, get) =>
      try {
        val df = get()
        df.columns // force the read now so a missing-sheet error surfaces here, not downstream
        log.info(s"RA input '$name' loaded")
        Some(df)
      } catch {
        case ex: Throwable =>
          log.warn(s"RA input '$name' skipped (${ex.getClass.getSimpleName}: ${ex.getMessage})")
          None
      }
    }
    if (frames.isEmpty)
      throw new IllegalStateException("No RA inputs could be read (need at least RA_BCEF).")
    frames.reduce((a, b) => a.unionByName(b, allowMissingColumns = true))
  }
}