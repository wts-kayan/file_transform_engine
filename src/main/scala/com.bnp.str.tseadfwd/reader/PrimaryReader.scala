package com.bnp.str.tseadfwd.reader

import com.typesafe.config.Config
import com.bnp.str.tseadfwd.utility.{PrimaryConstants, SchemaSelector}
import com.bnp.str.tseadfwd.utility.PrimaryUtilities._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.io.FileNotFoundException

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
   * All RA perimeter inputs unioned into one frame.
   *
   * Two ways to get there, decided by the configuration:
   *  - `tseadfwd_app.RA` present -> [[raInputDiscovered]]: the sheets are found IN THE WORKBOOK, so
   *    a new entity tab is picked up with no code and no conf change;
   *  - block absent -> [[raInputConfigured]]: the historical per-entity blocks (`RA_BCEF`, …).
   *
   * Either way the mapper keys every row by `PERIMETER` — a column INSIDE the sheet, never the sheet
   * name — so `unionByName` is all that is needed to bring an extra entity into the computation, and
   * PARAMETRAGE drives which of those perimeters actually produce matrices.
   */
  def raInput: DataFrame =
    RaSheetConfig.from(conf) match {
      case Some(cfg) => raInputDiscovered(cfg)
      case None      => raInputConfigured
    }

  /**
   * Dynamic path: discover the RA sheets, read each one, union.
   *
   * A discovered sheet passes two gates — its NAME matched the pattern in [[RaSheetDiscovery]], and
   * its CONTENT must carry the RA key columns, checked here now that the frame is readable. A sheet
   * that fails the second gate is skipped with a warning rather than failing the run: a workbook the
   * business edits will grow tabs that are not RA tables, and one of them must not stop tonight's
   * production run.
   */
  private def raInputDiscovered(cfg: RaSheetConfig): DataFrame = {
    val selection = RaSheetDiscovery.discover(cfg, sparkSession.sparkContext.hadoopConfiguration)
    log.info(selection.summary)

    val frames = selection.selected.flatMap { s =>
      try {
        val df = readExcelSheet(s.path, s.sheet, label = s"RA sheet '${s.sheet}'")(sparkSession)
        val missing = RaSheetDiscovery.missingColumns(df.columns.toSeq, cfg.requireColumns)
        if (missing.nonEmpty) {
          log.warn(s"RA sheet '${s.sheet}' in ${s.path} skipped (not an RA table: missing " +
            s"column(s) ${missing.mkString(", ")})")
          None
        } else {
          log.info(s"RA sheet '${s.sheet}' loaded from ${s.path}")
          Some(df)
        }
      } catch {
        case ex: Throwable =>
          log.warn(s"RA sheet '${s.sheet}' in ${s.path} skipped " +
            s"(${rootCause(ex).getClass.getSimpleName}: ${rootCause(ex).getMessage})")
          None
      }
    }

    if (frames.isEmpty)
      throw new IllegalStateException(
        s"No RA sheet could be read. Workbook(s): ${cfg.paths.mkString(", ")}; sheets matching " +
          s"'${cfg.sheetPattern}' and carrying ${cfg.requireColumns.mkString("/")}: none. " +
          (if (selection.skipped.isEmpty) "The workbook(s) listed no sheet at all — check the path resolves on the default filesystem."
           else "Sheets seen: " + selection.skipped.map(s => s"'${s.sheet}' (${s.reason})").mkString(", ")))

    frames.reduce((a, b) => a.unionByName(b, allowMissingColumns = true))
  }

  /**
   * Historical path: one conf block per entity. Each is read independently and any whose sheet is
   * **absent or unreadable is skipped with a warning** — the sample workbook only carries `RA_BCEF`,
   * whereas a full workbook also has BGL/BNL/FORTIS/LS.
   */
  private def raInputConfigured: DataFrame = {
    val sources: Seq[(String, () => DataFrame)] = Seq(
      PrimaryConstants.RA_BCEF   -> (() => ra_bcef),
      PrimaryConstants.RA_BGL    -> (() => ra_bgl),
      PrimaryConstants.RA_BNL    -> (() => ra_bnl),
      PrimaryConstants.RA_FORTIS -> (() => ra_fortis),
      PrimaryConstants.RA_LS     -> (() => ra_ls)
    )
    // Default Hadoop FileSystem the relative input paths resolve against (HDFS on the cluster,
    // local file:// when running locally). Reported in errors so a "file not found" is obvious.
    val defaultFs =
      try FileSystem.get(sparkSession.sparkContext.hadoopConfiguration).getUri.toString
      catch { case _: Throwable => "<unknown>" }

    val frames = sources.flatMap { case (name, get) =>
      try {
        val df = get()
        df.columns // force the read now so a missing-sheet error surfaces here, not downstream
        log.info(s"RA input '$name' loaded from ${resolvedPath(name)}")
        Some(df)
      } catch {
        case ex: Throwable =>
          val cause = rootCause(ex)
          val reason = cause match {
            case _: FileNotFoundException =>
              s"file not found on $defaultFs at path '${resolvedPath(name)}'"
            case other =>
              s"${other.getClass.getSimpleName}: ${other.getMessage}"
          }
          log.warn(s"RA input '$name' skipped ($reason)")
          None
      }
    }
    if (frames.isEmpty)
      throw new IllegalStateException(
        s"No RA inputs could be read (need at least ${PrimaryConstants.RA_BCEF}). " +
          s"Default filesystem is '$defaultFs'; each RA input resolves to: " +
          sources.map { case (name, _) => s"$name -> ${resolvedPath(name)}" }.mkString("; ") +
          s". Check the workbook exists at that path on '$defaultFs' (uploaded to HDFS, correct " +
          s"casing, readable by the run user) — relative paths resolve against the default " +
          s"filesystem, not the local edge node."
      )
    frames.reduce((a, b) => a.unionByName(b, allowMissingColumns = true))
  }

  /** Configured workbook path for an RA input, or "<unresolved>" if the key is missing. */
  private def resolvedPath(tableName: String): String =
    try addonInputConfig.getConfig(tableName).getString("path")
    catch { case _: Throwable => "<unresolved>" }

  /** Unwrap wrapper exceptions (Spark/connector often wrap the real FileNotFoundException). */
  private def rootCause(t: Throwable): Throwable = {
    var c = t
    while (c.getCause != null && c.getCause != c) c = c.getCause
    c
  }
}