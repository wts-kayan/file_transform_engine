package com.bnp.str.tseadfwd.reader

import com.bnp.str.tseadfwd.utility.PrimaryConstants
import com.crealytics.spark.excel.WorkbookReader
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

/**
 * Settings of the dynamic RA block, `tseadfwd_app.RA`.
 *
 * The block is OPTIONAL: when it is absent [[PrimaryReader]] falls back to the historical
 * per-entity blocks (`RA_BCEF`, `RA_BGL`, …), so an unconverted configuration keeps working.
 *
 * @param paths          workbooks to look in, IN ORDER. The order is the duplicate rule: the same
 *                       sheet name in two workbooks is loaded from the FIRST one (today `RA_BCEF`
 *                       exists in both `Inputs_RA.xlsx` and `Inputs_RA_v3.xlsx`).
 * @param sheetPattern   a sheet is a candidate when its name matches this regex, case-insensitively
 * @param requireColumns columns a candidate must actually carry to be loaded — the second gate, on
 *                       CONTENT rather than on the name. A workbook holds tabs that match any name
 *                       pattern one might write (a divider literally named "Inputs RA ->"), and a
 *                       sheet that is not an RA table must be skipped, not fed to the union.
 * @param include        conf key `includeSheets` — load these whatever the pattern says (normally empty)
 * @param exclude        conf key `excludeSheets` — never load these, checked first (e.g. a retired entity)
 */
final case class RaSheetConfig(
                                paths: Seq[String],
                                sheetPattern: String,
                                requireColumns: Seq[String],
                                include: Seq[String],
                                exclude: Seq[String]
                              ) {

  /** Case-insensitive, anchored as written by the user (the default anchors itself). */
  val pattern: Regex = ("(?i)" + sheetPattern).r
}

object RaSheetConfig {

  /** Conf block holding the dynamic RA settings. */
  final val BLOCK = "RA"

  /** Sheets named `RA_<entity>` / `RA <entity>` — what every RA tab is called today. */
  final val DEFAULT_PATTERN = "^RA[_ ].*"

  /** The RA key layout. A sheet without these columns is not an RA table, whatever it is called. */
  final val DEFAULT_REQUIRED: Seq[String] = Seq(
    PrimaryConstants.COL_PERIMETER,
    PrimaryConstants.COL_SEGMENT,
    PrimaryConstants.COL_RATE_TYPE,
    PrimaryConstants.COL_FWL_TYPE,
    PrimaryConstants.COL_METRIC)

  /**
   * Read `tseadfwd_app.RA`, or `None` when the block is absent — the caller then takes the legacy
   * per-entity path. A block present but with no `paths` is a configuration error, not a fallback:
   * silently reading nothing would look exactly like "the business added no entity".
   */
  def from(config: Config): Option[RaSheetConfig] = {
    import scala.collection.JavaConverters._

    val appConf = config.getConfig(PrimaryConstants.APP_CONF)
    if (!appConf.hasPath(BLOCK)) return None
    val ra = appConf.getConfig(BLOCK)

    def strList(key: String): Seq[String] =
      if (ra.hasPath(key)) ra.getStringList(key).asScala.toVector else Vector.empty

    // `paths` is the list form; `path` is accepted as the single-workbook shorthand.
    val paths =
      if (ra.hasPath("paths")) strList("paths")
      else if (ra.hasPath("path")) Vector(ra.getString("path"))
      else Vector.empty

    if (paths.isEmpty)
      throw new IllegalArgumentException(
        s"${PrimaryConstants.APP_CONF}.$BLOCK is present but names no workbook: set `paths = [...]` " +
          "(or the single-workbook shorthand `path`), or remove the block to use the per-entity " +
          "RA_* blocks.")

    Some(RaSheetConfig(
      paths = paths,
      sheetPattern = if (ra.hasPath("sheetPattern")) ra.getString("sheetPattern") else DEFAULT_PATTERN,
      requireColumns = if (ra.hasPath("requireColumns")) strList("requireColumns") else DEFAULT_REQUIRED,
      // `includeSheets` / `excludeSheets`, not `include` / `exclude`: `include` is a HOCON KEYWORD,
      // so a bare `include = [...]` makes the whole file fail to parse. The fields keep the short
      // names; only the conf keys carry the suffix.
      include = strList("includeSheets"),
      exclude = strList("excludeSheets")))
  }
}

/** One sheet selected for loading. */
final case class RaSheet(path: String, sheet: String)

/** One sheet left out, with the reason — reported so "why is my new entity missing?" is answerable. */
final case class RaSheetSkipped(path: String, sheet: String, reason: String)

/** Outcome of the selection: what will be read, and what was passed over and why. */
final case class RaSheetSelection(selected: Seq[RaSheet], skipped: Seq[RaSheetSkipped]) {

  /** One log line: every sheet accounted for, in one place. */
  def summary: String = {
    val loaded = if (selected.isEmpty) "none" else selected.map(s => s.sheet).mkString(", ")
    val left = if (skipped.isEmpty) "" else
      skipped.map(s => s"${s.sheet} (${s.reason})").mkString("; skipped: ", ", ", "")
    s"RA sheets selected: $loaded$left"
  }
}

/**
 * Finds the RA sheets to read instead of having them named in the code.
 *
 * The entity is NOT the sheet name: every RA sheet carries a `PERIMETER` column, the frames are
 * unioned by name, and PARAMETRAGE decides which perimeters produce matrices. So the reader only
 * needs to know WHICH SHEETS to read — which the workbook can answer itself. A business team adding
 * `RA_XYZ` then needs no code change and no conf change.
 *
 * Two gates, deliberately: the NAME must match the pattern, and the CONTENT must carry the RA key
 * columns ([[missingColumns]], applied by the caller once the sheet is read). Either alone is too
 * weak — real workbooks hold tabs like `Inputs RA ->` whose name says one thing and whose content
 * says another.
 */
object RaSheetDiscovery {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * List the sheet names of one workbook.
   *
   * Uses spark-excel's own [[WorkbookReader]] — already a dependency, and it reads through the
   * Hadoop `Configuration`, so a local path and an HDFS path behave identically, exactly as every
   * other input read here does. It is STREAM-based, so it cannot write back to the workbook: an
   * input file is never modified by being inspected (POI's `WorkbookFactory.create(File, ...)`
   * offers no such guarantee — do not use it on inputs).
   */
  def sheetNames(path: String, hadoopConf: Configuration): Seq[String] =
    WorkbookReader(Map("path" -> path, "location" -> path), hadoopConf).sheetNames

  /**
   * Pure selection: given what each workbook contains, decide what to read.
   *
   * @param perWorkbook (path, sheet names) in the configured path order
   */
  def select(perWorkbook: Seq[(String, Seq[String])], cfg: RaSheetConfig): RaSheetSelection = {
    val excluded = cfg.exclude.map(norm).toSet
    val included = cfg.include.map(norm).toSet

    val selected = Vector.newBuilder[RaSheet]
    val skipped = Vector.newBuilder[RaSheetSkipped]
    // Sheet names already taken, so the first workbook in `paths` wins a duplicate. Names are
    // compared case-insensitively: the same entity is spelled `RA_BCEF` in one file and `ra_ls` in
    // another, and loading a perimeter twice would double its rows in the union.
    val taken = scala.collection.mutable.Map.empty[String, String]

    perWorkbook.foreach { case (path, sheets) =>
      sheets.foreach { sheet =>
        val key = norm(sheet)
        val wanted =
          if (excluded.contains(key)) Left("excluded in the configuration")
          else if (included.contains(key)) Right(())
          else if (cfg.pattern.findFirstIn(sheet.trim).isDefined) Right(())
          else Left(s"name does not match ${cfg.sheetPattern}")

        wanted match {
          case Left(reason) => skipped += RaSheetSkipped(path, sheet, reason)
          case Right(_) =>
            taken.get(key) match {
              case Some(firstPath) =>
                skipped += RaSheetSkipped(path, sheet, s"already loaded from $firstPath")
              case None =>
                taken(key) = path
                selected += RaSheet(path, sheet)
            }
        }
      }
    }

    RaSheetSelection(selected.result(), skipped.result())
  }

  /**
   * The second gate: does the sheet actually carry the RA key columns?
   *
   * Returns the missing ones (empty = keep the sheet). Column names are compared case-insensitively
   * and trimmed, because a header cell may carry stray whitespace from Excel.
   */
  def missingColumns(columns: Seq[String], required: Seq[String]): Seq[String] = {
    val have = columns.map(norm).toSet
    required.filterNot(c => have.contains(norm(c)))
  }

  /** Discover across every configured workbook, tolerating one that cannot be opened. */
  def discover(cfg: RaSheetConfig, hadoopConf: Configuration): RaSheetSelection = {
    val perWorkbook = cfg.paths.map { path =>
      try path -> sheetNames(path, hadoopConf)
      catch {
        case ex: Throwable =>
          // A workbook that is absent or unreadable must not abort the run: the same tolerance the
          // per-entity reader had (a missing entity file is skipped with a warning). If NOTHING can
          // be read, the caller raises — one unreadable file among several is not fatal by itself.
          log.warn(s"RA workbook '$path' could not be opened (${ex.getClass.getSimpleName}: ${ex.getMessage}); skipped")
          path -> Seq.empty[String]
      }
    }
    select(perWorkbook, cfg)
  }

  /** Case-insensitive, whitespace-trimmed comparison key for a sheet or column name. */
  private def norm(s: String): String =
    Option(s).getOrElse("").trim.toUpperCase(java.util.Locale.ROOT)
}
