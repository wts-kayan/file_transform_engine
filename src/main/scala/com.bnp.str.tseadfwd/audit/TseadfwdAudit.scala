package com.bnp.str.tseadfwd.audit

import com.bnp.str.tseadfwd.utility.PrimaryConstants
import com.bnp.str.utilities.audit.RunAudit
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
 * tseadfwd-specific wiring for the shared, MODULE-AGNOSTIC [[RunAudit]]. It pulls this module's
 * run_history metadata out of the run config so the driver only has to call [[start]], and keeps
 * tseadfwd config-key knowledge out of both the driver and the shared audit package:
 *
 *   projection_dates <- parameters.as_of_date_quarter (the as-of / projection start quarter)
 *   scenarios        <- MACRO_VARIABLE.sheetNames (one sheet per scenario), as `["a", "b", …]`
 *   base_folder_name <- TS_EAD_FWD.tableName (the output table / base name)
 *
 * Each is optional: a missing key just leaves that (nullable) column empty.
 */
object TseadfwdAudit {

  final val MODULE_NAME = "tseadfwd"

  /**
   * Start the run_history audit for a tseadfwd run from its full application [[Config]].
   *
   * @param config   the parsed application config (root holds the `tseadfwd_app { }` block)
   * @param usedConf path of the application.conf used (recorded as `used_conf`)
   */
  def start(config: Config, usedConf: String)(implicit spark: SparkSession): RunAudit = {
    val appConf = PrimaryConstants.APP_CONF
    RunAudit.start(
      moduleName      = MODULE_NAME,
      auditConfig     = config.getConfig(s"$appConf.audit"),
      usedConf        = usedConf,
      projectionDates = optString(config, s"$appConf.parameters.as_of_date_quarter"),
      scenarios       = optStringList(config, s"$appConf.${PrimaryConstants.MACRO_VARIABLE}.sheetNames"),
      baseFolderName  = optString(config, s"$appConf.${PrimaryConstants.OUTPUT_EAD_FWD}.tableName")
    )
  }

  private def optString(cfg: Config, path: String): Option[String] =
    if (cfg.hasPath(path)) Some(cfg.getString(path)) else None

  /** Read a HOCON string list as `["a", "b", …]` — the run_history `scenarios` column format. */
  private def optStringList(cfg: Config, path: String): Option[String] =
    if (cfg.hasPath(path))
      Some(cfg.getStringList(path).asScala.map(s => "\"" + s + "\"").mkString("[", ", ", "]"))
    else None
}
