package com.bnp.str.utilities.audit

import com.bnp.str.climatetables.utility.PrimaryConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
 * climatetables-specific wiring for the shared, MODULE-AGNOSTIC [[RunAudit]]. It pulls this module's
 * run_history metadata out of the run config so the driver only has to call [[start]], and keeps
 * climatetables config-key knowledge out of the driver:
 *
 *   projection_dates <- input_constants.projection_dates (one entry per projected year)
 *   scenarios        <- input_constants.scenarios (one entry per climate scenario), as `["a", "b", …]`
 *   base_folder_name <- output_tables.base_folder_name (the ICAAP_TR_ output base name)
 *
 * Each is optional: a missing key just leaves that (nullable) column empty. The `audit { }` block
 * itself is optional too - when absent the run is audited with the [[RunAudit]] defaults.
 */
object ClimatetablesAudit {

  final val MODULE_NAME = "climatetables"

  /**
   * Start the run_history audit for a climatetables run from its full application [[Config]].
   *
   * @param config   the parsed application config (root holds the `climate_tables_app { }` block)
   * @param usedConf path of the application.conf used (recorded as `used_conf`)
   */
  def start(config: Config, usedConf: String)(implicit spark: SparkSession): RunAudit = {
    val appConf = PrimaryConstants.APP_CONF
    val inputConstants = s"$appConf.${PrimaryConstants.INPUT_CONSTANTS}"
    RunAudit.start(
      moduleName      = MODULE_NAME,
      auditConfig     = optConfig(config, s"$appConf.audit").getOrElse(ConfigFactory.empty()),
      usedConf        = usedConf,
      projectionDates = optStringList(config, s"$inputConstants.${PrimaryConstants.PROJECTION_DATES}"),
      scenarios       = optStringList(config, s"$inputConstants.${PrimaryConstants.SCENARIOS}"),
      baseFolderName  = optString(config,
        s"$appConf.${PrimaryConstants.OUTPUT_TABLES}.${PrimaryConstants.BASE_FOLDER_NAME}")
    )
  }

  private def optConfig(cfg: Config, path: String): Option[Config] =
    if (cfg.hasPath(path)) Some(cfg.getConfig(path)) else None

  private def optString(cfg: Config, path: String): Option[String] =
    if (cfg.hasPath(path)) Some(cfg.getString(path)) else None

  /** Read a HOCON string list as `["a", "b", …]` - the run_history column format. */
  private def optStringList(cfg: Config, path: String): Option[String] =
    if (cfg.hasPath(path))
      Some(cfg.getStringList(path).asScala.map(s => "\"" + s + "\"").mkString("[", ", ", "]"))
    else None
}
