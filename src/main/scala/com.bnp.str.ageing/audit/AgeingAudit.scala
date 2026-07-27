package com.bnp.str.ageing.audit

import com.bnp.str.ageing.utility.PrimaryConstants
import com.bnp.str.utilities.audit.RunAudit
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

object AgeingAudit {

  final val MODULE_NAME = "ageing"

  def start(config: Config, usedConf: String, scenarioNames: Seq[String])
           (implicit spark: SparkSession): RunAudit = {
    val appConf = PrimaryConstants.APP_CONF
    RunAudit.start(
      moduleName      = MODULE_NAME,
      auditConfig     = config.getConfig(s"$appConf.${PrimaryConstants.AUDIT}"),
      usedConf        = usedConf,
      projectionDates = asList(dateWindow(config, appConf)),
      scenarios       = asList(scenarioNames),
      baseFolderName  = optString(config, s"$appConf.${PrimaryConstants.OUTPUT_PATH}").map(baseName)
    )
  }

  private def dateWindow(cfg: Config, appConf: String): Seq[String] =
    Seq(
      optString(cfg, s"$appConf.${PrimaryConstants.INITIAL_AS_OF_DATE}"),
      optString(cfg, s"$appConf.${PrimaryConstants.AGEING_AS_OF_DATE}")
    ).flatten

  private def optString(cfg: Config, path: String): Option[String] =
    if (cfg.hasPath(path)) Some(cfg.getString(path)) else None

  private def asList(values: Seq[String]): Option[String] =
    if (values.nonEmpty) Some(values.map(v => "\"" + v + "\"").mkString("[", ", ", "]"))
    else None

  private def baseName(path: String): String =
    path.replace('\\', '/').split('/').last.replaceAll(PrimaryConstants.XLSX_EXTENSION_REGEX, "")
}
