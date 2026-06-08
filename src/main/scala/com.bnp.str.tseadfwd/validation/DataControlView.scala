package com.bnp.str.tseadfwd.validation

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/** Severity levels for a single technical-control check. */
object Severity {
  val Pass = "PASS"
  val Warn = "WARN"
  val Fail = "FAIL"
}

/**
 * Outcome of one input data-quality check run before the calculation.
 *
 * @param name     short check id (e.g. "RA.labels")
 * @param severity one of [[Severity]] (PASS / WARN / FAIL)
 * @param detail   human-readable explanation, including the offending values when relevant
 */
final case class ControlCheck(name: String, severity: String, detail: String) {
  def failed: Boolean = severity == Severity.Fail
  def warned: Boolean = severity == Severity.Warn
}

/** Rendering + persistence of the consolidated DATA CONTROL report. */
object DataControlView {

  /** Multi-line, log-friendly report block. */
  def renderReport(checks: Seq[ControlCheck]): String = {
    val fails = checks.count(_.failed)
    val warns = checks.count(_.warned)
    val verdict = if (fails > 0) Severity.Fail else if (warns > 0) Severity.Warn else Severity.Pass
    val lines = checks.map(c => f"  [${c.severity}%-4s] ${c.name} - ${c.detail}")
    s"DATA CONTROL - $verdict (${checks.size} checks: $fails FAIL, $warns WARN)\n" +
      lines.mkString("\n")
  }

  /** Machine-readable CSV body (one row per check). */
  def toCsv(checks: Seq[ControlCheck]): String = {
    def esc(s: String): String = "\"" + s.replace("\"", "\"\"") + "\""
    val header = "check,severity,detail"
    (header +: checks.map(c => Seq(c.name, c.severity, c.detail).map(esc).mkString(","))).mkString("\n")
  }

  /** Write the CSV report to `dir/fileName`, creating the directory if needed. Returns the path. */
  def writeCsv(dir: String, fileName: String, checks: Seq[ControlCheck]): String = {
    val path = Paths.get(dir, fileName)
    Option(path.getParent).foreach(Files.createDirectories(_))
    Files.write(path, toCsv(checks).getBytes(StandardCharsets.UTF_8))
    path.toString
  }
}
