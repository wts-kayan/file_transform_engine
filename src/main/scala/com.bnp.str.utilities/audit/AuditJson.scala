package com.bnp.str.utilities.audit

/**
 * Tiny dependency-free JSON writer used by the run-audit component so the `utilities`
 * package pulls in NO extra libraries (no Jackson/circe): the audit record is one compact,
 * single-line JSON object per run, which Spark's default `read.json` (JSONL) reads directly.
 *
 * Only what the audit needs is supported: flat objects of strings / numbers / booleans /
 * Option. Strings are escaped so free-text fields (error messages, stack traces) can hold
 * quotes, semicolons and newlines without corrupting the file — the main reason the audit
 * is stored as JSON rather than the project's usual ';'-delimited CSV.
 */
object AuditJson {

  /** Escape a string for embedding inside a JSON double-quoted literal. */
  def escape(s: String): String = {
    val sb = new StringBuilder(s.length + 16)
    s.foreach {
      case '"'  => sb.append("\\\"")
      case '\\' => sb.append("\\\\")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case c if c < 0x20 => sb.append("\\u%04x".format(c.toInt))
      case c    => sb.append(c)
    }
    sb.toString
  }

  /** A quoted, escaped JSON string literal. */
  def str(s: String): String = "\"" + escape(s) + "\""

  /** Render a single value as JSON (String -> quoted; number/boolean -> bare; None/null -> null). */
  def render(v: Any): String = v match {
    case null       => "null"
    case None       => "null"
    case Some(x)    => render(x)
    case s: String  => str(s)
    case b: Boolean => b.toString
    case n: Int     => n.toString
    case n: Long    => n.toString
    case n: Double  => n.toString
    case other      => str(other.toString)
  }

  /**
   * Build a compact single-line JSON object from ordered key/value pairs. Handy for callers
   * assembling the `params` snapshot, e.g. `AuditJson.obj("as_of" -> "2025Q4", "scale" -> 100)`.
   */
  def obj(fields: (String, Any)*): String =
    fields.map { case (k, v) => str(k) + ":" + render(v) }.mkString("{", ",", "}")
}
