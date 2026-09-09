package com.bnp.str.climatetables.utility

import org.slf4j.{Logger, LoggerFactory}

/**
 * Wrapper around the `require` that logs the failure first.
 *
 */
object Preconditions {

  private val defaultLogger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Checks `cond`. If it is false, logs `msg` at *error* level and throws
   * `IllegalArgumentException(msg)`.
   *
   * The method mirrors the signature of `require` so you can replace calls
   * with a single line change.
   *
   * @param cond   condition that must be true
   * @param msg    message for the exception (and the log)
   * @param logger optional logger - if omitted the default logger for this
   *               object is used
   */
  def requireLogged(cond: Boolean, msg: => String, logger: Logger = defaultLogger): Unit = {
    if (!cond) {
      logger.error(msg)                // log first
      throw new IllegalArgumentException(msg) // then fail
    }
  }
}
