package com.bnp.str.tseadfwd

import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

/**
 * One local SparkSession shared by every tseadfwd suite — starting a session per suite makes the
 * run several times longer for no benefit, and two live sessions in a JVM fight over the temp dirs.
 */
trait SparkTestSession { this: Suite =>
  @transient implicit lazy val spark: SparkSession = SparkTestSession.instance
}

object SparkTestSession {

  lazy val instance: SparkSession = {
    val session = SparkSession.builder()
      .appName("tseadfwd-tests")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session
  }
}
