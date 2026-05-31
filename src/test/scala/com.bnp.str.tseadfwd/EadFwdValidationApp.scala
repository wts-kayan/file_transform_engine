package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.reader.PrimaryReader
import com.bnp.str.tseadfwd.common.PrimaryRunner
import com.bnp.str.tseadfwd.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.io.Source

/**
 * Local validation harness: runs the real reader -> runner -> mapper pipeline against the
 * localRun sample, writes the output file, and prints a per-matrix/scenario max-abs-error
 * versus target_output/TS_EAD_FWD_25Q4_v1_small.csv.
 *
 * Run (deps are on the test classpath via spark-testing-base):
 *   mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        com.bnp.str.tseadfwd.EadFwdValidationApp
 */
object EadFwdValidationApp {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("ead-fwd-validate").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    implicit val conf = ConfigFactory.parseFile(new File("localRun/tseadfwd/application.conf")).resolve()

    val reader = new PrimaryReader()
    val df = new PrimaryRunner(reader, PrimaryConstants.OUTPUT_EAD_FWD).run_tseadfwd_runner()

    PrimaryUtilities.writeDataframe(df, PrimaryConstants.OUTPUT_EAD_FWD)
    println(">>> output written to localRun/tseadfwd/output/")

    val computed = df.collect().map(r =>
      (r.getString(0), r.getString(1), r.getString(2)) -> r.getString(3)).toMap

    val src = Source.fromFile("localRun/tseadfwd/target_output/TS_EAD_FWD_25Q4_v1_small.csv")
    val target = src.getLines().toVector.drop(1).map { l =>
      val a = l.split(";", -1); (a(0), a(1), a(2)) -> a(3)
    }.toMap
    src.close()
    println(s">>> computed rows: ${computed.size}   target rows: ${target.size}")

    def num(s: String): Double = s.replace(",", ".").toDouble
    def maxErr(mid: String, scen: String): Double = {
      var mx = 0.0
      for (k <- target.keys if k._1 == mid && k._2 == scen; v <- computed.get(k))
        mx = math.max(mx, math.abs(num(v) - num(target(k))))
      mx
    }
    val mids = target.keys.map(_._1).toSeq.distinct.sorted
    println(f"\n${"MATRIX_ID"}%-20s ${"C"}%10s ${"A"}%10s ${"O"}%10s ${"E"}%10s   (max abs error)")
    for (mid <- mids)
      println(f"$mid%-20s ${maxErr(mid,"C")}%10.2e ${maxErr(mid,"A")}%10.2e ${maxErr(mid,"O")}%10.2e ${maxErr(mid,"E")}%10.2e")
    spark.stop()
  }
}
