package com.bnp.str.ageing

import com.bnp.str.ageing.utility.PrimaryConstants._
import com.bnp.str.ageing.writer.PrimaryWriter
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}

/**
 * Unit tests for how `PrimaryWriter` resolves the CSV destination: taken from
 * `output.macroeconomic.csv_path` when configured, otherwise derived from the workbook path so
 * that configs predating the key keep working.
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.ageing.PrimaryWriterSpec
 */
class PrimaryWriterSpec extends AnyFunSuite with Matchers with SparkTestSession with BeforeAndAfterAll {

  import spark.implicits._

  private val workDir = Paths.get("target", "test-out", "writer").toAbsolutePath

  override def beforeAll(): Unit = {
    super.beforeAll()
    fs.delete(new Path(path("")), true)
    Files.createDirectories(workDir)
  }

  private def fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  private def path(name: String): String = workDir.resolve(name).toString.replace('\\', '/')

  private def config(entries: String*): Config = ConfigFactory.parseString(
    s"""$APP_CONF {
       |  ${entries.mkString("\n  ")}
       |}""".stripMargin)

  private def aged = Seq(("2025Q1", 1.0)).toDF(COL_DATE, "X")

  test("the CSV goes to the configured csv_path, independently of the workbook name") {
    val configured = path("elsewhere/scenarios-all.csv")
    implicit val c: Config = config(
      s"""$OUTPUT_PATH = "${path("book.xlsx")}"""",
      s"""$OUTPUT_CSV_PATH = "$configured"""")

    new PrimaryWriter().writeCsv(Seq("Central" -> aged))

    fs.exists(new Path(configured)) shouldBe true
    fs.exists(new Path(path("book.csv"))) shouldBe false // the derived name is NOT used
  }

  test("the CSV falls back next to the workbook when csv_path is not configured") {
    implicit val c: Config = config(s"""$OUTPUT_PATH = "${path("fallback/book.xlsx")}"""")

    new PrimaryWriter().writeCsv(Seq("Central" -> aged))

    fs.exists(new Path(path("fallback/book.csv"))) shouldBe true
  }

  test("the configured csv_path is honoured verbatim, extension included") {
    val configured = path("verbatim/aged_scenarios.txt")
    implicit val c: Config = config(
      s"""$OUTPUT_PATH = "${path("verbatim/book.xlsx")}"""",
      s"""$OUTPUT_CSV_PATH = "$configured"""")

    new PrimaryWriter().writeCsv(Seq("Central" -> aged))

    fs.exists(new Path(configured)) shouldBe true
  }
}
