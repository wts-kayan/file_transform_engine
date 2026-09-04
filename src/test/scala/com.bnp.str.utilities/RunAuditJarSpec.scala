package com.bnp.str.utilities.audit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for `run_history.used_jar` resolution — pure string work, no Spark session.
 *
 * The bug these pin: on YARN the classloader only sees the container-local copy of the jar
 * (`/hadoop/yarn/nm/usercache/<user>/filecache/25008/…`, or the bare `__app__.jar` placeholder), and
 * that path identifies nothing after the run — the NodeManager cache slot is per-node and gets
 * reused. `used_jar` has to carry the location the jar was UPLOADED to instead.
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.utilities.audit.RunAuditJarSpec
 */
class RunAuditJarSpec extends AnyFunSuite with Matchers {

  private val Jar = "str-file-transform-engine-1.4.2-RELEASE.jar"
  private val Staging = s"hdfs://ns/user/sttengineihm/.sparkStaging/application_1773889567248_10449/$Jar"

  private def resolve(localName: Option[String], conf: String*): Option[String] =
    RunAudit.resolveUploadedJar(localName, conf)

  test("YARN cluster: the __app__.jar placeholder resolves through its cache link to the upload URI") {
    resolve(Some("__app__.jar"), s"$Staging#__app__.jar") shouldBe Some(Staging)
  }

  test("YARN client: the container-local filecache copy resolves to the upload URI") {
    // what the classloader reports, and what used to be recorded verbatim
    val localised = s"/hadoop/yarn/nm/usercache/sttengineihm/filecache/25008/$Jar"
    resolve(Some(localised), s"$Staging#$Jar") shouldBe Some(Staging)
  }

  test("a jar already on HDFS is reported at the path it was submitted from, not the staging copy") {
    val onHdfs = s"hdfs://ns/user/sttengineihm/lib/$Jar"
    resolve(Some(Jar), onHdfs) shouldBe Some(onHdfs)
  }

  test("the APPLICATION jar is picked out of a --jars list, never a dependency") {
    val deps = Seq(
      "hdfs://ns/user/x/lib/spark-excel.jar",
      s"$Staging#__app__.jar",
      "hdfs://ns/user/x/lib/poi-ooxml.jar").mkString(",")

    resolve(Some("__app__.jar"), deps) shouldBe Some(Staging)
  }

  test("a local path is never reported as an upload location") {
    resolve(Some(Jar), s"file:///opt/app/$Jar") shouldBe None
    resolve(Some(Jar), s"/hadoop/yarn/nm/usercache/sttengineihm/filecache/25008/$Jar") shouldBe None
  }

  test("no match by name yields None - used_jar is then UNKNOWN, never a local path or a name") {
    resolve(Some("some-other-app.jar"), s"$Staging#__app__.jar") shouldBe None
    resolve(Some(Jar)) shouldBe None
    resolve(Some(Jar), "") shouldBe None
  }

  test("without a local name, a single distributed jar is taken and an ambiguous list is not") {
    resolve(None, Staging) shouldBe Some(Staging)
    resolve(None, s"$Staging,hdfs://ns/user/x/lib/poi-ooxml.jar") shouldBe None
    // the same jar named twice across two conf keys is not ambiguous
    resolve(None, Staging, s"$Staging#__app__.jar") shouldBe Some(Staging)
  }

  test("non-jar entries on the distributed cache are ignored") {
    // YARN localises the conf archive and __spark_libs__ through the same list
    val withNonJars = Seq(
      "hdfs://ns/user/x/.sparkStaging/app/__spark_conf__.zip#__spark_conf__",
      "hdfs://ns/user/x/.sparkStaging/app/__spark_libs__.zip#__spark_libs__",
      s"$Staging#__app__.jar").mkString(",")

    resolve(Some("__app__.jar"), withNonJars) shouldBe Some(Staging)
  }

  test("entries are whitespace-tolerant and any remote scheme counts as an upload") {
    resolve(Some("__app__.jar"), s"  $Staging#__app__.jar  ") shouldBe Some(Staging)
    val s3 = s"s3a://bucket/lib/$Jar"
    resolve(Some(Jar), s3) shouldBe Some(s3)
    val viewfs = s"viewfs://cluster/user/x/$Jar"
    resolve(Some(Jar), viewfs) shouldBe Some(viewfs)
  }
}
