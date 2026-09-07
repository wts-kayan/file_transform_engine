package com.bnp.str.utilities.audit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for `run_history.used_jar` name resolution — pure string work, no Spark session.
 *
 * What these pin: `used_jar` is always the jar's FILE NAME, never a path, and a YARN CLUSTER run
 * gets a real name instead of "UNKNOWN". In cluster mode the classloader only sees the `__app__.jar`
 * placeholder, and Spark does not always copy the application jar into `spark.jars`, so both of the
 * original fallbacks could come up empty at once. YARN's own distributed-cache record always carries
 * it.
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.utilities.audit.RunAuditJarSpec
 */
class RunAuditJarSpec extends AnyFunSuite with Matchers {

  private val Jar = "str-file-transform-engine-1.4.2-RELEASE.jar"
  private val Staging = s"hdfs://ns/user/sttengineihm/.sparkStaging/application_1773889567248_10449/$Jar"

  private def fromCache(conf: String*): Option[String] = RunAudit.appJarNameFromCache(conf)

  // ---- the YARN distributed-cache lookup (level 2) --------------------------

  test("the __app__.jar cache entry gives back the real jar NAME, not the URI it came from") {
    fromCache(s"$Staging#__app__.jar") shouldBe Some(Jar)
  }

  test("a jar already on HDFS resolves the same way") {
    fromCache(s"hdfs://ns/user/sttengineihm/lib/$Jar#__app__.jar") shouldBe Some(Jar)
  }

  test("only the entry tagged __app__.jar counts - a --jars dependency is never reported") {
    val withDeps = Seq(
      "hdfs://ns/user/x/lib/spark-excel.jar#spark-excel.jar",
      s"$Staging#__app__.jar",
      "hdfs://ns/user/x/lib/poi-ooxml.jar#poi-ooxml.jar").mkString(",")
    fromCache(withDeps) shouldBe Some(Jar)

    // no application entry at all: None, so the caller falls through rather than naming a dependency
    fromCache("hdfs://ns/user/x/lib/spark-excel.jar#spark-excel.jar") shouldBe None
  }

  test("the non-jar resources YARN localises alongside are ignored") {
    val withNonJars = Seq(
      "hdfs://ns/user/x/.sparkStaging/app/__spark_conf__.zip#__spark_conf__",
      "hdfs://ns/user/x/.sparkStaging/app/__spark_libs__.zip#__spark_libs__",
      s"$Staging#__app__.jar").mkString(",")
    fromCache(withNonJars) shouldBe Some(Jar)
  }

  test("nothing usable yields None, so detection falls through instead of guessing") {
    fromCache() shouldBe None
    fromCache("") shouldBe None
    fromCache("  ") shouldBe None
    // an entry with no source name behind the placeholder is not a name
    fromCache("#__app__.jar") shouldBe None
  }

  test("entries are whitespace-tolerant and the scheme is irrelevant to the NAME") {
    fromCache(s"  $Staging#__app__.jar  ") shouldBe Some(Jar)
    fromCache(s"s3a://bucket/lib/$Jar#__app__.jar") shouldBe Some(Jar)
    fromCache(s"viewfs://cluster/user/x/$Jar#__app__.jar") shouldBe Some(Jar)
    fromCache(s"file:///opt/app/$Jar#__app__.jar") shouldBe Some(Jar)
  }

  // ---- following the __app__.jar symlink (levels 1 and 2) -------------------

  /** What the container's filesystem would answer: the link resolves to the localized real file. */
  private val Localized = s"/hadoop/yarn/nm/usercache/lh60327/filecache/25008/$Jar"
  private def resolvesTo(target: String): String => String = _ => target

  test("YARN cluster: the __app__.jar placeholder is resolved through its symlink to the real name") {
    // exactly the case that produced UNKNOWN: the classloader reports only the placeholder
    RunAudit.jarNameFrom("__app__.jar", resolvesTo(Localized)) shouldBe Some(Jar)
    RunAudit.jarNameFrom("/data/1/yarn/nm/appcache/application_1/container_1/__app__.jar",
      resolvesTo(Localized)) shouldBe Some(Jar)
  }

  test("a real jar name is taken as-is - the link is followed ONLY for the placeholder") {
    // resolving unconditionally would rename a perfectly good jar wherever a deployment symlinks
    val wouldBeWrong = resolvesTo("/somewhere/else/decoy.jar")
    RunAudit.jarNameFrom(s"/apps/str/lib/$Jar", wouldBeWrong) shouldBe Some(Jar)
    RunAudit.jarNameFrom(s"/hadoop/yarn/nm/usercache/lh60327/filecache/25008/$Jar", wouldBeWrong) shouldBe Some(Jar)
  }

  test("an unresolvable placeholder yields None rather than the placeholder itself") {
    // getCanonicalPath returns the input unchanged when there is no link to follow
    RunAudit.jarNameFrom("__app__.jar", resolvesTo("__app__.jar")) shouldBe None
    RunAudit.jarNameFrom("__app__.jar", resolvesTo("/some/dir/__app__.jar")) shouldBe None
    RunAudit.jarNameFrom("__app__.jar", resolvesTo("")) shouldBe None
  }

  // ---- the name-only rule, shared by every level ----------------------------

  test("used_jar keeps the jar's FILE NAME, never the path around it") {
    // What the classloader reports on YARN. The directory is per-node and per-run - filecache/25008
    // is a NodeManager cache slot that gets reused - so only the name survives. The name still says
    // which BUILD ran.
    RunAudit.jarBaseName(s"/hadoop/yarn/nm/usercache/sttengineihm/filecache/25008/$Jar") shouldBe Jar
    RunAudit.jarBaseName(s"/apps/str/lib/$Jar") shouldBe Jar
    RunAudit.jarBaseName(s"C:\\build\\target\\$Jar") shouldBe Jar
    RunAudit.jarBaseName(Jar) shouldBe Jar
    RunAudit.jarBaseName("") shouldBe ""
    RunAudit.jarBaseName(null) shouldBe ""
  }
}
