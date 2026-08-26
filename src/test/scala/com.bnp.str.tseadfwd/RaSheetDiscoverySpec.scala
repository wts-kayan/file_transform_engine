package com.bnp.str.tseadfwd

import com.bnp.str.tseadfwd.reader.{RaSheetConfig, RaSheetDiscovery}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for the RA sheet SELECTION — which sheets of a workbook are loaded.
 *
 * Pure: no Spark, no workbook. `select` is handed the sheet names a workbook contains, which is what
 * makes the interesting cases (a divider tab, the same entity in two files) cheap to state.
 *
 * Run (offline, via the ScalaTest runner on the test classpath):
 *   java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
 *        org.scalatest.tools.Runner -o -s com.bnp.str.tseadfwd.RaSheetDiscoverySpec
 */
class RaSheetDiscoverySpec extends AnyFunSuite with Matchers {

  private def cfg(paths: Seq[String] = Seq("a.xlsx"),
                  pattern: String = RaSheetConfig.DEFAULT_PATTERN,
                  include: Seq[String] = Nil,
                  exclude: Seq[String] = Nil) =
    RaSheetConfig(paths, pattern, RaSheetConfig.DEFAULT_REQUIRED, include, exclude)

  /** The sheets a real workbook carries: a divider tab first, then the entities. */
  private val realWorkbook = Seq("a.xlsx" -> Seq("Inputs RA ->", "RA_BCEF", "RA_BGL", "RA_BNL"))

  test("the entity sheets are selected and the divider tab is not") {
    val out = RaSheetDiscovery.select(realWorkbook, cfg())

    out.selected.map(_.sheet) shouldBe Seq("RA_BCEF", "RA_BGL", "RA_BNL")
    out.skipped.map(_.sheet) shouldBe Seq("Inputs RA ->")
    out.skipped.head.reason should include("does not match")
  }

  test("a sheet the business adds later is picked up with no change here") {
    val out = RaSheetDiscovery.select(Seq("a.xlsx" -> Seq("RA_BCEF", "RA_XYZ")), cfg())

    out.selected.map(_.sheet) should contain("RA_XYZ")
  }

  test("matching is case-insensitive, because the real files are not consistent") {
    // The production conf reads sheet `ra_ls` lowercase next to `RA_BCEF` uppercase.
    val out = RaSheetDiscovery.select(Seq("a.xlsx" -> Seq("ra_ls", "Ra_Fortis")), cfg())

    out.selected.map(_.sheet) shouldBe Seq("ra_ls", "Ra_Fortis")
  }

  test("the same sheet in two workbooks is loaded from the first one listed") {
    // RA_BCEF really does exist in both Inputs_RA_v3.xlsx and Inputs_RA.xlsx.
    val out = RaSheetDiscovery.select(
      Seq("v3.xlsx" -> Seq("RA_BCEF"), "all.xlsx" -> Seq("RA_BCEF", "RA_BGL")),
      cfg(paths = Seq("v3.xlsx", "all.xlsx")))

    out.selected shouldBe Seq(
      com.bnp.str.tseadfwd.reader.RaSheet("v3.xlsx", "RA_BCEF"),
      com.bnp.str.tseadfwd.reader.RaSheet("all.xlsx", "RA_BGL"))
    out.skipped.map(s => (s.path, s.sheet)) shouldBe Seq(("all.xlsx", "RA_BCEF"))
    out.skipped.head.reason should include("already loaded from v3.xlsx")
  }

  test("the duplicate rule ignores casing, so one entity is never read twice") {
    val out = RaSheetDiscovery.select(
      Seq("v3.xlsx" -> Seq("RA_BCEF"), "all.xlsx" -> Seq("ra_bcef")),
      cfg(paths = Seq("v3.xlsx", "all.xlsx")))

    out.selected.size shouldBe 1
    out.skipped.head.reason should include("already loaded")
  }

  test("exclude keeps a retired entity out, and says so") {
    val out = RaSheetDiscovery.select(realWorkbook, cfg(exclude = Seq("RA_BNL")))

    out.selected.map(_.sheet) shouldBe Seq("RA_BCEF", "RA_BGL")
    out.skipped.find(_.sheet == "RA_BNL").get.reason should include("excluded")
  }

  test("include loads a sheet the pattern would have left out") {
    val out = RaSheetDiscovery.select(Seq("a.xlsx" -> Seq("PERIMETER_LS")), cfg(include = Seq("PERIMETER_LS")))

    out.selected.map(_.sheet) shouldBe Seq("PERIMETER_LS")
  }

  test("exclude beats include — the way to be certain a sheet never loads") {
    val out = RaSheetDiscovery.select(realWorkbook,
      cfg(include = Seq("RA_BNL"), exclude = Seq("RA_BNL")))

    out.selected.map(_.sheet) should not contain "RA_BNL"
  }

  test("a workbook that lists no sheet contributes nothing and does not fail the selection") {
    val out = RaSheetDiscovery.select(Seq("gone.xlsx" -> Seq.empty[String]), cfg())

    out.selected shouldBe empty
    out.skipped shouldBe empty
  }

  test("every sheet is accounted for in the summary line") {
    val summary = RaSheetDiscovery.select(realWorkbook, cfg()).summary

    summary should include("RA_BCEF")
    summary should include("skipped: Inputs RA ->")
  }

  // ---- gate 2: the content, not the name ------------------------------------

  test("a sheet is an RA table only if it carries the key columns") {
    val raLayout = Seq("PERIMETER", "SEGMENT", "RATE_TYPE", "FWL_TYPE", "METRIC", "M1", "M2")

    RaSheetDiscovery.missingColumns(raLayout, RaSheetConfig.DEFAULT_REQUIRED) shouldBe empty
    RaSheetDiscovery.missingColumns(Seq("Comment", "Value"), RaSheetConfig.DEFAULT_REQUIRED) should
      contain allOf("PERIMETER", "METRIC")
  }

  test("a header cell with stray whitespace or odd casing still counts as present") {
    val messy = Seq(" perimeter ", "Segment", "RATE_TYPE", "fwl_type", "METRIC ")

    RaSheetDiscovery.missingColumns(messy, RaSheetConfig.DEFAULT_REQUIRED) shouldBe empty
  }

  // ---- configuration --------------------------------------------------------

  test("no RA block means the per-entity blocks are used") {
    RaSheetConfig.from(ConfigFactory.parseString("tseadfwd_app { RA_BCEF { path = p, sheetNames = s } }")) shouldBe None
  }

  test("the block is read with defaults for everything but the workbooks") {
    val c = RaSheetConfig.from(ConfigFactory.parseString(
      """tseadfwd_app { RA { paths = ["a.xlsx", "b.xlsx"] } }""")).get

    c.paths shouldBe Seq("a.xlsx", "b.xlsx")
    c.sheetPattern shouldBe RaSheetConfig.DEFAULT_PATTERN
    c.requireColumns shouldBe RaSheetConfig.DEFAULT_REQUIRED
    c.include shouldBe empty
    c.exclude shouldBe empty
  }

  test("a single workbook may be given as `path`") {
    RaSheetConfig.from(ConfigFactory.parseString(
      """tseadfwd_app { RA { path = "a.xlsx" } }""")).get.paths shouldBe Seq("a.xlsx")
  }

  test("a block naming no workbook is an error, not a silent fallback") {
    // Defaulting here would read nothing and look exactly like "the business added no entity".
    val ex = intercept[IllegalArgumentException](RaSheetConfig.from(ConfigFactory.parseString(
      """tseadfwd_app { RA { sheetPattern = "^RA.*" } }""")))

    ex.getMessage should include("names no workbook")
  }
}
