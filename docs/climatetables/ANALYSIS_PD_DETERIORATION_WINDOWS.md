# Analysis — why `windowSpecAsOf` and `windowSpecAsOrig` were removed

> **Short answer:** they were not slow because ranking is slow. They were slow because the join they
> ranked could not be an equi-join. Every predicate is `portfolio === rule || rule === ""`, so
> Catalyst extracts no join key and plans a **`BroadcastNestedLoopJoin`**. Wildcard rules then make
> one portfolio row match many rules, the join emits all of them, and `row_number() == 1` sorts that
> exploded output only to throw almost all of it away — twice per run. The rule is now resolved on
> the **small side**, which is what `computeRatingDeterioration` next door already does.

Traced on branch `perf/climatetables-pd-deterioration`, commit `179f8bf`, against
[`PrimaryMapper.computePdDeterioration`](../../src/main/scala/com.bnp.str.climatetables/mapping/PrimaryMapper.scala).
Spark 3.5.4, Scala 2.12.18. All plans and timings below were produced by replaying the old and new
shapes side by side on synthetic data — the equivalence half of that harness now lives in
[`PdDeteriorationRuleSpec`](../../src/test/scala/com.bnp.str.climatetables/PdDeteriorationRuleSpec.scala).

---

## 1. What the code used to do

Two identical blocks, one resolving the rule *as of* the projection date and one *at origination*:

```scala
val windowSpecAsOf = Window
  .partitionBy(col("id_technique"))
  .orderBy(col("det_as_of.__det_rule_num__").cast("long").asc)

val withPdDeteriorationAsOf = dfDistinct.join(
  inputDeteriorationPdFiltered.as("det_as_of").hint("broadcast"),
  (
    (col("pd_model").substr(1, 6) === col("det_as_of.__det_pd_model_short_name__").substr(1, 6) ||
      col("det_as_of.__det_pd_model_short_name__") === "") &&
    (col("cd_pays_residence_cal") === col("det_as_of.__det_geographical_breakdown__") ||
      col("det_as_of.__det_geographical_breakdown__") === "") &&
    /* … four more of the same shape … */
  ),
  "left"
)
  .withColumn("__row_num__", row_number().over(windowSpecAsOf))
  .filter(col("__row_num__") === 1)
  .select(dfDistinct("*"),
    col("det_as_of.__det_value__").as("__det_value__"),
    col("det_as_of.__det_cap__").as("__det_cap__"))
```

The intent is a **priority rule table**: an empty cell in `input_deterioration_pd` is a wildcard
("this column does not constrain the match"), several rules can match one exposure, and the one with
the lowest `rule_num` wins. Join-then-rank is the textbook way to express that in Spark, and it is
correct. It is the *plan* it produces that is the problem.

## 2. Why it was expensive

### 2.1 No predicate is an equality, so there is no join key

Catalyst can only build a hash join from conjunctions of equalities. Here every conjunct is a
**disjunction** — `col === rule OR rule === ""` — and a disjunction yields no extractable key. The
`.hint("broadcast")` does not rescue this: it only chooses the build side. The physical plan is:

```
BroadcastNestedLoopJoin BuildRight, LeftOuter, ((… OR …) AND (… OR …) AND (… OR …))
```

Every portfolio row is evaluated against **every** rule. Cost is `|portfolio| × |rules|` predicate
evaluations, per resolution, so twice per `(date, scenario)` pair — and the driver runs those pairs
in parallel.

### 2.2 The join explodes the row count before the window shrinks it

This is the part that is easy to miss when reading the code. Because wildcards make rules overlap by
design, the left join does not emit one row per exposure — it emits one row **per matching rule**.
Measured on a 400 000-row portfolio:

| rules after the scenario/year filter | rows entering the window | fan-out |
|---|---|---|
| 40 | 2 298 705 | ×5.7 |
| 120 | 6 246 766 | ×15.6 |

`row_number() == 1` then discards 82 % / 94 % of what the join just built.

### 2.3 The ranking sorts the exploded rows, not the surviving ones

Spark 3.5 helps here, and it is worth being precise about how much. `WindowGroupLimit` *is* applied,
so a partial top-1 runs before the shuffle and only the winners cross the network:

```
Filter (__row_num__ = 1)
+- Window [row_number() … ], [id_technique], [_w0 ASC NULLS FIRST]
   +- WindowGroupLimit [id_technique], [_w0], row_number(), 1, Final
      +- Sort [id_technique, _w0]                     <- sort #2, on the reduced rows
         +- Exchange hashpartitioning(id_technique, 8)
            +- WindowGroupLimit …, Partial
               +- Sort [id_technique, _w0]            <- sort #1, on ALL 6.2M exploded rows
                  +- Project [ …, cast(__det_rule_num__ as bigint) AS _w0 ]
                     +- BroadcastNestedLoopJoin …
```

The partial limit sits **above** a full sort of the exploded output. So per resolution: one sort of
6.2M rows, one shuffle, one more sort.

One thing that was *not* a problem, for the record: the second window gets **no `Exchange`**.
`BroadcastNestedLoopJoin` preserves the streamed side's partitioning, so `withPdDeteriorationAsOf` is
still hash-partitioned on `id_technique` and only the sort is redone. `windowSpecAsOf` was the
expensive one of the pair.

### 2.4 The cast was on the wrong side of the join

Visible in the plan above as `Project [ …, cast(__det_rule_num__ as bigint) AS _w0 ]`, sitting
*above* the join. `__det_rule_num__` is a constant of the tiny rule table, but writing the cast
inside `orderBy` made it run once per **exploded row** — 6.2M times instead of 120.

## 3. Why the fix is "resolve on the small side"

[`computeRatingDeterioration`](../../src/main/scala/com.bnp.str.climatetables/mapping/PrimaryMapper.scala)
already does the right thing one step earlier in the same pipeline: it ranks `input_deterioration_rating` **by
itself**, on the small side, and only then equi-joins the winner onto the portfolio.

```scala
val windowSpec = Window
  .partitionBy(col("original_ctpr_id"), col("rating_scale"))
  .orderBy(col("rule_num").cast("long"))

val inputDeteriorationRatingFiltered = input_deterioration_rating
  .filter(…)
  .withColumn("row_num", row_number().over(windowSpec))
  .filter(col("row_num") === 1)   // <- ranking on ~dozens of rows, not on the portfolio
```

`computePdDeterioration` cannot copy that literally, because "the lowest matching `rule_num`" is not
well-defined on the rule table alone — with wildcards, *which* rule wins depends on the portfolio row
being matched. But the same principle applies: the rule table is broadcast-sized, so bring it to the
driver and turn it into an expression.

```scala
val orderedRules: Array[Row] = inputDeteriorationPdFiltered
  .orderBy(col(DET_RULE_NUM).cast("long").asc)
  .collect()

val withPdDeteriorationAsOf = dfDistinct
  .withColumn(PD_DET_AS_OF, resolvePdDeterioration(orderedRules, PD_DET_AS_OF_KEYS, matchType))
  .select(dfDistinct("*"),
    col(s"$PD_DET_AS_OF.value").as("__det_value__"),
    col(s"$PD_DET_AS_OF.cap").as("__det_cap__"))
```

`resolvePdDeterioration` folds the rules — already ordered by `rule_num` — into a first-match-wins
`CASE` chain, so the outermost `when` is the lowest `rule_num`. That is precisely what
`row_number() == 1` ordered on `rule_num` computes, without ranking anything.

### 3.1 The step that produces most of the win

Once the rule side is a known constant, a wildcard predicate can be **deleted at build time** instead
of evaluated per row:

```scala
Option(rule.getAs[String](ruleColumn)) match {
  case Some("")    => None                                   // wildcard: drop the predicate
  case Some(value) if ruleColumn == DET_PD_MODEL_SHORT_NAME =>
    Some(col(portfolioColumn).substr(1, 6) === lit(value).substr(1, 6))
  case Some(value) => Some(col(portfolioColumn) === lit(value))
  case None        => Some(lit(false))                       // null cell: rule can never match
}
```

A rule with four wildcard columns collapses from six disjunctions to two literal equalities. The
generated code then short-circuits per row, which is why the new shape is flat in the rule count
rather than linear.

### 3.2 The resulting plan

No join, no window, no exchange, no sort — the whole resolution folds into the projection that was
already there. `PdDeteriorationRuleSpec` asserts this directly, so a future change that reintroduces
a shuffle here fails the build:

```scala
plan should not include "Join"
plan should not include "Window"
plan should not include "Exchange"
```

## 4. Measured effect

400 000-row portfolio, `local[4]`, warm run (second execution, past JIT and codegen):

| rules | before | after | |
|---|---|---|---|
| 40 | 5.2 s | 1.2 s | **4.3×** |
| 120 | 11.1 s | 1.1 s | **10×** |

The old shape scales with the rule count — it is doing `|portfolio| × |rules|` work. The new one does
not. Two caveats on these figures:

- the sample is synthetic, and the multiplier depends on how sparse the real
  `input_deterioration_pd` is once filtered on scenario and year;
- if that filtered table ever holds thousands of rules, the `CASE` chain gets deep enough to exceed
  Spark's 64 KB codegen limit and fall back to interpreted evaluation — still correct, just slower.
  The rule count is now logged on every run so this is visible:
  `Resolve pd deterioration against N rule(s) for scenario=…, year=…`.

## 5. What had to be preserved, and is

The rewrite is only worth anything if it is bit-for-bit equivalent. Four behaviours are subtle enough
to be lost by accident, so each has a test:

| Behaviour | Old mechanism | Kept how |
|---|---|---|
| Empty rule cell is a wildcard | `rule === ""` is true, so the disjunction is true | the predicate is dropped |
| **Null** rule cell never matches | `col === null` is null, `null === ""` is null, `null AND …` is not true, so the join emits nothing | the rule condition becomes `lit(false)` |
| `pd_model` compares six characters only | `substr(1, 6) === substr(1, 6)` | `col(…).substr(1, 6) === lit(value).substr(1, 6)` |
| No match yields a typed null | left join | `lit(null).cast(matchType)`, where `matchType` is read off the rule table's own schema |

The third row is worth reading twice. `PDMODEL_A` and `PDMODEL_B` both truncate to `PDMODE`, so the
matcher cannot tell them apart. That is **existing** behaviour, deliberately preserved and now pinned
by a test — but if the rule table relies on `pd_model_short_name` values that differ only past
character 6, it is not doing what it looks like it does. Worth a separate business check.

## 6. Latent issue surfaced, not fixed

The sort key was `_w0 ASC NULLS FIRST`. A `rule_num` that does not parse as a long casts to **null**,
and null sorts **first** — so a malformed rule silently outranks every valid one. It is also
indistinguishable from a left-join miss, which produces the same all-null rule row.

Changing the ordering would change results, so the code reports it instead:

```scala
if (unparsableRuleNums > 0) {
  log.warn(s"$unparsableRuleNums deterioration pd rule(s) have a non-numeric rule_num; " +
    s"they take precedence over every valid rule. Check input_deterioration_pd.")
}
```

## 7. Deliberately left alone

`val dfDistinct = df.distinct()`, the first line of the method. It plans as
`Exchange hashpartitioning(<every column>)` — a full shuffle of the whole portfolio keyed on all of
its columns, on the wide FEM+TIERS frame. On the same sample it cost **0.9–1.2 s against 0.3 s for
the entire deterioration computation**, i.e. more than everything this document is about.

`dropDuplicates("id_technique")` would shuffle one narrow key instead, but it is not semantically
equivalent — it keeps an arbitrary row among duplicates, where `distinct()` requires full-row
equality. Narrowing it needs a uniqueness argument about the upstream frame that this change does not
have. Left as a separate follow-up.

## 8. Reproducing the measurements

The equivalence harness ships as a test:

```
mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt -DincludeScope=test
mvn -o test-compile
java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
     org.scalatest.tools.Runner -o -s com.bnp.str.climatetables.mapping.PdDeteriorationRuleSpec
```

It holds both shapes — `previousShape` is the join + `row_number()` code kept verbatim — so the
timing comparison can be rebuilt from it by swapping the fixtures for a `spark.range` of the desired
size and calling `.count()` on each.
