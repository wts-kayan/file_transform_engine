# Analysis — why `windowSpecAsOf` and `windowSpecAsOrig` were removed

> **Short answer:** they were not slow because ranking is slow. They were slow because the join they
> ranked could not be an equi-join. Every predicate is `portfolio === rule || rule === ""`, so
> Catalyst extracts no join key and plans a **`BroadcastNestedLoopJoin`**. Wildcard rules then make
> one portfolio row match many rules, the join emits all of them, and `row_number() == 1` sorts that
> exploded output only to throw almost all of it away — twice per run. The rule is now resolved on
> the **small side**, which is what `computeRatingDeterioration` next door already does.
>
> **Correction to the first attempt:** resolving on the small side as a `CASE` chain of one branch
> per rule (commit `179f8bf`) held only for the 40–120 rule fixtures it was tested on. The real
> `input_deterioration_pd` carries **~357 000 rules for a single scenario and year**, which
> overflowed the driver stack while building the expression tree. §3.1 has the trace. The shape that
> ships splits the rules by **wildcard pattern** and joins each pattern as the equality it is.

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
being matched. But the rule table can still be reduced against itself, and the wildcards can still be
turned into equalities. It took two attempts to get there.

### 3.1 What does not work: one `CASE` branch per rule

The first attempt collected the rules to the driver and folded them into a first-match-wins `CASE`
chain, lowest `rule_num` outermost — exactly the `row_number() == 1` semantics, with no ranking. On
the fixtures it was 4–10x faster than the join (§4). It does not survive the real input:

```
Resolve pd deterioration against 357071 rule(s) for scenario=DT, year=2030
file_transform_engine failed: null
java.lang.StackOverflowError: null
    at org.apache.spark.sql.catalyst.trees.TreeNode.$colon$colon(TreeNode.scala:1270)
    at org.apache.spark.sql.catalyst.expressions.TernaryExpression.children(TernaryExpression.scala:804)
    at org.apache.spark.sql.catalyst.trees.TreeNode.getDefaultTreePatternBits(TreeNode.scala:90)
```

`foldRight` over 357 071 rules builds a `CaseWhen` nested 357 071 deep, and the first recursive walk
over that tree exhausts the driver stack — before a single row is read. Depth was only the first
problem: even flattened, a chain evaluates its branches one after another, so the resolution stays
`|portfolio| x |rules|` work — the same product the `BroadcastNestedLoopJoin` was doing, just moved
into a projection.

A rule table that size is not "broadcast-sized in expression form". It has to be **indexed**, which
means a hash join, which means an equality.

### 3.2 What works: one equality join per wildcard pattern

The predicates are not equalities *as a set*, but each individual rule only constrains the columns it
has filled in. Group the rules by **which** columns those are — their wildcard pattern — and within
one group every rule constrains the same columns, so matching that group **is** an equi-join on them.
There are six columns, so at most 64 patterns, and in practice a handful.

```scala
val reducedRules = reducePdDeteriorationRules(inputDeteriorationPdFiltered)
  .persist(StorageLevel.MEMORY_AND_DISK_SER)
val masks = pdDeteriorationMasks(reducedRules)
```

Cost now scales with the number of *patterns*, not the number of rules:

```
Resolve pd deterioration against 310401 reduced rule(s) in 5 wildcard pattern(s) for scenario=DT, year=2030
```

### 3.3 Reducing the rule side: only the lowest `rule_num` can ever win

Two rules with the same pattern and the same key tuple are interchangeable except in precedence, so
the higher `rule_num` is unreachable and can be dropped. `min` over a struct led by the `rule_num` is
an argmin, so the surviving candidate carries its own `value` and `cap`:

```scala
rules
  .filter(DET_RULE_KEY_COLUMNS.map(col(_).isNotNull).reduce(_ && _))   // null cell: never matches
  .groupBy(DET_RULE_KEY_COLUMNS.map(c => pdDeteriorationKey(c, c).as(c)): _*)
  .agg(min(struct(
    col(DET_RULE_NUM).cast("long").as(CANDIDATE_ORDER),
    col(DET_VALUE).as(CANDIDATE_VALUE),
    col(DET_CAP).as(CANDIDATE_CAP)
  )).as(PD_DET_CANDIDATE))
```

Grouping on all six columns at once reduces every pattern in one pass: inside a pattern the wildcard
cells are constant (`""`), so they add nothing to the grouping. It also removes the redundancy the
input carries — 357 001 rules reduce to 310 401 candidates on the synthetic sample of §4 — and only
reduced rows are ever broadcast.

This is what makes the joins **fan-out-free**: at most one candidate per row per pattern, so the row
explosion §2.2 measured cannot happen, and no ranking is needed to undo it.

### 3.4 Picking the winner across patterns

A row ends up with one candidate column per pattern, null where that pattern did not match.
`array_min` over them keeps the lowest `rule_num`: struct ordering compares the leading field, and
null elements are skipped.

```scala
joined.select(portfolio("*"), array_min(array(candidates: _*)).as(output))
```

The catch-all pattern (every cell a wildcard) is not joined at all. It matches every row, so it is a
constant; joining its single row would cost a `BroadcastNestedLoopJoin` — Catalyst folds a constant
join key away, so a synthetic key does not help — and with it the whole-stage codegen of the
pipeline. `pdDeteriorationCatchAll` reads that one row and emits it as a literal struct instead.

### 3.5 The resulting plan

One `BroadcastHashJoin` per pattern per resolution, no window, no sort and no shuffle on the
portfolio side; the only exchange left is the rule-side aggregation, on a table three orders of
magnitude smaller than the portfolio. `PdDeteriorationRuleSpec` pins the shape, so a change that
reintroduces ranking or a nested loop fails the build:

```scala
plan should include ("BroadcastHashJoin")
plan should not include "Window"
plan should not include "SortMergeJoin"
plan should not include "BroadcastNestedLoopJoin"
```

## 4. Measured effect

Two measurements, because the two rewrites were tested at different scales.

**The join + `row_number()` shape against the `CASE` chain** — 400 000-row portfolio, `local[4]`,
warm run. The rule counts are the ones the fixtures used, and the ones the chain could handle:

| rules | join + window | `CASE` chain | |
|---|---|---|---|
| 40 | 5.2 s | 1.2 s | **4.3x** |
| 120 | 11.1 s | 1.1 s | **10x** |

The old shape scales with the rule count — it is doing `|portfolio| x |rules|` work. Extrapolating
that product to the real 357 071 rules gives about 1.4 x 10^11 predicate evaluations per resolution,
twice per run: the reason neither of these two shapes was ever going to run on the real input.

**The pattern-split shape at the real rule-table size** — same 400 000-row portfolio, `local[4]`,
357 001 rules across 5 wildcard patterns reducing to 310 401 candidates
([`PdDeteriorationBenchApp`](../../src/test/scala/com.bnp.str.climatetables/PdDeteriorationBenchApp.scala)):

| | reduce + resolve, both resolutions |
|---|---|
| cold | 14.4 s |
| warm | **6.9 s** |

That is the whole step: the rule-side aggregation, ten broadcast joins (five patterns x two
resolutions) and the two `array_min` projections, over 400 000 rows. The synthetic rule distribution
is a guess at the real one — what the number shows is that the step is bounded by the pattern count,
which the run logs, and no longer by the 357 071 rules.

## 5. What had to be preserved, and is

The rewrite is only worth anything if it is bit-for-bit equivalent. Four behaviours are subtle enough
to be lost by accident, so each has a test:

| Behaviour | Old mechanism | Kept how |
|---|---|---|
| Empty rule cell is a wildcard | `rule === ""` is true, so the disjunction is true | the column is left out of the pattern's join key |
| **Null** rule cell never matches | `col === null` is null, `null === ""` is null, `null AND …` is not true, so the join emits nothing | the rule is dropped before reduction |
| Null *portfolio* cell matches only a wildcard | same three-valued logic, on the other side | an equi-join key never matches on null |
| `pd_model` compares six characters only | `substr(1, 6) === substr(1, 6)` | `pdDeteriorationKey` truncates both the grouping key and the portfolio key |
| No match yields a typed null | left join | still a left join; `value` and `cap` keep the rule table's own types through the candidate struct |

The third row is worth reading twice. `PDMODEL_A` and `PDMODEL_B` both truncate to `PDMODE`, so the
matcher cannot tell them apart. That is **existing** behaviour, deliberately preserved and now pinned
by a test — but if the rule table relies on `pd_model_short_name` values that differ only past
character 6, it is not doing what it looks like it does. Worth a separate business check.

## 6. Latent issue surfaced, not fixed

The sort key was `_w0 ASC NULLS FIRST`. A `rule_num` that does not parse as a long casts to **null**,
and null orders **first** — so a malformed rule silently outranks every valid one. It is also
indistinguishable from a left-join miss, which produces the same all-null rule row. `min` and
`array_min` order nulls first too, so the behaviour is unchanged, deliberately.

Changing the ordering would change results, so the code reports it instead — counted on the reduced
table, i.e. only the malformed rules that actually win something:

```scala
val unparsableRuleNums = reducedRules
  .filter(col(s"$PD_DET_CANDIDATE.$CANDIDATE_ORDER").isNull)
  .count()
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

It holds both shapes — `previousShape` is the join + `row_number()` code kept verbatim — and one of
its cases runs 20 000 rules through the current shape and requires the two to agree, which is well
past the point where the `CASE` chain of §3.1 stopped building.

The rule-scale timings of §4 come from a `main`:

```
java -cp "target/classes;target/test-classes;$(cat cp.txt)" \
     com.bnp.str.climatetables.mapping.PdDeteriorationBenchApp
```

It generates a 400 000-row portfolio and a 357 001-rule table in five wildcard patterns, then times
`reducePdDeteriorationRules` + both `resolvePdDeterioration` calls cold and warm. Change the pattern
list at the top to match a real rule table once its wildcard distribution is known.
