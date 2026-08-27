# Analysis — does anything remove `EAD_RA_RATE <= 0` from the CSV output?

> **Short answer: no.** Nothing in the engine removes a row because its `EAD_RA_RATE` is negative or
> zero. Exactly one value-based row filter exists, and it works at the **top** of the range
> (`>= 1`), not the bottom. A non-positive rate is **reported and kept** — deliberately, and by
> business decision.

Traced on `main` at the merge of ticket 977, against
`localRun/tseadfwd/output/TS_EAD_FWD_25Q4_v1_small.csv`.

---

## 1. Every way a row can leave the output

The mapper emits **every** computed term ([`PrimaryMapper.matrixRows`]) — it filters nothing. Row
removal happens once, downstream, in
[`CoherenceCheckMapper.apply`](../../src/main/scala/com.bnp.str.tseadfwd/coherence/CoherenceCheckMapper.scala),
on the frame `MainDriver` is about to write. There are three operations, in this order:

| # | Operation | Keys on | Removes a row? |
|---|---|---|---|
| 1 | **CR01** `dropGroups` | a whole `(EAD_MATRIX_ID, SCENARIO_ID)` group whose **every** term `== 1` | **yes** — the whole group |
| 2 | **`exclude_ead_ra_rate_ge_1`** | `rate >= 1.0` | **yes** — that row |
| 3 | **CR02** `markNegativeValues` | `rate < 0`, or `<= 0` with `includeZero` | **no** — replaces the *value*, keeps the line |

```scala
// 1. CR01 — by value 1, and only when EVERY term of the curve is 1
val afterCr01 = if (removeCr01 && keys.nonEmpty) dropGroups(df, …) else df

// 2. the only value-based row filter in the engine. Note the direction: >= 1.
val filtered = if (cfg.excludeEadRaRateGe1) afterCr01.where(rate.isNull || rate < lit(1.0)) else afterCr01

// 3. CR02 — the row survives; only the cell is overwritten, and only if a marker is configured
val cleaned = if (markNegatives && replaced > 0L) markNegativeValues(filtered) else filtered
```

### The predicate that looks like it removes, and does not

Grepping for `<= 0` finds exactly one hit, and it is easy to misread:

```scala
private def cr02Hit: Column =
  if (cfg.negativeIncludesZero) rate <= lit(0.0) else rate < lit(0.0)
```

That is CR02's **detection** predicate, and it is used in three places — none of which removes a row
from the written frame:

| Use | What it does |
|---|---|
| `filtered.where(cr02Hit).count()` | counts, for the report; `filtered` is unchanged |
| `df.where(rate.isNotNull && cr02Hit)` | builds the report's listing, off the input frame |
| `when(cr02Hit, marker)` in `markNegativeValues` | overwrites the cell, keeps the row |

The frame that reaches `PrimaryWriter` is `cleaned`, which descends from `filtered` — and `filtered`
was never narrowed by `cr02Hit`.

None of the three removal operations tests for `<= 0` as a reason to drop a line. Filter 2 is the closest thing, and it
keeps everything **below** 1 — including every negative and every zero. It also explicitly keeps a
`null` rate (`rate.isNull ||`), on the stated grounds that an unparseable value is a finding to look
at rather than a row to disappear.

## 2. Why `<= 0` is reported and not removed

This is a decision, not an oversight, and it is recorded in two places.

**In the rule itself** — `CheckRule.NegativeEadRaRate` carries `removes = false`, with the reason:
*"the business wants to see it, not to have it silently disappear."* The report lists the offending
rows; the curve keeps its shape.

**In the business answers** — `OPEN_QUESTIONS_977` Q11/Q12 widened CR02 to fire on zero
(`includeZero = true`) while leaving it reporting-only. The design's own wording for CR02 is that
the line is kept because *"the term exists, and dropping it would leave a hole in the curve"*. A
term structure with terms missing in the middle is harder for a consumer to reason about than one
carrying an implausible number that the report names.

## 3. Can a non-positive value even reach the CSV?

Mostly not, by construction — which is worth knowing before anyone writes a filter for it.

**Negative: impossible with the default settings**, for two independent reasons:

* `PrimaryView.computeRa` stops the series at the first period where `RA >= RUNOFF_RA_CAP` (1.0), so
  every `(1 - RA)` factor stays positive and the cumulative product never turns negative;
* `vectorFactored` has a backstop anyway — `if (acc < 0.0 && !emitNegative) 1.0`.

Both are lifted only by `parameters.allow_negative_ead_ra_rate = true`, which exists precisely so a
negative can be *seen*. Turning it on to then delete the rows would defeat the switch.

**Zero: possible in principle, through two paths that have nothing to do with the business meaning.**

* **Rounding.** The output is written by `fmtNumber(value, 9)` — `HALF_UP` at nine decimals. Any true
  value below `5e-10` prints as `0`. The row is a legitimate, strictly positive deep-tail exposure
  that *renders* as zero.
* **`NaN` / infinity.** `fmtNumber` opens with `if (value.isNaN || value.isInfinite) return "0"`. A
  non-finite result is written to the CSV as `0`.

Both matter for this question: CR02 reads the **string** in the CSV, so it would flag either as
`zero exposure factor (exposure fully run off)` — which is the right thing to flag but the wrong
explanation. A remove-on-zero rule would silently delete them.

## 4. What the current output actually contains

| | rows |
|---|---|
| `< 0` | **0** |
| `== 0` | **0** |
| `0 < v < 1` | **12 240** |
| `>= 1` | **0** |

Smallest value anywhere: `0.38316` (`BNL_MORTGAGE_TF_Y` / `A`, terms 29+) — four orders of magnitude
clear of the rounding path in §3.

The coherence report for the same run reads **12 240 examined, 12 240 kept, 0 removed**, with CR01
and CR02 both `PASS`. So on this vintage nothing is removed by anything: filter 2 is switched on
(`exclude_ead_ra_rate_ge_1 = true`) but has nothing to act on, because a curve starts below 1 at
term 0 and decays.

## 5. If the removal were wanted anyway

It would be a small change in one place, but it needs a decision first, not a patch:

1. **A business decision.** Q11/Q12 settled that CR02 reports and keeps. Reversing that is their
   call, and it changes the shape of a delivered term structure.
2. **The code.** One predicate in `CoherenceCheckMapper.apply`, alongside the existing filter —
   `filtered.where(rate.isNull || rate > lit(0.0))` — plus `removes = true` on the rule and a config
   flag mirroring `exclude_ead_ra_rate_ge_1` (`exclude_ead_ra_rate_le_0`, off by default). The
   report already counts and lists what it drops, so the reporting side needs nothing new.
3. **What it would cost.** Holes mid-curve for any consumer reading the term structure as a
   sequence; and, per §3, the deletion of rows whose zero is a *rendering* artefact rather than a
   real one. If removal is wanted, filtering on the computed `Double` before `fmtNumber` — rather
   than on the string afterwards — would at least not conflate the two.

## 6. Verdict

No code removes `<= 0` lines. The only value-based removal is `>= 1`, at the opposite end of the
range, and it is on but idle in this vintage. Non-positive rates are reported by CR02 and kept on
purpose, negatives cannot occur at all without an explicit switch, and the zeros that could occur
are as likely to be a nine-decimal rounding artefact as a real run-off.
