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
[`ConsistencyCheckMapper.apply`](../../src/main/scala/com.bnp.str.tseadfwd/consistency/ConsistencyCheckMapper.scala),
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

### Negative — three conditions, all of which must hold

`EAD_RA_RATE` is a cumulative product of `(1 - RA_i)`. A factor turns negative only when
`RA_i > 1`, and one such factor flips the running product's sign for the rest of the curve. So a
negative in the output needs **all three** of:

**(a) A period whose loss exceeds the whole outstanding.** With `RA = -(RA_STAT + RA_FI + RE) / CRD`
and `CRD` negative, `RA > 1` means `(RA_STAT + RA_FI + RE) > |CRD|`. This is the *run-off cliff*: in
the deep tail the book amortises toward zero, so `|CRD|` collapses, and if the numerator does not
shrink with it the ratio explodes. [`OPEN_QUESTIONS`](OPEN_QUESTIONS.md) Q26 records exactly this
with constant `RA_FI`/`RE` in the v2 vintage, and Q30 the resulting blow-up — `INVEST -389` — which
is why the freeze guard exists at all. Q7 notes a second contributor: the CRD window
(`M[3q-2..3q]`) and the RA window (`M[3q-4..3q-1]`) are offset by about a month, so at a
discontinuity they straddle it differently and can push the ratio over on their own.

**(b) `parameters.allow_negative_ead_ra_rate = true`.** Two independent guards otherwise stop it
reaching the output, and the flag lifts both together — deliberately, since relaxing either alone
changes nothing:

* `PrimaryView.computeRa` truncates the series at the first `RA >= RUNOFF_RA_CAP` (1.0), so no
  factor is ever `<= 0` and the product cannot turn negative;
* `vectorFactored` backstops it anyway — `if (acc < 0.0 && !emitNegative) 1.0`.

**(c) The offending period must be inside the computed horizon.** `computeRa` stops at
`COMPUTED_HORIZON_Y` (30y) *or* when a window falls off the end of the data, whichever comes first.

### Tested: the current inputs cannot produce one

Running `MainDriver` with `allow_negative_ead_ra_rate = true`:

| Input | Rows | `< 0` |
|---|---|---|
| `Inputs_RA_v4.xlsx` (current) | 12 240 | **0** |
| `Inputs_RA_v2.xlsx` (the vintage Q26/Q30 describe) | 3 060 | **0** |

Condition (a) is nowhere near met. Recovering each period's `RA` from consecutive output terms
(`1 - EAD[t]/EAD[t-1]`), the **highest `RA` anywhere is 0.097** — a tenfold margin below the cliff
at 1.0.

Condition (c) also blocks it independently: with 361 monthly columns the last fully-windowed
quarterly term is **29.75**, and the curve is held flat from term 30 onward —

```
term 29.50  0.402790243
term 29.75  0.400841258   <- last computed
term 30.00  0.400841258   <- held flat from here
term 50.25  0.400841258
```

— so the computation stops one quarter short of term 30, which is where the historical blow-up sat.
The data runs out before the cliff.

**In short:** a negative needs a genuinely pathological input (a book amortising to nothing while its
FI/RE accruals stay flat), the flag switched on to see it, and enough months for the computation to
reach that far. Today none of the three holds, and the first is not close.

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

The consistency report for the same run reads **12 240 examined, 12 240 kept, 0 removed**, with CR01
and CR02 both `PASS`. So on this vintage nothing is removed by anything: filter 2 is switched on
(`exclude_ead_ra_rate_ge_1 = true`) but has nothing to act on, because a curve starts below 1 at
term 0 and decays.

## 5. If the removal were wanted anyway

It would be a small change in one place, but it needs a decision first, not a patch:

1. **A business decision.** Q11/Q12 settled that CR02 reports and keeps. Reversing that is their
   call, and it changes the shape of a delivered term structure.
2. **The code.** One predicate in `ConsistencyCheckMapper.apply`, alongside the existing filter —
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
