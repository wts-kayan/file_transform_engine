# Open Questions & Assumptions — EAD FWD

This log records the points the specification left ambiguous. For each one, the engine
currently uses the answer **chosen to reproduce the target file**
(`target_output/TS_EAD_FWD_25Q4_v1_small.csv`). Please confirm or override each.

Legend: ✅ chosen answer reproduces the target · ⚠️ judgement call, please confirm ·
⛔ blocked on data (cannot be reproduced from current inputs).

> **STATUS 2026-06-04 — target file under review.** The user has flagged that
> `target_output/TS_EAD_FWD_25Q4_v1_small.csv` may **not** be authoritative (it was built from
> the old RA/scenario vintage). Items below whose only evidence was "matches the target" are
> therefore *provisional* until a trusted reference exists; items confirmed by an explicit
> **user/business DECISION** stand regardless. New inputs received this session:
> `Inputs_RA_v2.xlsx` (RA, full precision) and `Scenario_EAD_FWD.xlsx` (per-scenario sheets).

> How to use: add your own questions at the bottom under **New questions**. When you
> answer one above, tick `Confirmed` and note any change needed.

---

## Q1 — Does `EAD_MATRIX_ID` include `RATE_TYPE`? ✔️ ANSWERED
- **Spec says:** `PERIMETER + "_" + SEGMENT + "_" + RATE_TYPE + "_Q/Y"`.
- **Target sample shows:** `BCEF_CONSO_Q` (no `RATE_TYPE`).
- **DECISION (user, keep spec):** include `RATE_TYPE` → `PERIMETER_SEGMENT_RATETYPE_(Q|Y)`,
  e.g. `BCEF_CONSO_TF_Q`.
- **Consequence:** output ids deliberately **differ from the sample target** ids (which
  dropped `RATE_TYPE`); the numeric values are unchanged. Distinct rate types (TF/TV) now
  form separate matrices, and the RA lookup is keyed by rate type.
- **Code:** `MatrixDef.matrixId`, `parseParametrage` group key, `collectRa`/`aggregateSegments`.
- Confirmed: [x]

## Q2 — Aggregated segment name ✔️ ANSWERED
- **Chosen:** when `AGGREGATION = YES`, use `AGGREGATED_SEGMENT_NAME` (so `INVEST_PRO` +
  `INVEST_CORP` → `INVEST`). Matches target (`BCEF_INVEST_*`).
- **DECISION (user):** keep using `AGGREGATION` / `AGGREGATED_SEGMENT_NAME`.
- **Why required:** INPUTS_RA has `INVEST_PRO` and `INVEST_CORP` separately (no `INVEST`
  row); the target has a single `INVEST`. These two columns are the only bridge — without
  them the output would be `BCEF_INVEST_PRO_TF_*` + `BCEF_INVEST_CORP_TF_*` (no `INVEST`),
  diverging from the target. (If a future RA vintage already contains pre-aggregated
  segments, the columns become a no-op and the mapping is 1:1.)
- Confirmed: [x]

## Q3 — FWL flag for an **aggregated** matrix ✔️ ANSWERED
- **Problem:** constituents disagree — `INVEST_PRO` = `NO`, `INVEST_CORP` = `YES`.
- **DECISION: `FWL = YES if ANY constituent is YES`** → `INVEST` is FWL=YES.
- **Evidence (target):** `BCEF_INVEST_Q` scenarios differ at 202/203 terms (FWL=YES
  behaviour). "ALL must be YES" would make INVEST identical across scenarios → ruled out.
- **Code:** `PrimaryMapper.parseParametrage` → `group.exists(_._6.equalsIgnoreCase(YES))`.
- **Open refinement (non-blocking):** the engine sums `PRO + CORP` and shocks the whole
  combined series; strictly only the FWL=YES constituent (`CORP`) should be shocked. This
  affects the shock *magnitude*, not whether INVEST varies — tied to the `ref_shock`
  calibration (Q12) and the stress-data vintage, so revisit then.
- Confirmed: [x]

## Q4 — Emit both Q and Y for every matrix? ✔️ ANSWERED
- **Spec:** `computation_frequency` selects `_Q` *or* `_Y`.
- **Target shows:** both `_Q` and `_Y` for every matrix.
- **DECISION (user):** always emit **both** Q and Y, like the target.
- **Code:** `getDataFrame` → `for { freq <- Seq(Quarterly, Yearly) }`.
- Confirmed: [x]

## Q5 — `FWL_TO_BE_APPLIED` source ✔️ ANSWERED
- **DECISION (user):** read directly from PARAMETRAGE column `FWL_TO_BE_APPLIED`
  (`YES` → scenario shock; `NO` → all scenarios = Central). Config-driven, no hardcoding.
- **Code:** `parseParametrage` → `group.exists(_._6.equalsIgnoreCase(YES))`; consumed in
  `matrixRows` (`centralRa` vs `scenarioRa`).
- **PARAMETRAGE** (`PARAMETRAGE_corrected.xlsx`), columns: `PERIMETER ; SEGMENT ;
  RATE_TYPE ; AGGREGATION ; AGGREGATED_SEGMENT_NAME ; FWL_TO_BE_APPLIED ; MACRO_VARIABLE ;
  PROJECTION_HORIZON`. Current BCEF rows:
  - **CONSO** — `BCEF ; CONSO ; TF ; NO ; <blank> ; YES ; IR_10Y_FR ; 3Y`.
  - **MORTGAGE** (row added; was absent) — `BCEF ; MORTGAGE ; TF ; NO ; <blank> ; YES ;
    IR_10Y_FR ; 3Y`.
  - **INVEST_PRO / INVEST_CORP** — both `AGGREGATION=YES → INVEST`, `FWL=YES`.
- **CONSO FWL — UPDATED 2026-06-02 (business confirmed `YES`):** CONSO is **forward-looking**,
  so `FWL_TO_BE_APPLIED = YES` (was temporarily set to `NO` to mirror the *sample* target, which
  shows CONSO identical across scenarios). The sample target is the stale vintage
  ([[ead-fwd-input-vintage-mismatch]]); with `YES` the engine applies the scenario shock and
  CONSO A/O/E **intentionally diverge from that sample**. Resolves the prior Q5/Q17 inconsistency.
- Confirmed: [x]

## Q6 — Core RA formula ✔️ ANSWERED
- **DECISION (user): confirmed.** `RA_i = -(RA_STAT_i + RA_FI_i + RE_i) / CRD_i` (BASELINE).
- **Evidence:** reproduces `BCEF_MORTGAGE_Q` Central to ~`1e-5` at low terms.
- **Code:** `PrimaryView.centralRa`.
- Confirmed: [x]

## Q7 — Period aggregation windows ✔️ ANSWERED
- **DECISION (user): confirmed.**
- **Quarterly:** RA metrics half-weight (`Q1 = M1 + M2/2`;
  `Qn = M[3n-4]/2 + M[3n-3] + M[3n-2] + M[3n-1]/2`); `CRD` = block mean of 3 months.
- **Yearly:** RA metrics = sum (Y1 = 6 months, Yn = 12); `CRD` = mean of window.
- **Evidence:** matches target at low terms; per-quarter residual oscillates in sign
  (data noise, not a window error).
- **Code:** `PrimaryView.aggregate`.
- Confirmed: [x]

## Q8 — Run-off handling when `CRD = 0` ✔️ ANSWERED
- **DECISION (user): confirmed.** `CRD = 0` → `RA = 0` (`VECTOR = 1`); the curve flattens
  instead of `0/0`.
- **Evidence:** the target plateaus when exposure runs off (e.g. CONSO ~term 16).
- **Code:** guards in `PrimaryView.centralRa` / `scenarioRa`.
- Confirmed: [x]

## Q9 — Flat-tail boundary ✔️ ANSWERED
- **DECISION (user): confirmed.** Compute to the last *computable* term, then hold flat to
  50/100. With 361 months that is term **29.75 (quarterly)** / **29 (yearly)** — the
  quarterly window for term 30 needs month 362. Grid sizes: 203 (Q), 52 (Y).
- **Note:** a longer input vintage moves the last-computed term to 30; flat behaviour unchanged.
- **Code:** `PrimaryView.computeRa` (stop condition) + `termSeries` (flat fill).
- Confirmed: [x]

## Q10 — Stress leg selection (FWL=YES) ⚠️ SUPERSEDED by Q33 (schema)
- **SUPERSEDED 2026-06-05:** the schema fixes the leg **by scenario** — Adverse/Extreme → STRESS(-),
  Optimistic → STRESS(+) — **not** by the macro-delta sign. The engine now selects the leg by
  scenario name (`PrimaryMapper.matrixRows`). The delta-sign rule below is retained for history.
- **Needed?** Yes — there are two stress legs (`STRESS (+)`/`STRESS (-)`); the engine must
  pick which to blend toward. The sign of the scenario's rate move selects it.
- **DECISION:** `delta < 0` → `STRESS (-)`; `delta ≥ 0` → `STRESS (+)`, where
  `delta = MACRO[scenario][projection_date] − MACRO[Central][projection_date]`.
- **Evidence (target, MORTGAGE_Q, direction):** `STRESS(-)−baseline` raises RA (+),
  `STRESS(+)−baseline` lowers it (−). Target Adverse/Extreme shocks are **+** (rates down →
  STRESS(-) ✓); target Optimistic shock is **−** (rates up → STRESS(+) ✓). All three match.
- **Caveat:** distinguishing Adverse from Extreme needs them to have different macro deltas
  (current file has them equal → Q15); magnitude is `ref_shock` (Q12). Neither is a flaw in
  this rule.
- **Code:** `PrimaryMapper.matrixRows` (leg chosen by `delta` sign).
- Confirmed: [x]

## Q11 — Macro shock source: single date vs path ✔️ ANSWERED
- **DECISION (user):** use the macro **path over a window** `shock_window_start..shock_window_end`
  (default `2021Q1..2025Q4`); **term 0 = window start, step 1 quarter** (yearly steps 1 year =
  4 quarters); past the window end the last delta is held. Replaces the single `projection_date`.
- **Why not a single date 2022Q1:** at 2022Q1 all scenarios are equal → delta = 0 → no shock.
  The path lets the delta ramp in (scenarios diverge from 2022Q3), giving a term-varying shock.
- **Result:** mechanism verified — shock ramps with the correct direction (Adverse/Extreme
  lower EAD, Optimistic higher). Numeric match unchanged vs target because the shock magnitude
  is still tiny (`ref_shock=1.0`, Q12) and the tail-vintage error dominates (Q15).
- **Config:** `tseadfwd_app.shock_window_start` / `shock_window_end`.
- **Code:** `PrimaryMapper.deltaPath` / `macroDeltaArray` / `shockWindow`; `PrimaryView.scenarioRa`.
- **UPDATE 2026-06-04:** with `Scenario_EAD_FWD.xlsx` (coverage **2025Q1..2028Q4**), the window was
  moved to `2025Q4..2028Q4`. The new scenario is **forward-looking** (no long history), so the
  window/term-0 anchor must be re-confirmed — see **Q25**. Mechanism unchanged.
- Confirmed: [x] (mechanism) · window anchor pending (Q25)

## Q12 — `ref_shock` calibration (FWL=YES shock magnitude) ✔️ RESOLVED (schema)
- **What it is:** the rate-shock magnitude the `STRESS (+)/(-)` legs represent;
  `weight = |delta| / ref_shock`, `fire_scen = fire_base + weight·(fire_stress − fire_base)`.
- **Calibration attempt (against target, MORTGAGE_Q Optimistic):** backed out the implied
  scaling `k = target_shock / (stress − baseline)` per term. It is **not constant** — it ramps
  `0.02 → 0.38 → 0.26` over terms 2–13. So **no single `ref_shock` reproduces the target shape**
  from the current data; the stress/baseline FI-RE of this RA vintage don't align with the
  target's (Q15), and Adverse=Extreme in the macro file.
- **Magnitude ballpark:** `ref_shock ≈ 0.05–0.08` (Optimistic ≈ 0.082) gets the right order;
  current `1.0` is ~100× too small (shock ≈ off).
- **DECISION:** keep `ref_shock = 1.0` (shock effectively off, documented) until the matching
  RA + scenario vintage arrive, then re-run this calibration (the `k` curve should flatten).
  Alternative on request: set `≈ 0.07` for a visible-but-approximate shock.
- See [`MISSING_INPUTS.md`](../MISSING_INPUTS.md).
- **UPDATE 2026-06-04:** corrected inputs (`Inputs_RA_v2.xlsx` + `Scenario_EAD_FWD.xlsx`) have now
  arrived, so the calibration *can* be re-run — **but** it needs a trusted reference output, which
  the current target may not be (see status note). Blocked on a trusted target, not on inputs.
- **RESOLVED 2026-06-05 (`Schema_EAD_FWD_20260601_v4.xlsx`):** there is **no `ref_shock` knob**. The
  schema fixes the shock magnitude as `RA_FI_RE_scen = RA_FI_RE_base − (ShockFI+ShockRE)·Rate/100`,
  where `Rate = (Macro_scen − Macro_Central)·100`, i.e. the magnitude is the per-term macro delta
  times the stress-vs-baseline detail difference. `ref_shock` removed from the engine
  (`scenarioRa` no longer takes it). The remaining judgement is the ×`Rate/100` itself — see the
  interpretation note in Q33.
- Confirmed: [x] (magnitude defined by the schema; no calibration parameter)

## Q13 — Macro-delta scaling (×100?) ✔️ ANSWERED
- **Spec STEP 3:** `Rate = (MACRO_scen − MACRO_central) × 100`.
- **DECISION (user):** keep the delta **raw**; the ×100 (and any unit scaling) is absorbed by
  `ref_shock`, so only one knob (Q12) needs calibrating. Mathematically equivalent.
- **Consequence:** `ref_shock` is expressed in raw-rate units (e.g. `0.0065` = 65 bps), not
  ×100 units. The Q12 ballpark `≈0.07` is in these raw units.
- **Code:** `PrimaryMapper.macroDeltaArray` (no ×100); `PrimaryView.scenarioRa` weight.
- Confirmed: [x]

## Q14 — Scenario `S` (Secto): out of scope? ⚠️ (merges former Q24)
- **Context:** not implemented — no Secto data in the scenario file, no `S` rows in the target.
- **Ask:** confirm Secto is **out of scope** for this delivery. It is a one-line addition
  (`"Secto" -> "S"` in the scenario-code map) once Secto data exists.
- Confirmed: [ ]

## Q15 — Adverse vs Extreme differentiation ✔️ RESOLVED
- **Finding:** in the current scenario file Adverse and Extreme are **byte-identical** at every
  quarter and macro variable, so the engine cannot produce distinct A vs E.
- **Evidence (BCEF_MORTGAGE_Q):**
  - Target wants them different: term 1 A=`0.935146`, E=`0.942747`; term 10 A=`0.612215`, E=`0.620127`.
  - Our output forces them equal: term 1 A=E=`0.941924`; term 10 A=E=`0.628110`.
  - Cause — scenario file `IR_10Y_FR`: Adverse=Extreme=`0.006/0.006/0.007` at 2023Q4/2024Q4/2025Q4.
- **Mechanism:** scenarios are differentiated only by `delta = MACRO[scen] − MACRO[Central]`;
  equal macro ⇒ equal delta ⇒ equal output **by construction**. Independent of `ref_shock`
  (it scales A and E equally). Not a code bug.
- **Direction hint from target:** Extreme EAD > Adverse EAD (less loss) ⇒ Extreme's `IR_10Y_FR`
  must sit **closer to Central** than Adverse's (smaller `|delta|`). Exact values must come from
  the source file.
- **Needed:** the scenario file version where Adverse ≠ Extreme. Engine handles them
  independently already — it just needs different numbers.
- **RESOLVED 2026-06-04 — corrected file received.** `Scenario_EAD_FWD.xlsx` makes Adverse ≠
  Extreme. On `IR_10Y_FR` the deltas vs Central now ramp to **A ≈ −0.0048** and **E ≈ −0.0068**
  by 2028Q4 (Optimistic ≈ +0.0030), so the engine emits **distinct A/O/E** (verified: INVEST &
  MORTGAGE diverge across all four scenarios). Note the new file makes Extreme's `|delta|`
  **larger** than Adverse's (Extreme further from Central ⇒ more loss) — the opposite of the old
  *sample-target* hint above, which is moot now the target is under review.
- Confirmed: [x] (engine produces distinct A vs E from the new file; magnitude now fixed by the
  schema's `Rate/100`, no `ref_shock` — Q12. Verified: term-10 INVEST C/O/A/E all distinct.)

## Q16 — Output number format ✔️ ANSWERED
- **DECISION (user): confirmed.** `;` delimiter, decimal **comma**, `EAD_RA_RATE` half-up at
  9 dp with trailing zeros stripped, `TERM` like `0`, `0,25`, `100`. Matches the target
  byte-for-byte in format.
- **Code:** `PrimaryMapper.fmtNumber`; write via `PrimaryUtilities.writeDataframe` (`;`, header).
- Confirmed: [x]

---

## New questions (add yours here)

## Q17 — Spec contradiction: "no difference between scenarios" under FWL=YES ✔️ ANSWERED
- **Spec says:** both FWL=YES cases (3rd & 4th) end with *"There is no differences between
  scenarios — the same value will be considered for all scenarios."*
- **Problem:** that is the FWL=**NO** behaviour and contradicts the FWL=YES purpose (scenario
  shock) and the target, where FWL=YES matrices (`MORTGAGE`, `INVEST`) **do** vary by scenario.
  Reads like a copy-paste from the FWL=NO cases.
- **DECISION (business, 2026-06-02):** for `FWL=YES`, scenarios **DO differ** (scenario-shocked);
  that sentence applies only to the FWL=NO cases. Same call that confirmed CONSO=`YES` (see Q5).
- **Engine:** FWL=YES → scenario-shocked (scenarios differ). See [[ead-fwd-formula]].
- Confirmed: [x]

## Q18 — Role of `PROJECTION_HORIZON` (= "3Y" in PARAMETRAGE) ⚠️
- **Finding:** the column exists in PARAMETRAGE but is **not consumed** by the engine. The
  flat-tail boundary was derived from input length (term 29.75 Q / 29 Y, see Q9), not from this
  field; the grid runs to 50/100 regardless.
- **Ask:** does `PROJECTION_HORIZON` drive anything (e.g. cap the computed curve at 3Y then hold
  flat), or is it informational?
- Confirmed: [ ]

## Q19 — `EAD_MATRIX_ID` naming: spec vs target (canonical form for production) ⚠️
- **Context:** Q1 chose the spec form `PERIMETER_SEGMENT_RATETYPE_(Q|Y)`; the target sample drops
  `RATE_TYPE` (`BCEF_CONSO_Q`). Output IDs therefore deliberately differ from the sample.
- **Ask:** for production, is the spec naming (with `RATE_TYPE`) the canonical ID, or should we
  match the target file and omit it?
- See Q1 (answered for the build; this confirms the production-canonical choice).
- **EVIDENCE 2026-06-04 (BNP run):** the real BNP production output emits `BCEF_CONSO_TF_Q`,
  `BCEF_INVEST_TF_Q`, `BCEF_MORTGAGE_TF_Q` — i.e. **with `RATE_TYPE`**, matching the spec form, not
  the no-`RATE_TYPE` sample target. This independently supports keeping `RATE_TYPE` in the id.
- **Side effect:** the local `EadFwdValidationApp` keys on the target's no-`RATE_TYPE` ids, so its
  per-matrix error is a **no-op** (no key overlap → prints `0.00e+00` without comparing). The
  harness needs an id-mapping fix before its numbers mean anything (see **Q28**).
- Confirmed: [x] (production uses `RATE_TYPE`, per real BNP output)

## Q20 — Multiple rate types: separate matrices or combined? ⚠️
- **Context:** the sample only has `TF`. The engine treats each rate type as a separate matrix.
- **Ask:** when `TV` (and others) exist, should each rate type be its own matrix, or combined per
  segment?
- Confirmed: [ ]

## Q21 — FWL flag & shock scope for aggregated segments (INVEST = PRO + CORP) ⚠️
- **Context:** constituents disagree (`PRO`=NO, `CORP`=YES); Q3 set FWL=YES if any is YES (matches
  target). But the engine currently shocks the **combined** PRO+CORP series.
- **Ask:** for an aggregated matrix, should only the FWL=YES constituent (`CORP`) be shocked, or the
  whole aggregated series? This changes the shock magnitude.
- **Link:** tied to Q3 open refinement and `ref_shock` calibration (Q12).
- Confirmed: [ ]

## Q22 — `computation_frequency`: Q, Y, or both? ⚠️
- **Context:** Q4 decided to always emit both (matches target); spec text says the flag selects Q
  *or* Y.
- **Ask:** should output always contain both Q and Y, or is frequency selected per matrix by config?
- See Q4 (answered for the build; this confirms the production rule).
- Confirmed: [ ]

## Q23 — The term 100 point ⚠️
- **Context:** after 50 there is a single term 100 holding the flat value.
- **Ask:** is term 100 a genuine reporting point (perpetuity proxy) or a sentinel? Confirm the grid
  is `0.25…50` then `100` (Q) / `1…50` then `100` (Y).
- See Q9 (flat-tail boundary).
- Confirmed: [ ]

## Q24 — (merged into Q14) ✅
- Secto scope is now tracked in **Q14**. This number is retired to avoid a duplicate.

## Q25 — Scenario input is now a per-scenario Excel workbook + forward-looking window ⚠️ NEW
- **Change 2026-06-04:** the scenario moved from a single CSV (one table with a `scenario` column,
  history 1993Q1..2025Q4) to **`Scenario_EAD_FWD.xlsx` — one sheet per scenario** (`Central`,
  `Adverse`, `Optimistic`, `Extreme`, plus a `_NOTES` sheet) covering only **2025Q1..2028Q4**.
  Columns per sheet: lowercase `date`, `IR_10Y_BE/FR/IT`, `CPI_CORE_YOY_US`, `CPI_YOY_US`, and
  (Adverse/Optimistic only) `CRE_PRICE_YOY*`. `readScenarioFromExcelSheets` reads each sheet,
  renames `date`→`Date`, tags `scenario = sheet name`, and unions by name.
- **Ask 1 (format):** is one-sheet-per-scenario the canonical production layout (vs the old single
  table)? Sheet names map directly to `Scenario_ID` (Central→C, …). Confirm the sheet-name set.
- **Ask 2 (window/term-0):** for a forward-looking scenario starting 2025Q1, what is **term 0** —
  the first scenario quarter (2025Q1), the reporting date (2025Q4), or something else? This sets the
  `shock_window_start` and how the macro delta path lines up with projection terms (see Q11).
- **Ask 3 (extra macro vars):** the file carries CPI/CRE columns beyond `IR_10Y_FR`; only the
  PARAMETRAGE `MACRO_VARIABLE` per matrix is used. Confirm that's intended (extras ignored).
- **Code:** `PrimaryUtilities.readScenarioFromExcelSheets`; `PrimaryReader.macro_variable`; config
  `MACRO_VARIABLE.sheetNames`.
- Confirmed: [ ]

## Q26 — Run-off when `CRD` plateaus at a NON-zero residual + constant `RA_FI`/`RE` ⛔ NEW — IMPORTANT
- **Finding:** with `Inputs_RA_v2.xlsx`, MORTGAGE (and INVEST) carry **constant** `RA_FI`/`RE`
  (e.g. MORTGAGE `RA_FI = 18.414`, `RE = 11.048` for all 361 months) instead of the old file's
  **decaying** values (`18,13,11,…`). CONSO is unaffected (its `RA_FI`/`RE` are 0).
- **Effect:** as the book amortizes, `CRD` runs down to a **constant residual** (MORTGAGE ≈
  −1615.93), *not* to 0. The spec run-off rule (Q8) only fires at `CRD == 0`, so it never triggers.
  With a near-constant numerator (`RA_STAT` small + constant `RA_FI+RE` ≈ 88) divided by a shrinking
  `CRD`, `RA = −(STAT+FI+RE)/CRD` **balloons** (≈0.01 → 0.06) and the cumulative product keeps
  decaying (MORTGAGE → ~0.04 by term 30) instead of plateauing.
- **Two possible root causes — business must decide:**
  1. **Data:** the constant `RA_FI`/`RE` in v2 is an artifact (forward-filled) and should *decay*
     with the exposure like the old file. With decaying FI/RE the numerator shrinks with `CRD` and
     `RA` stays ~0.01 (bounded) — no new rule needed.
  2. **Rule:** the constant residual is intentional, and the engine needs a **new run-off rule** —
     freeze `EAD_RA_RATE` (or force `RA = 0`) once the exposure stops amortizing (`CRD` reaches a
     constant residual / `ΔCRD ≈ 0`), not only when `CRD == 0`.
- **Note:** this is the same singularity that produced the earlier BNP `−387` blow-up on the *old*
  file (there `CRD` got tiny and `RA` exploded). v2 keeps it bounded but still over-decaying.
- **Ask:** in real production data, are MORTGAGE/INVEST `RA_FI` and `RE` **constant** across all
  months, or do they **decay** with the balance? And should run-off freeze at a non-zero `CRD`
  residual? See also Q18 (`PROJECTION_HORIZON`) — could a horizon cap be the intended plateau?
- **Code:** `PrimaryView.centralRa` / `scenarioRa` run-off guard (`if (c == 0.0) 0.0`).
- Confirmed: [ ]

## Q27 — Input label / locale robustness (label vocabulary across environments) ⚠️ NEW
- **Finding:** real inputs vary the key-column text by environment/locale, which silently broke the
  `(SEGMENT, RATE_TYPE, FWL_TYPE, METRIC)` lookup until hardened:
  - French-locale POI emits a **non-breaking space** (U+00A0) inside `STRESS (+)` / `STRESS (-)`.
  - `METRIC` appears as `RA_STAT`/`RA_FI` (underscore) in some files vs `RA STAT`/`RA FI` (space).
  - A BNP `PrimaryConstants` had the stress labels mis-typed as `BASELINE (+)/(−)` instead of
    `STRESS (+)/(−)` — which canonicalized to `BASELINE+/BASELINE-` and made every non-Central
    scenario empty until corrected.
- **Mitigation in engine:** `PrimaryMapper.canon()` now uppercases and strips all non-`[A-Z0-9+-]`
  (spaces, NBSP/NNBSP, underscores, parentheses) on both the map keys and the lookup tuple, plus
  `logRaKeyDiagnostics` logs the present FWL_TYPE/METRIC vocabulary and warns on a missing stress leg.
- **Ask:** confirm the canonical label vocabulary (`BASELINE`, `STRESS (+)`, `STRESS (-)`; `CRD`,
  `RA STAT`, `RA FI`, `RE`) and that aggressive normalization is acceptable (it cannot, by design,
  catch a *semantically wrong* label like the `BASELINE (+)` typo — only spacing/case/underscore).
- **Code:** `PrimaryMapper.canon`, `collectRa`, `aggregateSegments`, `logRaKeyDiagnostics`.
- Confirmed: [ ]

## Q28 — Canonical names across environments + fix the validation harness ⚠️ NEW
- **Finding (BNP vs sample):** the same logical inputs differ by name across environments:
  - `RATE_TYPE`: `TF` (sample/local) vs `ITF` (BNP).
  - `MACRO_VARIABLE`: `IR_10Y_FR` (local PARAMETRAGE) vs `IR_10Y_FRI` (BNP PARAMETRAGE).
  - `SEGMENT`: `INVEST_PRO` (local/v2) vs `INVEST_PROD` (BNP).
  - package/namespace differs (`com.bnp.str…` vs `com.bnpparibas.itg.fresh.str…`).
- **Ask 1:** are these per-environment naming conventions expected (so the id legitimately becomes
  `BCEF_*_ITF_*` on BNP), or should there be a canonical mapping (e.g. `ITF→TF`)?
- **Ask 2 (harness):** `EadFwdValidationApp` compares against the sample target's no-`RATE_TYPE` ids
  and so currently compares nothing (Q19). Fix it to map ids (strip/insert `RATE_TYPE`) so the error
  table is meaningful — once a trusted reference output exists.
- Confirmed: [ ]

## Q29 — Perimeter & product scope (BCEF only, or BGL/BNL/FORTIS/BPOST/BPLS too?) ⚠️ NEW
- **Context:** `PrimaryReader` currently reads **only `RA_BCEF`** (RA_BGL/BNL/FORTIS/LS are commented
  out). But `PARAMETRAGE_corrected` defines many more rows:
  - **BCEF** — CONSO, INVEST_PRO, INVEST_CORP, MORTGAGE (FWL=YES, macro `IR_10Y_FR`).
  - **BGL / BNL** — MORTGAGE, FWL=YES, macro `IR_10Y_BE` / `IR_10Y_IT`.
  - **FORTIS / BPOST** — MORTGAGE/INVEST, FWL=NO, macro `NONE`.
  - **~50 BPLS rows** — numeric segment codes (e.g. `10276`), **blank `RATE_TYPE`**, FWL=NO, macro `NONE`.
- **Ask 1 (scope):** is this delivery **BCEF-only**, or must the other perimeters be produced? If so,
  their RA Excel inputs (`RA_BGL`, `RA_BNL`, …) are needed (currently absent / reader disabled).
- **Ask 2 (macro mapping):** confirm the per-perimeter macro variable — BCEF→`IR_10Y_FR`,
  BGL→`IR_10Y_BE`, BNL→`IR_10Y_IT`. Each referenced column must exist in the scenario workbook
  (`Scenario_EAD_FWD.xlsx` carries FR/BE/IT, so cross-currency is feasible).
- **Ask 3 (BPLS / no rate type):** how should rows with a **numeric segment** and **no `RATE_TYPE`**
  be named and handled? FWL=NO, so no shock — but the id form needed deciding.
- **DECISION (user 2026-06-04):** drop a blank `RATE_TYPE` from the id (join only non-empty parts) →
  **`BPLS_10276_Q`** (single underscore), not `BPLS_10276__Q`. Ids with a rate type are unchanged
  (`BCEF_CONSO_TF_Q`). Scope (which perimeters) and the macro mapping (Asks 1–2) remain open.
- **Code:** `MatrixDef.matrixId` (`Seq(perimeter, outSegment, rateType).filter(_.nonEmpty) :+ suffix`).
- Confirmed: [ ] (Ask 3 decided; Asks 1–2 pending)

## Q30 — Run-off freeze rule on the deep-tail cliff (guard added 2026-06-04) ⚠️ NEW
- **Context:** a one-quarter cliff (`|CRD|` collapses to ~0 while the *offset* RA-metric window still
  includes pre-cliff months — see Q7) makes `RA` spike >1 and flips the cumulative product negative
  (observed `BCEF_INVEST_TF_Q` → −389). **Implemented guard:** `RA ≥ RUNOFF_RA_CAP (=1)` → treat as
  run off → `computeRa` stops → `termSeries` **freezes** `EAD_RA_RATE` at the last good value; plus a
  `[0,1]` clamp on `vectorFactored`. This bounds output and kills the negatives.
- **Ask:** confirm **freeze-at-last-value** is the intended treatment, vs alternatives: (a) cap `RA`
  just below 1 and keep accruing, (b) set `RA = 0` and continue flat (like the `CRD==0` rule), or
  (c) cap the curve at a fixed **projection horizon** (ties to Q18). And: is the right trigger
  `RA ≥ 1`, or a `CRD`-collapse / `ΔCRD` threshold?
- **Root-cause sub-question:** the cliff spike exists because **`CRD` (block-avg of `M[3q-2..3q]`)** and
  the **RA metrics (half-weight `M[3q-4..3q-1]`)** use **offset windows** (Q7), so a discontinuity hits
  them in different quarters. Should the two windows be **aligned** so a cliff hits both at once
  (removing the transient), or is the offset intentional? Linked to **Q26**.
- **Code:** `PrimaryView.RUNOFF_RA_CAP`, `centralRa` / `scenarioRa` (`.filter(_ < RUNOFF_RA_CAP)`),
  `vectorFactored` clamp.
- Confirmed: [ ]

## Q31 — PARAMETRAGE source of truth ⚠️ NEW
- **Context:** two files exist — `PARAMETRAGE.xlsx` and `PARAMETRAGE_corrected.xlsx`; the config and
  all runs use **`_corrected`** (it added the missing MORTGAGE row and carries full-precision values).
- **Ask:** confirm `PARAMETRAGE_corrected.xlsx` is the production source (and what the "correction"
  was), or specify how the authoritative PARAMETRAGE is delivered, so the config doesn't silently
  depend on a hand-patched file.
- Confirmed: [ ]

## Q32 — FWL=NO uses RA STAT only (FI/RE excluded) ✔️ RESOLVED (schema)
- **Finding (ticket cases 1 & 2, FWL=NO):** STEP 2 computes three details but STEP 3 builds the
  vector from **only** RA STAT (`VECTOR = 1 − DET RA STAT`); `DET RA FI`/`DET RE` looked like dead
  calculations.
- **RESOLVED 2026-06-05 (`Schema_EAD_FWD_20260601_v4.xlsx`, FWL=NO block STEP 2/3):** the schema is
  explicit — for **FWL=NO** the loss rate is **`RA = −RA_STAT/CRD`**; FI and RE are aggregated but
  **not** included. So the ticket's STEP 3 was correct, and the engine's old `centralRa` (which
  always added FI+RE) was **wrong for FWL=NO**. It went unnoticed because the sample/target only has
  BCEF (all FWL=YES, where FI+RE *are* included).
- **Fix:** added `PrimaryView.statOnlyRa` (`−RA_STAT/CRD`); `matrixRows` now routes FWL=NO →
  `statOnlyRa`, FWL=YES Central → `centralRa` (STAT+FI+RE). Unit-tested.
- **Code:** `PrimaryView.statOnlyRa`, `PrimaryMapper.matrixRows`. **Related:** Q6.
- Confirmed: [x]

## Q33 — FWL=YES scenario FI+RE shock formula ✔️ RESOLVED (schema)
- **Finding (ticket):** the shocked `RA FI RE (SCENARIO)` for A/O/E was never explicitly defined;
  the base label and the leading minus sign were ambiguous.
- **RESOLVED 2026-06-05 (`Schema_EAD_FWD_20260601_v4.xlsx`, FWL=YES STEP 2/4/5):** the schema gives
  the full formula, now implemented in `PrimaryView.scenarioRa`:
  ```
  Shock_FI(leg)  = (-FI_leg/CRD_leg) - (-FI_base/CRD_base)      # each leg's OWN CRD (STEP 2)
  RA_FI_RE_base  = -(FI_base + RE_base)/CRD_base                # leading minus confirmed (STEP 4)
  RA_FI_RE_scen  = RA_FI_RE_base - (Shock_FI + Shock_RE)*Rate/100
  RA             = RA_STAT_base + RA_FI_RE_scen                 # RA_STAT always baseline (STEP 5)
  ```
  - **Leg is fixed by scenario** (Adverse/Extreme → STRESS(-), Optimistic → STRESS(+)), **not** by
    the macro-delta sign — this **supersedes Q10**.
  - The base is the BASELINE-leg detail (schema labels it `(BASELINE)`; = the Central detail).
- **One interpretation (flagged):** the schema's STEP 4 cells show the shock *without* `Rate`, while
  STEP 3 computes it. The engine multiplies by `Rate/100` (so Adverse ≠ Extreme and the magnitude
  follows the macro path; no `ref_shock` — Q12). Confirm the business intends `×Rate/100`.
- **Code:** `PrimaryView.scenarioRa`, `PrimaryMapper.matrixRows`. **Related:** Q6, Q10 (superseded),
  Q12, Q15, Q17.
- Confirmed: [x] (formula implemented; `×Rate/100` interpretation pending business confirmation)
