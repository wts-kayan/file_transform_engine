# Open Questions & Assumptions — EAD FWD

This log records the points the specification left ambiguous. For each one, the engine
currently uses the answer **chosen to reproduce the target file**
(`target_output/TS_EAD_FWD_25Q4_v1_small.csv`). Please confirm or override each.

Legend: ✅ chosen answer reproduces the target · ⚠️ judgement call, please confirm ·
⛔ blocked on data (cannot be reproduced from current inputs).

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
- **PARAMETRAGE corrections** made to match the target (`PARAMETRAGE_corrected.xlsx`),
  columns: `PERIMETER ; SEGMENT ; RATE_TYPE ; AGGREGATION ; AGGREGATED_SEGMENT_NAME ;
  FWL_TO_BE_APPLIED ; MACRO_VARIABLE ; PROJECTION_HORIZON`:
  - **CONSO row** — `FWL_TO_BE_APPLIED` changed `YES → NO`
    (`BCEF ; CONSO ; TF ; NO ; <blank> ; NO ; IR_10Y_FR ; 3Y`).
  - **MORTGAGE row ADDED** (was absent) —
    `BCEF ; MORTGAGE ; TF ; NO ; <blank> ; YES ; IR_10Y_FR ; 3Y`.
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

## Q10 — Stress leg selection (FWL=YES) ✔️ ANSWERED
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
- Confirmed: [x]

## Q12 — `ref_shock` calibration (FWL=YES shock magnitude) ⛔
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
- Confirmed value: ____________ (pending corrected inputs)

## Q13 — Macro-delta scaling (×100?) ✔️ ANSWERED
- **Spec STEP 3:** `Rate = (MACRO_scen − MACRO_central) × 100`.
- **DECISION (user):** keep the delta **raw**; the ×100 (and any unit scaling) is absorbed by
  `ref_shock`, so only one knob (Q12) needs calibrating. Mathematically equivalent.
- **Consequence:** `ref_shock` is expressed in raw-rate units (e.g. `0.0065` = 65 bps), not
  ×100 units. The Q12 ballpark `≈0.07` is in these raw units.
- **Code:** `PrimaryMapper.macroDeltaArray` (no ×100); `PrimaryView.scenarioRa` weight.
- Confirmed: [x]

## Q14 — Scenario `S` (Secto) ✅
- **Chosen:** not implemented — no Secto data in the scenario file. Target has no `S` rows.
- **Note:** one-line addition (`"Secto" -> "S"`) once data exists.
- Confirmed: [ ]

## Q15 — Adverse vs Extreme differentiation ⛔
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
- Confirmed: [ ]   (pending corrected scenario file)

## Q16 — Output number format ✔️ ANSWERED
- **DECISION (user): confirmed.** `;` delimiter, decimal **comma**, `EAD_RA_RATE` half-up at
  9 dp with trailing zeros stripped, `TERM` like `0`, `0,25`, `100`. Matches the target
  byte-for-byte in format.
- **Code:** `PrimaryMapper.fmtNumber`; write via `PrimaryUtilities.writeDataframe` (`;`, header).
- Confirmed: [x]

---

## New questions (add yours here)

## Q17 — Spec contradiction: "no difference between scenarios" under FWL=YES ⚠️
- **Spec says:** both FWL=YES cases (3rd & 4th) end with *"There is no differences between
  scenarios — the same value will be considered for all scenarios."*
- **Problem:** that is the FWL=**NO** behaviour and contradicts the FWL=YES purpose (scenario
  shock) and the target, where FWL=YES matrices (`MORTGAGE`, `INVEST`) **do** vary by scenario.
  Reads like a copy-paste from the FWL=NO cases.
- **Ask:** confirm that for FWL=YES scenarios should differ (scenario-shocked), and that sentence
  applies only to the FWL=NO cases.
- **Engine assumption:** FWL=YES → scenario-shocked (scenarios differ). See [[ead-fwd-formula]].
- Confirmed: [ ]

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
- Confirmed: [ ]

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

## Q24 — Scenario `S` (Secto) scope ⚠️
- **Context:** not implemented — no Secto data in the scenario file, no `S` rows in the target.
- **Ask:** confirm Secto is out of scope for this delivery; one-line addition once data exists.
- See Q14.
- Confirmed: [ ]
