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

## Q2 — Aggregated segment name ✅
- **Chosen:** when `AGGREGATION = YES`, use `AGGREGATED_SEGMENT_NAME` (so `INVEST_PRO` +
  `INVEST_CORP` → `INVEST`). Matches target (`BCEF_INVEST_*`).
- Confirmed: [ ]

## Q3 — FWL flag for an **aggregated** matrix ⚠️
- **Problem:** constituents disagree — `INVEST_PRO` = `NO`, `INVEST_CORP` = `YES`.
- **Chosen:** `FWL = YES if ANY constituent is YES` → `INVEST` is FWL=YES. Matches target
  (INVEST values differ across scenarios).
- **Alternatives:** "all must be YES", or "use a designated lead segment".
- **Code:** `PrimaryMapper.parseParametrage` → `group.exists(_._6.equalsIgnoreCase(YES))`.
- Confirmed: [ ]   Override (if any): ____________

## Q4 — Emit both Q and Y for every matrix? ✅
- **Spec:** `computation_frequency` selects `_Q` *or* `_Y`.
- **Target shows:** both `_Q` and `_Y` for every matrix.
- **Chosen:** always emit both frequencies. Matches target.
- Confirmed: [ ]

## Q5 — `FWL_TO_BE_APPLIED` source ✅
- **Chosen:** read directly from PARAMETRAGE column `FWL_TO_BE_APPLIED`
  (`YES` → scenario shock; `NO` → all scenarios = Central). Config-driven, no hardcoding.
- **Note:** required `PARAMETRAGE_corrected.xlsx` (CONSO set to `NO`, `MORTGAGE` row added)
  to match the target's behaviour.
- Confirmed: [ ]

## Q6 — Core RA formula ✅
- **Chosen:** `RA_i = -(RA_STAT_i + RA_FI_i + RE_i) / CRD_i` (BASELINE), reverse-engineered.
- **Evidence:** reproduces `BCEF_MORTGAGE_Q` Central to ~`1e-5` at low terms.
- Confirmed: [ ]

## Q7 — Period aggregation windows ✅
- **Chosen (quarterly):** RA metrics half-weight (`Q1 = M1 + M2/2`;
  `Qn = M[3n-4]/2 + M[3n-3] + M[3n-2] + M[3n-1]/2`); `CRD` = block mean of 3 months.
- **Chosen (yearly):** RA metrics = sum (Y1 = 6 months, Yn = 12); `CRD` = mean of window.
- **Evidence:** matches target at low terms; per-quarter residual oscillates in sign
  (data noise, not a window error).
- Confirmed: [ ]

## Q8 — Run-off handling when `CRD = 0` ✅
- **Chosen:** `CRD = 0` → `RA = 0` (`VECTOR = 1`); the curve flattens instead of `0/0`.
- **Evidence:** the target plateaus when exposure runs off (e.g. CONSO ~term 16).
- Confirmed: [ ]

## Q9 — Flat-tail boundary ✅
- **Chosen:** compute to the last *computable* term, then hold flat to 50/100.
  With 361 months that is term **29.75 (quarterly)** / **29 (yearly)** — the quarterly
  window for term 30 needs month 362. Grid sizes: 203 (Q), 52 (Y). Matches target structure.
- **Note:** a longer input vintage moves the last-computed term to 30; flat behaviour unchanged.
- Confirmed: [ ]

## Q10 — Stress leg selection (FWL=YES) ⚠️
- **Chosen:** `delta < 0` → use `STRESS (-)` leg; `delta ≥ 0` → use `STRESS (+)` leg,
  where `delta = MACRO[scenario][projection_date] − MACRO[Central][projection_date]`.
- Confirmed: [ ]   Override (if any): ____________

## Q11 — `projection_date` ⚠️
- **Chosen:** `2025Q4` (from the output name `…25Q4…` and the last date in the scenario file).
- **Config:** `tseadfwd_app.projection_date`.
- Confirmed: [ ]   Correct value: ____________

## Q12 — `ref_shock` calibration (FWL=YES shock magnitude) ⛔
- **What it is:** the rate-shock magnitude the `STRESS (+)/(-)` legs represent;
  `weight = |delta| / ref_shock`, `fire_scen = fire_base + weight·(fire_stress − fire_base)`.
- **Chosen:** placeholder `1.0` → weight ≈ 0.0065 → A/O/E ≈ Central (shock almost off).
- **Cannot reproduce the target** until we know this value (or can calibrate against a
  target produced from a known scenario file). See [`MISSING_INPUTS.md`](../MISSING_INPUTS.md).
- **Needed:** the rate magnitude the stress legs encode (e.g. ±100 bps) **or** approval to
  reverse-engineer it from the target.
- Confirmed value: ____________

## Q13 — Macro-delta scaling (×100?) ⚠️
- **Spec STEP 3:** `Rate = (MACRO_scen − MACRO_central) × 100`.
- **Chosen:** code uses the **raw** delta; the ×100 (and any unit scaling) is absorbed by
  `ref_shock`, so only one knob needs calibrating. Equivalent once `ref_shock` is set.
- Confirmed: [ ]

## Q14 — Scenario `S` (Secto) ✅
- **Chosen:** not implemented — no Secto data in the scenario file. Target has no `S` rows.
- **Note:** one-line addition (`"Secto" -> "S"`) once data exists.
- Confirmed: [ ]

## Q15 — Adverse vs Extreme differentiation ⛔
- **Finding:** in the current scenario file Adverse and Extreme are **identical** at every
  quarter and macro variable, so the engine cannot produce distinct A vs E.
- **Chosen:** rely on each scenario's own macro delta to differentiate (works automatically
  once the file has differing values). `ref_shock` does **not** affect A-vs-E, only magnitude.
- **Needed:** the scenario file version where Adverse ≠ Extreme (you indicated this exists).
- Confirmed: [ ]

## Q16 — Output number format ✅
- **Chosen:** `;` delimiter, decimal **comma**, `EAD_RA_RATE` half-up at 9 dp with trailing
  zeros stripped, `TERM` like `0`, `0,25`, `100`. Matches target byte-for-byte in format.
- Confirmed: [ ]

---

## New questions (add yours here)

- Q__ —
- Q__ —
