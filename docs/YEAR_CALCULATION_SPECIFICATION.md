# EAD FWD — Year (Annual) Calculation Specification

Authoritative description of how the engine **must** compute the **yearly** EAD term structure
(`EAD_MATRIX_ID` ending in `_Y`).

> **STATUS — TARGET MODEL (rewritten from the Excel reference).** This revision replaces the previous
> *pure-stress* yearly model with the **OAT-10Y forward-looking sensitivity model** reconstructed from
> `docs/EDB_EAD_FWD_BCEF_reconstruction.xlsx` (tab `Calc and Interpol #Mortgage`, and the two rule
> tabs `Règles de calcul (extraites)` / `Term 1 – Règles ordonnées`). The previous behaviour is kept
> verbatim in `docs/YEAR_CALCULATION_SPECIFICATION.pure-stress.bak.md` for reference. Items still to be
> confirmed against the source workbook are flagged **⚠ CONFIRM** inline and collected in
> [§7 Open points](#7-open-points-to-confirm-before-coding).

**Scope of this change.** *Yearly only.* The quarterly path (`PrimaryView.scenarioRa`) is **not**
changed and is **not** described here. The yearly scenario formula is a **separate computation** that
lives in `PrimaryViewYearly` (see [§6 Code separation](#6-code-separation)). Where this model happens
to coincide structurally with the quarterly macro-shock path, that is noted but the two remain
independent code paths.

Core code (target): **`PrimaryViewYearly`** — standalone yearly formula core (own aggregation, period
loop, RA-detail formulas, and the new OAT-sensitivity scenario formula); `PrimaryView` — shared
frequency-agnostic primitives only (`vectorFactored`, `termGrid`/`termSeries`, constants);
`PrimaryMapper` — parsing, leg selection, OAT-curve wiring, and frequency dispatch.

---

## 0. Inputs

### 0.1 Exposure / loss series (per matrix)

Per `(PERIMETER, SEGMENT, RATE_TYPE, FWL_TYPE, METRIC)` the input is a monthly series `M1…M361`
(30y horizon = `30·12 + 1`), with `FWL_TYPE ∈ {BASELINE, STRESS (+), STRESS (-)}` and
`METRIC ∈ {CRD, RA STAT, RA FI, RE}`.

| Excel cells | Block | Metrics |
|-------------|-------|---------|
| O131–O135 | **BASELINE** | CRD, RA/RE, RA STAT, RA FI, RE |
| O139–O143 | **STRESS (−)** (`−100` bp leg) | CRD, RA/RE, RA STAT, RA FI, RE |
| O147–O151 | **STRESS (+)** (`+100` bp leg) | CRD, RA/RE, RA STAT, RA FI, RE |

> Each stress leg has its **own CRD** (O139 for `−100`, O147 for `+100`). The leg CRD is used **only**
> inside the per-leg sensitivity (§2.2); the *final* scenario loss is always divided by the
> **baseline** CRD.

**Flat extrapolation.** A series shorter than `M361` is forward-filled to 361 months holding the last
observed value (`PrimaryView.padForward`, `INPUT_MONTHS = 361`).

### 0.2 OAT-10Y curve (per scenario) — `Scenario_EAD_FWD.xlsx`

The forward-looking driver is the **10-year OAT (French govt yield)**, macro variable **`IR_10Y_FR`**,
read per scenario from `localRun/tseadfwd/input/Scenario_EAD_FWD.xlsx` (one sheet per scenario:
`Central`, `Adverse`, `Optimistic`, `Extreme`; column `C = IR_10Y_FR`, **decimal** form e.g. `0.034`).

This is the source of the Excel's OAT block (O160–O164). Worked confirmation, `IR_10Y_FR` at **2026Q4**:

| Scenario | `IR_10Y_FR` (decimal) | as % |
|----------|----------------------|------|
| Central     | `0.03400` | 3.400 |
| Optimistic  | `0.03650` | 3.650 |
| Adverse     | `0.03075` | 3.075 |
| Extreme     | `0.03275` | 3.275 |

These are the four values supplied as ground truth and they match the workbook's OAT inputs.

---

## 1. STEP 1 — Annual aggregation convention *(unchanged, validated)*

The two metric kinds are combined **differently** over the year window:
- **CRD** → **MEAN**, `SUM(window) / divisor` (average exposure over the year).
- **RA STAT / RA FI / RE** → **raw SUM** over the window (the annual loss; no divisor).

So every RA detail `−RA_metric / CRD` is **annual-loss-SUM ÷ average-exposure**.

| Year (term) | Months | CRD divisor | RA metrics | OAT sample (anniversary) |
|-------------|--------|-------------|------------|--------------------------|
| Y1 (term 0) | M1 … M6 | 6 | SUM(M1..M6) | as-of quarter (idx 0) |
| Y2 (term 1) | M7 … M18 | 12 | SUM(M7..M18) | as-of + 1y (idx 4) |
| Y3 (term 2) | M19 … M30 | 12 | SUM(M19..M30) | as-of + 2y (idx 8) |
| Yₙ (term n−1), n ≥ 2 | M(12n−17) … M(12n−6) | 12 | SUM over window | as-of + (n−1)y (idx (n−1)·4) |

> Y1 is a half-year (mid-year start, 6 months = 2 quarters); every later year is a full 12-month
> (4-quarter), non-overlapping block. Windows do **not** overlap and boundary months are **not**
> half-weighted (that is a quarterly-only convention).

Code: `PrimaryViewYearly.aggregate` — `if (isCrd) SUM(window)/len else SUM(window)`.
A term whose window exceeds the available months yields no value (the series stops there).

The OAT curve is **point-sampled at the annual anniversary of the as-of quarter** — shock-window index
`(period−1)·4` (Y1 = as-of quarter, Y2 = as-of + 1y, …) — **not** averaged over the year. With
`as_of_date_quarter = 2025Q4`, Y2 samples **2026Q4**, which matches both the supplied ground-truth
OAT values (§0.2) and the Excel's annual-period cadence (column O = `T4-2026 → T3-2027`). Code:
`PrimaryMapper.oatDeltaYearly`. (Y1 lands on the as-of quarter where every scenario equals Central, so
term 0 carries no OAT spread — divergence starts at Y2.)

The term → period → year mapping: `term` is in years (`YEARLY_STEP = 1.0`); period `p = term + 1`
(term 0 = period 1 = Y1).

---

## 2. STEP 2 — RA detail per scenario (the OAT-sensitivity model)

All quantities below are the STEP-1 annual aggregates for year `i`. Every `det(x, c)` is the
"detail" `−x / c` (with `det = 0` when `c = 0`).

### 2.1 Baseline details (every scenario shares these)

```
stat_det      = −RA_STAT_base / CRD_base                         # Excel O172 — RA Stat. (Y)
fire_base_det = −(RA_FI_base + RE_base) / CRD_base               # Excel O173 — RA-RE FI Baseline
```

`stat_det` is **fixed at baseline in every scenario** — the RA statistical component never moves.
Only the FI/RE component is scenario-adjusted.

### 2.2 Per-leg sensitivities to a ±100 bp shock (Excel O155–O158)

For the `+100` leg (used by Optimistic) and the `−100` leg (used by Adverse & Extreme):

```
sensFI(+100) = (−RA_FI_+100 / CRD_+100) − (RA_FI_base / CRD_base)     # O155
sensRE(+100) = (−RE_+100   / CRD_+100) − (RE_base   / CRD_base)       # O156
sensFI(−100) = (−RA_FI_−100 / CRD_−100) − (RA_FI_base / CRD_base)     # O157
sensRE(−100) = (−RE_−100   / CRD_−100) − (RE_base   / CRD_base)       # O158
```

A sensitivity is the **change in the FI (resp. RE) loss-rate produced by a full 100 bp shock**, each
leg measured against the baseline using **its own CRD**.
**⚠ CONFIRM sign convention** — the workbook `Lisez-moi` (note 2) flags that RA STAT/FI/RE may be
stored **negative** (the leading `−` re-flips them); the exact form of the baseline subtrahend
(`(O134/O131)` vs `det(...)`) must be reconciled before coding.

### 2.3 OAT spread vs baseline, in "100 bp blocks" (Excel O167–O169)

```
ΔOAT_scen = (IR_10Y_FR_scen − IR_10Y_FR_central) · 100            # O167 fav / O168 adv / O169 sev
```

With `IR_10Y_FR` in **decimal** form, `·100` rescales a decimal gap into **number of 100 bp blocks**
(100 bp = 1 pp = 0.01 decimal, so `Δdecimal · 100 = Δ / 0.01`). Example, 2026Q4:
`ΔOAT_opt = (0.03650 − 0.03400)·100 = +0.250`,
`ΔOAT_adv = (0.03075 − 0.03400)·100 = −0.325`,
`ΔOAT_ext = (0.03275 − 0.03400)·100 = −0.125`.
Because Adverse and Extreme have **different** OAT spreads, **they are no longer identical** (the key
fix vs the old pure-stress model — see §5).

### 2.4 Scenario FI/RE detail and RA(Y) (Excel O174–O182)

The scenario FI/RE detail is the baseline FI/RE detail adjusted by the sensitivities scaled by the OAT
spread:

```
fire_scen_det = fire_base_det − ( (fire_base_det · sensFI + fire_base_det · sensRE) · ΔOAT_scen )
              = fire_base_det · ( 1 − (sensFI + sensRE) · ΔOAT_scen )
```
*(Excel O174 = O173 − ((O173·O155 + O173·O156)·O167), and analogously O175/O176.)*

```
RA_i(scenario) = stat_det + fire_scen_det                        # Excel O179–O182 = O172 + O17{3,4,5,6}
```

with the per-scenario leg / sensitivity / spread selection:

| Engine scenario | Excel scenario | Stress leg & sensitivities | OAT spread |
|-----------------|----------------|----------------------------|------------|
| Central     | Baseline           | — (no adjustment; `fire_scen_det = fire_base_det`) | 0 |
| Optimistic  | Favourable         | `+100` (O155/O156) | `ΔOAT_opt` (O167) |
| Adverse     | Adverse            | `−100` (O157/O158) | `ΔOAT_adv` (O168) |
| Extreme     | Severely adverse   | `−100` (O157/O158) | `ΔOAT_ext` (O169) |

> **✓ CONFIRMED & IMPLEMENTED — the `× fire_base_det` factor.** The Excel multiplies the shock term by
> `fire_base_det` (`O173`), i.e. the adjustment is **proportional to the baseline FI/RE rate**. This
> factor appears **identically in all three workbook locations** (`Calc and Interpol #Mortgage` O174–
> O176, plus both rule tabs), so it is kept verbatim. It differs from the quarterly engine's
> **additive** shock (`fire_base_det + shockSign·(sensFI+sensRE)·delta`, no `×O173`) — that path is
> unchanged. Consequence (validated numerically): because `fire_base_det` is small, the per-scenario
> spread is modest (a few 1e-6 on RA in the sample), so non-Central curves sit close to Central. The
> `shockSign` (+ for Optimistic, − for Adverse/Extreme) is absorbed into the sign of `ΔOAT_scen`.

### 2.5 FWL = NO  (`PrimaryViewYearly.statOnlyRa`) *(unchanged)*

```
RA_i = stat_det                          # RA STAT only; FI and RE EXCLUDED
```
The OAT model in §2.1–§2.4 applies only when `FWL_TO_BE_APPLIED = YES`.

---

## 3. STEP 6–7 — Vector and vector factored (final EAD TS) *(unchanged)*

```
VECTOR_i      = 1 − RA_i                                          # Excel O185–O188 = 1 − O179..O182
EAD_RA_RATE_i = Π(VECTOR_1 … VECTOR_i)                            # Excel O191–O194 (compounding)
```
`PrimaryView.vectorFactored` + `termSeries`.

**Grid & run-off.**
- Computed year by year while `term ≤ COMPUTED_HORIZON_Y (30)`; output grid is `0 … FLAT_MAX_Y (50)`
  by 1, plus the tail term `100`. Past the computed horizon the last value is held flat.
- `CRD_base_i == 0` → `RA_i = 0` (exposure fully run off, `VECTOR = 1`).
- `RA_i ≥ RUNOFF_RA_CAP (1.0)` → run-off: the period is dropped and the curve freezes at the last good
  value (`computeRa` stops). A loss rate ≥ 100% is non-physical.

> The Excel compounds the **previous rolling window's** cumulative product (`O191 = C191·O185`) — the
> engine's per-scenario cumulative product `Π VECTOR` is the same compounding expressed over the term
> grid rather than across overlapping windows.

---

## 4. Worked example skeleton — `BCEF_MORTGAGE_TF_Y`, Adverse vs Extreme, term 1 (Y2)

Annual aggregation over M7–M18 (CRD ÷12; RA STAT/FI/RE summed), OAT point-sampled at the Y2 anniversary
(as-of + 1y = 2026Q4 when as-of = 2025Q4):

```
stat_det      = −RA_STAT_base / CRD_base
fire_base_det = −(RA_FI_base + RE_base) / CRD_base
sensFI(−100)  = (−RA_FI_−100/CRD_−100) − (RA_FI_base/CRD_base)
sensRE(−100)  = (−RE_−100  /CRD_−100) − (RE_base  /CRD_base)

ΔOAT_adv      = (IR_10Y_FR_adverse,Y2 − IR_10Y_FR_central,Y2) · 100      # < 0
ΔOAT_ext      = (IR_10Y_FR_extreme,Y2 − IR_10Y_FR_central,Y2) · 100      # < 0, ≠ ΔOAT_adv

fire_adv_det  = fire_base_det · (1 − (sensFI(−100)+sensRE(−100)) · ΔOAT_adv)
fire_ext_det  = fire_base_det · (1 − (sensFI(−100)+sensRE(−100)) · ΔOAT_ext)   # differs from adverse

RA_adv(1)     = stat_det + fire_adv_det     ;  VECTOR_adv = 1 − RA_adv(1)
RA_ext(1)     = stat_det + fire_ext_det     ;  VECTOR_ext = 1 − RA_ext(1)
EAD_RA_RATE(1)= EAD_RA_RATE(0) · VECTOR
```

> The `YearAnalysisDriver` job emits this breakdown (Markdown + CSV) per matrix/scenario/term against
> `Scenario_EAD_FWD.xlsx` once the model is implemented; today
> `localRun/tseadfwd/output/ANALYSIS_YEAR_GENERATED.md` still reflects the **old pure-stress** model
> (Adverse == Extreme everywhere) and must be regenerated.

---

## 5. What changes vs the previous (pure-stress) yearly model

The old model computed each scenario **entirely on its own stress leg**
(`RA = −(STAT+FI+RE)/CRD` from the leg). This revision replaces it because the Excel reference does
**not** do that. Concretely:

1. **OAT-10Y forward-looking adjustment is now applied** (§2.2–§2.4). The `IR_10Y_FR` curve from
   `Scenario_EAD_FWD.xlsx` drives the FI/RE adjustment; the old model ignored the macro variable
   entirely on the yearly path.
2. **ADVERSE ≠ EXTREME.** They share the `−100` leg/sensitivities but use different OAT spreads
   (`ΔOAT_adv` vs `ΔOAT_ext`), so they now diverge. The old model produced identical curves.
3. **RA STAT fixed at baseline** in every scenario (§2.1); the old model took STAT from the leg.
4. **Final denominator is baseline CRD;** the leg CRD enters only inside the sensitivity (§2.2). The
   old model divided everything by the leg CRD.
5. **Shock is scaled, not full.** The adjustment is a fraction of the 100 bp shock set by the OAT
   spread (e.g. `|ΔOAT| ≈ 0.13–0.33` blocks in the sample), where the old model applied the full leg.

`CONSO` segments have `RA_FI = RE = 0`, so `fire_base_det = 0` and every scenario collapses to Central
under **both** models — the change is observable only on segments with non-zero FI/RE (e.g. INVEST,
MORTGAGE).

---

## 6. Code separation

- **Yearly scenario formula → `PrimaryViewYearly.scenarioRa`** is **rewritten** to take the baseline
  series **and** the selected stress leg series **and** the per-year OAT spread, implementing §2. It
  no longer delegates to `centralRa` on the leg.
- **`PrimaryView` (quarterly) is untouched.** Its `scenarioRa` keeps the additive macro-shock form;
  the frequency dispatch in `PrimaryMapper` continues to route `Yearly → PrimaryViewYearly`,
  everything else → `PrimaryView`.
- **`PrimaryMapper`** gains the yearly OAT wiring: read `IR_10Y_FR` per scenario from
  `Scenario_EAD_FWD.xlsx`, aggregate to the annual window (§1), compute `ΔOAT_scen` (§2.3), and pass
  the baseline + leg series + `ΔOAT_scen` into `PrimaryViewYearly.scenarioRa`. It already selects the
  leg by scenario (Optimistic → STRESS(+), Adverse/Extreme → STRESS(−)); that mapping is reused.
- Keep the yearly OAT helpers (annual OAT aggregation, sensitivity, `ΔOAT`) in `PrimaryViewYearly`
  (or a small `YearlyScenario` helper) so the quarterly and yearly shock maths stay physically
  separate files/functions.

---

## 7. Open points

> **Implementation status.** The model in §1–§6 is **implemented**:
> `PrimaryViewYearly.scenarioRa` (new OAT-sensitivity formula), `PrimaryMapper.oatDeltaYearly` +
> the `Yearly` dispatch in `matrixRows`/`termRowsFor` (OAT wiring), and the `Term0AnalysisDriver`
> yearly worked-steps renderer. Quarterly is untouched. Item 1 below is **resolved** (confirmed in all
> three workbook locations); items 2–6 remain validation questions that do **not** block the build.

1. ~~**`× fire_base_det` factor**~~ — **RESOLVED**: confirmed identical in all three workbook
   locations and implemented verbatim (§2.4).
2. **Sign convention** (§2.2) — whether RA STAT/FI/RE (and CRD) are stored negative; reconcile the
   `(O134/O131)` baseline term vs `det()`. (`Lisez-moi` notes 2 & 3.)
3. **OAT sampling** (§1/§2.3) — **RESOLVED to point-at-anniversary** (`(period−1)·4`), confirmed by
   the ground-truth 2026Q4 values matching engine Y2. Open sub-point: this anniversary sampling is
   offset by the half-year Y1 from the engine's RA windows (Y2 RA = M7–M18 spans 2026Q2–2027Q1, but
   its OAT is read at 2026Q4) — confirm anniversary sampling is preferred over RA-window-start
   alignment if the two ever need to coincide.
4. **OAT beyond the forecast tail** — `Scenario_EAD_FWD.xlsx` only covers 2025Q1–2028Q4. Decide
   forward-fill (hold last, consistent with `padForward`) vs the quarterly convention of `delta = 0`
   past the projection horizon.
5. **Scenario→leg/curve asymmetry** — Favourable uses `+100`, both Adverse and Severely-adverse use
   `−100` (`Lisez-moi` note 3, "à valider"). Confirm this is intended for the yearly path.
6. **Severely-adverse OAT cell** — Excel `O164` (Z-column) was blank in the photo; the value is taken
   from the `Extreme` sheet of `Scenario_EAD_FWD.xlsx`. Confirm `Extreme ≡ Severely adverse`.

---

## Cross-references

- Excel reference: `docs/EDB_EAD_FWD_BCEF_reconstruction.xlsx`
- OAT curve input: `localRun/tseadfwd/input/Scenario_EAD_FWD.xlsx` (`IR_10Y_FR`)
- Previous (pure-stress) spec: `docs/YEAR_CALCULATION_SPECIFICATION.pure-stress.bak.md`
- Yearly formula core: `src/main/scala/com.bnp.str.tseadfwd/mapping/PrimaryViewYearly.scala`
- Shared primitives + quarterly core: `…/mapping/PrimaryView.scala` *(do not change for this work)*
- Orchestration / leg selection / OAT wiring: `…/mapping/PrimaryMapper.scala`
- Yearly analysis generator: `…/job/YearAnalysisDriver.scala`
- Open items & decision history: `docs/OPEN_QUESTIONS.md`
