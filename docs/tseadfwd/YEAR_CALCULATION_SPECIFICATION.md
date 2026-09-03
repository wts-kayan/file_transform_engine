# EAD FWD — Year (Annual) Calculation Specification

Authoritative description of how the engine computes the **yearly** EAD term structure
(`EAD_MATRIX_ID` ending in `_Y`). **Implemented and merged to `main`.**

> **STATUS — IMPLEMENTED & VALIDATED.** This revision replaces the previous *pure-stress* yearly model
> with the **OAT-10Y forward-looking sensitivity model** from
> `docs/EDB_EAD_FWD_BCEF_reconstruction.xlsx` (tab `Calc and Interpol #Mortgage`, and the two rule
> tabs `Règles de calcul (extraites)` / `Term 1 – Règles ordonnées`), **including the workbook's `×O173`
> factor**, with the OAT spread scaled `·10000` (§2.3). It reproduces the workbook to 8 dp
> (`BCEF_MORTGAGE_TF_Y;A;1 = 0.90785228`). The previous behaviour is kept
> verbatim in `docs/YEAR_CALCULATION_SPECIFICATION.pure-stress.bak.md` for reference. Remaining
> validation questions against the source workbook are collected in
> [§7 Open points](#7-open-points). The model is **merged to `main`**; the functional/technical
> framing (output columns, the `exclude_ead_ra_rate_ge_1` option) lives in
> [`FUNCTIONAL_SPECIFICATION.md`](FUNCTIONAL_SPECIFICATION.md) §4.7.2/§4.9 and
> [`TECHNICAL_SPECIFICATION.md`](TECHNICAL_SPECIFICATION.md) §4.3.

**Scope of this change.** *Yearly only.* The quarterly path (`PrimaryView.scenarioRa`) is **not**
changed and is **not** described here. The yearly scenario formula is a **separate computation** that
lives in `PrimaryViewYearly` (see [§6 Code separation](#6-code-separation)). Where this model happens
to coincide structurally with the quarterly macro-shock path, that is noted but the two remain
independent code paths.

Core code: **`PrimaryViewYearly`** — standalone yearly formula core (own aggregation, period
loop, RA-detail formulas, and the OAT-sensitivity scenario formula); `PrimaryView` — shared
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

## 2. STEP 2 — RA detail per scenario (the OAT-10Y sensitivity model, ×O173)

All quantities below are the STEP-1 annual aggregates for year `i`. Every `det(x, c)` is the
"detail" `−x / c` (with `det = 0` when `c = 0`).

### 2.1 Baseline details (every scenario shares these)

```
stat_det      = −RA_STAT_base / CRD_base                         # Excel O172 — RA Stat. (Y)
fire_base_det = −(RA_FI_base + RE_base) / CRD_base               # Excel O173 — RA-RE FI Baseline
```

`stat_det` is **fixed at baseline in every scenario** — the RA statistical component never moves.
Only the FI/RE component is scenario-adjusted.

### 2.2 Per-leg sensitivities to a ±100 bp shock (Excel O155–O158, literal)

For the `+100` leg (Optimistic) and the `−100` leg (Adverse & Extreme), exactly as the workbook cells:

```
sensFI = (−RA_FI_leg / CRD_leg) − (RA_FI_base / CRD_base)     # O155 (+100) / O157 (−100)
sensRE = (−RE_leg   / CRD_leg) − (RE_base   / CRD_base)       # O156 (+100) / O158 (−100)
```

> Implemented as the **literal Excel** cell `(−O150/O147)−(O134/O131)` (i.e. `det(leg) − ratio(base)`,
> with `det(x,c)=−x/c`, `ratio(x,c)=x/c`). A leg whose CRD has run off (`CRD_leg = 0`) makes the whole
> sensitivity `0` (Excel `SIERREUR`). Direction is carried by the **sign of ΔOAT** (§2.3), not by the
> sensitivity — Adverse's OAT spread is negative, Optimistic's positive.

### 2.3 OAT spread vs baseline (Excel O167–O169)

```
ΔOAT_scen = (IR_10Y_FR_scen − IR_10Y_FR_central) · 10000          # O167 fav / O168 adv / O169 sev
```

The workbook computes `O167 = (OAT_scen − OAT_base)·100` with OAT in **percent** (e.g. `3.40`); the
scenario file (`Scenario_EAD_FWD.xlsx`) stores `IR_10Y_FR` in **decimal** (`0.034`), and
`percent = decimal·100`, so the equivalent factor on the decimal input is `·100·100 = ·10000`.
Example, 2026Q4:
`ΔOAT_opt = (0.03650 − 0.03400)·10000 = +25.0`,
`ΔOAT_adv = (0.03075 − 0.03400)·10000 = −32.5`,
`ΔOAT_ext = (0.03275 − 0.03400)·10000 = −12.5`.
Adverse and Extreme have **different** OAT spreads, so they differ (unlike the old pure-stress model).

### 2.4 Scenario FI/RE detail and RA(Y) (Excel O174–O182)

The scenario FI/RE detail is the baseline detail scaled by `(1 − sens·ΔOAT)` — the literal workbook
form, **with** the `×fire_base_det` (`×O173`) factor:

```
fire_scen_det = fire_base_det · (1 − (sensFI + sensRE) · ΔOAT_scen)      # = O173 − ((O173·sens)·O167)
RA_i(scenario) = stat_det + fire_scen_det
```

with the per-scenario leg / spread selection:

| Engine scenario | Excel scenario | Stress leg (sensitivities) | OAT spread |
|-----------------|----------------|----------------------------|------------|
| Central     | Baseline           | — (no shock; `fire_scen_det = fire_base_det`) | 0 |
| Optimistic  | Favourable         | `+100` (O155/O156) | `ΔOAT_opt` (> 0) |
| Adverse     | Adverse            | `−100` (O157/O158) | `ΔOAT_adv` (< 0) |
| Extreme     | Severely adverse   | `−100` (O157/O158) | `ΔOAT_ext` (< 0) |

The sign of `ΔOAT` gives the correct direction (Adverse/Extreme worse, Optimistic better); no separate
`shockSign` is needed.

> **✓ VALIDATED against the workbook.** With the correct OAT scale (`·10000`, §2.3) the `×O173` factor
> is right and reproduces the Excel **exactly**. Worked check — `BCEF_MORTGAGE_TF_Y`, Adverse, year 2:
> `fire_base_det = 0.00701853`, `sensFI+sensRE = 0.06310285`, `ΔOAT_adv = −32.5` →
> `fire_scen_det = 0.00701853·(1 − 0.06310285·(−32.5)) = 0.02141246`, `RA = 0.06827077`,
> `EAD term 1 = 0.97437351·(1 − 0.06827077) = 0.90785228` — matches cell `Z192`/the workbook value.
> *(History: an earlier build used `·100` instead of `·10000`, which made the `×O173` spread look
> negligible and prompted a brief "drop ×O173" detour; the real bug was the OAT scale.)*

### 2.5 FWL = NO  (`PrimaryViewYearly.statOnlyRa`) *(unchanged)*

```
RA_i = stat_det                          # RA STAT only; FI and RE EXCLUDED
```
The OAT model in §2.1–§2.4 applies only when `FWL_TO_BE_APPLIED = YES`.

---

## 3. STEP 6–7 — Vector and vector factored (final EAD TS)

```
VECTOR_i      = MIN(1; 1 − RA_i)                                  # Excel O185–O188, capped at 1
                                                                  #   (spec update 02/09/2026)
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
stat_det      = −RA_STAT_base / CRD_base            = 0.04685834
fire_base_det = −(RA_FI_base + RE_base) / CRD_base  = 0.00701853
sensFI        = (−RA_FI_−100/CRD_−100) − (RA_FI_base/CRD_base)   # literal Excel O157
sensRE        = (−RE_−100  /CRD_−100) − (RE_base  /CRD_base)     # O158 ; sensFI+sensRE = 0.06310285

ΔOAT_adv      = (0.03075 − 0.034)·10000 = −32.5
ΔOAT_ext      = (0.03275 − 0.034)·10000 = −12.5      # ≠ ΔOAT_adv

fire_adv_det  = fire_base_det · (1 − (sensFI+sensRE)·ΔOAT_adv)
              = 0.00701853 · (1 − 0.06310285·(−32.5)) = 0.02141246
RA_adv(1)     = 0.04685834 + 0.02141246 = 0.06827077
EAD_RA_RATE_adv(1) = EAD(0) · (1 − RA_adv(1)) = 0.97437351 · 0.93172923 = 0.90785228   ✓ (= workbook)
```
For `BCEF_MORTGAGE_TF_Y` (inputs v3, corrected RE) this gives term 1: Optimistic `0.92315787` >
Central `0.92187732` > Extreme `0.91648307` > Adverse `0.90785228`.

> The `YearAnalysisDriver` job emits this breakdown (Markdown + CSV) per matrix/scenario/term against
> `Scenario_EAD_FWD.xlsx`. Re-run the engine after any input change; a stale
> `localRun/tseadfwd/output/*.csv` will not reflect the latest inputs.

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

- **Yearly scenario formula → `PrimaryViewYearly.scenarioRa`** takes the baseline series, the selected
  stress leg series, and the per-year OAT spread, implementing §2 (it does **not** delegate to
  `centralRa` on the leg, as the old pure-stress model did).
- **`PrimaryView` (quarterly) is untouched.** Its `scenarioRa` keeps the additive macro-shock form;
  the frequency dispatch in `PrimaryMapper` routes `Yearly → PrimaryViewYearly`, everything else →
  `PrimaryView`.
- **`PrimaryMapper`** holds the yearly OAT wiring: `oatDeltaYearly` reads `IR_10Y_FR` per scenario from
  `Scenario_EAD_FWD.xlsx`, samples `ΔOAT_scen` at the year anniversary (§2.3), and passes the baseline
  + leg series + `ΔOAT_scen` into `PrimaryViewYearly.scenarioRa`. The leg-by-scenario mapping
  (Optimistic → STRESS(+), Adverse/Extreme → STRESS(−)) is shared with the quarterly path.
- The quarterly and yearly shock maths live in **separate functions** (`PrimaryView.scenarioRa` /
  `oatDeltaYearly` vs `PrimaryViewYearly.scenarioRa`), so the two frequencies evolve independently.

---

## 7. Open points

> **Implementation status.** The model in §1–§6 is **implemented**:
> `PrimaryViewYearly.scenarioRa` (new OAT-sensitivity formula), `PrimaryMapper.oatDeltaYearly` +
> the `Yearly` dispatch in `matrixRows`/`termRowsFor` (OAT wiring), and the `Term0AnalysisDriver`
> yearly worked-steps renderer. Quarterly is untouched. Items 1–3 below are **resolved** (workbook
> match in §2.4); items 4–6 remain validation questions that do **not** block the build.

1. ~~**`× fire_base_det` factor**~~ — **RESOLVED & KEPT**: the factor is in all three workbook cells
   and is correct. An earlier build made the spread look negligible, but the cause was the **OAT
   scale** (`·100` vs the correct `·10000`, §2.3), not the factor. With the right scale the `×O173`
   form reproduces the workbook **exactly** (`A;1 = 0.90785228`, §2.4). No `shockSign` is used.
2. **Sign convention** (§2.2) — implemented as the **literal** Excel cell `(−O150/O147)−(O134/O131)`
   (`det(leg) − ratio(base)`); validated by the exact workbook match in §2.4. (`Lisez-moi` notes 2 & 3.)
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

- Functional framing (output columns, exclusion option): [`FUNCTIONAL_SPECIFICATION.md`](FUNCTIONAL_SPECIFICATION.md) §4.7.2, §4.9
- Technical framing (code dispatch, `oatDeltaYearly`, config): [`TECHNICAL_SPECIFICATION.md`](TECHNICAL_SPECIFICATION.md) §3.3, §4.3
- Excel reference: `docs/EDB_EAD_FWD_BCEF_reconstruction.xlsx`
- OAT curve input: `localRun/tseadfwd/input/Scenario_EAD_FWD.xlsx` (`IR_10Y_FR`)
- Previous (pure-stress) spec: `docs/YEAR_CALCULATION_SPECIFICATION.pure-stress.bak.md`
- Yearly formula core: `src/main/scala/com.bnp.str.tseadfwd/mapping/PrimaryViewYearly.scala`
- Shared primitives + quarterly core: `…/mapping/PrimaryView.scala` *(do not change for this work)*
- Orchestration / leg selection / OAT wiring: `…/mapping/PrimaryMapper.scala`
- Yearly analysis generator: `…/job/YearAnalysisDriver.scala`
- Open items & decision history: `docs/OPEN_QUESTIONS.md`
