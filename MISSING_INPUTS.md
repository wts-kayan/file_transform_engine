# Missing / incorrect input data — EAD FWD TS

The EAD FWD engine is implemented and runs end-to-end. The **computation logic is
validated**: for the Central scenario the output matches the target file
(`localRun/tseadfwd/target_output/TS_EAD_FWD_25Q4_v1_small.csv`) to ~`1e-5`
(e.g. `BCEF_MORTGAGE_Q` term 1).

An **exact** match is currently blocked by two input files that are a different /
incomplete vintage than the one used to produce the target. `PARAMETRAGE` has
already been corrected (`PARAMETRAGE_corrected.xlsx`). The two remaining items are
below. Both are *correct-vintage replacements* of files we already have — no change
to the computation logic is expected once they arrive.

---

## Item 1 — Scenario / macro file (drives the FWL=YES shock: A / O / E)

**File:** `localRun/tseadfwd/input/scenariosClassique_ifrs9Sect_TNR_2026SP03.csv`

### 1a. Adverse and Extreme are identical in this file
The Adverse and Extreme rows are **byte-identical for every macro variable
(`IR_10Y_FR`, `IR_10Y_BE`, `IR_10Y_IT`) at every quarter**:

| Quarter | Adverse IR_10Y_FR | Extreme IR_10Y_FR |
|---------|-------------------|-------------------|
| 2024Q1…2025Q3 | 0.006 | 0.006 |
| 2025Q4 (projection date) | 0.007 | 0.007 |

But the **target output gives different values for A and E** — e.g.
`BCEF_MORTGAGE_Q`, term 1: Adverse = `0.935145851`, Extreme = `0.942746967`.

Because BCEF's only macro driver is `IR_10Y_FR` (per PARAMETRAGE), and A = E there,
**no computation can reproduce distinct Adverse vs Extreme values from this file.**

➡️ **Needed:** the real scenario file where **Adverse ≠ Extreme** (this sample has
them duplicated).

### 1b. Stress-leg calibration constant (`ref_shock`)
The FWL=YES scenarios apply a parallel shock that interpolates the INPUTS_RA
`STRESS (+)` / `STRESS (-)` legs, scaled by the macro rate delta at the projection
date:

```
delta(scenario) = macro[scenario][projection_date] - macro[Central][projection_date]
weight          = delta / ref_shock
```

Measured deltas at `projection_date = 2025Q4` (vs Central = 0.0135):

| Scenario | IR_10Y_FR | delta |
|----------|-----------|-------|
| Optimistic | 0.0350 | +0.0215 |
| Adverse | 0.0070 | −0.0065 |
| Extreme | 0.0070 | −0.0065 |

➡️ **Needed:** the magnitude that the `STRESS (+)` / `STRESS (-)` columns represent
(e.g. ±100 bps) — i.e. the value of `ref_shock` (config key
`tseadfwd_app.ref_shock`, currently a placeholder `1.0`). Alternatively, the
FWL=YES methodology note or one fully-worked scenario example. This can be
reverse-engineered against the target once 1a and Item 2 are supplied.

---

## Item 2 — RA input file (tail / runoff vintage)

**File:** `localRun/tseadfwd/input/Inputs_RA.xlsx`, sheet `RA_BCEF`

The 361 monthly values (CRD / RA STAT / RA FI / RE × BASELINE / STRESS (+) /
STRESS (-)) are a different vintage than the target's. Proof:

- `MORTGAGE` CRD is still **−1860 at month 360** (exposure has not run off), whereas
  the target **plateaus at `0.46688` from term 30 onward** — meaning the target's RA
  exposure runs off well before term 30.
- Central matches early but drifts in the tail; `CONSO` runs off to CRD = 0 at
  ~month 207 in this sample.

➡️ **Needed:** the `RA_BCEF` monthly series for the **25Q4 reference** that actually
produced `TS_EAD_FWD_25Q4_v1_small.csv` (exposure that runs off like the target).

---

## Summary

| Item | File | What to provide |
|------|------|-----------------|
| 1 | `scenariosClassique_ifrs9Sect_TNR_2026SP03.csv` | Version where **Adverse ≠ Extreme**, plus the `ref_shock` magnitude (or methodology) |
| 2 | `Inputs_RA.xlsx` (`RA_BCEF`) | The **25Q4-matching** RA monthly series (exposure that runs off like the target) |
| — | `PARAMETRAGE` | ✅ already corrected → `PARAMETRAGE_corrected.xlsx` (CONSO `FWL=NO`, `MORTGAGE` row added) |

Notes:
- Scenario **S (Secto)** is intentionally not implemented — there is no Secto data in
  the scenario file (only Central / Adverse / Optimistic / Extreme). Adding it later
  is a one-line change once data exists.
- `EAD_MATRIX_ID` in the target omits `RATE_TYPE`; only one rate type (`TF`) is
  present in the sample.
