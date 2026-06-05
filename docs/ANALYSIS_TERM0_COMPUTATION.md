# Analysis — Detailed Term-0 (Central) computation

Branch: `analysis/term0-computation`. Worked example of the `EAD_RA_RATE` at **Term 0**, **Central**,
**quarterly** for four matrices, following the business schema
(`Schema_EAD_FWD_20260601_v4.xlsx`) as implemented in `PrimaryView` / `PrimaryMapper`.

| Matrix | `RATE_TYPE` | `FWL_TO_BE_APPLIED` | Macro var | Central loss-rate rule |
|---|---|---|---|---|
| `BCEF / MORTGAGE` | TF | **YES** | `IR_10Y_FR` | `RA = -(RA_STAT + RA_FI + RE) / CRD` |
| `BGL / MORTGAGE`  | TF | **YES** | `IR_10Y_BE` | `RA = -(RA_STAT + RA_FI + RE) / CRD` |
| `BNL / MORTGAGE`  | TF | **YES** | `IR_10Y_IT` | `RA = -(RA_STAT + RA_FI + RE) / CRD` |
| `BPLS / 45629`    | *(blank in PARAMETRAGE)* | **NO** | `NONE` | `RA = -RA_STAT / CRD` *(FI/RE excluded)* |

> **Central applies no scenario shock.** The macro `Rate/100` shock only differentiates
> Adverse / Optimistic / Extreme. For the Central scenario the loss rate is the plain rule above:
> `centralRa` (STAT+FI+RE) for FWL=YES, `statOnlyRa` (STAT only) for FWL=NO.

---

## 0. Common steps (Term 0 = quarter Q1, period p = 1)

The output term grid is `0, 0.25, 0.5, …`; **Term 0 maps to the first quarter Q1** (period 1).

**Step 1 — quarterly aggregation of the monthly inputs `M1..M3`** (`PrimaryView.aggregate`):

```
CRD_Q1     = (CRD_M1 + CRD_M2 + CRD_M3) / 3        # CRD = block mean of 3 months
RA_STAT_Q1 = RA_STAT_M1 + RA_STAT_M2 / 2           # RA metrics: half-weight, Q1 special form
RA_FI_Q1   = RA_FI_M1   + RA_FI_M2   / 2
RE_Q1      = RE_M1      + RE_M2      / 2
```
(All from the **BASELINE** leg. Q1 uses only `M1` + ½·`M2`; later quarters use the 4-month
half-weight window `M[3n-4]/2 + M[3n-3] + M[3n-2] + M[3n-1]/2`.)

**Step 2 — Central loss rate** (`if CRD_Q1 == 0 → RA_Q1 = 0`, the run-off guard):
- FWL=YES: `RA_Q1 = -(RA_STAT_Q1 + RA_FI_Q1 + RE_Q1) / CRD_Q1`
- FWL=NO : `RA_Q1 = -(RA_STAT_Q1) / CRD_Q1`

**Step 3 — vector & factored value:**
```
VECTOR_Q1          = 1 - RA_Q1
VECTOR_FACTORED_Q1 = VECTOR_Q1            # first period seeds the cumulative product
EAD_RA_RATE(Term 0) = VECTOR_FACTORED_Q1
```

---

## 1. `BCEF_MORTGAGE_TF_Q` — Central — Term 0  ✅ (data available)

**Inputs** — `RA_BCEF` sheet, `MORTGAGE / TF / BASELINE` (`Inputs_RA_v2.xlsx`, full precision):

| Metric | M1 | M2 | M3 |
|---|---|---|---|
| CRD     | −92924.788279 | −92367.666543 | −91797.273187 |
| RA STAT | 380.217259 | 376.391307 | 372.536440 |
| RA FI   | 18.414014 | 18.414014 | 18.414014 |
| RE      | 11.048408 | 11.048408 | 11.048408 |

**Step 1 — aggregate Q1:**
```
CRD_Q1     = (-92924.788279 - 92367.666543 - 91797.273187) / 3 = -92363.242670
RA_STAT_Q1 = 380.217259 + 376.391307 / 2 = 568.412913
RA_FI_Q1   = 18.414014  + 18.414014  / 2 = 27.621021
RE_Q1      = 11.048408  + 11.048408  / 2 = 16.572612
```

**Step 2 — Central RA (FWL=YES → STAT+FI+RE):**
```
numerator = 568.412913 + 27.621021 + 16.572612 = 612.606546
RA_Q1     = -(612.606546) / (-92363.242670) = 0.00663261
```

**Step 3 — vector / factored:**
```
VECTOR_Q1           = 1 - 0.00663261 = 0.99336739
EAD_RA_RATE(Term 0) = 0.99336739  ≈  0,993367
```

(With the older rounded `Inputs_RA.xlsx` — CRD ≈ −92363.33, STAT ≈ 568, FI ≈ 27, RE ≈ 16.5 — the
result is the same to ~5 dp.)

---

## 2. `BGL_MORTGAGE_TF_Q` — Central — Term 0  (FWL=YES; data not in local sample)

Identical recipe to §1 (FWL=YES → STAT+FI+RE). The macro var is `IR_10Y_BE`, but **Central uses no
shock**, so the macro var is irrelevant at Term 0 Central. Plug the `RA_BGL` MORTGAGE/TF/BASELINE
`M1..M3` into:
```
CRD_Q1     = (CRD_M1 + CRD_M2 + CRD_M3) / 3
RA_STAT_Q1 = RA_STAT_M1 + RA_STAT_M2/2
RA_FI_Q1   = RA_FI_M1   + RA_FI_M2/2
RE_Q1      = RE_M1      + RE_M2/2
RA_Q1      = -(RA_STAT_Q1 + RA_FI_Q1 + RE_Q1) / CRD_Q1
EAD(Term 0) = 1 - RA_Q1
```
**Data:** the local RA workbook only contains the `RA_BCEF` sheet; an `RA_BGL` sheet (or a workbook
carrying perimeter `BGL`) is required to produce a number.

---

## 3. `BNL_MORTGAGE_TF_Q` — Central — Term 0  (FWL=YES; data not in local sample)

Identical to §2 with macro var `IR_10Y_IT` (again irrelevant for Central Term 0). Needs the `RA_BNL`
MORTGAGE/TF/BASELINE `M1..M3`:
```
RA_Q1       = -(RA_STAT_Q1 + RA_FI_Q1 + RE_Q1) / CRD_Q1
EAD(Term 0) = 1 - RA_Q1
```

---

## 4. `BPLS_45629_Q` — Central — Term 0  (FWL=NO → **stat-only**; data not in local sample)

`BPLS / 45629` is **FWL=NO**, so the Central loss rate uses **RA STAT only** — `RA_FI` and `RE` are
aggregated but **not** included (schema FWL=NO block; engine `statOnlyRa`):
```
CRD_Q1      = (CRD_M1 + CRD_M2 + CRD_M3) / 3
RA_STAT_Q1  = RA_STAT_M1 + RA_STAT_M2/2
RA_Q1       = -(RA_STAT_Q1) / CRD_Q1            # FI/RE excluded
EAD(Term 0) = 1 - RA_Q1
```
**Notes**
- `RATE_TYPE` is **blank** for `BPLS/45629` (`PARAMETRAGE_corrected.xlsx`), so the id is
  **`BPLS_45629_Q`** — the blank rate-type token is dropped (see Q29), not `BPLS_45629_I_Q`.
- **Data:** no `BPLS` perimeter in the local RA workbook; a sheet with `BPLS / 45629` BASELINE
  `M1..M3` is required to produce a number.

---

## Data availability summary

The local RA workbooks (`Inputs_RA.xlsx`, `Inputs_RA_v2.xlsx`) contain **only the `RA_BCEF` sheet**
(perimeter `BCEF`). So §1 (BCEF MORTGAGE) is fully worked; §§2–4 give the exact formula and need
their perimeter's RA sheet (`RA_BGL`, `RA_BNL`, and a `BPLS` sheet) to compute actual numbers.
