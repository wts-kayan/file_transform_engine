# EAD FWD TS — Computation Specification

## Context

Currently, the computation to create the **EAD FWD TS** file is done and input manually by the Risk team. To avoid operational issues due to its complexity, the aim of this US is to compute it automatically in the TWIST projection.

To do so, there is a US dedicated to TWIST layout behaviour.

This US is to specify how to compute in the engine.

**Main goal:** The Risk team will input “Scenario” (as already is input), `PARAMETRAGE` and `INPUTS_RA` file (both attached) in TWIST (**Parameters > Projection**). The engine will compute the final EAD FWD Term Structure.

---

## Management rules

In the new computation, the **EAD_MATRIX_ID** is defined by the rule:

- `PERIMETER * "_" * SEGMENT * "" * RATE_TYPE * "_Q"` if computation_frequency = Quarterly
- `PERIMETER * "_" * SEGMENT * "" * RATE_TYPE * "_Y"` if computation_frequency = Annually

**Scenario_ID** will be determined as:

- `C` – Central
- `A` – Adverse
- `O` – Optimistic
- `E` – Extreme
- `S` – Secto

### TERM

- When **Q** computation: starts at 0 and each term is 0.25, 0.5, … 50 and 100
- When **Y** computation: starts at 0 and the next are 1, 2 … 50 and 100

---

## EAD_RA_RATE

To replicate how EAD_RA_RATE is computed, we have **4 cases**:

- **1st CASE:** IF `FWL_TO_BE_APPLIED = "NO"` AND QUARTERLY COMPUTATION
- **2nd CASE:** IF `FWL_TO_BE_APPLIED = "NO"` AND YEARLY COMPUTATION
- **3rd CASE:** IF `FWL_TO_BE_APPLIED = "YES"` AND QUARTERLY COMPUTATION
- **4th CASE:** IF `FWL_TO_BE_APPLIED = "YES"` AND YEARLY COMPUTATION

**Note:**

1. Each case is calculated with some specificities (detailed below) and the engine should follow the same formula.
2. For each case, we will need to calculate for each **PERIMETER × SEGMENT × RATE_TYPE × FWL_TYPE × METRIC × SCENARIO and TERM**.

---

## 1st CASE — FWL_TO_BE_APPLIED = “NO” AND QUARTERLY COMPUTATION

FOR EACH `PERIMETER × SEGMENT × RATE_TYPE × FWL_TYPE × METRIC`, calculate for each term (0 to 30):

### STEP 1 — Calculate the avg for each Metric (CRD, RA STAT, RA, FI, RE)

```
CRD_Qi      = SUM(CRD_M1, CRD_M2, CRD_M3) / 3
RA STAT_Q1  = RA STAT_M1 + (RA STAT_M2 / 2)
for next quarters (example): [RA STAT_M2 / 2] + RA STAT_M3 + RA STAT_M4 + [RA STAT_M5 / 2]
RA FI_Qi    = Same formula as RA_STAT
RE_Qi       = Same formula as RA_STAT
```

### STEP 2 — RA DETAIL

```
DET RA STAT_Qi = -RA STAT_Qi / CRD_Qi
DET RA FI_Qi   = Same formula as RA_STAT
DET RE_Qi      = Same formula as RA_STAT
```

### STEP 3 — VECTOR

```
VECTOR_RA_Qi = 1 - DET RA STAT_Qi
```

### STEP 4 — VECTOR FACTORED (FINAL EAD TS VALUE)

```
VECTOR_FACTORED_RA_Q1 = VECTOR_RA_Q1
VECTOR_FACTORED_RA_Qn = VECTOR_FACTORED_RA_Qn-1 * VECTOR_RA_Qn
```

**Example:**

```
VECTOR_FACTORED_RA_Q1 = VECTOR_RA_Q1
VECTOR_FACTORED_RA_Q2 = VECTOR_FACTORED_RA_Q1 * VECTOR_RA_Q2
VECTOR_FACTORED_RA_Q3 = VECTOR_FACTORED_RA_Q2 * VECTOR_RA_Q3
```

> From term 30.25 to 50 and 100 the value is the same as term 30.
> There are no differences between scenarios — the same value will be considered for all scenarios.

---

## 2nd CASE — FWL_TO_BE_APPLIED = “NO” AND YEARLY COMPUTATION

FOR EACH `PERIMETER × SEGMENT × RATE_TYPE × FWL_TYPE × METRIC`, calculate for each term (0 to 30):

### STEP 1 — Annual average computation for each Metric (CRD, RA STAT, RA, FI, RE)

```
CRD_Y1               = SUM(CRD_M1, CRD_M2, CRD_M3, CRD_M4, CRD_M5, CRD_M6) / 6
CRD_Y2 (and next)    = SUM(CRD_M7, … , CRD_M18) / 12

RA STAT_Y1           = SUM(RA STAT_M1, … , RA STAT_M6)
for next years (example): SUM(RA STAT_M7, … , RA STAT_M18) — following the next 12 terms
RA FI_Yi             = Same formula as RA_STAT
RE_Yi                = Same formula as RA_STAT
```

### STEP 2 — RA DETAIL

```
DET RA STAT_Yi = -RA STAT_Yi / CRD_Yi
DET RA FI_Yi   = Same formula as RA_STAT
DET RE_Yi      = Same formula as RA_STAT
```

### STEP 3 — VECTOR

```
VECTOR_RA_Yi = 1 - DET RA STAT_Yi
```

### STEP 4 — VECTOR FACTORED (FINAL EAD TS VALUE)

```
VECTOR_FACTORED_RA_Y1 = VECTOR_RA_Y1
VECTOR_FACTORED_RA_Yn = VECTOR_FACTORED_RA_Yn-1 * VECTOR_RA_Yn
```

**Example:**

```
VECTOR_FACTORED_RA_Y1 = VECTOR_RA_Y1
VECTOR_FACTORED_RA_Y2 = VECTOR_FACTORED_RA_Y1 * VECTOR_RA_Y2
VECTOR_FACTORED_RA_Y3 = VECTOR_FACTORED_RA_Y2 * VECTOR_RA_Y3
```

> From term 31 to 50 and 100 the value is the same as term 30.
> There are no differences between scenarios — the same value will be considered for all scenarios.

---

## 3rd CASE — FWL_TO_BE_APPLIED = “YES” AND QUARTERLY COMPUTATION

FOR EACH `PERIMETER × SEGMENT × RATE_TYPE × FWL_TYPE × METRIC`, calculate for each term (0 to 30):

### STEP 1 — Calculate the avg for each Metric (CRD, RA STAT, RA, FI, RE) and for BASELINE, STRESS- and STRESS+

```
CRD_Q1* (BASELINE)  = SUM(CRD_M1 (BASELINE), CRD_M2 (BASELINE), CRD_M3 (BASELINE)) / 3
CRD_Q1* (STRESS -)  = SUM(CRD_M1 (STRESS -), CRD_M2 (STRESS -), CRD_M3 (STRESS -)) / 3
CRD_Q1* (STRESS +)  = SUM(CRD_M1 (STRESS +), CRD_M2 (STRESS +), CRD_M3 (STRESS +)) / 3
```

*same formula for next quarters*

**RA STAT_Qi, RA FI_Qi and RE_Qi:**

```
RA STAT_Q1 (BASELINE)  = RA STAT_M1 (BASELINE)  + [(RA STAT_M2 (BASELINE)) / 2]
RA STAT_Q1 (STRESS -)  = RA STAT_M1 (STRESS -)  + [(RA STAT_M2 (STRESS -)) / 2]
RA STAT_Q1 (STRESS +)  = RA STAT_M1 (STRESS +)  + [(RA STAT_M2 (STRESS +)) / 2]

RA STAT_Qn (BASELINE)  = [(RA STAT_Mn (BASELINE)) / 2] + RA STAT_Mn+1 (BASELINE)
                         + RA STAT_Mn+2 (BASELINE) + [(RA STAT_Mn+3 (BASELINE)) / 2]
RA STAT_Qn (STRESS -)  = [(RA STAT_Mn (STRESS -)) / 2] + RA STAT_Mn+1 (STRESS -)
                         + RA STAT_Mn+2 (STRESS -) + [(RA STAT_Mn+3 (STRESS -)) / 2]
RA STAT_Qn (STRESS +)  = [(RA STAT_Mn (STRESS +)) / 2] + RA STAT_Mn+1 (STRESS +)
                         + RA STAT_Mn+2 (STRESS +) + [(RA STAT_Mn+3 (STRESS +)) / 2]
```

### STEP 2 — Parallel shock computation (computed only for RA FI and RE — same computation for both)

```
Shock_RA FI_Qi (STRESS +) = [-RA FI_Qn (STRESS +) / CRD_Qn (STRESS +)] - [-RA FI_Qn (BASELINE) / CRD_Qn (BASELINE)]
Shock_RA FI_Qi (STRESS -) = [-RA FI_Qn (STRESS -) / CRD_Qn (STRESS -)] - [-RA FI_Qn (BASELINE) / CRD_Qn (BASELINE)]
```

### STEP 3 — Rate curve Delta from baseline

Calculated for Optimistic, Adverse and Extreme — check in `PARAMETRAGE` the info to be used (column G “MACRO_VARIABLE”).

```
Rate_(Optimistic/Adverse/Extreme)_Qi =
    [Macro_variable_Qi (Optimistic/Adverse/Extreme) - Macro_variable_Qi (Baseline)] * 100
```

### STEP 4 — RA Detail (for all RA: RA STAT and RA FI + RE, per scenario)

```
RA STAT_Qi = -RA STAT_Qi (BASELINE) / CRD_Qi (BASELINE)

RA FI_RE_Qi (CENTRAL)          = - [RA FI_Qi (BASELINE) + RE_Qi (BASELINE)] / CRD_Qi (BASELINE)
RA FI_RE_Qi (ADVERSE/EXTREME)  = RA FI_RE_Qi (BASELINE)
                                 - [Shock_RA FI_Qi (STRESS -) + Shock_RE_Qi (STRESS -)] * Rate_(Adverse/Extreme)_Qi / 100
RA FI_RE_Qi (OPTIMISTIC)       = RA FI_RE_Qi (BASELINE)
                                 - [Shock_RA FI_Qi (STRESS +) + Shock_RE_Qi (STRESS +)] * Rate_Optimistic_Qi / 100
```

### STEP 5 — RA Total (for each scenario)

```
RA_Qi (Scenario) = RA STAT_Qi (Scenario) + RA FI_RE_Qi (Scenario)
```

### STEP 6 — VECTOR (for each scenario)

```
VECTOR_RA_Qi (Scenario) = 1 - RA_Qi (Scenario)
```

### STEP 7 — VECTOR FACTORED (FINAL EAD TS)

```
VECTOR_FACTORED_RA_Q1 (Scenario) = VECTOR_RA_Q1 (Scenario)
VECTOR_FACTORED_RA_Qi (Scenario) = VECTOR_FACTORED_RA_Qi-1 (Scenario) * VECTOR_RA_Qi (Scenario)
```

> From term 30.25 to 50 and 100 the value is the same as term 30.
> There are no differences between scenarios — the same value will be considered for all scenarios.

---

## 4th CASE — FWL_TO_BE_APPLIED = “YES” AND YEARLY COMPUTATION

FOR EACH `PERIMETER × SEGMENT × RATE_TYPE × FWL_TYPE × METRIC`, calculate for each term (0 to 30):

### STEP 1 — Calculate the avg for each Metric (CRD, RA STAT, RA, FI, RE) and for BASELINE, STRESS- and STRESS+

```
CRD_Y1 (BASELINE)  = SUM(CRD_M1 (BASELINE), … , CRD_M6 (BASELINE)) / 6
CRD_Y1 (STRESS -)  = SUM(CRD_M1 (STRESS -), … , CRD_M6 (STRESS -)) / 6
CRD_Y1 (STRESS +)  = SUM(CRD_M1 (STRESS +), … , CRD_M6 (STRESS +)) / 6

CRD_Y2* (BASELINE) = SUM(CRD_M7 (BASELINE), … , CRD_M18 (BASELINE)) / 12
CRD_Y2* (STRESS -) = SUM(CRD_M7 (STRESS -), … , CRD_M18 (STRESS -)) / 12
CRD_Y2* (STRESS +) = SUM(CRD_M7 (STRESS +), … , CRD_M18 (STRESS +)) / 12
```

*same formula for next years*

**RA STAT_Yi, RA FI_Yi and RE_Yi:**

```
RA STAT_Y1 (BASELINE)  = SUM(RA STAT_M1 (BASELINE), … , RA STAT_M6 (BASELINE))
RA STAT_Y1 (STRESS -)  = SUM(RA STAT_M1 (STRESS -), … , RA STAT_M6 (STRESS -))
RA STAT_Y1 (STRESS +)  = SUM(RA STAT_M1 (STRESS +), … , RA STAT_M6 (STRESS +))

RA STAT_Y2* (BASELINE) = SUM(RA STAT_M7 (BASELINE), … , RA STAT_M18 (BASELINE))
RA STAT_Y2* (STRESS -) = SUM(RA STAT_M7 (STRESS -), … , RA STAT_M18 (STRESS -))
RA STAT_Y2* (STRESS +) = SUM(RA STAT_M7 (STRESS +), … , RA STAT_M18 (STRESS +))
```

### STEP 2 — Parallel shock computation (computed only for RA FI and RE — same computation for both)

```
Shock_RA FI_Yi (STRESS +) = [-RA FI_Yn (STRESS +) / CRD_Yn (STRESS +)] - [-RA FI_Yn (BASELINE) / CRD_Yn (BASELINE)]
Shock_RA FI_Yi (STRESS -) = [-RA FI_Yn (STRESS -) / CRD_Yn (STRESS -)] - [-RA FI_Yn (BASELINE) / CRD_Yn (BASELINE)]
```

### STEP 3 — Rate curve Delta from baseline

Calculated for Optimistic, Adverse and Extreme — check in `PARAMETRAGE` the info to be used (column G “MACRO_VARIABLE”).

```
Rate_(Optimistic/Adverse/Extreme)_Yi =
    [Macro_variable_Yi (Optimistic/Adverse/Extreme) - Macro_variable_Yi (Baseline)] * 100
```

### STEP 4 — RA Detail (for all RA: RA STAT and RA FI + RE, per scenario)

```
RA STAT_Yi = -RA STAT_Yi (BASELINE) / CRD_Yi (BASELINE)

RA FI_RE_Yi (CENTRAL)          = - [RA FI_Y1 (BASELINE) + RE_Y1 (BASELINE)] / CRD_Y1 (BASELINE)
RA FI_RE_Yi (ADVERSE/EXTREME)  = RA FI_RE_Yi (BASELINE)
                                 - [Shock_RA FI_Yi (STRESS -) + Shock_RE_Yi (STRESS -)] * Rate_(Adverse/Extreme)_Yi / 100
RA FI_RE_Yi (OPTIMISTIC)       = RA FI_RE_Yi (BASELINE)
                                 - [Shock_RA FI_Yi (STRESS +) + Shock_RE_Yi (STRESS +)] * Rate_Optimistic_Yi / 100
```

### STEP 5 — RA Total (for each scenario)

```
RA_Yi (Scenario) = RA STAT_Yi (BASELINE) + RA FI_RE_Yi (Scenario)
```

### STEP 6 — VECTOR (for each scenario)

```
VECTOR_RA_Yi (Scenario) = 1 - RA_Yi (Scenario)
```

### STEP 7 — VECTOR FACTORED (FINAL EAD TS)

```
VECTOR_FACTORED_RA_Y1 (Scenario) = VECTOR_RA_Y1 (Scenario)
VECTOR_FACTORED_RA_Yi (Scenario) = VECTOR_FACTORED_RA_Yi-1 (Scenario) * VECTOR_RA_Yi (Scenario)
```

> From term 31 to 50 and 100 the value is the same as term 30.
> There are no differences between scenarios — the same value will be considered for all scenarios.
