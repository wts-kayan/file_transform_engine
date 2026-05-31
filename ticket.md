# EAD FWD TS Computation Specification

## Context

Currently, the computation to create the EAD FWD TS file is done and input manually by Risk team and to avoid operational issues due it complexity, the aim of this US is to compute automatically in the TWIST projection.

To do so, there is an US dedicated to TWIST layout behaviour.

This US is to specify how to compute in the enginehereher   

The main goal is: Risk team will input **"Scenario"** (as already is input), **PARAMETRAGE** and **INPUTS_RA** file (both attached) in TWIST (Parameters > Projection). The engine will compute the final EAD FWD Term Structure.

## Management rules

In the new computation, the **EAD_MATRIX_ID** is defined by the rule:

* `PERIMETER + "_" + SEGMENT + "_" + RATE_TYPE + "_Q"` if `computation_frequency = quarterly`
* `PERIMETER + "_" + SEGMENT + "_" + RATE_TYPE + "_Y"` if `computation_frequency = Annually`

### Scenario_ID

* C - Central
* A - Adverse
* O - Optimistic
* E - Extreme
* S - Secto

## TERM

When Q computation, will start in 0 and each term is:

`0.25, 0.5, ....50 and 100`

When Y computation, will start in 0 and the next are:

`1, 2...50 and 100`

---

# EAD_RA_RATE

To replicate how EAD_RA_RATE, we have 4 cases:

1. IF FWL_TO_BE_APPLIED = "NO" AND QUARTERLY COMPUTATION
2. IF FWL_TO_BE_APPLIED = "NO" AND YEARLY COMPUTATION
3. IF FWL_TO_BE_APPLIED = "YES" AND QUARTERLY COMPUTATION
4. IF FWL_TO_BE_APPLIED = "YES" AND YEARLY COMPUTATION

## Note

1. Each case is calculated with some specificities (detailed below) and the engine should follow the same formula.
2. For each case, we will need to calculate for each:

`PERIMETER × SEGMENT × RATE_TYPE × FWL_TYPE × METRIC × SCENARIO × TERM`

---

# 1st CASE: IF FWL_TO_BE_APPLIED = "NO" AND QUARTERLY COMPUTATION

For each term (0 to 30):

## STEP 1: Calculate the avg for each Metric (CRD, RA STAT, RA, FI, RE)

```text
CRD_Qi = SUM(CRD_M1, CRD_M2, CRD_M3)/3

RA STAT_Q1 = RA STAT_M1 + (RA STAT_M2/2)

for next quarters (example):
[RA STAT_M2 /2] + RA STAT_M3 + RA STAT_M4 + [RA STAT_M5 /2]

RA FI_Qi = Same formula for RA_STAT

RE_Qi = Same formula for RA_STAT
```

## STEP 2 RA DETAIL

```text
DET RA STAT_Qi = -RA STAT_Qi / CRD_Qi

DET RA FI_Qi = Same formula for RA_STAT

DET RE_Qi = Same formula for RA_STAT
```

## STEP 3 VECTOR

```text
VECTOR_RA_Qi = 1-DET RA STAT_Qi
```

## STEP 4 - VECTOR FACTORED (FINAL EAD TS VALUE)

```text
VECTOR_FACTORED_RA_Q1 = VECTOR_RA_Q1

VECTOR_FACTORED_RA_Qn = VECTOR_FACTORED_RA_Qn * VECTOR_RA_Qn-1
```

Example:

```text
VECTOR_FACTORED_RA_Q1 = VECTOR_RA_Q1

VECTOR_FACTORED_RA_Q2 = VECTOR_FACTORED_RA_Q1 * VECTOR_RA_Q2

VECTOR_FACTORED_RA_Q3 = VECTOR_FACTORED_RA_Q2 * VECTOR_RA_Q3
```

From term 30,25 to 50 and 100 the value is the same of term 30.

---

# 2nd CASE: IF FWL_TO_BE_APPLIED = "NO" AND YEARLY COMPUTATION

For each term (0 to 30):

## STEP 1 : Annual average computation for each Metric (CRD, RA STAT, RA, FI, RE)

```text
CRD_Y1 = SUM(SUM(CRD_M1, CRD_M2, CRD_M3, CRD_M4, CRD_M5 , CRD_M6) / 6

CRD_Y2 (and all the next) = SUM(CRD_M7 , ... , CRD_M18) / 12

RA STAT_Y1 = SUM (RA STAT_M1, ... , RA STAT_M6)

for next quarters (example):
SUM (RA STAT_M7,..., RA STAT_M18) - following the next 12 terms

RA FI_Yi = Same formula for RA_STAT

RE_Yi = Same formula for RA_STAT
```

## STEP 2 RA DETAIL

```text
DET RA STAT_Yi = -RA STAT_Yi / CRD_Yi

DET RA FI_Yi = Same formula for RA_STAT

DET RE_Yi = Same formula for RA_STAT
```

## STEP 3 VECTOR

```text
VECTOR_RA_Yi = 1-DET RA STAT_Yi
```

## STEP 4 - VECTOR FACTORED (FINAL EAD TS VALUE)

```text
VECTOR_FACTORED_RA_Y1 = VECTOR_RA_Y1

VECTOR_FACTORED_RA_Yn = VECTOR_FACTORED_RA_Yn * VECTOR_RA_Yn-1
```

From term 31 to 50 and 100 the value is the same of term 30.

There is no differences between scenarios - The same value will be considered for all scenarios.

---

# 3rd CASE: IF FWL_TO_BE_APPLIED = "YES" AND QUARTERLY COMPUTATION

## STEP 1

```text
CRD_Q1* (BASELINE)= SUM(CRD_M1 (BASELINE), CRD_M2 (BASELINE), CRD_M3 (BASELINE)) / 3

CRD_Q1* (STRESS -)= SUM(CRD_M1 STRESS -, CRD_M2 (STRESS -, CRD_M3 STRESS -) / 3

CRD_Q1* (STRESS +)= SUM(CRD_M1 STRESS +, CRD_M2 (STRESS +, CRD_M3 STRESS +) / 3
```

*same formula for next quarters*

```text
RA STAT_Q1 (BASELINE)= RA STAT_M1 (BASELINE) + [(RA STAT_M2 (BASELINE))/2]

RA STAT_Q1 (STRESS-)= RA STAT_M1 (STRESS-) + [(RA STAT_M2 (STRESS-))/2]

RA STAT_Q1 (STRESS+)= RA STAT_M1 (STRESS+) + [(RA STAT_M2 (STRESS+))/2]

RA STAT_Qn (BASELINE)= [(RA STAT_Mn (BASELINE))/2] + RA STAT_Mn+1 (BASELINE) + RA STAT_Mn+2 (BASELINE) + [(RA STAT_Mn+3 (BASELINE))/2]
```

## STEP 2 - Parallel shock computation

```text
Shock_RA FI_Qi (STRESS +)

Shock_RA FI_Qi (STRESS -)
```

Computed only for RA FI and RE (same computation for both).

## STEP 3 - Rate curve Delta from baseline

```text
Rate_(Optimistic/Adverse/Extreme)_Qi =
[Macro_variable_Qi (Optimistic/Adverse/Extreme) - Macro_variable_Qi (Baseline)] * 100
```

## STEP 4 - RA Detail

```text
RA STAT_Qi = -RA STAT_Qi (BASELINE) / CRD_Qi (BASELINE)

RA FI RE_Qi (CENTRAL)=
[RA FI_Qi (BASELINE) + RE_Qi (BASELINE)] / CRD_Qi (BASELINE)
```

## STEP 5 - RA Total

```text
RA_Qi (SCENARIO)= RA STAT_Qi (BASELINE) + RA FI RE_Qi (SCENARIO)
```

## STEP 6 - VECTOR

```text
VECTOR_RA_Qi (Scenario)= 1-RA_Qi (Scenario)
```

## STEP 7 - VECTOR FACTORED (FINAL EAD TS)

```text
VECTOR_FACTORED_RA_Q1 (Scenario)= VECTOR_RA_Q1 (Scenario)

VECTOR_FACTORED_RA_Qi (Scenario)=
VECTOR_FACTORED_RA_Qi-1 (Scenario) * VECTOR_RA_Qi (Scenario)
```

From term 30,25 to 50 and 100 the value is the same of term 30.

There is no differences between scenarios - The same value will be considered for all scenarios.

---

# 4th CASE: IF FWL_TO_BE_APPLIED = "YES" AND YEARLY COMPUTATION

## STEP 1

```text
CRD_Y1 (BASELINE)= SUM(CRD_M1 (BASELINE), ..., CRD_M6 (BASELINE)) / 6

CRD_Y1 (STRESS -)= SUM(CRD_M1 (STRESS -), ..., CRD_M6 (STRESS -)) / 6

CRD_Y1 (STRESS +)= SUM(CRD_M1 (STRESS +), ..., CRD_M3 (STRESS +)) / 6
```

## STEP 2 - Parallel shock computation

Computed only for RA FI and RE (same computation for both).

## STEP 3 - Rate curve Delta from baseline

```text
Rate_(Optimistic/Adverse/Extreme)_Yi =
[Macro_variable_Yi (Optimistic/Adverse/Extreme) - Macro_variable_Yi (Baseline)] * 100
```

## STEP 4 - RA Detail

```text
RA STAT_Yi = -RA STAT_Yi (BASELINE) / CRD_Yi (BASELINE)

RA FI RE_Yi (CENTRAL)=
[RA FI_Y1 (BASELINE) + RE_Y1 (BASELINE)] / CRD_Y1 (BASELINE)
```

## STEP 5 - RA Total

```text
RA_Yi (SCENARIO)= RA STAT_Yi (BASELINE) + RA FI RE_Yi (SCENARIO)
```

## STEP 6 - VECTOR

```text
VECTOR_RA_Yi (Scenario)= 1-RA_Yi (Scenario)
```

## STEP 7 - VECTOR FACTORED (FINAL EAD TS)

```text
VECTOR_FACTORED_RA_Y1 (Scenario)= VECTOR_RA_Y1 (Scenario)

VECTOR_FACTORED_RA_Yi (Scenario)=
VECTOR_FACTORED_RA_Yi-1 (Scenario) * VECTOR_RA_Yi (Scenario)
```

From term 31 to 50 and 100 the value is the same of term 30.

There is no differences between scenarios - The same value will be considered for all scenarios.
