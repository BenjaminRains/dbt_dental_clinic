# Daily Net Collections — Validation Findings (2026-06-24)

**Golden export**: `golden/daily_payments_06242026_ins_pat_split.csv`  
**OD total**: $11,197.40  
**Status**: `within_tolerance` (staging and mart after fix)

---

## OD report parameters

- Date: 2026-06-24 (export window 6/24–6/25; only 6/24 had activity)
- Providers: All, Include Unearned
- Group by: Check, splits by provider
- All insurance / patient / claim payment types

---

## Reconciliation recipe (matches OD)

| Source | Table | Date column | 2026-06-24 total |
| --- | --- | --- | --- |
| Patient | `staging.stg_opendental__payment` | `payment_date` | $7,219.60 |
| Insurance | `staging.stg_opendental__claimpayment` | `check_date` | $3,977.80 |

PayType mapping (definition `item_name`):

| ID | Name | OD section |
| --- | --- | --- |
| 69 | Check | $1,051.30 |
| 70 | Cash | $677.00 |
| 71 | Credit Card | $4,172.10 |
| 634 | Cherry | $1,319.20 |
| 467 | Metlife EFT | $3,977.80 (claimpayment only) |

---

## Issues found and resolved

### 1. ETL lag (−$156.30) — resolved

OD included PayNum **931419** (Teller, $106.30) and **931420** (Skinner, $50.00) before analytics sync. After `mdc etl run -- --tables payment --tables paysplit`, staging matched OD exactly.

### 2. Mart omitted insurance and filtered patient payments — fixed in `mart_daily_payments.sql`

Prior mart used `fact_payment` only:

- No `claimpayment` path (−$3,977.80)
- `int_payment_split` dropped unallocated paysplits (Cherry, unallocated checks)
- PayType flags used 0/1 instead of clinic IDs (69/70/71/634)

Mart now unions patient staging + claim payment staging.

---

## Comparison SQL

- `compare/compare_daily_collections.sql` — mart vs OD total
- `compare/investigate_daily_collections_2026-06-24.sql` — full investigation queries

---

## Operational notes

- Run ETL before same-day KPI validation; late-posted payments (high PayNum) may lag.
- `int_insurance_payment_allocated` uses `insurance_finalized_date`, not `check_date` — do not use for Daily Payments KPI.
