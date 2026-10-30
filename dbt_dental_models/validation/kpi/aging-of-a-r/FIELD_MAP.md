# Aging of A/R — OD Report ↔ Mart Field Map

**Goal:** Same practice total AR (and aging buckets) as OpenDental's **Aging of A/R** report.

**Mart:** `marts.mart_ar_summary` (grain: patient × provider × `snapshot_date`)  
**OD report:** Reports → Standard → Monthly → Aging of A/R  
**Golden files:** `golden/od_aging_of_a_r_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv`

**Primary measure (registry):** practice `total_ar_balance` ≡ `SUM(mart_ar_summary.total_balance)`  
**API primary field:** `GET /ar/kpi-summary` → `total_ar_outstanding`

---

## OD report options (lock after first export)

Fill from the report header / options dialog on the golden as-of day. Defaults below are
**hypotheses** until confirmed in `findings/` and a screenshot under `screenshots/`.

| Option | Locked value | Notes |
| --- | --- | --- |
| As-of / report date | **2026-09-30** | OpenDental rebuilds aging from transactions. It does not read `patient.BalTotal`. |
| Grouping | **Guarantor `BalTotal`** | Non-guarantor `BalTotal` is 0. Positive sum equals the guarantor sum. |
| Patient status | **Patient + Inactive** (`PatStatus` 0, 2) | Same inclusion as `dim_patient` / the mart |
| Balance filter | **`BalTotal > 0`** | Credit families are excluded. Net family total that day was $187,169.37. |
| Include write-offs / adjustments | Stored `BalTotal` | Printed report can recalculate. This close uses the stored balance. |
| Include insurance estimates | Not in the total | OD `InsEst` on this population was $114,811.46. Mart `insurance_estimate` was $1,791,482.25. |
| Clinic / provider | All | Single clinic |
| Age of account basis | Mart buckets: days since procedure, scaled to `total_balance` | Secondary. Stored OD buckets foot to the same total with a different split. |

**Report totals from OpenDental's own query, as of 2026-09-30** ([compare/od_aging_report_2026-09-30.sql](./compare/od_aging_report_2026-09-30.sql)):

| Column | Amount |
| --- | ---: |
| 0–30 | $83,193.42 |
| 31–60 | $40,990.62 |
| 61–90 | $28,564.79 |
| > 90 | $122,674.50 |
| Total | $275,423.33 |
| -Ins Est | $114,829.85 |
| -W/O Est | $0.00 |
| Patient | $160,593.48 |
| Families | 739 |

The four aging columns add up to Total. Patient = Total − Ins Est − W/O Est.

`mart_ar_summary` does not run this query. The 2026-10-12 match to stored `BalTotal` ($282,624.23) is a different number.

---

## Mart column validation rules

| Mart column | Source | Validation |
| --- | --- | --- |
| `snapshot_date` | `current_date` at dbt build | = OD as-of date |
| `total_balance` | `dim_patient.total_balance` (= source `patient.BalTotal`) | Sum over positive-balance patients = OD report **total** |
| `balance_0_30_days` | Scaled from `int_ar_balance` aging `0-30` | Secondary vs OD 0–30 / Current column |
| `balance_31_60_days` | Scaled `31-60` | Secondary |
| `balance_61_90_days` | Scaled `61-90` | Secondary |
| `balance_over_90_days` | Scaled `90+` (+ orphan → over-90 per DBT-FND-005) | Secondary |
| `patient_responsibility` | Mart split | Optional vs OD if report separates |
| `insurance_estimate` | Mart split | Optional |

**Inclusion:** mart keeps patients with `total_balance > 0` (Patient / Inactive). Zero-balance
patients are excluded (matches typical Aging of A/R outstanding-only view).

**Bucket scaling:** bucket columns are scaled so they sum to `total_balance` per patient. Prefer
**total** sign-off first; document bucket deltas without blocking.

---

## API surface (in scope)

| API field | Mart / SQL | Shown in |
| --- | --- | --- |
| `total_ar_outstanding` | `SUM(total_balance)` on latest snapshot | AR dashboard KPI |
| `current_amount` | `SUM(balance_0_30_days)` | KPI summary |
| `over_90_amount` | `SUM(balance_over_90_days)` | KPI summary |
| aging-summary `Current` | `SUM(balance_0_30_days)` | Aging chart |
| aging-summary `30 Day` | `SUM(balance_31_60_days)` | Aging chart |
| aging-summary `60 Day` | `SUM(balance_61_90_days)` | Aging chart |
| aging-summary `90 Day` | `SUM(balance_over_90_days)` | Aging chart |

**Out of scope for this KPI:** `dso_days`, `collection_rate`, `ar_ratio`, priority-queue,
risk-distribution, aging-trends (see plan / registry `known_deltas`).

Live smoke:

```bash
python api/tests/kpi/verify_aging_ar.py
```

---

## Comparison SQL index

| File | Layer |
| --- | --- |
| [compare/compare_aging_ar_staging.sql](./compare/compare_aging_ar_staging.sql) | Mart vs `dim_patient` / `int_ar_balance` reconstruction |
| [compare/compare_aging_ar_total.sql](./compare/compare_aging_ar_total.sql) | Mart total vs OD golden total |
| [compare/compare_aging_ar_buckets.sql](./compare/compare_aging_ar_buckets.sql) | Mart buckets vs OD bucket columns |
| [compare/compare_aging_ar_api.sql](./compare/compare_aging_ar_api.sql) | API-equivalent SQL vs mart |
