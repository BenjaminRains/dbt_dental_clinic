# KPI Validation Registry



Maps dashboard KPIs and mart models to OpenDental standard reports. Each row's **folder** under

`validation/kpi/` uses the **OD report name** in kebab-case (e.g. `payments` for Payments).



The registry tracks **logic parity**, not just number matching: `our_field_or_measure` is what we

claim equals OD's definition; `known_deltas` lists accepted rule or timing differences; tolerance

is the check that implemented logic produces the same result as OD on golden dates.



**Status legend**: `not_started` · `golden_exported` · `compare_sql_draft` · `within_tolerance` · `delta_documented`



---



## Registry



| status | folder (OD report) | kpi_name | our_model | our_field_or_measure | od_report | od_menu_path | tolerance | known_deltas |

| --- | --- | --- | --- | --- | --- | --- | --- | --- |

| within_tolerance | [daily-payments](./daily-payments/) | Daily net collections | `mart_daily_payments` | `net_collections_amount` | Payments | Reports → Standard → Daily → Payments | ±0.5% or ±$10 | **Complete (2026-06-26):** 3 golden dates; layers 1–4 PASS; [VALIDATION_REPORT.md](./daily-payments/VALIDATION_REPORT.md). ETL before same-day validate; income transfers net to $0. |

| not_started | [production-and-income](./production-and-income/) | Total production | `mart_provider_performance` | `total_production` | Production and Income | Reports → Standard → *TBD* | ±0.5% | Completed vs TP status; fee vs UCR |

| not_started | [production-and-income](./production-and-income/) | Total collections | `mart_provider_performance` | `total_collections` | Production and Income | Reports → Standard → *TBD* | ±0.5% | Income vs refund direction |

| not_started | [production-and-income](./production-and-income/) | Collection rate | `mart_provider_performance` | `collection_efficiency` | Production and Income | Reports → Standard → *TBD* | ±1.0% | Rolling 365d vs OD default window |

| not_started | [aging-of-a-r](./aging-of-a-r/) | AR total | `mart_ar_summary` | `total_ar_balance` | Aging of A/R | Reports → Standard → Monthly → Aging of A/R | ±$50 or ±0.5% | Simplified DSO in exposures |

| not_started | *TBD* | Referral production | `mart_referral_source_kpis` | `production_value_in_period` | *TBD* | *TBD* | ±1.0% | Three `period_basis` slices |



---



## Field definitions



| Column | Description |

| --- | --- |

| `folder` | Directory under `validation/kpi/` — kebab-case OD report name |

| `our_model` | dbt mart in schema `marts` |

| `our_field_or_measure` | Column or measure from exposures / model YAML |

| `od_report` | Name as shown in OpenDental Reports UI |

| `tolerance` | Acceptable delta vs OD golden — see [Tolerance](./README.md#tolerance) in KPI README |



### Tolerance rules (global)



Each registry row states that KPI's thresholds (absolute dollars and/or percent). A compare run **PASS**es when **either** threshold is met:



- `abs(mart − od) ≤ amount_abs` (e.g. $10), **or**

- `abs(mart − od) / abs(od) < pct` (e.g. 0.005 = 0.5%)



**WARN** when the delta exceeds tolerance but is still within **2×** the percent threshold (below 1.0%). **FAIL** otherwise. Zero OD and zero mart → PASS. Missing OD golden → PENDING.



Snapshot YAML (`golden_manifest.yml`) and compare SQL must use the same `amount_abs` / `pct` as this table.



---



## Validation windows (payments)



| window_id | date_from | date_to | notes |

| --- | --- | --- | --- |

| pilot_2025h2 | 2025-07-01 | 2026-06-12 | Aligns with `powerbi/report_checklist.md` |

| daily_2026-06-24 | 2026-06-24 | 2026-06-24 | Golden `od_daily_payments_06242026_06242026.csv`; PASS — local `findings/2026-06-24.md` |

| daily_2025-10-07 | 2025-10-07 | 2025-10-07 | Golden `od_daily_payments_10072025_10072025.csv`; PASS — local `findings/2025-10-07.md` |

| daily_2025-11-08 | 2025-11-08 | 2025-11-08 | Golden `od_daily_payments_11082025_11082025.csv`; PASS (exact) — local `findings/2025-11-08.md` |



Golden path: `daily-payments/golden/`



**Naming:** `od_daily_payments_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv` — compact MMDDYYYY for report from/to dates (same date twice for single-day exports). Example: `od_daily_payments_06242026_06242026.csv`. Snapshot: `golden/snapshots/od_daily_payments_06242026_06242026.snapshot.yml`.



---



## Comparison SQL index (daily-payments)



| compare_sql | status |

| --- | --- |

| [compare/compare_daily_collections.sql](./daily-payments/compare/compare_daily_collections.sql) | OD validated — all three golden dates |

| [compare/compare_daily_collections_staging.sql](./daily-payments/compare/compare_daily_collections_staging.sql) | mart vs staging |

| [compare/compare_daily_collections_api.sql](./daily-payments/compare/compare_daily_collections_api.sql) | API-equivalent SQL vs mart vs OD |

| [compare/compare_daily_payments_all_fields.sql](./daily-payments/compare/compare_daily_payments_all_fields.sql) | all mart columns + OD sections |

| [compare/compare_daily_payments_row_level.sql](./daily-payments/compare/compare_daily_payments_row_level.sql) | PHI row-level golden diff |

| [compare/investigate_daily_collections_2025-10-07.sql](./daily-payments/compare/investigate_daily_collections_2025-10-07.sql) | OD validated 2025-10-07 |

| [compare/investigate_daily_collections_2025-11-08.sql](./daily-payments/compare/investigate_daily_collections_2025-11-08.sql) | OD validated 2025-11-08 |

| `api/tests/kpi/verify_daily_collections.py` | Live API smoke — **3/3 PASS** (2026-06-26) |



Future reports: add compare SQL under `production-and-income/compare/`, `aging-of-a-r/compare/`, etc.

