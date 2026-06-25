# KPI Validation Registry

Maps dashboard KPIs and mart models to OpenDental standard reports. Fill in OD menu paths and
manual URLs as you confirm them against your OpenDental version and `reference/opendental_manual/`.

**Status legend**: `not_started` · `golden_exported` · `compare_sql_draft` · `within_tolerance` · `delta_documented`

---

## Registry

| status | kpi_name | our_model | our_field_or_measure | od_report | od_menu_path | od_manual_url | date_basis | grain | tolerance | known_deltas |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| not_started | Daily net collections | `mart_daily_payments` | `net_collections_amount` | Daily Payments | Reports → Standard → *TBD* | *TBD* | payment date | day | ±0.5% or ±$10 | Refunds negative; unmapped payment types in `other_*` |
| not_started | Total production | `mart_provider_performance` | `total_production` | Production and Income | Reports → Standard → *TBD* | *TBD* | procedure date | day × provider | ±0.5% | Completed vs TP status; fee vs UCR |
| not_started | Total collections | `mart_provider_performance` | `total_collections` | Production and Income | Reports → Standard → *TBD* | *TBD* | payment date | day × provider | ±0.5% | Income vs refund direction |
| not_started | Collection rate | `mart_provider_performance` | `collection_efficiency` | Production and Income | Reports → Standard → *TBD* | *TBD* | mixed | day × provider | ±1.0% | Rolling 365d in exposures vs OD default window |
| not_started | AR total | `mart_ar_summary` | `total_ar_balance` | AR Aging | Reports → Standard → *TBD* | *TBD* | as-of snapshot | patient × provider × day | ±$50 or ±0.5% | Simplified DSO formula in exposures |
| not_started | Referral production | `mart_referral_source_kpis` | `production_value_in_period` | *TBD* | *TBD* | *TBD* | procedure month | month × referral × basis | ±1.0% | Three `period_basis` slices; do not sum patients across referrers |

---

## Field definitions

| Column | Description |
| --- | --- |
| `our_model` | dbt mart or fact in schema `marts` |
| `our_field_or_measure` | Column or documented measure from exposures / model YAML |
| `od_report` | Name as shown in OpenDental Reports UI |
| `od_menu_path` | Exact navigation path; include default filters from screenshot |
| `od_manual_url` | e.g. `https://opendental.com/manual243/reportproductionincome.html` |
| `date_basis` | Which date column OD uses vs our mart (`ProcDate`, `DatePay`, etc.) |
| `grain` | Aggregation level for comparison |
| `tolerance` | PASS if within; WARN if within 2× tolerance; else FAIL |
| `known_deltas` | Documented intentional or discovered differences |

---

## Validation windows

Record frozen windows used for golden exports so comparisons stay reproducible.

| window_id | date_from | date_to | notes |
| --- | --- | --- | --- |
| pilot_2025h2 | 2025-07-01 | 2026-06-12 | Aligns with `powerbi/report_checklist.md` |

---

## Golden file index

Maintained in `golden/golden_manifest.yml` when exports exist. CSV files live in `golden/` only
(local). Example structure: `golden/golden_manifest.example.yml`.

---

## Comparison SQL index

| kpi_name | compare_sql | status |
| --- | --- | --- |
| Daily net collections | `compare/compare_daily_collections.sql` | placeholder |
| Total production | `compare/compare_production_by_provider.sql` | not started |
| AR total | `compare/compare_ar_aging.sql` | not started |
