# Platform findings index

Findings that affect **multiple KPIs** or the **data platform** itself — not a single OD
report definition. KPI-specific validation instances live under
`dbt_dental_models/validation/kpi/{kpi}/findings/{date}.md`.

## ETL findings

| ID | Title | Type | Status |
| --- | --- | --- | --- |
| [ETL-FND-001](./ETL-FND-001-replica-row-drift-procedurelog.md) | Replica row drift — `procedurelog` | Pipeline defect + observability gap | Mitigated (local KPI PASS; clinic deploy pending) |

See also: [ETL replica fidelity roadmap](../etl/ETL_REPLICA_FIDELITY_ROADMAP.md) (schema analyzer, Layer 0, Sunday full refresh).

## dbt findings

| ID | Title | Type | Status |
| --- | --- | --- | --- |
| [DBT-FND-001](./DBT-FND-001-insurance-coverage-plan-id-grain.md) | `int_insurance_coverage` keys on `patplan_id` as `insurance_plan_id` | Mart / dbt defect | Open |
| [DBT-FND-002](./DBT-FND-002-ar-shared-calcs-metadata-status-filter.md) | AR shared_calcs metadata null — blank ProcStatus labels | Mart / dbt defect | Partially fixed |
| [DBT-FND-003](./DBT-FND-003-claim-snapshot-claimproc-fanout.md) | `int_claim_snapshot` fan-out on claim+procedure joins | Mart / dbt defect | Fixed |
| [DBT-FND-004](./DBT-FND-004-ar-analysis-claim-payment-distinct-collapse.md) | `int_ar_analysis` DISTINCT collapses same-dollar claimproc pays | Mart / dbt defect | Fixed |
| [DBT-FND-005](./DBT-FND-005-ar-summary-orphan-balance-no-aging.md) | `mart_ar_summary` dim balance with zero aging buckets | Mart / dbt defect | Fixed |

## Finding types (taxonomy)

| Type | Meaning | Example |
| --- | --- | --- |
| **Pipeline defect** | Sync design or config fails to keep replica equal to source | Stale `ProcStatus` after TP → Complete |
| **Observability gap** | Drift possible but no automated detection | KPI manual compare found 75% production gap |
| **Mart / dbt defect** | Transform logic wrong vs staging | Would show staging ≠ mart |
| **Data quality** | Source clinic data wrong or inconsistent | Not this case — MySQL matches OD golden |
| **KPI definition** | Warehouse intentionally differs from OD report | Document in `FIELD_MAP.md` |
| **ETL lag** | New rows not yet copied (fresh inserts) | daily-payments 2026-06-24 same-day PayNums |

Implementation for ETL-FND-001 tracked in [TODO.md](../../TODO.md#etl-fnd-001--replica-row-drift-procedurelog).
