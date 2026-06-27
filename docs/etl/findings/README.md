# ETL platform findings index

Findings that affect **multiple KPIs** or the **replication pipeline** itself — not a single OD
report definition.

| ID | Title | Type | Status |
| --- | --- | --- | --- |
| [ETL-FND-001](./ETL-FND-001-replica-row-drift-procedurelog.md) | Replica row drift — `procedurelog` | Pipeline defect + observability gap | Mitigated (local KPI PASS; clinic deploy pending) |
| [ETL replica fidelity roadmap](../ETL_REPLICA_FIDELITY_ROADMAP.md) | Platform plan (pre-CDC) | Schema analyzer, replicator/loader (Phase 1–2 done), Layer 0, Sunday full refresh | Active |

## Finding types (taxonomy)

| Type | Meaning | Example |
| --- | --- | --- |
| **Pipeline defect** | Sync design or config fails to keep replica equal to source | Stale `ProcStatus` after TP → Complete |
| **Observability gap** | Drift possible but no automated detection | KPI manual compare found 75% production gap |
| **Mart / dbt defect** | Transform logic wrong vs staging | Would show staging ≠ mart |
| **Data quality** | Source clinic data wrong or inconsistent | Not this case — MySQL matches OD golden |
| **KPI definition** | Warehouse intentionally differs from OD report | Document in `FIELD_MAP.md` |
| **ETL lag** | New rows not yet copied (fresh inserts) | daily-payments 2026-06-24 same-day PayNums |

KPI-specific validation instances link here from `{kpi}/findings/{date}.md`.

Implementation tracked in [TODO.md](../../../TODO.md#etl-fnd-001--replica-row-drift-procedurelog).
