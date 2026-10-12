# Aging of A/R (OpenDental) — KPI validation

**OD report:** Reports → Standard → Monthly → Aging of A/R  
**Mart:** `opendental_analytics.marts.mart_ar_summary`  
**Status:** **`delta_documented` (2026-07-16)** — mart↔dim PASS; live OD total FAIL (warehouse patient lag)  
**Formal report:** [VALIDATION_REPORT.md](./VALIDATION_REPORT.md)

## End-to-end workflow

| Step | Layer | Artifact | Status |
| --- | --- | --- | --- |
| 1 | Business rules | [FIELD_MAP.md](./FIELD_MAP.md) | Locked for source proxy; confirm on UI export |
| 2 | OD golden | Source-proxy snapshot (+ UI CSV pending) | Provisional |
| 3 | Staging ↔ mart ↔ OD | `compare/*.sql`, `findings/2026-07-16.md` | Staging PASS; OD FAIL |
| 4 | API | `verify_aging_ar.py` | In-scope `/ar/*` |
| 5 | Registry | [KPI_VALIDATION_REGISTRY.md](../KPI_VALIDATION_REGISTRY.md) | `delta_documented` |

## Same-day golden rule

`mart_ar_summary` rebuilds with `snapshot_date = current_date`. Aging buckets are relative to
**today**, not a historical as-of store. Export OD Aging of A/R and rebuild the mart on the
**same calendar day** (Central US clinic calendar).

**Primary measure:** practice total AR = `SUM(total_balance)` for latest `snapshot_date`.  
**Secondary:** aging buckets (`balance_0_30_days` … `balance_over_90_days`). Bucket drift does
not block total sign-off if totals PASS (known scaling from `int_ar_balance` → dim balance).

## Tolerance

Registry: **±$50 or ±0.5%** (PASS when either threshold is met).

## Key files

- [FIELD_MAP.md](./FIELD_MAP.md) — OD options ↔ mart columns ↔ API fields
- [compare/](./compare/) — DBeaver SQL (staging → mart → OD → API)
- [golden/](./golden/) — OD CSV exports (local, gitignored)
- [findings/](./findings/) — per-snapshot notes
- `api/tests/kpi/verify_aging_ar.py` — live API smoke (in-scope `/ar/*` only)

## API endpoints in scope

| Endpoint | Fields |
| --- | --- |
| `GET /ar/snapshot-dates` | Latest `snapshot_date` |
| `GET /ar/kpi-summary` | `total_ar_outstanding` (primary) |
| `GET /ar/aging-summary` | Bucket `amount` / `percentage` (secondary) |

Out of scope: `/ar/priority-queue`, `/ar/risk-distribution`, `/ar/aging-trends`, DSO fields,
`/reports/ar/summary`.

## Golden naming

`od_aging_of_a_r_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv`  
Example (as-of 2026-07-16): `od_aging_of_a_r_07162026_07162026.csv`
