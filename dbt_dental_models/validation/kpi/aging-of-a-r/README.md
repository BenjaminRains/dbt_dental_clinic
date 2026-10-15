# Aging of A/R (OpenDental) — KPI validation

**OD report:** Reports → Standard → Monthly → Aging of A/R  
**Mart:** `opendental_analytics.marts.mart_ar_summary`  
**Status:** **`delta_documented`** — Both sides are **2026-10-12**. OpenDental's report query totals **$287,110.32**. The mart is stored `BalTotal`, **$282,624.23**, and does not run that query.  
**Formal report:** [VALIDATION_REPORT.md](./VALIDATION_REPORT.md)

## Side by side

Both columns are 2026-10-12. The dollar gap is the measure, not a date mismatch.

| Measure | OpenDental 2026-10-12 | OD source | Mart 2026-10-12 | Mart source |
| --- | ---: | --- | ---: | --- |
| Total | $287,110.32 | Family ledger rebuilt from transactions through 2026-10-12. Does not read `patient.BalTotal`. | $282,624.23 | Sum of stored `patient.BalTotal` on the guarantor |
| 0–30 | $95,239.25 | Charges with TranDate from 2026-09-12 through 2026-10-12, after credits pay the oldest charges first | $49,945.21 | Procedures dated 0–30 days before 2026-10-12, then scaled so the four buckets sum to `BalTotal` |
| 31–60 | $38,552.66 | Charges with TranDate from 2026-08-13 through 2026-09-11, after credits pay the oldest charges first | $25,484.84 | Procedures dated 31–60 days before 2026-10-12, then scaled so the four buckets sum to `BalTotal` |
| 61–90 | $28,388.60 | Charges with TranDate from 2026-07-14 through 2026-08-12, after credits pay the oldest charges first | $15,742.24 | Procedures dated 61–90 days before 2026-10-12, then scaled so the four buckets sum to `BalTotal` |
| > 90 | $124,929.81 | Charges with TranDate before 2026-07-14, after credits pay the oldest charges first | $191,451.93 | Procedures dated more than 90 days before 2026-10-12, plus any `BalTotal` with no procedure rows, then scaled so the four buckets sum to `BalTotal` |
| -Ins Est | $116,070.74 | `claimproc.InsPayEst` on lines that are NotReceived, or Received after 2026-10-12, and whose ProcDate is on or before 2026-10-12 | $1,791,482.25 | `claim.InsPayEst` on procedures that still have a balance, minus insurance already paid on those procedures |
| -W/O Est | $0.00 | `claimproc.WriteOff` on those open lines | — | `mart_ar_summary` has no write-off estimate column |
| Patient | $171,039.58 | Total − Ins Est − W/O Est | — | `greatest(BalTotal − insurance_estimate, 0)` on each patient row |
| Rows | 753 | Guarantors of any PatStatus with a bucket of at least $0.005 | 736 | PatStatus Patient or Inactive, and `BalTotal` > 0 |

Query: [compare/od_aging_report_2026-10-12.sql](./compare/od_aging_report_2026-10-12.sql). The 2026-09-30 export remains in [compare/od_aging_report_2026-09-30.sql](./compare/od_aging_report_2026-09-30.sql) and `golden/od_aging_of_a_r_09302026_09302026.csv`. That golden file is an earlier as-of date, so it is not this comparison.

## End-to-end workflow

| Step | Layer | Artifact | Status |
| --- | --- | --- | --- |
| 1 | Business rules | [FIELD_MAP.md](./FIELD_MAP.md) | Patient + Inactive, positive guarantor `BalTotal` |
| 2 | OD golden | `golden/od_aging_of_a_r_09302026_09302026.csv` | As-of 2026-09-30. CSV date line says 10/12/2026 (print day). |
| 3 | Staging ↔ mart ↔ OD | `compare/*.sql` | Staging PASS; total PASS; buckets secondary |
| 4 | API | `verify_aging_ar.py` | Not re-run this close |
| 5 | Registry | [KPI_VALIDATION_REGISTRY.md](../KPI_VALIDATION_REGISTRY.md) | `within_tolerance` |

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
