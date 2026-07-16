# Validation Report: Daily Production by Procedure vs OpenDental

| Field | Value |
| --- | --- |
| **Report ID** | `kpi-daily-production-by-procedure-001` |
| **Report date** | 2026-07-16 |
| **Workflow** | **Complete** — layers 0–3 signed off on 3 golden dates (API/frontend TBD) |
| **Registry status** | `within_tolerance` |
| **KPI name** | Daily production by procedure |
| **Subject model** | `marts.fact_procedure` (aggregate by `date_id` / complete fees) |
| **Primary measure** | `sum(actual_fee)` where complete production aligns with OD DateComplete |
| **Reference report** | OpenDental → Reports → Standard → Daily → Production by Procedure |
| **Validator** | Analytics / dbt validation workflow (clinic secure site) |

---

## 1. Executive summary

We validated that warehouse complete-production totals (raw → staging → `fact_procedure`)
implement the same business logic OpenDental uses in **Daily → Production by Procedure**:
**DateComplete** + **ProcStatus = 2 (Complete)** + procedure fee by code.

Three spot-check dates (heavy weekday, baseline weekday, Saturday) all **PASS exact** at total
and by-code grain.

**Recommendation:** Register as validated once the measure is exposed in the clinic API/frontend.
Re-run golden compare after material changes to `procedurelog` ETL, staging, or `fact_procedure`.

---

## 2. Golden dates

| Date | Scenario | OD rows | OD fees | Codes | Layers 0–3 | Findings |
| --- | --- | ---: | ---: | ---: | --- | --- |
| 2026-06-10 | Weekday baseline | 140 | $15,239.00 | 28 | **PASS** | [findings/2026-06-10.md](./findings/2026-06-10.md) |
| 2025-11-18 | Heavy weekday | 202 | $36,589.00 | 48 | **PASS** (exact) | [findings/2025-11-18.md](./findings/2025-11-18.md) |
| 2026-02-07 | Saturday | 79 | $22,344.00 | 25 | **PASS** (exact) | [findings/2026-02-07.md](./findings/2026-02-07.md) |

Sundays excluded from sampling (clinic closed).

---

## 3. Business rules

Documented in [FIELD_MAP.md](./FIELD_MAP.md):

- Report date = **DateComplete**, not ProcDate
- Include **ProcStatus = 2** only (exclude Existing Prior / status 4)
- Totals = sum of procedure fees; by-code qty + fees must match OD rows

---

## 4. Platform dependency

First golden (2026-06-10) initially **FAIL**ed due to replica row drift on `procedurelog`
(TP → Complete in-place updates). Fixed by [ETL-FND-001](../../../../docs/findings/ETL-FND-001-replica-row-drift-procedurelog.md)
(lookback re-sync + drift check). Closed 2026-07-16 after clinic marts PASS.

---

## 5. Out of scope

| Item | Notes |
| --- | --- |
| API / frontend layer | KPI not yet a dedicated clinic API endpoint / validatedKpis entry |
| Clinic RDS re-compare for new dates | Local warehouse signed off; publish path already proven on 2026-06-10 |

---

## 6. Artifacts

- Golden CSVs (local, gitignored): `golden/od_daily_production_by_procedure_*.csv`
- Snapshots (PHI-free): `golden/snapshots/*.snapshot.yml`
- Compare SQL: `compare/compare_daily_production_by_procedure_*.sql`
