# Daily Payments (OpenDental) — KPI validation

**OD report:** Reports → Standard → Daily → Payments  
**Mart:** `opendental_analytics.marts.mart_daily_payments`  
**Status:** **`within_tolerance` — workflow complete (2026-06-26)**  
**Formal report:** [VALIDATION_REPORT.md](./VALIDATION_REPORT.md)

## End-to-end workflow (complete)

| Step | Layer | Artifact | Status |
| --- | --- | --- | --- |
| 1 | Business rules | [FIELD_MAP.md](./FIELD_MAP.md) | Done |
| 2 | OD golden exports | `golden/*.csv` (3 spot-check dates) | Done |
| 3 | Staging ↔ mart ↔ OD | `compare/*.sql`, `findings/*.md` | 3/3 PASS |
| 4 | API / frontend | `verify_daily_collections.py`, Practice Manager Home | 3/3 PASS |
| 5 | Registry + app | [KPI_VALIDATION_REGISTRY.md](../KPI_VALIDATION_REGISTRY.md), `validatedKpis.ts` | Done |

Use this folder as the **template** for the next KPI (`daily-production-by-procedure`, `aging-of-a-r`, …).

## Golden spot-check dates

| Date | Scenario | OD net | Mart / OD | API (layer 4) |
| --- | --- | ---: | --- | --- |
| 2026-06-24 | Weekday; patient + insurance; mart fix + ETL | $11,197.40 | PASS | PASS |
| 2025-10-07 | Weekday; mixed PayTypes + insurance | $21,747.30 | PASS (exact) | PASS |
| 2025-11-08 | Saturday; patient-only (Credit Card) | $3,791.65 | PASS (exact) | PASS |

API smoke: `python api/tests/kpi/verify_daily_collections.py` — **3/3 PASS** (2026-06-26, local).

## Key files

- [FIELD_MAP.md](./FIELD_MAP.md) — OD Amount / sections ↔ mart columns
- [findings/](./findings/) — per-date notes
- [compare/](./compare/) — DBeaver SQL (staging → mart → OD → **API**)
- [golden/](./golden/) — OD CSV exports (local, gitignored)
- [scripts/README.md](./scripts/README.md) — golden CSV → PHI-free snapshot parser
- `api/tests/kpi/verify_daily_collections.py` — live API smoke test (all three golden dates)
