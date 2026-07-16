# Daily Production by Procedure (OpenDental) — KPI validation

**OD report:** Reports → Standard → Daily → Production by Procedure  
**Warehouse:** `marts.fact_procedure` (aggregate by `date_complete`, `procedure_status = 2`)  
**Status:** `within_tolerance` — **3/3** golden spot-checks PASS (layers 0–3); API/frontend TBD  
**Sign-off:** [VALIDATION_REPORT.md](./VALIDATION_REPORT.md)

## Golden spot-check dates

| Date | Scenario | OD total fees | Staging / OD | Mart / OD | API |
| --- | --- | ---: | --- | --- | --- |
| 2026-06-10 | Weekday baseline; mixed categories | $15,239.00 | **PASS** | **PASS** | — |
| 2025-11-18 | Heavy weekday (Tue) | $36,589.00 | **PASS** (exact) | **PASS** | — |
| 2026-02-07 | Saturday (no Sundays — clinic closed) | $22,344.00 | **PASS** (exact) | **PASS** | — |

## End-to-end workflow

| Step | Layer | Artifact | Status |
| --- | --- | --- | --- |
| 1 | Business rules | [FIELD_MAP.md](./FIELD_MAP.md) | DateComplete + status 2 documented |
| 2 | OD golden export | `golden/od_daily_production_by_procedure_*.csv` | 3 dates done |
| 3 | Staging ↔ mart ↔ OD | `compare/*.sql`, `findings/*.md` | **3/3 PASS** |
| 4 | API / frontend | TBD when KPI exposed in app | — |
| 5 | Registry | [KPI_VALIDATION_REGISTRY.md](../KPI_VALIDATION_REGISTRY.md) | `within_tolerance` |

Template: follow [daily-payments](../daily-payments/) for four-layer validation.

## Key files

- [FIELD_MAP.md](./FIELD_MAP.md) — OD Total Fees ↔ warehouse
- [findings/2026-06-10.md](./findings/2026-06-10.md) — weekday baseline (**PASS**)
- [findings/2025-11-18.md](./findings/2025-11-18.md) — heavy weekday (**PASS**)
- [findings/2026-02-07.md](./findings/2026-02-07.md) — Saturday (**PASS**)
- [compare/](./compare/) — compare SQL (`opendental_analytics`)
- [golden/](./golden/) — OD CSV (local, gitignored)
- [scripts/README.md](./scripts/README.md) — golden CSV → snapshot parser

## Golden CSV naming

```
od_daily_production_by_procedure_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv
```

Example: `od_daily_production_by_procedure_06102026_06102026.csv` for 2026-06-10.

**Note:** The filename stem is authoritative for the report date. The CSV `Date:` line can show the
wrong day — the parser prefers the filename and warns on mismatch.

