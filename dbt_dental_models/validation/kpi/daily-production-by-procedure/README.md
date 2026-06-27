# Daily Production by Procedure (OpenDental) — KPI validation

**OD report:** Reports → Standard → Daily → Production by Procedure  
**Warehouse:** `marts.fact_procedure` (aggregate by `date_complete`, `procedure_status = 2`)  
**Status:** `compare_sql_draft` — **1/3** golden spot-checks PASS (need 2+ more for `within_tolerance`)

## Golden spot-check dates

| Date | Scenario | OD total fees | Staging / OD | Mart / OD | API |
| --- | --- | ---: | --- | --- | --- |
| 2026-06-10 | First golden; mixed categories (exams, perio, surgery, misc) | $15,239.00 | **PASS** (2026-06-26) | **PASS** | — |

## End-to-end workflow

| Step | Layer | Artifact | Status |
| --- | --- | --- | --- |
| 1 | Business rules | [FIELD_MAP.md](./FIELD_MAP.md) | DateComplete + status 2 documented |
| 2 | OD golden export | `golden/od_daily_production_by_procedure_06102026_06102026.csv` | Done |
| 3 | Staging ↔ mart ↔ OD | `compare/*.sql`, `findings/*.md` | **2026-06-10 PASS** — [findings/2026-06-10.md](./findings/2026-06-10.md) |
| 4 | API / frontend | TBD when KPI exposed in app | — |
| 5 | Registry | [KPI_VALIDATION_REGISTRY.md](../KPI_VALIDATION_REGISTRY.md) | `compare_sql_draft` |

Template: follow [daily-payments](../daily-payments/) for four-layer validation.

## Key files

- [FIELD_MAP.md](./FIELD_MAP.md) — OD Total Fees ↔ warehouse
- [findings/2026-06-10.md](./findings/2026-06-10.md) — first golden date (**PASS**)
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
