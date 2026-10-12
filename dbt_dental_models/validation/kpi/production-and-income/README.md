# Production and Income (OpenDental) — KPI validation

**OD report:** Reports → Standard → Production and Income (This Year, and the same grid for a prior year)  
**Warehouse:** no mart owns this report yet. Compare SQL reconstructs it from `raw.*` using [FIELD_MAP.md](./FIELD_MAP.md).  
**Status:** `delta_documented` — source rules match both year grids (2026-10-08). Existing marts differ on purpose.

This is the focus KPI set. Daily Payments and Daily Production by Procedure stay as they are.
They are different OpenDental reports.

## Golden windows

| Window | Months | Stability | What it locks |
| --- | --- | --- | --- |
| `year_2025` | Jan–Dec 2025 | Closed year | All eight columns, including Treat Plan Prepayment exclusion and the July/August write-off reversal |
| `year_2026_ytd` | Jan–Sep 2026 | Closed months | Same rules, no excluded unearned types, one $16 write-off in June |
| `year_2026_oct_asof` | Oct 2026 | Open, as of the 2026-10-08 report | Point-in-time only. Re-export before treating a later October as a failure |

November and December 2026 were zero because the report was run in October. They are not golden months.

Screenshots of the monthly grids are the current golden (no patient-level CSV yet).
Monthly amounts live in [compare/compare_production_and_income_monthly.sql](./compare/compare_production_and_income_monthly.sql).

## Result on 2026-10-08

Source reconstruction (`raw` procedurelog, adjustment, claimproc, paysplit) matches every golden month to the penny.

| Check | 2025 | 2026 through September |
| --- | --- | --- |
| Production, adjustments, write-off, patient income, unearned, insurance | PASS | PASS |
| `mart_daily_production_by_procedure` vs Production | Off $3,106.40 (date completed) | Off on closed months (date completed) |
| `mart_daily_payments` vs Total Income | Short $40,152.63 (type 439 excluded by OD) | Matches (no type 439) |
| Staging adjustments | PASS | Behind raw (incremental `AdjDate` filter) |
| Staging claimproc insurance | Short $284.60 (procedure date before 2023) | PASS |

## Next goldens

Export these from OpenDental when convenient. Same report, not Daily Payments or Production by Procedure.

1. One **closed** month grouped by provider (CSV). Proves the rules still hold below the clinic total.
2. Re-export **October 2026** after month end and replace the as-of row.
3. Any later month that posts a write-off or a Treat Plan Prepayment, so those two rules stay tested after 2025.

## Key files

- [FIELD_MAP.md](./FIELD_MAP.md) — column rules and which current models do not implement them
- [compare/compare_production_and_income_monthly.sql](./compare/compare_production_and_income_monthly.sql) — DBeaver / psql against `opendental_analytics`
- [findings/](./findings/) — local notes (gitignored)
