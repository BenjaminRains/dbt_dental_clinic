# DBT-FND-002 — AR shared_calcs metadata null via blank ProcStatus labels

**Type:** Mart / dbt defect  
**Status:** Partially fixed (2026-07-09 Batch 8b)  
**Found:** 2026-07-09 (dbt test failure triage Batch 8b)  
**Primary models:** `int_ar_shared_calculations`, `int_ar_transaction_history`, `int_procedure_complete`

## Summary

`int_ar_shared_calculations` pulled procedure metadata from `BaseProcedures`, which filtered:

```sql
WHERE procedure_status_desc IN ('Complete', 'Existing Current')
```

On clinic analytics (2026-07-09), **`procedure_status_desc` is blank on every** `int_procedure_complete` row (DefCat label join for ProcStatus does not populate). The filter matched **zero** rows, so the LEFT JOIN to `pc` never attached `_loaded_at` / `_created_at` / `_updated_at` / `_created_by`. Downstream `int_ar_transaction_history` inherited 100% null source metadata (~84,744 rows × 4 tests).

Numeric `procedure_status` is populated and correct (OD: 2=Complete dominates shared_calcs joins).

## Evidence (clinic analytics, 2026-07-09)

| Check | Result |
| --- | --- |
| `int_procedure_complete` metadata not null | **164,919 / 164,919** |
| `int_ar_shared_calculations` `_loaded_at` not null | **0 / 87,131** |
| Join shared→procedure on `procedure_id` only | **81,810** hits |
| Same join + `procedure_status_desc IN ('Complete','Existing Current')` | **0** hits |
| Status mix on shared_calcs→procedure joins | **81,800** status=2; desc blank |
| `int_ar_transaction_history` metadata null | **84,744 / 84,744** (all types) |

## Batch 8b fix

- `BaseProcedures` filter → `procedure_status IN (2, 3)` (Complete, ExistingCurrent).
- Soft `_loaded_at/_created_at/_updated_at/_created_by` not_null → **warn** on txn history (no-proc / unmatched rows remain; ~9k after fix).
- Raise `days_outstanding` band to **[-200, 3000] warn** (post-restore max ~2689).
- Soft txn-history `transaction_date` floor to **2020** warn; fix `payment_source_type` accepted_values to boolean.

Selective verify: `PASS=61 WARN=10 ERROR=0` on `int_ar_shared_calculations` + `int_ar_transaction_history`.

## Remaining work

1. Fix `int_procedure_complete` ProcStatus definition join (`category_id = 4` + `item_value`) so `procedure_status_desc` populates — currently blank for all statuses.
2. Thread metadata from payment/adjustment sources for rows with no procedure_id (~3.5k).
3. Revisit aging join `OR t.procedure_id IS NULL` fan-out risk on `BaseProcedures`.
