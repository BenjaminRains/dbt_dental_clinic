### Refactor Plan: PostgresLoader incremental safety fix
Scope: `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`

Problem Summary
During a recent run, loading `raw.sheetfield` failed with a PostgreSQL `UniqueViolation` on the primary key `"SheetFieldNum"`. The error occurred because:

- `load_table_chunked()` selected `copy_csv` as the insert strategy for a large table
- The code then set `use_upsert = (insert_strategy == 'optimized_upsert')`
- This resulted in `use_upsert=False`, causing plain INSERT statements
- Incremental extraction with OR logic (`SheetFieldNum` OR `DateTimeSig`) reselected existing rows
- Duplicate primary keys raised `UniqueViolation`

Root Cause
The `use_upsert` flag is incorrectly tied to the `insert_strategy` rather than the table's need for idempotency. Any table with a primary key doing incremental loads must use UPSERT to handle overlapping data windows safely.

Solution: Always UPSERT for Primary Key Tables
For tables with real primary keys (not just `id`), always use UPSERT during incremental loads regardless of the chosen batch size strategy.

Implementation
Location: `load_table_chunked()` method, in the chunk insertion section

Current code:
```python
# Insert chunk - Always use UPSERT for tables with primary keys
table_config = self.get_table_config(table_name)
primary_key = table_config.get('primary_key')
use_upsert = True if primary_key and primary_key != 'id' else not force_full
```

Change to:
```python
# Determine upsert based on table structure, not strategy
primary_key = table_config.get('primary_key')
has_real_primary_key = bool(primary_key) and primary_key != 'id'
use_upsert = (not force_full) and has_real_primary_key

if self.bulk_insert_optimized(table_name, converted_chunk, use_upsert=use_upsert):
    ...
```

Rationale:

- Batch sizing remains controlled by `insert_strategy` via `optimized_batch_size`
- Conflict handling is now decoupled from strategy
- Idempotency is guaranteed for all tables with primary keys
- Full refreshes still use plain INSERT for performance (table is truncated)

Code Changes Required

- In `load_table_chunked()`, replace the existing `use_upsert` logic (appears twice in the method)
- Add logging: `logger.debug(f"Using {'UPSERT' if use_upsert else 'INSERT'} for {table_name} (force_full={force_full}, has_pk={has_real_primary_key})")`

Testing Plan
Unit Tests:

- Verify `use_upsert=True` when `force_full=False` and `primary_key` is not `id`
- Verify `use_upsert=False` when `force_full=True` regardless of primary key
- Verify `use_upsert=False` when `primary_key='id'` or `None`

Integration Test:

- Run `sheetfield` incremental load with pre-seeded analytics data containing existing IDs
- Confirm no `UniqueViolation` errors
- Verify row counts match and existing rows are updated

Regression Test:

- Run full pipeline on 5-10 representative tables
- Compare row counts before/after
- Monitor for any new errors

What NOT to Do
Do not implement staging table architecture unless you have concrete evidence it's needed:

- Current chunked UPSERT performance is acceptable
- The failure was a logic bug, not a performance issue
- Staging tables add complexity: creation, cleanup, two-phase commits, additional failure modes
- You already have multiple loading strategies (standard, streaming, chunked, parallel) that may not all be used

Rollout Plan

- Immediate: Implement the fix in a feature branch
- Test: Run unit tests and integration test with `sheetfield`
- Deploy: Merge to main and deploy
- Monitor: Watch next ETL run for any `UniqueViolation` errors
- Validate: Confirm all tables with primary keys load successfully

Future Considerations
Before adding more strategies or complexity:

- Instrument code to identify which of the 51 methods are actually used in production
- Remove unused loading strategies
- Simplify the codebase
- Only then evaluate if additional optimizations are needed

Risk Assessment
Risk Level: Low
Why:

- Single, focused change with clear logic
- Preserves existing performance characteristics
- No new failure modes introduced
- Easy to rollback if issues arise

Mitigation:

- Deploy during low-usage window
- Monitor first few table loads closely
- Keep previous version ready for quick rollback

