# Refactor Plan: PostgresLoader Full Refresh Tracking Fix

## Problem Summary

After a recent force_full run, many tables in `etl_load_status` have NULL `last_primary_value` and `primary_column_name`, causing subsequent incremental loads to fall back to inefficient time-based OR logic instead of using primary key increments.

**Root Cause:**
- Full refresh methods call `_get_last_primary_value()` to get the primary value for tracking
- `_get_last_primary_value()` reads from `etl_load_status` table 
- But `etl_load_status` was populated during the full refresh with NULL values
- This creates a cycle: NULL → NULL, so tracking never gets populated with actual primary key values

**Evidence:**
- `sheetfield` tracking shows: `last_primary_value: NULL, primary_column_name: NULL`
- Current ETL run logs: "Last primary value: None" 
- Query falls back to: `WHERE (SheetFieldNum > 'timestamp' ...) OR (DateTimeSig > 'timestamp' ...)`
- Results in large "bulk" upserts instead of minimal incremental loads

## Scope of Problem

**Affected Methods:**
- `load_table_standard()` - lines 1671-1674
- `load_table_chunked()` - lines 2021-2024  
- `load_table_copy_csv()` - lines 2640-2643
- `load_table_parallel()` - lines 2912-2915
- `load_table_streaming()` - lines 832-835

**Impact:**
- 62 tables using `or_logic` strategy affected
- All tables with real primary keys (not just `id`) lose efficient incremental loading
- Subsequent runs process more data than necessary
- Performance degradation on large tables

## Current Code Flow (Broken)

```python
# In full refresh success blocks:
if primary_column and primary_column != 'none':
    last_primary_value = self._get_last_primary_value(table_name)  # Returns NULL from tracking
    
self._update_load_status(table_name, rows_loaded, 'success', last_primary_value, primary_column)
# Writes NULL back to tracking, perpetuating the problem
```

## Solution 1: Modify `_get_last_primary_value()` with Data Fallback

**Approach:** Add fallback logic to existing method to compute from data when tracking is NULL.

**Implementation:**
```python
def _get_last_primary_value(self, table_name: str) -> Optional[str]:
    """Get the last primary column value for incremental loading."""
    try:
        with self.analytics_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT last_primary_value, primary_column_name
                FROM {self.analytics_schema}.etl_load_status
                WHERE table_name = :table_name
                AND load_status = 'success'
                ORDER BY _loaded_at DESC
                LIMIT 1
            """), {"table_name": table_name}).fetchone()
            
            if result:
                last_primary_value, primary_column_name = result
                
                # NEW: If tracking has NULL, compute from actual data
                if last_primary_value is None and primary_column_name:
                    logger.info(f"Tracking has NULL primary value for {table_name}, computing from data")
                    max_result = conn.execute(text(f"""
                        SELECT MAX("{primary_column_name}") 
                        FROM {self.analytics_schema}.{table_name}
                    """)).scalar()
                    last_primary_value = str(max_result) if max_result is not None else None
                    logger.info(f"Computed last_primary_value for {table_name}: {last_primary_value}")
                
                return last_primary_value
            return None
    except Exception as e:
        logger.error(f"Error getting last primary value for {table_name}: {str(e)}")
        return None
```

**Pros:**
- Fixes all methods at once
- No new methods added
- Backward compatible
- Self-healing for future full refreshes

**Cons:**
- Adds database query to every call
- Slightly more complex logic

## Solution 2: Inline Data Computation in Full Refresh Paths

**Approach:** Replace `_get_last_primary_value()` calls in full refresh success blocks with direct data queries.

**Implementation:**
```python
# In each full refresh success block, replace:
# last_primary_value = self._get_last_primary_value(table_name)

# With:
last_primary_value = None
if primary_column and primary_column != 'none':
    try:
        with self.analytics_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT MAX("{primary_column}") 
                FROM {self.analytics_schema}.{table_name}
            """)).scalar()
            last_primary_value = str(result) if result is not None else None
            logger.info(f"Computed last_primary_value for {table_name}: {last_primary_value}")
    except Exception as e:
        logger.warning(f"Could not compute last primary value for {table_name}: {str(e)}")

self._update_load_status(table_name, rows_loaded, 'success', last_primary_value, primary_column)
```

**Pros:**
- No changes to existing methods
- Clear separation of concerns
- Only affects full refresh paths

**Cons:**
- Code duplication across 5 methods
- More changes required
- Doesn't fix the root cause in `_get_last_primary_value()`

## Recommendation

**Solution 1** is preferred because:
- Fixes the root cause in one place
- Self-healing for future full refreshes
- No code duplication
- Maintains existing method contracts

## Testing Plan

1. **Unit Test:** Verify `_get_last_primary_value()` returns computed value when tracking is NULL
2. **Integration Test:** Run full refresh on `sheetfield`, verify tracking gets populated with MAX(SheetFieldNum)
3. **Regression Test:** Run incremental load on `sheetfield`, verify it uses `SheetFieldNum > last_primary_value` instead of OR logic
4. **Performance Test:** Compare row counts before/after fix to ensure minimal incremental loads

## Risk Assessment

**Risk Level:** Low
- Single method change with clear fallback logic
- No breaking changes to existing interfaces
- Easy to rollback if issues arise
- Fixes a clear data consistency issue

## Implementation Steps

1. Modify `_get_last_primary_value()` with Solution 1
2. Test with `sheetfield` full refresh
3. Verify tracking gets populated correctly
4. Run incremental load to confirm efficient query generation
5. Deploy and monitor other tables
