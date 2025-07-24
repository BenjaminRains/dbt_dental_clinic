# DBT Staging Models Refactor Plan

## Problem Summary
After the ETL pipeline run, dbt parsing fails with `'config' is undefined` error. This is caused by multiple issues in the staging models that prevent dbt from properly parsing the project.

## Issues Identified

### 1. ETL Tracking Columns Missing (CRITICAL PRIORITY)
**Problem**: The ETL pipeline is NOT adding tracking columns (`_loaded_at`, `_extracted_at`) to the raw tables, but staging models expect these columns from `standardize_metadata_columns()`.

**Current ETL Behavior**:
```sql
-- ETL pipeline does this:
SELECT * FROM table_name
-- NO tracking columns added!
```

**Staging Models Expect**:
```sql
-- standardize_metadata_columns() expects:
current_timestamp as _loaded_at,  -- From ETL pipeline
"DateEntry" as _created_at,       -- From source
"DateTStamp" as _updated_at       -- From source
```

**Impact**: Staging models reference `_loaded_at` that doesn't exist in raw tables, causing parsing failures.

**Solution Options**:
1. **Fix ETL Pipeline** (Recommended): Modify `_build_load_query()` to add tracking columns
2. **Fix Staging Models** (Quick Fix): Modify `standardize_metadata_columns()` to handle missing columns

### 2. Incorrect Incremental Logic - `_updated_at` Reference (PRIORITY)
**Problem**: Many staging models use `_updated_at` in their incremental logic, but `_updated_at` is created by the `standardize_metadata_columns` macro which runs AFTER the incremental filter.

**Current Pattern (Incorrect)**:
```sql
{% if is_incremental() %}
    and "DateTStamp" > (select max(_updated_at) from {{ this }})
{% endif %}
```

**Solution**: Replace `_updated_at` with the source timestamp column used in `standardize_metadata_columns`:
```sql
{% if is_incremental() %}
    and "DateTStamp" > (select max("DateTStamp") from {{ this }})
{% endif %}
```

**Approach**: 
- Identify all models using `max(_updated_at)` in incremental logic
- Replace with the corresponding source timestamp column from `standardize_metadata_columns` parameters
- Test each model individually to ensure correct behavior

### 3. Template Macro Issues (RESOLVED ✅)
**Problem**: The `int_model_template.sql`, `int_yml_template.sql`, `staging_model_template.sql`, and `int_quality_gate.sql` macros were causing parsing issues.

**Status**: These macros were **NOT being used** anywhere in the codebase and have been disabled by renaming them to `.disabled` extensions.

**Note**: The `standardize_metadata_columns` and `transform_id_columns` macros ARE being used extensively and should remain active.

### 4. ETL Logging Issue (MEDIUM)
**Problem**: All ETL pipeline runs append to the same log file (`etl_pipeline.log`), making it difficult to debug specific runs and track issues across different executions.

**Impact**: 
- Cannot easily isolate errors from specific runs
- Difficult to track progress and identify patterns
- Performance analysis is challenging
- Debugging specific issues is time-consuming

**Solution**: 
1. ✅ **Implemented**: Modified `etl_pipeline/config/logging.py` to support run-specific log files
2. ✅ **Added**: `setup_run_logging()` function that creates timestamped log files
3. ✅ **Updated**: Default logger to use run-specific files by default
4. ✅ **Created**: Example script demonstrating the new logging functionality

**New Log File Format**: `etl_pipeline_run_YYYYMMDD_HHMMSS.log`

**Benefits**:
- Each run gets its own log file
- Easy to track specific run issues
- Better debugging and troubleshooting
- Performance tracking per run

### 6. [Additional Issues to be identified]

## Implementation Strategy

### Phase 1: Fix ETL Tracking Columns (CRITICAL)
1. Modify `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`
2. Update `_build_load_query()` to add tracking columns:
   ```sql
   SELECT *, 
          CURRENT_TIMESTAMP as _loaded_at,
          CURRENT_TIMESTAMP as _extracted_at
   FROM table_name
   ```
3. Test ETL pipeline with one table
4. Verify tracking columns appear in raw tables
5. Test dbt parsing after ETL fix

### Phase 2: Fix Incremental Logic (CURRENT FOCUS)
1. Create a script to identify all files with `max(_updated_at)` pattern
2. Map each file to its correct source timestamp column from `standardize_metadata_columns` parameters
3. Apply fixes systematically in small batches
4. Test with `dbt parse` after each batch

### Phase 3: [Additional phases to be defined]

## Testing Approach
- Fix ETL pipeline first and test with one table
- Run `dbt parse` after ETL fix
- Fix incremental logic issues in small batches
- Test individual models with `dbt run --select model_name`
- Verify incremental behavior works correctly

## Notes
- The ETL pipeline successfully copies data but doesn't add tracking columns
- Raw tables in `opendental_analytics.raw` are missing `_loaded_at` columns
- Staging models expect tracking columns that don't exist
- Template macros have been disabled and are no longer causing parsing issues
- Core macros (`standardize_metadata_columns`, `transform_id_columns`) are actively used and working correctly
- ETL tracking tables (`raw.etl_load_status`, `raw.etl_transform_status`) have tracking columns and work correctly 