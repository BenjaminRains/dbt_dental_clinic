# PostgresLoader Incremental Loading Refactor Document

## **Files Affected**

### **Primary Files:**
- `etl_pipeline/etl_pipeline/loaders/postgres_loader.py` - Main PostgresLoader class modifications
- `etl_pipeline/etl_pipeline/config/tables.yml` - Configuration enhancements
- `etl_pipeline/scripts/analyze_opendental_schema.py` - **CRITICAL: Schema analyzer that creates tables.yml**
- `raw.etl_load_status` - Database table (one-time SQL fix)

### **Test Files:**
- `etl_pipeline/tests/unit/loaders/test_postgres_loader_unit.py` - Unit tests for new methods
- `etl_pipeline/tests/integration/loaders/test_postgres_loader_integration.py` - Integration tests
- `etl_pipeline/tests/comprehensive/loaders/test_postgres_loader.py` - Comprehensive tests

### **Documentation Files:**
- `docs/postgresloader_incremental_refactor.md` - This document
- `etl_pipeline/docs/etl_pipeline_methods.md` - Update method documentation

### **Database Files:**
- `analysis/etl_tracking/etl_tracking_ddl.sql` - ETL tracking table structure
- `macros/create_etl_tracking_tables.sql` - dbt macro for tracking tables

## **Executive Summary**

The PostgresLoader incremental loading system is fundamentally broken due to missing tracking records and flawed incremental logic. This document outlines the problems, affected components, and detailed fixes required to restore proper incremental loading functionality.

## **Problem Statement**

### **Current Issues**

1. **Missing Tracking Records**: Only 1 table (`securitylog`) has an entry in `etl_load_status`
2. **Overly Restrictive AND Logic**: Missing 34% of incremental updates due to AND logic
3. **No Data Quality Validation**: Bad dates (1902, 1980) trigger false incremental loads
4. **Single Timestamp Approach**: Same timestamp used for all incremental columns

### **Impact**

- Tables with `incremental_columns` configured are not loading incrementally
- System falls back to full loads, causing performance issues
- Data quality issues lead to incorrect incremental logic
- 34% of legitimate incremental updates are being missed

## **Current State Analysis**

### **Example: `adjustment` Table**

**Configuration (tables.yml):**
```yaml
adjustment:
  incremental_columns:
    - SecDateTEdit
    - AdjDate
    - ProcDate
    - DateEntry
```

**Current Behavior:**
- No tracking record exists in `etl_load_status`
- `_get_last_load()` returns `None`
- System falls back to full load
- **Result**: Always does full load instead of incremental

**Data Quality Issues:**
- `AdjDate`: 1902-07-31 (obviously wrong)
- `DateEntry`: 1980-01-01 (potentially wrong)
- `SecDateTEdit`: 2020-04-24 to 2025-07-18 (reasonable)
- `ProcDate`: 2001-01-01 to 2025-07-18 (reasonable)

**Incremental Logic Problem:**
- **AND Logic**: Only 3,713 rows captured (all 4 columns must be updated)
- **OR Logic**: 5,651 rows would be captured (any column updated)
- **Missing**: 1,938 rows (34% of incremental updates)

## **Files and Classes Affected**

### **1. Primary File: `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`**

#### **Class: `PostgresLoader`**

**Methods to Modify:**

1. **`load_table()`** (lines 285-386)
   - **Problem**: No tracking record creation after successful load
   - **Fix**: Add tracking record creation/update after successful load

2. **`load_table_chunked()`** (lines 387-541)
   - **Problem**: No tracking record creation after successful load
   - **Fix**: Add tracking record creation/update after successful load

3. **`_build_load_query()`** (lines 584-654)
   - **Problem**: Uses AND logic, no data quality validation
   - **Fix**: Add OR logic option, data quality filters

4. **`_build_count_query()`** (lines 655-682)
   - **Problem**: Uses AND logic, no data quality validation
   - **Fix**: Add OR logic option, data quality filters

5. **`_get_last_load()`** (lines 683-708)
   - **Problem**: Returns `None` when no tracking record exists
   - **Fix**: Create tracking record if none exists

**New Methods to Add:**

1. **`_ensure_tracking_record_exists()`**
   - Create initial tracking record if none exists

2. **`_update_tracking_record()`**
   - Create/update tracking record after successful load

3. **`_filter_valid_incremental_columns()`**
   - Filter out columns with data quality issues

4. **`_validate_incremental_dates()`**
   - Validate date ranges for incremental columns

### **2. Configuration File: `etl_pipeline/etl_pipeline/config/tables.yml`**

**Current Structure:**
```yaml
adjustment:
  incremental_columns:
    - SecDateTEdit
    - AdjDate
    - ProcDate
    - DateEntry
```

**Enhanced Structure Needed:**
```yaml
adjustment:
  incremental_columns:
    - SecDateTEdit
    - AdjDate
    - ProcDate
    - DateEntry
  incremental_strategy: "or_logic"  # or "and_logic"
  data_quality_filters:
    min_date: "2000-01-01"
    max_date: "2030-12-31"
  column_priorities:
    - SecDateTEdit  # Most reliable column first
    - AdjDate
    - ProcDate
    - DateEntry
```

### **3. Schema Analyzer: `etl_pipeline/scripts/analyze_opendental_schema.py`**

**CRITICAL ROLE:** This script is responsible for creating the `tables.yml` configuration file that defines which tables have `incremental_columns`.

**Current Logic (lines 411-481):**
```python
def find_incremental_columns(self, table_name: str, schema_info: Dict) -> List[str]:
    """Find suitable incremental columns for a table using OpenDental-specific patterns."""
    # OpenDental-specific timestamp column patterns (in order of preference)
    timestamp_patterns = [
        'DateTStamp', 'DateTimeStamp', 'SecDateTEdit', 'SecDateTEntry',
        'DateTimeEntry', 'DateTEntry', 'DateTimeCreated', 'DateCreated',
        'DateTimeModified', 'DateModified', 'DateTimeLastActive',
        # ... more patterns
    ]
    
    # Returns ALL matching columns without data quality validation
    return [col[0] for col in incremental_candidates]
```

**Problems:**
1. **No Data Quality Validation**: Includes columns with bad dates (1902, 1980)
2. **No Column Prioritization**: Returns all matching columns without ranking
3. **No Date Range Validation**: Doesn't check if dates are reasonable
4. **No Business Logic**: Doesn't consider which columns are most reliable

**Required Changes:**
1. Add data quality validation in `find_incremental_columns()`
2. Add column prioritization logic
3. Add date range validation
4. Add business logic for column selection

### **4. Database Schema: `raw.etl_load_status`**

**Current State:**
```sql
-- Only 1 record exists
id|table_name |last_loaded            |rows_loaded|load_status|
--+-----------+-----------------------+-----------+-----------+
 5|securitylog|2025-06-06 20:06:09.388|    3696895|success    |
```

**Missing Records Needed:**
```sql
-- Need to create records for all tables with incremental_columns
adjustment|2024-01-01 00:00:00|0|pending
allergy   |2024-01-01 00:00:00|0|pending
claim     |2024-01-01 00:00:00|0|pending
patient   |2024-01-01 00:00:00|0|pending
```

## **Detailed Fixes Required**

### **Fix 1: Add Tracking Record Management**

**File:** `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`

**Add these methods:**

```python
def _ensure_tracking_record_exists(self, table_name: str) -> bool:
    """Ensure a tracking record exists for the table."""
    try:
        with self.analytics_engine.connect() as conn:
            # Check if record exists
            result = conn.execute(text("""
                SELECT COUNT(*) FROM raw.etl_load_status 
                WHERE table_name = :table_name
            """), {"table_name": table_name}).scalar()
            
            if result == 0:
                # Create initial tracking record
                conn.execute(text("""
                    INSERT INTO raw.etl_load_status (
                        table_name, last_loaded, rows_loaded, load_status, 
                        _loaded_at, _created_at, _updated_at
                    ) VALUES (
                        :table_name, '2024-01-01 00:00:00', 0, 'pending',
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                """), {"table_name": table_name})
                conn.commit()
                logger.info(f"Created initial tracking record for {table_name}")
            
            return True
            
    except Exception as e:
        logger.error(f"Error ensuring tracking record for {table_name}: {str(e)}")
        return False

def _update_tracking_record(self, table_name: str, rows_loaded: int, load_status: str = 'success'):
    """Create or update tracking record after successful load."""
    try:
        with self.analytics_engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO raw.etl_load_status (
                    table_name, last_loaded, rows_loaded, load_status, 
                    _loaded_at, _created_at, _updated_at
                ) VALUES (
                    :table_name, CURRENT_TIMESTAMP, :rows_loaded, :load_status,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT (table_name) DO UPDATE SET
                    last_loaded = CURRENT_TIMESTAMP,
                    rows_loaded = :rows_loaded,
                    load_status = :load_status,
                    _updated_at = CURRENT_TIMESTAMP
            """), {
                "table_name": table_name,
                "rows_loaded": rows_loaded,
                "load_status": load_status
            })
            conn.commit()
            logger.info(f"Updated tracking record for {table_name}: {rows_loaded} rows, {load_status}")
            
    except Exception as e:
        logger.error(f"Error updating tracking record for {table_name}: {str(e)}")
```

**Modify `load_table()` method:**

```python
def load_table(self, table_name: str, force_full: bool = False) -> bool:
    # ... existing code ...
    
    # Ensure tracking record exists before loading
    if not self._ensure_tracking_record_exists(table_name):
        logger.error(f"Failed to ensure tracking record for {table_name}")
        return False
    
    # ... existing loading code ...
    
    # After successful load, update tracking record
    if rows_data:
        self._update_tracking_record(table_name, len(rows_data))
    
    return True
```

### **Fix 2: Improve Incremental Logic**

**Modify `_build_load_query()` method:**

```python
def _build_load_query(self, table_name: str, incremental_columns: List[str], 
                     force_full: bool = False, use_or_logic: bool = True) -> str:
    """Build query with improved incremental logic."""
    
    # Data quality validation
    valid_columns = self._filter_valid_incremental_columns(incremental_columns)
    
    if force_full or not valid_columns:
        return f"SELECT * FROM {table_name}"
    
    last_load = self._get_last_load(table_name)
    if not last_load:
        return f"SELECT * FROM {table_name}"
    
    # Build conditions with OR logic (configurable)
    conditions = []
    for col in valid_columns:
        conditions.append(f"{col} > '{last_load}'")
    
    if use_or_logic:
        where_clause = " OR ".join(conditions)
    else:
        where_clause = " AND ".join(conditions)
    
    return f"SELECT * FROM {table_name} WHERE {where_clause}"

def _filter_valid_incremental_columns(self, columns: List[str]) -> List[str]:
    """Filter out columns with data quality issues."""
    # Add data quality checks here
    # For now, return all columns - implement filtering later
    return columns
```

### **Fix 3: One-Time Database Fix**

**Run this SQL before the next ETL run:**

```sql
-- Run this on the ANALYTICS PostgreSQL database (opendental_analytics)
-- Create missing tracking records for tables with incremental_columns

INSERT INTO raw.etl_load_status (
    table_name, 
    last_loaded, 
    rows_loaded, 
    load_status, 
    _loaded_at, 
    _created_at, 
    _updated_at
) VALUES 
('adjustment', '2024-01-01 00:00:00', 0, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('allergy', '2024-01-01 00:00:00', 0, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('claim', '2024-01-01 00:00:00', 0, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('patient', '2024-01-01 00:00:00', 0, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (table_name) DO NOTHING;
```

## **Implementation Order**

### **Phase 1: Fix Schema Analyzer (Immediate)**

1. **Fix `analyze_opendental_schema.py`** (lines 411-481)
   - **Method:** `find_incremental_columns()`
   - **Changes:**
     - Add date range validation (exclude dates before 2000, after 2030)
     - Add column prioritization logic (limit to 2-3 most reliable columns)
     - Add business logic for column selection
     - Add data quality checks before adding columns to `tables.yml`
   - **Impact:** Prevents bad columns from being added to configuration

2. **Regenerate `tables.yml`** with improved schema analyzer
   ```bash
   cd etl_pipeline
   python scripts/analyze_opendental_schema.py
   ```

### **Phase 2: Fix PostgresLoader (Before Next ETL Run)**

1. **Run the SQL fix** to create missing tracking records
2. **Add the new methods** to PostgresLoader
3. **Modify `load_table()` and `load_table_chunked()`** to create/update tracking records
4. **Test with one table** to verify incremental loading works

### **Phase 3: Improve Incremental Logic (After Next ETL Run)**

1. **Implement OR logic** in `_build_load_query()`
2. **Add data quality filters** in `_filter_valid_incremental_columns()`
3. **Add configuration options** to tables.yml
4. **Test with multiple tables** to verify improved incremental loading

## **Testing Strategy**

### **Unit Tests to Add:**

```python
def test_ensure_tracking_record_exists():
    """Test that tracking records are created for tables without them."""
    
def test_update_tracking_record():
    """Test that tracking records are updated after successful loads."""
    
def test_or_logic_vs_and_logic():
    """Test that OR logic captures more incremental updates than AND logic."""
    
def test_data_quality_filters():
    """Test that obviously wrong dates are filtered out."""
```

### **Integration Tests to Add:**

```python
def test_incremental_loading_with_tracking():
    """Test that tables with tracking records load incrementally."""
    
def test_incremental_loading_without_tracking():
    """Test that tables without tracking records get tracking records created."""
```

## **Success Criteria**

1. **All tables with `incremental_columns` have tracking records**
2. **Incremental loads capture more data than current AND logic**
3. **No data quality issues (wrong dates) in incremental loads**
4. **Tracking records are created/updated after each successful load**
5. **System falls back gracefully when incremental loading fails**

## **Risk Mitigation**

1. **Backup tracking table** before running SQL fix
2. **Test with one table first** before applying to all tables
3. **Monitor incremental load performance** to ensure it's faster than full loads
4. **Add logging** to track tracking record creation/updates
5. **Implement rollback plan** if issues arise

## **Monitoring and Validation**

### **Key Metrics to Track:**

1. **Tracking Record Coverage**: Percentage of tables with `incremental_columns` that have tracking records
2. **Incremental Load Success Rate**: Percentage of incremental loads that succeed
3. **Data Capture Rate**: Percentage of incremental updates captured vs. total available
4. **Performance Improvement**: Time saved by incremental loads vs. full loads

### **Validation Queries:**

```sql
-- Check tracking record coverage
SELECT 
    COUNT(*) as tables_with_incremental_columns,
    COUNT(e.table_name) as tables_with_tracking_records,
    ROUND(COUNT(e.table_name) * 100.0 / COUNT(*), 2) as coverage_percentage
FROM (
    SELECT 'adjustment' as table_name UNION ALL
    SELECT 'allergy' UNION ALL
    SELECT 'claim' UNION ALL
    SELECT 'patient'
) t
LEFT JOIN raw.etl_load_status e ON t.table_name = e.table_name;

-- Check incremental load performance
SELECT 
    table_name,
    last_loaded,
    rows_loaded,
    load_status,
    _updated_at
FROM raw.etl_load_status
WHERE table_name IN ('adjustment', 'allergy', 'claim', 'patient')
ORDER BY _updated_at DESC;
```

## **Conclusion**

This refactor will fix the fundamental issues with incremental loading while maintaining backward compatibility and providing a foundation for future improvements. The phased approach allows for incremental improvements while maintaining system stability.

The immediate focus should be on creating missing tracking records and ensuring the PostgresLoader creates/updates tracking records after successful loads. This will restore basic incremental loading functionality.

Future phases can address the AND vs. OR logic improvements and data quality validation to further enhance the incremental loading system. 