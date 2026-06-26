# Commlog High Churn Investigation: ETL vs Source Updates

> **Status (2026-06-26):** Investigation **complete** (2025-12-12). Primary fix **deployed** — `IS DISTINCT FROM` change detection in `_build_upsert_sql()`. Follow-on monitoring/contracts **pending** — see [§ Status](#status-2026-06) and [TODO.md — ETL Raw Ingestion Contract](../../TODO.md#etl-raw-ingestion-contract--pipeline-health-metrics). Historical sections below describe pre-fix behavior where noted.

## Investigation Summary

This investigation identified a critical issue in the ETL pipeline's raw data ingestion process. Through empirical analysis comparing source timestamps with PostgreSQL statistics, we discovered:

**What We Identified**:
- ✅ **A semantic mismatch between MVCC and ETL intent**: The ETL treats UPSERT as a conflict-resolution mechanism, but PostgreSQL MVCC records every conflict resolution as a real update operation
- ✅ **A common but subtle analytics engineering failure mode**: Unconditional UPSERT updates without change detection cause no-op updates that inflate `n_tup_upd` metrics
- ✅ **Empirical validation**: Source timestamps vs Postgres stats confirmed the root cause - extraction is correct, but unconditional UPSERT causes churn

**Root Cause (historical):** `PostgresLoader._build_upsert_sql()` previously generated `ON CONFLICT DO UPDATE` without a change predicate. PostgreSQL MVCC recorded every conflict resolution as an update, even when row values were identical.

**Fix (2025–2026):** `_build_upsert_sql()` now adds `WHERE (col IS DISTINCT FROM EXCLUDED.col OR …)` on conflict updates. See `etl_pipeline/etl_pipeline/loaders/postgres_loader.py` and `test_data_quality_unit.py::test_build_upsert_sql_success`.

**Impact (pre-fix baseline):** High `n_tup_upd` counts (`upd_del_per_live = 1.91`) indicated nearly 2 updates per live tuple — write amplification, index bloat, inflated dead tuples. Re-run churn query after fix to confirm improvement (see § Status).

---

## References

### Code Files

**ETL Pipeline Core Components**:
- `etl_pipeline/etl_pipeline/core/simple_mysql_replicator.py` - MySQL extraction logic, incremental copy methods, watermark tracking
- `etl_pipeline/etl_pipeline/loaders/postgres_loader.py` - **ACTIVE** PostgreSQL loading logic, upsert SQL generation (`_build_upsert_sql` method)
  - **Note**: This is the default loader used in production
  - Upsert logic uses primary key from table config
  - **Change detection:** `IS DISTINCT FROM` WHERE clause on `ON CONFLICT DO UPDATE` (prevents no-op updates)
- `etl_pipeline/etl_pipeline/loaders/postgres_loader_refactor_load_strategies.py` - **REFACTORED** version (not default)
  - **Note**: Only used if `ETL_USE_REFACTORED_LOADER` environment variable is set
  - Has incomplete upsert implementation (TODO at line 1254, assumes first column is unique)
  - Same boolean conversion issue as current loader
- `etl_pipeline/etl_pipeline/config/tables.yml` - Table configuration including `commlog` extraction strategy and incremental columns
- `etl_pipeline/etl_pipeline/core/postgres_schema.py` - Schema conversion and data type transformations (MySQL → PostgreSQL)
  - **Key Method**: `convert_row_data_types()` - Converts boolean fields (TINYINT 0/1 → boolean false/true)
- `etl_pipeline/etl_pipeline/orchestration/table_processor.py` - Main ETL orchestration that coordinates extraction and loading
  - **Line 739**: Checks `ETL_USE_REFACTORED_LOADER` env var to choose loader implementation

**dbt Models**:
- `dbt_dental_models/models/staging/opendental/stg_opendental__commlog.sql` - Staging model for commlog transformations
- `dbt_dental_models/models/staging/opendental/_stg_opendental__commlog.yml` - Staging model documentation and tests

### Documentation

- `etl_pipeline/docs/PIPELINE_ARCHITECTURE.md` - Overall ETL pipeline architecture and data flow
- `etl_pipeline/docs/DATA_CONTRACTS.md` - Data contract specifications and table configuration structure
- `etl_pipeline/README.md` - ETL pipeline overview and component descriptions

### Database Schemas

- **MySQL Replication**: `opendental_replication.commlog` - Intermediate replication database
- **PostgreSQL Analytics**: `opendental_analytics.raw.commlog` - Final raw data table
- **PostgreSQL Staging**: `opendental_analytics.public_staging.stg_opendental__commlog` - dbt staging model

### Key Methods Referenced

- `SimpleMySQLReplicator._copy_incremental_unified()` - Incremental extraction using CommlogNum watermark
- `SimpleMySQLReplicator._copy_incremental_records()` - Unified incremental copy method
- `PostgresLoader._build_upsert_sql()` - Generates PostgreSQL upsert SQL with `ON CONFLICT DO UPDATE`
- `PostgresLoader.bulk_insert_optimized()` - Bulk insert/upsert execution with conflict handling

---

## Executive Summary

The `raw.commlog` table shows high update churn (`n_tup_upd = 1,602,741` with `upd_del_per_live = 1.91`), indicating nearly 2 updates per live tuple. This investigation aims to determine whether the high churn is caused by:

1. **Real source updates**: OpenDental updating existing communication log records during normal clinical operations
2. **ETL logic issues**: The ETL pipeline performing unnecessary no-op updates when data hasn't actually changed
3. **Extraction strategy gaps**: Missing updates to existing rows due to watermark-based incremental extraction

## Evidence from Database Statistics

### PostgreSQL Table Statistics

From `pg_stat_user_tables` for `raw.commlog`:

| Metric | Value | Interpretation |
|--------|-------|---------------|
| `n_live_tup` | 840,436 | Current live rows |
| `n_tup_ins` | 1,643,722 | Total inserts (1.96x live rows) |
| `n_tup_upd` | 1,602,741 | Total updates (1.91x live rows) |
| `n_tup_del` | 0 | No deletions |
| `upd_del_per_live` | 1.91 | **Nearly 2 updates per live row** |
| `last_autovacuum` | 2025-08-05 | Last vacuum was 4+ months ago |

**Key Observation**: The ratio of `n_tup_upd / n_live_tup = 1.91` suggests that on average, each row has been updated almost twice. This is suspiciously high for a communication log table where records should be mostly append-only.

### Daily Update Pattern

From the daily rows changed analysis:

```
day                    |rows_changed|
-----------------------+------------+
2025-11-16 00:00:00.000|          15|
2025-11-17 00:00:00.000|         644|
2025-11-18 00:00:00.000|         409|
...
2025-12-12 00:00:00.000|         261|
```

**Average**: ~350 rows changed per day over the observed period.

## Evidence from ETL Pipeline Code

### 1. Extraction Strategy (MySQL → Replication)

**Configuration** (`etl_pipeline/config/tables.yml`):
```yaml
commlog:
  extraction_strategy: incremental
  primary_incremental_column: CommlogNum
  incremental_columns: [CommlogNum, DateTStamp, DateTEntry]
  incremental_strategy: or_logic
  primary_key: CommlogNum
```

**Actual Extraction Logic** (`simple_mysql_replicator.py`):
```python
# Uses CommlogNum as watermark
WHERE CommlogNum > :last_processed
ORDER BY CommlogNum
LIMIT 50000
```

**Critical Issue**: The extraction uses `CommlogNum` as the watermark, which means:
- ✅ New communication logs (new `CommlogNum`) are captured
- ❌ Updates to existing rows (same `CommlogNum` but changed `DateTStamp` or other fields) are **missed** unless the source also assigns a new `CommlogNum`

### 2. PostgreSQL Upsert Logic

**Current Implementation** (`postgres_loader.py:2544-2562`):
```python
def _build_upsert_sql(self, table_name: str, column_names: List[str]) -> str:
    table_config = self.get_table_config(table_name)
    primary_key = table_config.get('primary_key', 'id')
    columns = ', '.join([f'"{col}"' for col in column_names])
    placeholders = ', '.join([f':{col}' for col in column_names])
    # Update all columns except the primary key
    update_columns = [f'"{col}" = EXCLUDED."{col}"' 
                     for col in column_names if col != primary_key]
    update_clause = ', '.join(update_columns) if update_columns else ''
    return f"""
        INSERT INTO {self.analytics_schema}.{table_name} ({columns})
        VALUES ({placeholders})
        ON CONFLICT ("{primary_key}") DO UPDATE SET
            {update_clause}
    """
```

**Generated SQL for `commlog`**:
```sql
INSERT INTO raw.commlog (
    "CommlogNum", "PatNum", "CommDateTime", "CommType", "Note", 
    "Mode_", "SentOrReceived", "UserNum", "Signature", "SigIsTopaz",
    "DateTStamp", "DateTimeEnd", "CommSource", "ProgramNum", 
    "DateTEntry", "ReferralNum", "CommReferralBehavior"
)
VALUES (:CommlogNum, :PatNum, ...)
ON CONFLICT ("CommlogNum") DO UPDATE SET
    "PatNum" = EXCLUDED."PatNum",
    "CommDateTime" = EXCLUDED."CommDateTime",
    "CommType" = EXCLUDED."CommType",
    "Note" = EXCLUDED."Note",
    -- ... ALL 16 columns updated on every conflict
    "DateTStamp" = EXCLUDED."DateTStamp",
    ...
```

**Critical Issue**: The upsert **always updates all columns on conflict without a change predicate** (no WHERE clause). This causes:
- **Unconditional updates**: Every conflict resolution triggers an UPDATE, even when data is identical
- **No-op updates**: PostgreSQL MVCC records every conflict resolution as a real update operation
- **High `n_tup_upd` counts**: Even perfectly identical rows increment the update counter
- **Unnecessary write amplification**: Dead tuples accumulate from no-op updates, causing index bloat

**Root Cause**: The `ON CONFLICT DO UPDATE` statement has no WHERE clause to check if data actually changed. This is a semantic mismatch between MVCC behavior and ETL intent - the ETL treats UPSERT as conflict resolution, but PostgreSQL records every conflict resolution as an update.

### 3. dbt Staging Model

**Staging Model** (`stg_opendental__commlog.sql`):
```sql
{{ config(
    materialized='incremental',
    unique_key='commlog_id'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'commlog') }}
    where {{ clean_opendental_date('"CommDateTime"') }} >= '2023-01-01'
    {% if is_incremental() %}
        and {{ clean_opendental_date('"CommDateTime"') }} > (select max(_loaded_at) from {{ this }})
    {% endif %}
),
```

**Note**: The staging model uses `_loaded_at` for incremental logic, which is derived from the ETL load timestamp, not from source timestamps like `DateTStamp`.

## Hypothesis

### Hypothesis 1: ETL No-Op Updates (Most Likely)

**Theory**: The ETL pipeline is performing unnecessary updates when:
1. The same `CommlogNum` is extracted multiple times (due to watermark tracking issues or re-processing)
2. The row data hasn't changed, but the upsert still updates all columns
3. This causes high `n_tup_upd` counts without actual data changes

**Evidence Supporting**:
- Upsert always updates all columns on conflict
- No comparison to check if data actually changed
- Watermark-based extraction could re-process rows if watermark tracking is reset or inconsistent

### Hypothesis 2: Real Source Updates

**Theory**: OpenDental is legitimately updating existing communication log records:
- `DateTStamp` updates when communications are modified
- `Note` field updates when communication content is edited
- Other fields updated during normal clinical operations

**Evidence Supporting**:
- Communication logs might be editable in OpenDental
- `DateTStamp` is an update timestamp, suggesting records can be modified
- Daily update pattern (~350 rows/day) could represent real clinical activity

### Hypothesis 3: Extraction Strategy Gap

**Theory**: The watermark-based extraction misses updates to existing rows:
- If OpenDental updates a row's `DateTStamp` but keeps the same `CommlogNum`, the ETL won't extract it
- Only new rows (new `CommlogNum`) are captured
- This could cause stale data in analytics

**Evidence Supporting**:
- Extraction uses `CommlogNum > last_processed`, not `DateTStamp > last_processed`
- Configuration lists `DateTStamp` as an incremental column but it's not used in extraction

## Proposed Investigation Queries

**Database Structure Note**:
- **MySQL Replication Database**: `opendental_replication` (MySQL) - Copy of source OpenDental database
- **PostgreSQL Analytics Database**: `opendental_analytics` (PostgreSQL) - Transformed data warehouse
  - **Schema**: `raw` - Contains transformed raw source tables (e.g., `raw.commlog`)
  - **Schema**: `public_staging` - Contains dbt staging models (e.g., `stg_opendental__commlog`)

All queries below specify which database they should be run against.

### Investigation 1: Check for No-Op Updates

**Purpose**: Determine if rows are being updated when data hasn't actually changed.

**Query 1A - PostgreSQL (Analytics Database)**: Check for patterns indicating re-processing
```sql
-- Check if the same CommlogNum appears in recent loads with identical data
-- This would indicate the ETL is re-processing the same rows

WITH recent_loads AS (
    SELECT 
        "CommlogNum",
        md5(ROW(
            "PatNum", "CommDateTime", "CommType", "Note", "Mode_",
            "SentOrReceived", "UserNum", "Signature", "SigIsTopaz",
            "DateTStamp", "DateTimeEnd", "CommSource", "ProgramNum",
            "DateTEntry", "ReferralNum", "CommReferralBehavior"
        )::text) AS data_hash,
        DATE("DateTStamp") AS source_date,
        "DateTStamp" AS source_timestamp
    FROM raw.commlog
    WHERE "DateTStamp" >= CURRENT_DATE - INTERVAL '30 days'
),
load_patterns AS (
    SELECT 
        "CommlogNum",
        data_hash,
        COUNT(*) AS times_seen,
        MIN(source_date) AS first_seen,
        MAX(source_date) AS last_seen,
        COUNT(DISTINCT data_hash) AS distinct_data_versions
    FROM recent_loads
    GROUP BY "CommlogNum", data_hash
)
SELECT 
    CASE 
        WHEN times_seen > 1 AND distinct_data_versions = 1 THEN 'REPROCESSED_SAME_DATA'
        WHEN times_seen > 1 AND distinct_data_versions > 1 THEN 'REPROCESSED_CHANGED_DATA'
        ELSE 'SINGLE_LOAD'
    END AS load_pattern,
    COUNT(*) AS commlog_count,
    SUM(times_seen) AS total_loads,
    AVG(times_seen) AS avg_loads_per_commlog
FROM load_patterns
GROUP BY load_pattern
ORDER BY commlog_count DESC;
```

**Query 1B - MySQL (Replication Database)**: Get data hashes for comparison
```sql
-- Run this in opendental_replication database to get current state
-- Then compare with PostgreSQL raw.commlog to find discrepancies

SELECT 
    CommlogNum,
    MD5(CONCAT(
        COALESCE(CAST(PatNum AS CHAR), ''), '|',
        COALESCE(CAST(CommDateTime AS CHAR), ''), '|',
        COALESCE(CAST(CommType AS CHAR), ''), '|',
        COALESCE(Note, ''), '|',
        COALESCE(CAST(Mode_ AS CHAR), ''), '|',
        COALESCE(CAST(SentOrReceived AS CHAR), ''), '|',
        COALESCE(CAST(UserNum AS CHAR), ''), '|',
        COALESCE(Signature, ''), '|',
        COALESCE(CAST(SigIsTopaz AS CHAR), ''), '|',
        COALESCE(CAST(DateTStamp AS CHAR), ''), '|',
        COALESCE(CAST(DateTimeEnd AS CHAR), ''), '|',
        COALESCE(CAST(CommSource AS CHAR), ''), '|',
        COALESCE(CAST(ProgramNum AS CHAR), ''), '|',
        COALESCE(CAST(DateTEntry AS CHAR), ''), '|',
        COALESCE(CAST(ReferralNum AS CHAR), ''), '|',
        COALESCE(CAST(CommReferralBehavior AS CHAR), '')
    )) AS data_hash,
    DateTStamp,
    CommDateTime
FROM commlog
WHERE DateTStamp >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
ORDER BY CommlogNum DESC
LIMIT 1000;
```

**Query 1C - PostgreSQL (Analytics Database)**: Compare with replication data
```sql
-- After running Query 1B, manually compare or use this to check specific CommlogNum values
-- This checks if rows in raw match what should be in replication

WITH raw_sample AS (
    SELECT 
        "CommlogNum",
        md5(ROW(
            "PatNum", "CommDateTime", "CommType", "Note", "Mode_",
            "SentOrReceived", "UserNum", "Signature", "SigIsTopaz",
            "DateTStamp", "DateTimeEnd", "CommSource", "ProgramNum",
            "DateTEntry", "ReferralNum", "CommReferralBehavior"
        )::text) AS data_hash,
        "DateTStamp",
        "CommDateTime"
    FROM raw.commlog
    WHERE "DateTStamp" >= CURRENT_DATE - INTERVAL '7 days'
)
SELECT 
    "CommlogNum",
    data_hash,
    "DateTStamp",
    "CommDateTime"
FROM raw_sample
ORDER BY "CommlogNum" DESC
LIMIT 1000;
-- Compare the data_hash values with Query 1B results manually
-- If hashes differ for the same CommlogNum, data has changed
-- If hashes match but rows were updated, it's a no-op update
```

**Expected Results**:
- If `REPROCESSED_SAME_DATA` rows exist → ETL is performing no-op updates
- If `REPROCESSED_CHANGED_DATA` rows exist → Source is legitimately updating data
- If hashes differ between replication and raw → Data transformation issue or source updates

**Investigation Results Summary** (2025-12-12):

### Investigation 1: No-Op Updates Check
- ✅ Query 1A: All 9,233 rows show `SINGLE_LOAD` pattern (avg_loads_per_commlog = 1.0)
  - **Finding**: Rows are NOT being re-processed multiple times
  - **Conclusion**: Re-processing is NOT the cause of high churn
  
- ⚠️ Query 1B vs 1C: **Data hashes DO NOT MATCH** between MySQL replication and PostgreSQL raw
  - Example: CommlogNum 869442
    - MySQL hash: `233e4b9a2380590caa7068f140c61f73`
    - PostgreSQL hash: `565cbb826d85fa76a6e83e53ee8e2f0c`
    - **DIFFERENT!**
  - **Finding**: Every row has a different hash between systems
  - **Root Cause Identified**: Data transformation during ETL is changing the data representation

### Investigation 2: Source Update Pattern Analysis
- ✅ Source is **append-only** - zero updates to existing records
  - All 1,319 rows in last 7 days are `NEW_COMMLOG`
  - Daily pattern: `unique_commlogs_updated = total_updates` (perfect 1:1)
  - **Conclusion**: Source updates are NOT causing high churn

### Investigation 3: ETL Load Pattern Analysis
- ✅ ETL extraction is working correctly
  - Watermark tracking accurate (last processed = 869442)
  - No duplicate processing (`row_count = unique_commlog_nums`)
  - Sequential CommlogNum ranges, no re-processing
  - **Conclusion**: ETL extraction issues are NOT causing high churn

**Critical Discovery**: 
1. Source is append-only (no updates to existing records)
2. ETL extraction is correct (no re-processing)
3. **PRIMARY ISSUE**: Unconditional UPSERT updates all columns on conflict without change detection
4. PostgreSQL MVCC records every conflict resolution as a real update, even for no-op updates
5. This explains why `n_tup_upd` is high even though all rows are new inserts

**Root Cause Confirmed**: The `PostgresLoader._build_upsert_sql()` method generates unconditional `ON CONFLICT DO UPDATE` statements without a WHERE clause to check if data actually changed. This causes PostgreSQL MVCC to record every conflict resolution as an update operation, even when row values are identical. Representation differences (boolean conversion) may amplify the issue but are not required to trigger churn - even perfectly identical rows would increment `n_tup_upd`.

### Investigation 2: Source Update Pattern Analysis

**Purpose**: Check if the MySQL replication database shows evidence of updates to existing rows.

**Query** (for MySQL replication database `opendental_replication`):
```sql
-- Check for rows where DateTStamp has been updated but CommlogNum is old
-- This indicates updates to existing communication logs

SELECT 
    DATE(DateTStamp) AS update_date,
    COUNT(DISTINCT CommlogNum) AS unique_commlogs_updated,
    COUNT(*) AS total_updates,
    MIN(CommlogNum) AS oldest_commlog_num,
    MAX(CommlogNum) AS newest_commlog_num
FROM commlog
WHERE DateTStamp >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY DATE(DateTStamp)
ORDER BY update_date DESC;

-- Check for rows where CommlogNum is old but DateTStamp is recent
-- This would indicate updates to existing records

SELECT 
    CASE 
        WHEN CommlogNum < (SELECT MAX(CommlogNum) - 100000 FROM commlog) 
        THEN 'OLD_COMMLOG_RECENT_UPDATE'
        ELSE 'NEW_COMMLOG'
    END AS record_type,
    COUNT(*) AS row_count,
    COUNT(DISTINCT DATE(DateTStamp)) AS distinct_update_dates
FROM commlog
WHERE DateTStamp >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
GROUP BY record_type;
```

**Expected Results**:
- If `OLD_COMMLOG_RECENT_UPDATE` rows exist → Source is updating existing records
- If only `NEW_COMMLOG` rows exist → Source is append-only

**Investigation Results** (2025-12-12):
- ✅ **Daily Update Pattern**: Shows consistent new record creation
  - Each day: `unique_commlogs_updated = total_updates` (perfect 1:1 ratio)
  - CommlogNum ranges are sequential and increasing (e.g., 869182-869442 on 2025-12-12)
  - **Finding**: Source is creating new records, not updating existing ones
  
- ✅ **Record Type Analysis**: All 1,319 rows in last 7 days are `NEW_COMMLOG`
  - **Finding**: Zero `OLD_COMMLOG_RECENT_UPDATE` rows
  - **Conclusion**: Source is **append-only** - no updates to existing records
  
- 📊 **Daily Volume**: Average ~350 rows/day (matches the daily churn pattern observed)
  - Range: 78-644 rows per day
  - Consistent with normal clinical operations creating new communication logs

**Critical Finding**: The source MySQL database is **NOT updating existing records**. All high churn is from new record inserts, not source updates. This confirms that the hash mismatch issue (from Investigation 1) is the root cause - the ETL is transforming data in ways that make the upsert think every row needs updating.

### Investigation 3: ETL Load Pattern Analysis

**Purpose**: Check if the same CommlogNum is being loaded multiple times.

**Query** (PostgreSQL analytics database `opendental_analytics`):
```sql
-- Check etl_load_status to see load patterns
SELECT 
    table_name,
    last_primary_value AS last_commlog_num_processed,
    rows_loaded,
    load_status,
    last_load_date,
    _loaded_at
FROM raw.etl_load_status
WHERE table_name = 'commlog'
ORDER BY _loaded_at DESC
LIMIT 20;

-- Check for duplicate CommlogNum processing
-- (This would require tracking table or log analysis)
-- Alternative: Check if recent loads are re-processing old CommlogNum values

SELECT 
    DATE("DateTStamp") AS source_date,
    MIN("CommlogNum") AS min_commlog_num,
    MAX("CommlogNum") AS max_commlog_num,
    COUNT(*) AS row_count,
    COUNT(DISTINCT "CommlogNum") AS unique_commlog_nums
FROM raw.commlog
WHERE "DateTStamp" >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE("DateTStamp")
ORDER BY source_date DESC;
```

**Investigation Results** (2025-12-12):
- ✅ **ETL Load Status**: Last processed CommlogNum = 869442 (matches latest in source)
  - Load status: `success`
  - Rows loaded: 261 (matches daily source pattern)
  - **Finding**: ETL watermark tracking is working correctly
  
- ✅ **Source Date Analysis**: Perfect alignment between source dates and CommlogNum ranges
  - Each date: `row_count = unique_commlog_nums` (no duplicates)
  - CommlogNum ranges are sequential and non-overlapping
  - **Finding**: No duplicate processing, no re-processing of old records
  
- 📊 **Data Consistency**: Raw table matches source patterns exactly
  - Daily row counts match source daily update counts
  - CommlogNum ranges align perfectly
  - **Conclusion**: ETL extraction is working correctly - all rows are new inserts

**Critical Finding**: The ETL is correctly extracting only new records (no re-processing). The watermark tracking is accurate. This further confirms that the hash mismatch (Investigation 1) is causing the upsert to update rows unnecessarily, even though they're new inserts that should only be inserted once.

### Investigation 4: Compare Replication vs Raw Data Hash

**Purpose**: Direct comparison of MySQL replication data vs PostgreSQL raw data to identify discrepancies.

**Query 4A - PostgreSQL (Analytics Database)**: Get hash of recent rows
```sql
-- Run in opendental_analytics database, raw schema
SELECT 
    "CommlogNum",
    "DateTStamp",
    md5(ROW(
        "PatNum", "CommDateTime", "CommType", "Note", "Mode_",
        "SentOrReceived", "UserNum", "Signature", "SigIsTopaz",
        "DateTStamp", "DateTimeEnd", "CommSource", "ProgramNum",
        "DateTEntry", "ReferralNum", "CommReferralBehavior"
    )::text) AS data_hash,
    _loaded_at
FROM raw.commlog
WHERE "DateTStamp" >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY "CommlogNum" DESC
LIMIT 1000;
```

**Query 4B - MySQL (Replication Database)**: Get same rows from replication
```sql
-- Run in opendental_replication database
SELECT 
    CommlogNum,
    DateTStamp,
    MD5(CONCAT(
        COALESCE(CAST(PatNum AS CHAR), ''), '|',
        COALESCE(CAST(CommDateTime AS CHAR), ''), '|',
        COALESCE(CAST(CommType AS CHAR), ''), '|',
        COALESCE(Note, ''), '|',
        COALESCE(CAST(Mode_ AS CHAR), ''), '|',
        COALESCE(CAST(SentOrReceived AS CHAR), ''), '|',
        COALESCE(CAST(UserNum AS CHAR), ''), '|',
        COALESCE(Signature, ''), '|',
        COALESCE(CAST(SigIsTopaz AS CHAR), ''), '|',
        COALESCE(CAST(DateTStamp AS CHAR), ''), '|',
        COALESCE(CAST(DateTimeEnd AS CHAR), ''), '|',
        COALESCE(CAST(CommSource AS CHAR), ''), '|',
        COALESCE(CAST(ProgramNum AS CHAR), ''), '|',
        COALESCE(CAST(DateTEntry AS CHAR), ''), '|',
        COALESCE(CAST(ReferralNum AS CHAR), ''), '|',
        COALESCE(CAST(CommReferralBehavior AS CHAR), '')
    )) AS data_hash
FROM commlog
WHERE DateTStamp >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
ORDER BY CommlogNum DESC
LIMIT 1000;
```

**Comparison Method**:
1. Export results from both queries to CSV
2. Join on `CommlogNum` and compare `data_hash` values
3. Identify rows where:
   - Hashes match but `_loaded_at` is recent → Potential no-op update
   - Hashes differ → Source data changed or transformation issue
   - CommlogNum exists in raw but not replication → Data sync issue
   - CommlogNum exists in replication but not raw → Extraction gap

**Expected Results** (Based on Investigation 1):
- ⚠️ Hashes will differ for all rows (already confirmed in Investigation 1)
- This investigation will help identify:
  - Which specific CommlogNum values have hash mismatches
  - Whether the mismatch is consistent across all rows
  - If there are any patterns (e.g., certain date ranges, CommlogNum ranges)
  - The magnitude of hash differences

**Investigation 4 Purpose**:
- Validate the hash mismatch finding from Investigation 1 on a larger sample (1000 rows)
- Identify if hash differences are consistent or vary by row
- Prepare for Query 1D (column-by-column analysis) to identify specific transformation causes

**Investigation 4 Results** (2025-12-12):
- ✅ **Individual Column Hashes Match**: PatNum, CommDateTime, DateTStamp, and Note hashes are identical
  - Example CommlogNum 869442:
    - PatNum hash: `b53e7dc2bf929b9f76ab8117eb16cdb2` (matches)
    - CommDateTime hash: `c2036c14ebdddeddea72863869889a65` (matches)
    - DateTStamp hash: `3b8adbd17e64bd356f905c5951dee524` (matches)
    - Note hash: `48f806e253bcc70824deff017a2ef65a` (matches)

- ⚠️ **Boolean Field Conversion Identified**: The transformation converts MySQL TINYINT to PostgreSQL boolean
  - **MySQL**: `SigIsTopaz = 0`, `CommReferralBehavior = 0`
  - **PostgreSQL**: `SigIsTopaz = false`, `CommReferralBehavior = false`
  - **Impact**: When calculating full row hash, `0` vs `false` causes hash mismatch
  - **Root Cause Found**: Boolean conversion is causing the hash differences!

- 📊 **Timestamp Display Difference**: PostgreSQL shows `.000` milliseconds, MySQL doesn't
  - This is likely just display formatting, not actual data difference
  - Individual DateTStamp hashes match, confirming timestamps are equivalent

**Critical Discovery**: The boolean field conversion (TINYINT 0/1 → boolean false/true) contributes to hash mismatches, but the **primary root cause is unconditional UPSERT updates** without change detection.

### Refactor Status and Impact

**Question**: Could the refactor be causing the high churn?

**Answer**: **No, the refactor is NOT the cause** of the current high churn issue.

**Evidence**:
1. **Refactored Loader is NOT Active**: The refactored loader (`PostgresLoaderRefactored`) is only used if `ETL_USE_REFACTORED_LOADER` environment variable is set to `'true'` or `'1'`
   - Default behavior uses the current `PostgresLoader` (line 776 in `table_processor.py`)
   - The refactored version is opt-in only

2. **Both Loaders Have Same Boolean Conversion Issue**: 
   - Both use `PostgresSchema.convert_row_data_types()` which converts TINYINT to boolean
   - Both would have the same hash mismatch problem
   - The boolean conversion happens in `PostgresSchema`, not in the loader

3. **Refactored Loader Has Separate (Incomplete) Implementation**:
   - `_build_upsert_sql()` in refactored version (line 1250) has a TODO comment
   - Uses `columns[0]` as conflict key instead of actual primary key from config
   - This would cause different issues (wrong conflict resolution) but is NOT related to current churn
   - Current loader correctly uses primary key from table config

4. **Current Loader is Correctly Implemented**:
   - Uses primary key from `tables.yml` config (line 2549-2550)
   - Upsert logic is correct, just lacks change detection
   - The issue is the boolean conversion in `PostgresSchema`, not the loader implementation

**Conclusion**: The refactor is not related to the current high churn. The root cause is the boolean field conversion in `PostgresSchema.convert_row_data_types()`, which affects both loader implementations equally. 

**Evidence**:
- Individual column hashes for PatNum, CommDateTime, DateTStamp, and Note **all match** between MySQL and PostgreSQL
- Boolean fields (`SigIsTopaz`, `CommReferralBehavior`) are represented differently:
  - MySQL: `0` (TINYINT)
  - PostgreSQL: `false` (BOOLEAN)
- When calculating full row hash, the string representation difference (`0` vs `false`) causes the hash to differ
- **Result**: Hash mismatch even though data is semantically identical

**Why This Causes High Churn**:
1. ETL extracts rows incrementally (correctly, no re-processing)
2. Rows are loaded via `bulk_insert_optimized()` which uses `_build_upsert_sql()`
3. UPSERT executes `ON CONFLICT DO UPDATE` **unconditionally** (no change predicate)
4. PostgreSQL MVCC records every conflict resolution as an update
5. **Result**: High `n_tup_upd` even when source data hasn't changed

**Important**: The unconditional UPSERT would cause churn even if:
- All rows are new inserts (not re-processed) ✅
- Source data hasn't changed (append-only) ✅
- Data values are perfectly identical (no-op updates) ✅

Representation differences (boolean conversion) may amplify the issue but are **not required** to trigger churn.

### Investigation 5: Check for Re-Processing Patterns

**Purpose**: Determine if the ETL is re-processing the same rows multiple times.

**Query** (PostgreSQL analytics database `opendental_analytics`):
```sql
-- Analyze the relationship between CommlogNum and DateTStamp
-- to see if old CommlogNum values are being updated with new DateTStamp values

WITH commlog_analysis AS (
    SELECT 
        "CommlogNum",
        "DateTStamp",
        "DateTEntry",
        DATE("DateTStamp") AS stamp_date,
        DATE("DateTEntry") AS entry_date,
        CASE 
            WHEN "CommlogNum" < (SELECT MAX("CommlogNum") - 100000 FROM raw.commlog) 
            THEN 'OLD_COMMLOG'
            ELSE 'NEW_COMMLOG'
        END AS commlog_age_category,
        CASE 
            WHEN "DateTStamp" >= CURRENT_DATE - INTERVAL '7 days'
            THEN 'RECENT_UPDATE'
            ELSE 'OLD_UPDATE'
        END AS update_recency
    FROM raw.commlog
    WHERE "DateTStamp" IS NOT NULL
)
SELECT 
    commlog_age_category,
    update_recency,
    COUNT(*) AS row_count,
    COUNT(DISTINCT "CommlogNum") AS unique_commlogs,
    MIN("CommlogNum") AS min_commlog_num,
    MAX("CommlogNum") AS max_commlog_num,
    MIN("DateTStamp") AS earliest_stamp,
    MAX("DateTStamp") AS latest_stamp
FROM commlog_analysis
GROUP BY commlog_age_category, update_recency
ORDER BY commlog_age_category, update_recency;
```

**Expected Results**:
- If `OLD_COMMLOG` + `RECENT_UPDATE` exists → Source is updating old records
- If only `NEW_COMMLOG` + `RECENT_UPDATE` exists → Source is append-only, ETL may be re-processing

## Recommended Next Steps

1. **Run Investigation 1** (No-Op Updates Check) - Highest priority
   - This will definitively show if ETL is updating rows when data hasn't changed
   - Run on a sample of recent data first, then expand if needed

2. **Run Investigation 2** (Source Update Pattern) - High priority
   - Requires access to OpenDental source MySQL
   - Will show if source is legitimately updating existing records

3. **Run Investigation 3** (ETL Load Pattern) - Medium priority
   - Check ETL load status table to understand load frequency and patterns
   - Identify if watermark tracking is working correctly

4. **Run Investigation 4** (Data Hash Comparison) - Medium priority
   - Most comprehensive but requires coordination between systems
   - Best for final verification

5. **Run Investigation 5** (Re-Processing Patterns) - Low priority
   - Quick check using only analytics database
   - Good for initial hypothesis validation

## Potential Fixes (Pending Investigation Results)

### If ETL No-Op Updates Confirmed:

**Fix Option 1**: Conditional UPDATE in upsert
```sql
ON CONFLICT ("CommlogNum") DO UPDATE SET
    "PatNum" = EXCLUDED."PatNum",
    ...
WHERE raw.commlog."DateTStamp" IS DISTINCT FROM EXCLUDED."DateTStamp"
   OR raw.commlog."Note" IS DISTINCT FROM EXCLUDED."Note"
   -- ... compare all columns
```

**Fix Option 2**: Hash-based comparison
```sql
ON CONFLICT ("CommlogNum") DO UPDATE SET
    ... (all columns)
WHERE md5(ROW(raw.commlog.*)::text) != md5(ROW(EXCLUDED.*)::text)
```

### If Source Updates Confirmed:

**Fix Option 1**: Change extraction strategy to use `DateTStamp` watermark
```yaml
commlog:
  primary_incremental_column: DateTStamp  # Instead of CommlogNum
```

**Fix Option 2**: Use multi-column incremental logic with OR strategy
- Extract rows where `CommlogNum > last_CommlogNum OR DateTStamp > last_DateTStamp`
- This captures both new rows and updates to existing rows

### If Extraction Gap Confirmed:

**Fix**: Implement change data capture (CDC) or timestamp-based extraction
- Use `DateTStamp` as primary incremental column
- Or implement full table comparison for small tables
- Or use replication binlog if available

## Investigation Results Summary

### Query 1A Results: Re-Processing Check
- **Result**: All 9,233 rows show `SINGLE_LOAD` pattern (avg_loads_per_commlog = 1.0)
- **Finding**: Rows are NOT being re-processed multiple times
- **Conclusion**: Re-processing is NOT the cause of high churn

### Query 1B vs 1C Results: Data Hash Comparison
- **Result**: **Data hashes DO NOT MATCH** between MySQL replication and PostgreSQL raw
- **Example**: CommlogNum 869442
  - MySQL replication hash: `233e4b9a2380590caa7068f140c61f73`
  - PostgreSQL raw hash: `565cbb826d85fa76a6e83e53ee8e2f0c`
  - **DIFFERENT!**
- **Finding**: Every sampled row has a different hash between systems
- **Root Cause Identified**: 
  - The ETL transformation process (MySQL → PostgreSQL) is modifying data representation
  - This causes the upsert to always see data as "different" even when source data hasn't changed
  - Upsert updates the row every time → High churn

### Root Cause Analysis

The investigation reveals that **data transformation during ETL is causing hash mismatches**, which makes the upsert logic think data has changed when it hasn't. 

### Key Findings from All Investigations:

1. ✅ **Source is append-only** (Investigation 2)
   - Zero updates to existing records
   - All rows are new inserts
   - Source updates are NOT causing churn

2. ✅ **ETL extraction is correct** (Investigation 3)
   - Watermark tracking works perfectly
   - No duplicate processing
   - No re-processing of old records
   - Extraction issues are NOT causing churn

3. ⚠️ **Data transformation hash mismatch** (Investigation 1)
   - Every row has different hash between MySQL and PostgreSQL
   - This causes upsert to always see "different" data
   - Even new inserts trigger UPDATE instead of just INSERT
   - **This is the root cause of high churn**

### Why Hash Mismatch Causes High Churn:

The upsert logic uses `ON CONFLICT DO UPDATE`, which means:
- For new rows: Should INSERT (no conflict)
- For existing rows: Should UPDATE only if data changed

**But**: Because the hash differs between systems, the upsert logic can't properly detect if data has changed. Even though:
- Source data hasn't changed (append-only)
- Rows are new inserts (not re-processed)
- The transformation hash mismatch makes the system think every row needs updating

### Root Cause Identified: Unconditional UPSERT Updates

**Investigation 4 + Code Analysis Confirmed**: The primary root cause is **unconditional UPSERT updates without change detection**, not hash mismatches.

**Refactor Status Check**:
- ✅ **Current Loader is Active**: The default `PostgresLoader` (not refactored version) is being used
  - Refactored loader only activates if `ETL_USE_REFACTORED_LOADER=true` environment variable is set
  - Current loader's upsert logic correctly uses primary key from table config
- ⚠️ **Refactored Loader Has Separate Issue**: The refactored version has an incomplete upsert implementation
  - Uses first column instead of actual primary key (TODO at line 1254)
  - Would cause different issues if activated, but is NOT the cause of current churn
- ✅ **Both Loaders Share Same Problem**: Both use unconditional UPSERT without change detection

**The Primary Problem**:
- `PostgresLoader._build_upsert_sql()` generates: `ON CONFLICT DO UPDATE SET ...` with **no WHERE clause**
- Every conflict resolution triggers an UPDATE, regardless of whether data changed
- PostgreSQL MVCC records this as a real update operation
- **Even perfectly identical rows would increment `n_tup_upd`**

**The Secondary Factor (Representation Differences)**:
- **MySQL** stores booleans as `TINYINT`: `0` (false) or `1` (true)
- **PostgreSQL** stores booleans as `BOOLEAN`: `false` or `true`
- These representation differences may increase update frequency but are **not required** to trigger churn
- The unconditional UPSERT would cause churn even without representation differences

**Affected Columns in commlog**:
- `SigIsTopaz`: MySQL `0` → PostgreSQL `false`
- `CommReferralBehavior`: MySQL `0` → PostgreSQL `false`

**Why This Causes High Churn**:
1. ETL extracts rows incrementally (correctly, no re-processing)
2. Rows are loaded via `bulk_insert_optimized()` which uses `_build_upsert_sql()`
3. UPSERT executes `ON CONFLICT DO UPDATE` **unconditionally** (no change predicate)
4. PostgreSQL MVCC records every conflict resolution as an update
5. **Result**: High `n_tup_upd` even when source data hasn't changed

### Other Potential Causes (Not Confirmed):

1. **Timestamp Precision**: PostgreSQL shows `.000` milliseconds in display, but individual DateTStamp hashes match, so this is likely just display formatting
2. **Hash Calculation Method**: Different hash functions (`MD5(CONCAT(...))` vs `md5(ROW(...)::text)`) may handle row structure differently, but boolean conversion is the primary issue
3. **Other Boolean Fields**: There may be other boolean fields in the table that also cause hash mismatches

## Conclusion

**Primary Root Cause Confirmed**: The high churn is caused by **unconditional UPSERT updates without change detection**. The `PostgresLoader._build_upsert_sql()` method generates SQL that always updates all columns on conflict, with no WHERE clause to prevent no-op updates. This causes PostgreSQL MVCC to record every conflict resolution as a real update, even when:
- Source data hasn't changed (append-only source)
- Rows are new inserts (not re-processed)
- Data values are identical (no-op updates)

### Investigation Summary:

| Investigation | Finding | Impact on Churn |
|--------------|---------|----------------|
| **1A: Re-processing Check** | No re-processing (all SINGLE_LOAD) | ❌ Not a cause |
| **1B vs 1C: Hash Comparison** | Hashes differ between systems | ⚠️ **Secondary factor** (amplifies but not required) |
| **2: Source Update Pattern** | Source is append-only (no updates) | ❌ Not a cause |
| **3: ETL Load Pattern** | Extraction correct, no duplicates | ❌ Not a cause |
| **Code Analysis** | Unconditional UPSERT (no change predicate) | ✅ **PRIMARY ROOT CAUSE** |

### Root Cause Chain (Precise):

1. **Source**: Creates new records (append-only) ✅
2. **ETL Extraction**: Correctly extracts new records only ✅
3. **ETL Transformation**: Converts data types (including boolean TINYINT `0` → boolean `false`) ✅
4. **UPSERT Execution**: Unconditionally updates on conflict **without change predicate** ⚠️
5. **PostgreSQL MVCC**: Records every UPDATE operation, even no-op updates ⚠️
6. **Result**: High `n_tup_upd` even though all rows are new inserts and data is semantically identical ❌

### Root Cause Details:

**The Primary Problem: Unconditional UPSERT**:
- `_build_upsert_sql()` generates: `ON CONFLICT DO UPDATE SET ...` with **no WHERE clause**
- Every conflict resolution triggers an UPDATE, regardless of whether data changed
- PostgreSQL MVCC records this as a real update operation
- **Even perfectly identical rows would increment `n_tup_upd`**

**The Secondary Factor: Representation Differences**:
- Boolean conversion (TINYINT `0` → boolean `false`) creates representation differences
- These differences may increase update frequency but are **not required** to trigger churn
- The unconditional UPSERT would cause churn even without representation differences

**Why Individual Column Hashes Match**:
- PatNum, CommDateTime, DateTStamp, Note hashes are identical
- These columns don't have boolean conversion issues
- Confirms the transformation is correct except for boolean representation
- **But even if all hashes matched, the unconditional UPSERT would still cause churn**

### Why This Matters:

The upsert uses `ON CONFLICT DO UPDATE`, which should:
- **INSERT** new rows (no conflict)
- **UPDATE** existing rows only if data changed

But because of hash mismatch:
- New rows are inserted correctly
- **BUT** the hash comparison logic (if any) or the transformation process makes the system think rows need updating
- This causes unnecessary UPDATE operations on new inserts

**Next Steps**:
1. ✅ **Investigation Complete**: Unconditional UPSERT identified as primary root cause
2. **Fix Option 1: Add Change Predicate to UPSERT** - Add WHERE clause to only update when data actually changed
3. **Fix Option 2: Store Data Hash Column** - Add `data_hash` column to raw table, compare before updating
4. **Fix Option 3: Use IS DISTINCT FROM Comparison** - Compare columns using PostgreSQL's `IS DISTINCT FROM` operator
5. **Fix Option 4: Conditional UPDATE Logic** - Only update if at least one column value differs

### Recommended Fix:

**Add change predicate to UPSERT** to prevent no-op updates:

```sql
-- Modified _build_upsert_sql to only update when data changes
ON CONFLICT ("CommlogNum") DO UPDATE SET
    "PatNum" = EXCLUDED."PatNum",
    "CommDateTime" = EXCLUDED."CommDateTime",
    -- ... all columns
WHERE raw.commlog."PatNum" IS DISTINCT FROM EXCLUDED."PatNum"
   OR raw.commlog."CommDateTime" IS DISTINCT FROM EXCLUDED."CommDateTime"
   OR raw.commlog."Note" IS DISTINCT FROM EXCLUDED."Note"
   -- ... compare all columns
```

**Alternative: Hash-based approach** (simpler but less precise):
```sql
-- Store data_hash in raw table, compare before updating
ON CONFLICT ("CommlogNum") DO UPDATE SET
    ... (all columns),
    data_hash = EXCLUDED.data_hash
WHERE raw.commlog.data_hash IS DISTINCT FROM EXCLUDED.data_hash
```

**Implementation Location**: Modify `PostgresLoader._build_upsert_sql()` method (line 2544-2562) to add the WHERE clause.

### Diagnostic Query: Identify Transformation Differences

To identify which specific columns or transformations are causing the hash differences, run this comparison:

**Query 1D - Column-by-Column Comparison**:
```sql
-- Run this in PostgreSQL to compare specific columns between systems
-- First, get a sample from PostgreSQL raw
SELECT 
    "CommlogNum",
    "PatNum",
    "CommDateTime",
    "CommType",
    "Note",
    "Mode_",
    "SentOrReceived",
    "UserNum",
    "Signature",
    "SigIsTopaz",
    "DateTStamp",
    "DateTimeEnd",
    "CommSource",
    "ProgramNum",
    "DateTEntry",
    "ReferralNum",
    "CommReferralBehavior",
    -- Individual column hashes to identify which columns differ
    md5(COALESCE("PatNum"::text, '')) AS patnum_hash,
    md5(COALESCE("CommDateTime"::text, '')) AS commdatetime_hash,
    md5(COALESCE("DateTStamp"::text, '')) AS datestamp_hash,
    md5(COALESCE("Note"::text, '')) AS note_hash
FROM raw.commlog
WHERE "CommlogNum" IN (869442, 869441, 869440)  -- Sample CommlogNums
ORDER BY "CommlogNum" DESC;
```

Then compare with MySQL replication:
```sql
-- Run this in MySQL replication database
SELECT 
    CommlogNum,
    PatNum,
    CommDateTime,
    CommType,
    Note,
    Mode_,
    SentOrReceived,
    UserNum,
    Signature,
    SigIsTopaz,
    DateTStamp,
    DateTimeEnd,
    CommSource,
    ProgramNum,
    DateTEntry,
    ReferralNum,
    CommReferralBehavior,
    -- Individual column hashes
    MD5(COALESCE(CAST(PatNum AS CHAR), '')) AS patnum_hash,
    MD5(COALESCE(CAST(CommDateTime AS CHAR), '')) AS commdatetime_hash,
    MD5(COALESCE(CAST(DateTStamp AS CHAR), '')) AS datestamp_hash,
    MD5(COALESCE(Note, '')) AS note_hash
FROM commlog
WHERE CommlogNum IN (869442, 869441, 869440)
ORDER BY CommlogNum DESC;
```

Compare the individual column hashes to identify which columns are being transformed differently.

---

## Building on This Investigation

This investigation revealed a fundamental pattern that can be applied across all raw ingestion tables. To prevent similar issues and establish ongoing pipeline health monitoring, we should:

### 1. Define a Reusable "Raw Ingestion Contract"

**Purpose**: Establish a standard contract that all raw ingestion processes must follow to prevent no-op updates and ensure data integrity.

**Contract Components**:

1. **Change Detection Requirement**:
   - All UPSERT operations must include a change predicate (WHERE clause)
   - Updates should only occur when data actually changes
   - Options: `IS DISTINCT FROM` comparison, hash-based comparison, or conditional UPDATE logic

2. **Update Semantics**:
   - Define when an UPDATE is appropriate vs when INSERT-only behavior is expected
   - Document expected update patterns (e.g., append-only tables should have minimal updates)
   - Specify handling of representation differences (e.g., boolean conversion)

3. **Metrics and Monitoring**:
   - Define acceptable `upd_del_per_live` ratios per table type
   - Establish thresholds for churn alerts
   - Document expected update patterns based on source behavior

4. **Validation Rules**:
   - Source update patterns (append-only vs mutable)
   - Extraction strategy (incremental vs full refresh)
   - Expected conflict resolution behavior

**Implementation**:
- Add contract validation to `PostgresLoader._build_upsert_sql()`
- Create contract definitions in `etl_pipeline/config/tables.yml`
- Document contract requirements in `etl_pipeline/docs/DATA_CONTRACTS.md`

### 2. Turn Churn into an Ongoing Pipeline Health Metric

**Purpose**: Transform the churn analysis into a reusable monitoring system that detects similar issues across all raw tables.

**Health Metric Components**:

1. **Churn Ratio Calculation**:
   ```sql
   -- Reusable query for any raw table
   SELECT 
       schemaname,
       tablename,
       n_live_tup,
       n_tup_upd,
       n_tup_ins,
       CASE 
           WHEN n_live_tup > 0 
           THEN ROUND(n_tup_upd::numeric / n_live_tup::numeric, 2)
           ELSE NULL
       END AS upd_per_live_ratio,
       CASE 
           WHEN n_live_tup > 0 
           THEN ROUND((n_tup_upd + n_tup_del)::numeric / n_live_tup::numeric, 2)
           ELSE NULL
       END AS churn_ratio
   FROM pg_stat_user_tables
   WHERE schemaname = 'raw'
   ORDER BY churn_ratio DESC;
   ```

2. **Alert Thresholds**:
   - **Critical**: `churn_ratio > 2.0` (more than 2 updates per live tuple)
   - **Warning**: `churn_ratio > 1.0` (more than 1 update per live tuple)
   - **Info**: `churn_ratio > 0.5` (more than 0.5 updates per live tuple)

3. **Table Classification**:
   - **Append-only tables**: Expected `churn_ratio < 0.1` (minimal updates)
   - **Mutable tables**: Expected `churn_ratio < 1.0` (reasonable update frequency)
   - **Reference tables**: Expected `churn_ratio < 0.5` (infrequent updates)

4. **Automated Monitoring**:
   - Create a dbt model or Python script that runs daily
   - Compare churn ratios against expected thresholds
   - Alert on anomalies (e.g., sudden increases in churn)
   - Track churn trends over time

**Implementation**:
- Create `dbt_dental_models/models/monitoring/raw_table_churn_metrics.sql`
- Add to `etl_pipeline/monitoring/pipeline_health.py`
- Integrate into existing monitoring/alerting system
- Document in `etl_pipeline/docs/PIPELINE_MONITORING.md`

**Benefits**:
- Early detection of similar issues across all raw tables
- Proactive identification of pipeline degradation
- Data-driven insights into ETL behavior
- Validation that fixes are working (churn should decrease after fix)

---

## Status (2026-06)

### Investigation — complete

| Phase | Status | Date |
| --- | --- | --- |
| Empirical analysis (Queries 1A–5, hash comparison, source patterns) | ✅ Done | 2025-12-12 |
| Root cause confirmed (unconditional UPSERT) | ✅ Done | 2025-12-12 |
| Primary code fix (`IS DISTINCT FROM` in `_build_upsert_sql`) | ✅ Deployed | In `postgres_loader.py`; unit tests in `test_data_quality_unit.py` |

**Investigation conclusions (unchanged):** Source commlog is append-only; ETL extraction/watermark is correct; churn was driven by UPSERT semantics, not re-processing or source updates.

### Primary fix — deployed

```sql
-- Pattern now generated by _build_upsert_sql() (simplified)
ON CONFLICT ("CommlogNum") DO UPDATE SET ...
WHERE (target."PatNum" IS DISTINCT FROM EXCLUDED."PatNum" OR ... )
```

Applies to **all** raw tables using `PostgresLoader` upsert — not commlog-only.

### Remaining follow-on (not complete)

Tracked in [TODO.md — ETL Raw Ingestion Contract & Pipeline Health Metrics](../../TODO.md#etl-raw-ingestion-contract--pipeline-health-metrics):

| Item | Status |
| --- | --- |
| Raw ingestion contract in `tables.yml` | ❌ Pending |
| `etl_pipeline/docs/DATA_CONTRACTS.md` | ❌ Pending |
| `dbt_dental_models/models/monitoring/raw_table_churn_metrics.sql` | ❌ Pending |
| Churn alerts in `etl_pipeline/monitoring/pipeline_health.py` | ❌ Pending |
| `etl_pipeline/docs/PIPELINE_MONITORING.md` | ❌ Pending |
| Post-fix verification on `raw.commlog` stats | ❌ Pending |

### Post-fix verification query (PostgreSQL / DBeaver)

Re-run after several incremental loads post-fix; compare to pre-fix baseline (`upd_del_per_live = 1.91`):

```sql
SELECT
    schemaname,
    relname AS tablename,
    n_live_tup,
    n_tup_upd,
    n_tup_ins,
    ROUND(n_tup_upd::numeric / NULLIF(n_live_tup, 0), 2) AS upd_per_live_ratio,
    ROUND((n_tup_upd + n_tup_del)::numeric / NULLIF(n_live_tup, 0), 2) AS churn_ratio,
    last_autovacuum,
    last_vacuum
FROM pg_stat_user_tables
WHERE schemaname = 'raw'
  AND relname = 'commlog';
```

**Note:** `pg_stat_user_tables` counters are cumulative since stats reset — a drop in *incremental* churn is visible only after reset or by comparing ratio trends over new load windows. For a clean read, reset stats once post-deploy (`pg_stat_reset()` on the table, ops-only) or track daily snapshot table when monitoring is built.

### Doc maintenance

- §818–903 and earlier code-analysis passages describe **pre-fix** behavior; kept for investigation record.
- When monitoring ships, move churn thresholds from §1021–1106 into `PIPELINE_MONITORING.md` and link back here as the commlog case study.

---

**Document Created**: 2025-01-XX  
**Investigation Completed**: 2025-12-12  
**Document Updated**: 2026-06-26  
**Status**: Investigation complete; primary UPSERT fix deployed; monitoring/contracts pending (TODO)
