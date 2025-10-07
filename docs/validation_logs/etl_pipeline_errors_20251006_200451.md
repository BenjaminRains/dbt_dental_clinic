# ETL Pipeline Error Log - Run 2025-10-06 20:04:51

## Summary
This document records all errors encountered during the ETL pipeline run on October 6, 2025 at 20:04:51. The pipeline encountered multiple types of errors affecting data extraction and loading operations.

## Error Categories

### 1. Column Mismatch Errors (MySQL Schema Issues)
These tables failed due to unknown columns in the MySQL source database:

#### Tables with Unknown Column Errors:
- **document**: Unknown column 'ChartLetterStatus'
- **appointment**: Unknown column 'IsMirrored'  
- **histappointment**: Unknown column 'IsMirrored'
- **inssub**: Unknown column 'SecurityHash'
- **insverify**: Unknown column 'SecurityHash'
- **insverifyhist**: Unknown column 'SecurityHash'
- **statement**: Unknown column 'ShowTransSinceBalZero'

#### Error Details:
```
Error: (1054, "Unknown column 'COLUMN_NAME' in 'field list'")
Impact: Complete extraction failure for affected tables
Status: 0 rows copied, marked as failed
```

### 2. Schema Mismatch Errors
- **computerpref**: Column count mismatch - PostgreSQL has 33 columns, MySQL has 34 columns

### 3. Missing Table Errors (PostgreSQL Analytics Database)
These tables are referenced in the ETL configuration but do not exist in the PostgreSQL analytics database:

#### Missing Tables in raw schema:
- eroutingactiondef
- eroutingdef  
- eroutingdeflink
- evaluationcriteriondef
- evaluationdef
- fielddeflink
- hl7def
- hl7deffield
- hl7defmessage
- hl7defsegment
- insfilingcodesubtype
- payortype
- questiondef
- toothgriddef
- vaccinedef

#### Error Details:
```
Error: (psycopg2.errors.UndefinedTable) relation "raw.TABLE_NAME" does not exist
Impact: Cannot compute row counts or MAX values for tracking
Status: ETL tracking operations fail for these tables
```

## Failed Tables Summary

### Complete Extraction Failures (7 tables):
1. **document** - ChartLetterStatus column missing
2. **appointment** - IsMirrored column missing  
3. **histappointment** - IsMirrored column missing
4. **inssub** - SecurityHash column missing
5. **insverify** - SecurityHash column missing
6. **insverifyhist** - SecurityHash column missing
7. **statement** - ShowTransSinceBalZero column missing

### Schema Mismatch (1 table):
1. **computerpref** - Column count difference (33 vs 34)

### Missing in Analytics Database (15 tables):
1. eroutingactiondef
2. eroutingdef
3. eroutingdeflink
4. evaluationcriteriondef
5. evaluationdef
6. fielddeflink
7. hl7def
8. hl7deffield
9. hl7defmessage
10. hl7defsegment
11. insfilingcodesubtype
12. payortype
13. questiondef
14. toothgriddef
15. vaccinedef

## Recommended Actions

### Immediate Actions:
1. **Verify MySQL Schema**: Check if the missing columns exist in the source OpenDental database
2. **Update Table Configurations**: Remove or update references to non-existent columns
3. **Schema Synchronization**: Investigate the computerpref column count mismatch

### Missing Tables Investigation:
1. **Verify Source Tables**: Check if these tables exist in the source MySQL database
2. **Update ETL Configuration**: Remove references to non-existent tables from the ETL configuration
3. **Database Schema Audit**: Perform a comprehensive audit of available tables in both source and target databases

### Long-term Actions:
1. **Schema Validation**: Implement pre-ETL schema validation to catch these issues earlier
2. **Error Handling**: Improve error handling to continue processing other tables when some fail
3. **Monitoring**: Set up alerts for extraction failures to enable faster response

## Error Impact Assessment

- **Total Tables Affected**: 23 tables
- **Complete Failures**: 7 tables (0 rows copied)
- **Schema Issues**: 1 table (column count mismatch)
- **Missing References**: 15 tables (tracking failures)

The ETL pipeline continued processing other tables despite these failures, indicating the error handling allows for partial success scenarios.

## Method Usage Analysis

Based on the method usage tracking data from the same ETL run:

### PostgresLoader Method Calls:
- **PostgresLoader.load_table**: 79 calls
  - First seen: 2025-10-06T20:02:19.262273
  - Last seen: 2025-10-06T20:28:42.291324
  - Used in both runs: 2025-10-06T20:02:19.262273 and 2025-10-06T20:04:51.543569

- **PostgresLoader.load_table_standard**: 74 calls
  - First seen: 2025-10-06T20:02:19.262273
  - Last seen: 2025-10-06T20:28:42.291324
  - Primary loading method for most tables

- **PostgresLoader._build_load_query**: 79 calls
  - First seen: 2025-10-06T20:02:19.262273
  - Last seen: 2025-10-06T20:28:42.305681
  - Query building method called for each table load

- **PostgresLoader.load_table_chunked**: 7 calls
  - First seen: 2025-10-06T20:04:51.543569
  - Last seen: 2025-10-06T20:08:18.091825
  - Used for larger tables requiring chunked processing

- **PostgresLoader.load_table_streaming**: 5 calls
  - First seen: 2025-10-06T20:04:51.543569
  - Last seen: 2025-10-06T20:16:00.984295
  - Used for tables requiring streaming load approach

### Method Usage Insights:
- **Total Method Calls**: 234 across all PostgresLoader methods
- **Primary Loading Strategy**: Standard loading (74/79 successful loads)
- **Alternative Strategies**: Chunked (7 tables) and Streaming (5 tables) used for specific cases
- **Run Duration**: Methods were active from 20:02:19 to 20:28:42 (approximately 26 minutes)
- **Multiple Runs**: The system processed tables across two distinct run sessions

The method usage data shows that despite the extraction failures documented above, the PostgresLoader successfully processed 79 tables using various loading strategies, indicating the ETL pipeline's resilience in handling partial failures.

## Solution and Troubleshooting

### Root Cause Analysis
The errors indicate schema drift between the ETL configuration and the actual database schemas:

1. **Column Mismatches**: The ETL configuration references columns that no longer exist in the source MySQL database
2. **Schema Evolution**: OpenDental database schema has evolved since the last schema analysis
3. **Configuration Staleness**: The `tables.yml` configuration file is out of sync with current database structure

### Immediate Solutions

#### 1. Re-run Schema Analysis
The most effective solution is to regenerate the ETL configuration using the current database schema:

```bash
cd etl_pipeline
python scripts/analyze_opendental_schema.py
```

This will:
- Analyze the current MySQL schema structure
- Detect all available columns for each table
- Generate updated `tables.yml` configuration
- Remove references to non-existent columns
- Update incremental column detection

#### 2. Verify Schema Changes
Based on the appointment table DDL provided, the `IsMirrored` column is indeed missing from the current schema. The ETL configuration likely references an older version of the table structure.

**Current table schemas** (from provided DDLs):

**appointment table**:
- Does NOT include `IsMirrored` column
- Does include `SecurityHash` column

**statement table**:
- Does NOT include `ShowTransSinceBalZero` column
- Includes 29 columns total (StatementNum, PatNum, DateSent, etc.)

**inssub table**:
- Does NOT include `SecurityHash` column
- Includes 13 columns total (InsSubNum, PlanNum, Subscriber, etc.)

**Expected columns causing errors**:
- `IsMirrored` - Missing from appointment and histappointment tables
- `ChartLetterStatus` - Missing from document table
- `SecurityHash` - Present in appointment but missing from inssub, insverify, insverifyhist (confirmed via current DDL)
- `ShowTransSinceBalZero` - Missing from statement table (confirmed via current DDL)

#### 3. Manual Configuration Updates (Alternative)
If schema analysis cannot be run immediately, manually update the ETL configuration:

1. **Remove missing columns** from table configurations in `etl_pipeline/config/tables.yml`
2. **Update incremental column lists** to exclude non-existent columns
3. **Verify primary key columns** are still valid

### Troubleshooting Steps

#### Step 1: Verify Source Database Schema
Connect to the source MySQL database and verify current table structures:

```sql
-- Check appointment table structure
DESCRIBE appointment;

-- Check for IsMirrored column specifically
SELECT COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'opendental' 
AND TABLE_NAME = 'appointment' 
AND COLUMN_NAME = 'IsMirrored';

-- Check statement table structure
DESCRIBE statement;

-- Check for ShowTransSinceBalZero column specifically
SELECT COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'opendental' 
AND TABLE_NAME = 'statement' 
AND COLUMN_NAME = 'ShowTransSinceBalZero';

-- Check inssub table structure
DESCRIBE inssub;

-- Check for SecurityHash column specifically
SELECT COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'opendental' 
AND TABLE_NAME = 'inssub' 
AND COLUMN_NAME = 'SecurityHash';
```

#### Step 2: Compare with ETL Configuration
Check what columns the ETL configuration expects:

```bash
# Search for missing column references in configuration
grep -r "IsMirrored" etl_pipeline/config/
grep -r "ShowTransSinceBalZero" etl_pipeline/config/
grep -r "ChartLetterStatus" etl_pipeline/config/
```

#### Step 3: Update Configuration
After schema analysis, verify the updated configuration:

```bash
# Check if missing columns are still referenced
grep -r "IsMirrored" etl_pipeline/config/tables.yml
grep -r "ShowTransSinceBalZero" etl_pipeline/config/tables.yml
grep -r "ChartLetterStatus" etl_pipeline/config/tables.yml
```

#### Step 4: Test with Single Table
Test the fix with a single problematic table:

```bash
# Test problematic tables individually
etl-run --tables appointment
etl-run --tables statement
etl-run --tables document
etl-run --tables inssub
etl-run --tables insverify
etl-run --tables insverifyhist
```

### Prevention Strategies

#### 1. Regular Schema Monitoring
- Run schema analysis monthly or after OpenDental updates
- Set up alerts for schema changes
- Monitor ETL logs for column mismatch errors

#### 2. Schema Validation
- Implement pre-ETL schema validation
- Add column existence checks before extraction
- Use database introspection to verify configurations

#### 3. Configuration Management
- Version control schema configurations
- Document schema changes and their impact
- Maintain mapping between OpenDental versions and ETL configurations

### Expected Outcomes After Fix

After running the schema analysis and updating the configuration:

1. **Eliminated Errors**: All "Unknown column" errors should be resolved
2. **Updated Incremental Columns**: Only existing columns will be used for incremental loading
3. **Improved Performance**: Accurate column detection will optimize extraction strategies
4. **Reduced Maintenance**: Automated schema detection reduces manual configuration updates

### Next Steps

1. **Immediate**: Run `python scripts/analyze_opendental_schema.py`
2. **Validation**: Test ETL run with updated configuration
3. **Monitoring**: Set up alerts for future schema drift
4. **Documentation**: Update this error log with resolution status

## Log File Reference
Source: `etl_pipeline/logs/etl_pipeline/etl_pipeline_run_20251006_200451.log`
Method Usage: `etl_pipeline/logs/method_usage.json`
Analysis Date: 2025-10-06
Total Log Lines: 50,359
