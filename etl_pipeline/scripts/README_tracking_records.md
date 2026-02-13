# Tracking Records Generation

## Overview

This directory contains scripts for generating and managing ETL tracking records in the PostgreSQL analytics database.

## Files

### `create_missing_tracking_records.sql`
- **Purpose**: Static SQL script to create missing tracking records
- **Scope**: PostgreSQL analytics database (`raw.etl_load_status`)
- **Usage**: Run in DBeaver or psql
- **Pros**: Simple, direct SQL execution
- **Cons**: Hardcoded table list, manual regeneration needed

### `create_missing_tracking_records.py`
- **Purpose**: Python script using ETL pipeline infrastructure
- **Scope**: PostgreSQL analytics database (`raw.etl_load_status`)
- **Usage**: Run with Python, uses Settings and connections
- **Pros**: Dynamic from `tables.yml`, better error handling, logging
- **Cons**: Requires Python environment setup

### `generate_tracking_records_sql.py`
- **Purpose**: Dynamic SQL script generator from `tables.yml` configuration
- **Input**: `etl_pipeline/config/tables.yml`
- **Output**: SQL script with current table list
- **Usage**: Regenerate when `tables.yml` changes

## Configuration Dependency

The tracking records are based on the `tables.yml` configuration file. Specifically, the script creates tracking records for all tables that have `incremental_columns` defined.

### When to Regenerate

**Regenerate the SQL script when:**
- New tables are added to `tables.yml` with `incremental_columns`
- Existing tables have their `incremental_columns` configuration changed
- Tables are removed from `tables.yml` that had `incremental_columns`

**You DON'T need to regenerate when:**
- `incremental_columns` values change (the script only looks at table names)
- Other configuration changes (batch_size, extraction_strategy, etc.)
- Tables without `incremental_columns` are added/removed

## Usage Options

### Option 1: Python Script (Recommended)
```bash
# Run the Python script (uses ETL pipeline infrastructure)
python scripts/create_missing_tracking_records.py
```

**Advantages:**
- ✅ Uses existing ETL pipeline Settings and connections
- ✅ Dynamic from `tables.yml` configuration
- ✅ Better error handling and logging
- ✅ Checks if tracking tables exist
- ✅ Only creates missing records (idempotent)
- ✅ Detailed verification output

### Option 2: SQL Script in DBeaver
```sql
-- Copy and paste the SQL script into DBeaver
-- Or run from command line:
psql -d opendental_analytics -f scripts/create_missing_tracking_records.sql
```

**Advantages:**
- ✅ Simple SQL execution
- ✅ No Python environment required
- ✅ Direct database access

### Option 3: Dynamic SQL Generation
```bash
# Regenerate the SQL script from current tables.yml
python scripts/generate_tracking_records_sql.py > scripts/create_missing_tracking_records.sql

# Run the updated script
psql -d opendental_analytics -f scripts/create_missing_tracking_records.sql
```

## Example Configuration

From `tables.yml`:
```yaml
tables:
  adjustment:
    incremental_columns:
    - SecDateTEdit
    - AdjDate
    - ProcDate
    - DateEntry
  patient:
    incremental_columns:
    - DateModified
  appointment:
    incremental_columns:
    - DateTStamp
```

This generates tracking records for: `adjustment`, `patient`, `appointment`

## Verification

After running any script, verify the records were created:
```sql
SELECT 
    table_name, 
    last_loaded, 
    rows_loaded, 
    load_status,
    _created_at
FROM raw.etl_load_status 
ORDER BY table_name;
```

## Python Script Features

The Python script (`create_missing_tracking_records.py`) includes:

- **Configuration Loading**: Reads `tables.yml` dynamically
- **Connection Management**: Uses ETL pipeline ConnectionFactory
- **Error Handling**: Comprehensive error checking and logging
- **Table Validation**: Checks if tracking tables exist
- **Idempotent Operation**: Only creates missing records
- **Verification**: Shows created records with details
- **Environment Support**: Works with test/clinic environments

## Notes

- All scripts use `NOT EXISTS` or similar logic to avoid duplicate records
- All records are created with `'pending'` status and `'2024-01-01 00:00:00'` timestamp
- Scripts are safe to run multiple times (idempotent)
- Only affects `raw.etl_load_status` table (PostgresLoader tracking)
- Does not affect `etl_copy_status` (SimpleMySQLReplicator tracking) or `etl_transform_status` (dbt tracking)

## Recommendation

**Use the Python script** (`create_missing_tracking_records.py`) for:
- Production environments
- Automated deployments
- Better error handling and logging
- Dynamic configuration from `tables.yml`

**Use the SQL script** (`create_missing_tracking_records.sql`) for:
- Quick manual execution in DBeaver
- Environments without Python setup
- Simple one-time operations 