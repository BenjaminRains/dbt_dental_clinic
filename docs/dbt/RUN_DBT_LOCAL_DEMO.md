# Running dbt Models Locally Against opendental_demo Database

This guide explains how to run all dbt models locally against your local PostgreSQL `opendental_demo` database using the `demo` target.

## Prerequisites

1. **Local PostgreSQL Database**: Ensure `opendental_demo` database exists and has synthetic data loaded
2. **Database Connection**: Verify you can connect to the database using DBeaver or `psql`
3. **Environment Variables**: Set `DEMO_POSTGRES_*` environment variables

## Quick Start

Run this PowerShell script from the project root to set up and run all models:

```powershell
# Set environment variables for local demo database
$env:DEMO_POSTGRES_HOST = "localhost"
$env:DEMO_POSTGRES_PORT = "5432"
$env:DEMO_POSTGRES_DB = "opendental_demo"
$env:DEMO_POSTGRES_USER = "postgres"  # Change if using different user
$env:DEMO_POSTGRES_PASSWORD = "your_password"  # Your local PostgreSQL password
$env:DEMO_POSTGRES_SCHEMA = "raw"

# Navigate to dbt project
cd dbt_dental_models

# Test connection
dbt debug --target demo

# Run all models
dbt run --target demo
```

## Step 1: Set Environment Variables

The `demo` target in `profiles.yml` uses these environment variables:

```powershell
# From project root
$env:DEMO_POSTGRES_HOST = "localhost"
$env:DEMO_POSTGRES_PORT = "5432"
$env:DEMO_POSTGRES_DB = "opendental_demo"
$env:DEMO_POSTGRES_USER = "postgres"  # or your local PostgreSQL username
$env:DEMO_POSTGRES_PASSWORD = "your_password"  # your local PostgreSQL password
$env:DEMO_POSTGRES_SCHEMA = "raw"  # default schema for dbt operations
```

**Note**: If you're using a different PostgreSQL user (e.g., `opendental_demo_user`), adjust `DEMO_POSTGRES_USER` accordingly.

## Step 2: Navigate to dbt Project

```powershell
cd dbt_dental_models
```

## Step 3: Test Connection

Verify dbt can connect to your local database:

```powershell
dbt debug --target demo
```

This should show:
- ✅ Connection successful
- ✅ Profile configuration valid
- ✅ Dependencies installed

## Step 4: Run All Models

Run all dbt models against the demo database:

```powershell
dbt run --target demo
```

This will:
1. Create/update all staging models in the `staging` schema
2. Create/update all intermediate models in the `intermediate` schema
3. Create/update all mart models in the `marts` schema (including `mart_ar_summary`)

**Expected Output**: You should see progress for all 157 models, with completion messages like:
```
OK created sql table model raw_marts.mart_ar_summary ................ [SELECT X in Y.XXs]
```

## Step 5: Verify Results

### Check mart_ar_summary Row Count

In DBeaver, run:

```sql
SELECT COUNT(*) as total_rows
FROM raw_marts.mart_ar_summary;
```

### Check Upstream Dependencies

If `mart_ar_summary` has 0 rows, verify upstream models have data:

```sql
-- Check int_ar_balance (should have rows with include_in_ar = true and current_balance > 0)
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE include_in_ar = true AND current_balance > 0) as eligible_rows,
    COUNT(*) FILTER (WHERE procedure_date >= CURRENT_DATE - INTERVAL '18 months') as recent_procedures
FROM raw_intermediate.int_ar_balance;

-- Check dim_patient (should have patients with total_balance > 0)
SELECT 
    COUNT(*) as total_patients,
    COUNT(*) FILTER (WHERE patient_status IN ('Patient', 'Inactive')) as active_patients,
    COUNT(*) FILTER (WHERE total_balance > 0) as patients_with_balance
FROM raw_marts.dim_patient;

-- Check fact_claim (for billed_last_year calculation)
SELECT 
    COUNT(*) as total_claims,
    COUNT(*) FILTER (WHERE claim_date >= CURRENT_DATE - INTERVAL '365 days') as recent_claims
FROM raw_marts.fact_claim;
```

## Troubleshooting: Why mart_ar_summary Has 0 Rows

The `mart_ar_summary` model has several filters that could result in 0 rows:

### Filter 1: Procedure Date Range (18 months)
**Location**: `int_ar_balance.sql` line 77, `mart_ar_summary.sql` line 94

```sql
WHERE procedure_date >= CURRENT_DATE - INTERVAL '18 months'
```

**Issue**: If your synthetic data has procedure dates older than 18 months, they will be excluded.

**Check**:
```sql
SELECT 
    MIN(procedure_date) as oldest_procedure,
    MAX(procedure_date) as newest_procedure,
    CURRENT_DATE - INTERVAL '18 months' as cutoff_date
FROM raw.procedure;
```

**Fix**: If procedures are too old, regenerate synthetic data with recent dates:
```powershell
cd etl_pipeline/synthetic_data_generator
.\generate.ps1 -Patients 5000 -StartDate (Get-Date).AddMonths(-12).ToString("yyyy-MM-dd")
```

### Filter 2: Include in AR Flag
**Location**: `mart_ar_summary.sql` line 92

```sql
WHERE ab.include_in_ar = true AND ab.current_balance > 0
```

**Issue**: `include_in_ar` is `FALSE` when `current_balance <= 0` (fully paid procedures).

**Check**:
```sql
SELECT 
    COUNT(*) as total_procedures,
    COUNT(*) FILTER (WHERE include_in_ar = true) as included_in_ar,
    COUNT(*) FILTER (WHERE current_balance > 0) as with_balance,
    COUNT(*) FILTER (WHERE include_in_ar = true AND current_balance > 0) as eligible
FROM raw_intermediate.int_ar_balance
WHERE procedure_date >= CURRENT_DATE - INTERVAL '18 months';
```

**Fix**: If all procedures are fully paid, the synthetic data generator may need adjustment to create outstanding balances.

### Filter 3: Patient Status
**Location**: `mart_ar_summary.sql` line 193

```sql
WHERE dp.patient_status IN ('Patient', 'Inactive')
```

**Issue**: If patient statuses don't match these exact values, patients will be excluded.

**Check**:
```sql
SELECT DISTINCT patient_status, COUNT(*) as count
FROM raw_marts.dim_patient
GROUP BY patient_status;
```

**Fix**: Ensure synthetic data uses correct patient status values.

### Filter 4: Patient Total Balance
**Location**: `mart_ar_summary.sql` line 194

```sql
WHERE dp.total_balance > 0
```

**Issue**: Only patients with outstanding balances are included.

**Check**:
```sql
SELECT 
    COUNT(*) as total_patients,
    COUNT(*) FILTER (WHERE total_balance > 0) as with_balance,
    AVG(total_balance) as avg_balance
FROM raw_marts.dim_patient
WHERE patient_status IN ('Patient', 'Inactive');
```

**Fix**: If `dim_patient.total_balance` is 0 for all patients, check if payments are being applied correctly in the synthetic data.

### Filter 5: Final Filter
**Location**: `mart_ar_summary.sql` line 583

```sql
WHERE ae.total_balance != 0 OR ae.billed_last_year > 0
```

**Issue**: This is the final filter that requires either a balance or recent billing activity.

**Check**:
```sql
-- Check what would pass the final filter
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE total_balance != 0) as with_balance,
    COUNT(*) FILTER (WHERE billed_last_year > 0) as with_billing,
    COUNT(*) FILTER (WHERE total_balance != 0 OR billed_last_year > 0) as eligible
FROM (
    -- Simplified version of ar_aggregated CTE
    SELECT 
        pb.patient_id,
        pb.total_balance,
        COALESCE(SUM(rc.total_billed), 0) as billed_last_year
    FROM (
        SELECT 
            dp.patient_id,
            dp.total_balance
        FROM raw_marts.dim_patient dp
        WHERE dp.patient_status IN ('Patient', 'Inactive')
            AND dp.total_balance > 0
    ) pb
    LEFT JOIN (
        SELECT 
            fc.patient_id,
            SUM(fc.billed_amount) as total_billed
        FROM raw_marts.fact_claim fc
        WHERE fc.claim_date >= CURRENT_DATE - INTERVAL '365 days'
        GROUP BY fc.patient_id
    ) rc ON pb.patient_id = rc.patient_id
    GROUP BY pb.patient_id, pb.total_balance
) ae;
```

## Quick Diagnostic Query

Run this comprehensive diagnostic to identify the issue:

```sql
WITH diagnostics AS (
    SELECT 
        'int_ar_balance' as model_name,
        COUNT(*) as total_rows,
        COUNT(*) FILTER (WHERE include_in_ar = true AND current_balance > 0 
                         AND procedure_date >= CURRENT_DATE - INTERVAL '18 months') as eligible_rows
    FROM raw_intermediate.int_ar_balance
    
    UNION ALL
    
    SELECT 
        'dim_patient' as model_name,
        COUNT(*) as total_rows,
        COUNT(*) FILTER (WHERE patient_status IN ('Patient', 'Inactive') 
                         AND total_balance > 0) as eligible_rows
    FROM raw_marts.dim_patient
    
    UNION ALL
    
    SELECT 
        'fact_claim' as model_name,
        COUNT(*) as total_rows,
        COUNT(*) FILTER (WHERE claim_date >= CURRENT_DATE - INTERVAL '365 days') as eligible_rows
    FROM raw_marts.fact_claim
    
    UNION ALL
    
    SELECT 
        'mart_ar_summary' as model_name,
        COUNT(*) as total_rows,
        COUNT(*) as eligible_rows
    FROM raw_marts.mart_ar_summary
)
SELECT * FROM diagnostics
ORDER BY model_name;
```

## Next Steps

1. **If all upstream models have data but `mart_ar_summary` is empty**: Check the join conditions and filters in the model
2. **If upstream models are empty**: Regenerate synthetic data with appropriate date ranges and outstanding balances
3. **If data exists but filters exclude everything**: Adjust the synthetic data generator to create data that passes the filters

## Running Tests

After running models, validate data quality:

```powershell
dbt test --target demo
```

This will run all tests defined in the `schema.yml` files to ensure data quality.

## Generating Documentation

Generate and view dbt documentation:

```powershell
dbt docs generate --target demo
dbt docs serve --target demo
```

This will start a local server (usually at `http://localhost:8080`) where you can explore the dbt project documentation and lineage graphs.
