# Fix Plan: Demo Database Collection Rate & Patient Balances Issues

**Status:** Planning Complete  
**Date:** 2025-01-09  
**Goal:** Fix collection rate calculation and patient balances API errors on demo database (opendental_demo)

---

## Problem Summary

### Issue 1: Collection Rate Showing 0% on Demo Database
- **Location:** Executive Dashboard, Collection Rate KPI
- **Expected:** Collection rate should be > 0% (typically 80-100% for healthy practice)
- **Current:** Showing 0% despite code fixes being implemented
- **Root Cause Hypothesis:** 
  1. dbt models not run on demo database after code changes
  2. Payment data missing or not properly linked in demo database
  3. Date matching issue between payments and appointments

### Issue 2: Top 10 Patient Balances API Error
- **Location:** Executive Dashboard, "Top 10 Patient Balances" component
- **Error:** `Validation error: path.patient_id: Input should be a valid integer, unable to parse string as an integer`
- **Root Cause:** `patient_id` column in `mart_ar_summary` is stored as TEXT/VARCHAR in demo database instead of INTEGER
- **Impact:** API Pydantic model expects `int` but receives `str`

### High-Level Issue: Shared Models Between Environments
- **Problem:** Same dbt models used for both local dev (production database) and EC2 demo builds
- **Risk:** Type mismatches, data structure differences, or missing data in demo environment
- **Impact:** Models may work in production but fail in demo due to:
  - Different data types in raw tables
  - Missing columns or data in synthetic data
  - Different data generation patterns

---

## Investigation Plan

### Phase 1: Diagnose Patient ID Type Issue (Priority 1)

#### Step 1.1: Verify Data Types in Demo Database
```sql
-- Check patient_id type in mart_ar_summary
SELECT 
    column_name,
    data_type,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'marts'
  AND table_name = 'mart_ar_summary'
  AND column_name = 'patient_id';

-- Check raw source data type
SELECT 
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'raw'
  AND table_name = 'patient'
  AND column_name = 'PatNum';
```

#### Step 1.2: Check Sample Data
```sql
-- Check if patient_id values are strings or integers
SELECT 
    patient_id,
    pg_typeof(patient_id) as data_type,
    patient_id::text as as_text,
    CASE 
        WHEN patient_id::text ~ '^[0-9]+$' THEN 'numeric_string'
        ELSE 'non_numeric'
    END as pattern_check
FROM marts.mart_ar_summary
LIMIT 10;

-- Check raw PatNum values
SELECT 
    "PatNum",
    pg_typeof("PatNum") as data_type,
    "PatNum"::text as as_text
FROM raw.patient
LIMIT 10;
```

#### Step 1.3: Check Staging Model Output
```sql
-- Verify staging model produces integer patient_id
SELECT 
    patient_id,
    pg_typeof(patient_id) as data_type
FROM staging.stg_opendental__patient
LIMIT 10;
```

**Expected Findings:**
- If `raw.patient.PatNum` is TEXT → Need to fix ETL pipeline or synthetic data generator
- If `staging.stg_opendental__patient.patient_id` is TEXT → Need to fix `transform_id_columns` macro usage
- If `marts.mart_ar_summary.patient_id` is TEXT → Need to add explicit cast in mart model

---

### Phase 2: Diagnose Collection Rate Issue (Priority 2)

#### Step 2.1: Verify dbt Models Run Status
```bash
# On EC2 demo instance, check if models were run
dbt run --select mart_production_summary --target demo --dry-run
dbt run --select int_payment_split --target demo --dry-run
```

#### Step 2.2: Check Payment Data Exists
```sql
-- Check if payments exist in demo database
SELECT 
    COUNT(*) as total_payments,
    COUNT(DISTINCT patient_id) as patients_with_payments,
    SUM(payment_amount) as total_collections,
    MIN(payment_date) as earliest_payment,
    MAX(payment_date) as latest_payment
FROM marts.fact_payment
WHERE payment_date >= CURRENT_DATE - INTERVAL '365 days'
  AND payment_direction = 'Income';

-- Check payment_metrics (int_payment_split) data
SELECT 
    COUNT(*) as total_rows,
    SUM(patient_payments) as total_patient_payments,
    SUM(insurance_payments) as total_insurance_payments
FROM intermediate.int_payment_split
WHERE payment_date >= CURRENT_DATE - INTERVAL '365 days';
```

#### Step 2.3: Check Production Data Exists
```sql
-- Check if production data exists
SELECT 
    COUNT(*) as total_procedures,
    SUM(actual_fee) as total_production,
    MIN(date_day) as earliest_date,
    MAX(date_day) as latest_date
FROM marts.fact_procedure fp
INNER JOIN marts.dim_date dd ON fp.date_id = dd.date_id
WHERE dd.date_day >= CURRENT_DATE - INTERVAL '365 days';
```

#### Step 2.4: Verify Collection Rate Calculation
```sql
-- Manual collection rate calculation
WITH payment_totals AS (
    SELECT 
        SUM(pm.patient_payments) + SUM(pm.insurance_payments) as total_collections
    FROM intermediate.int_payment_split pm
    WHERE pm.payment_date >= CURRENT_DATE - INTERVAL '365 days'
),
production_totals AS (
    SELECT 
        SUM(fp.actual_fee) as total_production
    FROM marts.fact_procedure fp
    INNER JOIN marts.dim_date dd ON fp.date_id = dd.date_id
    WHERE dd.date_day >= CURRENT_DATE - INTERVAL '365 days'
)
SELECT 
    pt.total_collections,
    pr.total_production,
    CASE 
        WHEN pr.total_production > 0 
        THEN (pt.total_collections / pr.total_production) * 100
        ELSE 0
    END as collection_rate_pct
FROM payment_totals pt
CROSS JOIN production_totals pr;
```

#### Step 2.5: Check Date Matching
```sql
-- Check if payment dates align with appointment dates
SELECT 
    DATE_TRUNC('month', fa.appointment_date) as appointment_month,
    COUNT(DISTINCT fa.appointment_id) as appointments,
    COUNT(DISTINCT fp.payment_id) as payments,
    SUM(fp.payment_amount) as payment_amount
FROM marts.fact_appointment fa
LEFT JOIN marts.fact_payment fp 
    ON fa.patient_id = fp.patient_id
    AND DATE_TRUNC('day', fa.appointment_date) = DATE_TRUNC('day', fp.payment_date)
WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY DATE_TRUNC('month', fa.appointment_date)
ORDER BY appointment_month DESC;
```

---

## Fix Plan

### Fix 1: Patient ID Type Conversion (CRITICAL)

#### Option A: Fix at API Layer (Quick Fix)
**File:** `api/services/patient_service.py`

Add type conversion in `get_top_patient_balances()`:
```python
def get_top_patient_balances(db: Session, limit: int = 10):
    """Get top N patients by total balance from AR summary"""
    query = text("""
        WITH latest_snapshots AS (
            SELECT DISTINCT ON (patient_id)
                mas.patient_id::integer as patient_id,  -- Explicit cast to integer
                mas.total_balance,
                -- ... rest of query
            FROM raw_marts.mart_ar_summary mas
            WHERE mas.total_balance > 0
            ORDER BY mas.patient_id, mas.snapshot_date DESC
        )
        SELECT 
            ls.patient_id,
            -- ... rest of fields
        FROM latest_snapshots ls
        ORDER BY ls.total_balance DESC
        LIMIT :limit
    """)
    result = db.execute(query, {"limit": limit})
    rows = result.fetchall()
    
    # Convert patient_id to int if it's a string
    converted_rows = []
    for row in rows:
        row_dict = dict(row._mapping)
        if 'patient_id' in row_dict and isinstance(row_dict['patient_id'], str):
            try:
                row_dict['patient_id'] = int(row_dict['patient_id'])
            except (ValueError, TypeError):
                logger.warning(f"Could not convert patient_id to int: {row_dict['patient_id']}")
                continue
        converted_rows.append(row_dict)
    
    return converted_rows
```

**Pros:** Quick fix, handles both string and integer types  
**Cons:** Doesn't fix root cause, adds conversion overhead

#### Option B: Fix at dbt Model Layer (Recommended)
**File:** `dbt_dental_models/models/marts/mart_ar_summary.sql`

Add explicit cast in final SELECT:
```sql
final as (
    select
        -- Date and dimensions
        dd.date_id,
        ab.patient_id::integer as patient_id,  -- Explicit cast to ensure integer type
        ab.provider_id,
        -- ... rest of fields
    from ar_base ab
    -- ... rest of query
)
```

**Also check:** Ensure `stg_opendental__patient.patient_id` is integer:
```sql
-- In stg_opendental__patient.sql, verify transform_id_columns macro is used correctly
{{ transform_id_columns([
    {'source': '"PatNum"', 'target': 'patient_id'},  -- This should produce integer
]) }}
```

**Pros:** Fixes root cause, ensures consistency  
**Cons:** Requires dbt model rebuild

#### Option C: Fix at ETL/Raw Data Layer (If Needed)
If raw data has `PatNum` as TEXT, fix in ETL pipeline or synthetic data generator:
- **ETL Pipeline:** Ensure `PatNum` is loaded as INTEGER
- **Synthetic Data Generator:** Ensure `PatNum` is generated as INTEGER

---

### Fix 2: Collection Rate Calculation (HIGH PRIORITY)

#### Step 2.1: Verify dbt Models Are Up-to-Date
```bash
# On EC2 demo instance
cd /path/to/dbt_dental_models
dbt run --select int_payment_split mart_production_summary --target demo
```

#### Step 2.2: Verify Payment Data Generation
If payments are missing in demo database:
- **Check synthetic data generator:** Ensure payments are being generated
- **Verify payment dates:** Ensure payments have dates within last 365 days
- **Check payment types:** Ensure payment types 69, 70, 71 are correctly categorized

#### Step 2.3: Add Diagnostic Logging
**File:** `api/services/reports_service.py` or `api/services/ar_service.py`

Add logging to collection rate calculation:
```python
# In get_ar_kpi_summary or similar function
logger.info(f"Collection rate calculation: production={total_production}, collections={total_collections}, rate={collection_rate}")
```

#### Step 2.4: Verify Date Alignment
If payments exist but dates don't align:
- **Check synthetic data generator:** Ensure payment dates align with appointment dates
- **Consider:** Using a date range window (e.g., ±7 days) for matching

---

## Implementation Steps

### Step 1: Immediate Fix (Patient ID Type)
1. **Add API-level type conversion** (Option A) - 30 minutes
   - Update `patient_service.py` to cast `patient_id` to integer
   - Test with demo database
   - Deploy to EC2

2. **Verify root cause** - 1 hour
   - Run diagnostic queries on demo database
   - Check data types at each layer (raw → staging → marts)

3. **Fix root cause** (Option B) - 2 hours
   - Add explicit cast in `mart_ar_summary.sql`
   - Run dbt models on demo database
   - Verify fix

### Step 2: Collection Rate Fix
1. **Run dbt models** - 30 minutes
   - Verify models are up-to-date on demo database
   - Run `int_payment_split` and `mart_production_summary`

2. **Diagnose data issues** - 1-2 hours
   - Check if payments exist
   - Check if production data exists
   - Verify date alignment

3. **Fix data generation** (if needed) - 2-4 hours
   - Update synthetic data generator
   - Regenerate demo data
   - Re-run dbt models

### Step 3: Long-Term Solution (Environment-Specific Models)
1. **Create environment-specific configurations** - 4-6 hours
   - Add `dbt_project.yml` variables for environment detection
   - Create conditional logic in models for type handling
   - Document environment differences

2. **Add data validation** - 2-3 hours
   - Create dbt tests for data type consistency
   - Add API-level validation
   - Create monitoring alerts

---

## Testing Plan

### Test 1: Patient ID Type Fix
```bash
# Test API endpoint
curl -X GET "https://api.dbtdentalclinic.com/patients/top-balances?limit=10" \
  -H "X-API-Key: your-key"

# Verify response has integer patient_id values
# Check frontend dashboard loads without errors
```

### Test 2: Collection Rate Fix
```bash
# Test dashboard KPI endpoint
curl -X GET "https://api.dbtdentalclinic.com/reports/dashboard/kpis" \
  -H "X-API-Key: your-key"

# Verify collection_rate > 0%
# Check executive dashboard displays correct value
```

### Test 3: End-to-End Verification
1. Load executive dashboard
2. Verify "Top 10 Patient Balances" component loads without errors
3. Verify Collection Rate KPI shows non-zero value
4. Verify other KPIs still work correctly

---

## Risk Assessment

### Low Risk
- **API-level type conversion:** Safe, handles both types
- **Adding explicit casts in SQL:** Safe, only affects type, not values

### Medium Risk
- **Running dbt models on demo:** May take time, but safe
- **Updating synthetic data generator:** Could affect other data, but demo only

### High Risk
- **Changing raw data types:** Could break existing queries
- **Modifying core dbt models:** Could affect production if not tested

---

## Success Criteria

✅ **Patient Balances Fix:**
- Top 10 Patient Balances component loads without errors
- API returns integer `patient_id` values
- No validation errors in frontend

✅ **Collection Rate Fix:**
- Collection rate shows > 0% on executive dashboard
- Value is reasonable (typically 80-100% for healthy practice)
- Calculation matches manual verification

✅ **Long-Term:**
- Environment-specific configurations documented
- Data type consistency tests in place
- Monitoring alerts configured

---

## Related Files

### dbt Models
- `dbt_dental_models/models/marts/mart_ar_summary.sql` - AR summary mart
- `dbt_dental_models/models/marts/mart_production_summary.sql` - Production summary with collection rate
- `dbt_dental_models/models/intermediate/system_c_payment/int_payment_split.sql` - Payment categorization
- `dbt_dental_models/models/staging/opendental/stg_opendental__patient.sql` - Patient staging
- `dbt_dental_models/macros/utils/transform_id_columns.sql` - ID transformation macro

### API Files
- `api/services/patient_service.py` - Patient service with top balances
- `api/models/patient.py` - Pydantic models
- `api/services/ar_service.py` - AR service with collection rate
- `api/routers/reports.py` - Dashboard KPI endpoints

### Frontend Files
- `frontend/src/pages/Dashboard.tsx` - Executive dashboard
- `frontend/src/types/api.ts` - TypeScript types

---

## Next Steps

1. **Immediate:** Implement API-level type conversion for patient_id (30 min)
2. **Short-term:** Run diagnostic queries to identify root causes (1-2 hours)
3. **Medium-term:** Fix root causes in dbt models (2-4 hours)
4. **Long-term:** Implement environment-specific configurations (4-6 hours)

---

## Notes

- Demo database may have different data characteristics than production
- Synthetic data generator may need updates to match production patterns
- Consider creating a data validation script to compare demo vs production data types
- Document any environment-specific differences in `docs/deployment/`
