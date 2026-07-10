-- Diagnostic Queries for mart_ar_summary Empty Results
-- Run these queries in DBeaver to identify which filter is excluding all data
-- Replace 'raw' schema with your actual schema name if different

-- ============================================================================
-- OVERVIEW: Check row counts across all related models
-- ============================================================================
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
        'fact_payment' as model_name,
        COUNT(*) as total_rows,
        COUNT(*) FILTER (WHERE payment_date >= CURRENT_DATE - INTERVAL '365 days') as eligible_rows
    FROM raw_marts.fact_payment
    
    UNION ALL
    
    SELECT 
        'mart_ar_summary' as model_name,
        COUNT(*) as total_rows,
        COUNT(*) as eligible_rows
    FROM raw_marts.mart_ar_summary
)
SELECT * FROM diagnostics
ORDER BY model_name;

-- ============================================================================
-- FILTER 1: Check int_ar_balance date range filter (18 months)
-- ============================================================================
SELECT 
    'Date Range Check' as check_type,
    COUNT(*) as total_procedures,
    COUNT(*) FILTER (WHERE procedure_date >= CURRENT_DATE - INTERVAL '18 months') as within_18_months,
    COUNT(*) FILTER (WHERE procedure_date < CURRENT_DATE - INTERVAL '18 months') as older_than_18_months,
    MIN(procedure_date) as oldest_procedure,
    MAX(procedure_date) as newest_procedure,
    CURRENT_DATE - INTERVAL '18 months' as cutoff_date
FROM raw_intermediate.int_ar_balance;

-- ============================================================================
-- FILTER 2: Check int_ar_balance include_in_ar and current_balance filters
-- ============================================================================
SELECT 
    'Balance Filter Check' as check_type,
    COUNT(*) as total_procedures,
    COUNT(*) FILTER (WHERE include_in_ar = true) as include_in_ar_true,
    COUNT(*) FILTER (WHERE include_in_ar = false) as include_in_ar_false,
    COUNT(*) FILTER (WHERE current_balance > 0) as current_balance_positive,
    COUNT(*) FILTER (WHERE current_balance <= 0) as current_balance_zero_or_negative,
    COUNT(*) FILTER (WHERE include_in_ar = true AND current_balance > 0) as eligible_for_ar_aging,
    AVG(current_balance) FILTER (WHERE current_balance > 0) as avg_positive_balance,
    SUM(current_balance) FILTER (WHERE current_balance > 0) as total_positive_balance
FROM raw_intermediate.int_ar_balance
WHERE procedure_date >= CURRENT_DATE - INTERVAL '18 months';

-- ============================================================================
-- FILTER 3: Check combined int_ar_balance filters (all conditions together)
-- ============================================================================
SELECT 
    'Combined int_ar_balance Filters' as check_type,
    COUNT(*) as total_procedures,
    COUNT(*) FILTER (WHERE include_in_ar = true 
                     AND current_balance > 0 
                     AND procedure_date >= CURRENT_DATE - INTERVAL '18 months') as passes_all_filters,
    COUNT(DISTINCT patient_id) FILTER (WHERE include_in_ar = true 
                                       AND current_balance > 0 
                                       AND procedure_date >= CURRENT_DATE - INTERVAL '18 months') as unique_patients,
    COUNT(DISTINCT provider_id) FILTER (WHERE include_in_ar = true 
                                         AND current_balance > 0 
                                         AND procedure_date >= CURRENT_DATE - INTERVAL '18 months') as unique_providers
FROM raw_intermediate.int_ar_balance;

-- ============================================================================
-- FILTER 4: Check dim_patient status and balance filters
-- ============================================================================
SELECT 
    'Patient Status Check' as check_type,
    COUNT(*) as total_patients,
    COUNT(*) FILTER (WHERE patient_status IN ('Patient', 'Inactive')) as status_eligible,
    COUNT(*) FILTER (WHERE patient_status NOT IN ('Patient', 'Inactive')) as status_not_eligible,
    COUNT(DISTINCT patient_status) as distinct_statuses,
    STRING_AGG(DISTINCT patient_status, ', ') as all_statuses
FROM raw_marts.dim_patient;

SELECT 
    'Patient Balance Check' as check_type,
    COUNT(*) as total_patients,
    COUNT(*) FILTER (WHERE total_balance > 0) as with_positive_balance,
    COUNT(*) FILTER (WHERE total_balance = 0) as with_zero_balance,
    COUNT(*) FILTER (WHERE total_balance < 0) as with_negative_balance,
    AVG(total_balance) FILTER (WHERE total_balance > 0) as avg_positive_balance,
    SUM(total_balance) FILTER (WHERE total_balance > 0) as total_positive_balance
FROM raw_marts.dim_patient
WHERE patient_status IN ('Patient', 'Inactive');

-- ============================================================================
-- FILTER 5: Check combined dim_patient filters
-- ============================================================================
SELECT 
    'Combined dim_patient Filters' as check_type,
    COUNT(*) as total_patients,
    COUNT(*) FILTER (WHERE patient_status IN ('Patient', 'Inactive') 
                     AND total_balance > 0) as passes_all_filters
FROM raw_marts.dim_patient;

-- ============================================================================
-- JOIN CHECK: Check if patients from dim_patient match with int_ar_balance
-- ============================================================================
SELECT 
    'Join Compatibility Check' as check_type,
    COUNT(DISTINCT dp.patient_id) as dim_patient_eligible_count,
    COUNT(DISTINCT ab.patient_id) as int_ar_balance_eligible_count,
    COUNT(DISTINCT CASE 
        WHEN dp.patient_id = ab.patient_id 
        THEN dp.patient_id 
    END) as matching_patients
FROM raw_marts.dim_patient dp
CROSS JOIN (
    SELECT DISTINCT patient_id
    FROM raw_intermediate.int_ar_balance
    WHERE include_in_ar = true 
      AND current_balance > 0 
      AND procedure_date >= CURRENT_DATE - INTERVAL '18 months'
) ab
WHERE dp.patient_status IN ('Patient', 'Inactive')
  AND dp.total_balance > 0;

-- ============================================================================
-- DETAILED: Sample patients that should pass filters
-- ============================================================================
-- Check a sample of patients from dim_patient that should be eligible
SELECT 
    dp.patient_id,
    dp.patient_status,
    dp.total_balance,
    dp.primary_provider_id,
    COUNT(ab.procedure_id) as eligible_procedures,
    SUM(ab.current_balance) as total_procedure_balance
FROM raw_marts.dim_patient dp
LEFT JOIN raw_intermediate.int_ar_balance ab
    ON dp.patient_id = ab.patient_id
    AND ab.include_in_ar = true
    AND ab.current_balance > 0
    AND ab.procedure_date >= CURRENT_DATE - INTERVAL '18 months'
WHERE dp.patient_status IN ('Patient', 'Inactive')
  AND dp.total_balance > 0
GROUP BY dp.patient_id, dp.patient_status, dp.total_balance, dp.primary_provider_id
ORDER BY dp.total_balance DESC
LIMIT 20;

-- ============================================================================
-- DETAILED: Sample procedures from int_ar_balance that should pass filters
-- ============================================================================
SELECT 
    ab.patient_id,
    ab.provider_id,
    ab.procedure_date,
    ab.current_balance,
    ab.include_in_ar,
    ab.aging_bucket,
    dp.patient_status,
    dp.total_balance as patient_total_balance
FROM raw_intermediate.int_ar_balance ab
LEFT JOIN raw_marts.dim_patient dp
    ON ab.patient_id = dp.patient_id
WHERE ab.include_in_ar = true
  AND ab.current_balance > 0
  AND ab.procedure_date >= CURRENT_DATE - INTERVAL '18 months'
ORDER BY ab.current_balance DESC
LIMIT 20;

-- ============================================================================
-- FINAL FILTER CHECK: Check the final WHERE clause condition
-- ============================================================================
-- This simulates the final filter in mart_ar_summary:
-- WHERE ae.total_balance != 0 OR ae.billed_last_year > 0
WITH ar_base_simulation AS (
    SELECT 
        dp.patient_id,
        dp.primary_provider_id,
        dp.total_balance,
        COALESCE(SUM(ab.current_balance), 0) as calculated_balance
    FROM raw_marts.dim_patient dp
    LEFT JOIN raw_intermediate.int_ar_balance ab
        ON dp.patient_id = ab.patient_id
        AND ab.include_in_ar = true
        AND ab.current_balance > 0
        AND ab.procedure_date >= CURRENT_DATE - INTERVAL '18 months'
    WHERE dp.patient_status IN ('Patient', 'Inactive')
      AND dp.total_balance > 0
    GROUP BY dp.patient_id, dp.primary_provider_id, dp.total_balance
),
billed_last_year_check AS (
    SELECT 
        ab.patient_id,
        COALESCE(SUM(fc.billed_amount), 0) as billed_last_year
    FROM ar_base_simulation ab
    LEFT JOIN raw_marts.fact_claim fc
        ON ab.patient_id = fc.patient_id
        AND fc.claim_date >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY ab.patient_id
)
SELECT 
    'Final Filter Check' as check_type,
    COUNT(*) as total_patients,
    COUNT(*) FILTER (WHERE ab.total_balance != 0 OR bl.billed_last_year > 0) as passes_final_filter,
    COUNT(*) FILTER (WHERE ab.total_balance = 0 AND bl.billed_last_year = 0) as fails_final_filter
FROM ar_base_simulation ab
LEFT JOIN billed_last_year_check bl
    ON ab.patient_id = bl.patient_id;

-- ============================================================================
-- PROVIDER JOIN CHECK: Verify dim_provider has matching providers
-- ============================================================================
SELECT 
    'Provider Join Check' as check_type,
    COUNT(DISTINCT dp.primary_provider_id) as dim_patient_providers,
    COUNT(DISTINCT prov.provider_id) as dim_provider_count,
    COUNT(DISTINCT CASE 
        WHEN dp.primary_provider_id = prov.provider_id 
        THEN dp.primary_provider_id 
    END) as matching_providers
FROM raw_marts.dim_patient dp
CROSS JOIN raw_marts.dim_provider prov
WHERE dp.patient_status IN ('Patient', 'Inactive')
  AND dp.total_balance > 0;

-- ============================================================================
-- DATE DIMENSION JOIN CHECK: Verify dim_date has today's date
-- ============================================================================
SELECT 
    'Date Dimension Check' as check_type,
    COUNT(*) FILTER (WHERE date_day = CURRENT_DATE) as has_today,
    MIN(date_day) as earliest_date,
    MAX(date_day) as latest_date,
    CURRENT_DATE as today
FROM raw_marts.dim_date;

-- ============================================================================
-- COMPLETE PIPELINE SIMULATION: Step-by-step through the CTEs
-- ============================================================================
-- This simulates the entire mart_ar_summary logic step by step

-- Step 1: ar_aging_from_int
WITH ar_aging_from_int AS (
    SELECT 
        ab.patient_id,
        ab.provider_id,
        SUM(CASE WHEN ab.aging_bucket = '0-30' THEN ab.current_balance ELSE 0 END) as balance_0_30_days,
        SUM(ab.current_balance) as total_balance_from_procedures
    FROM raw_intermediate.int_ar_balance ab
    WHERE ab.include_in_ar = true
      AND ab.current_balance > 0
      AND ab.procedure_date >= CURRENT_DATE - INTERVAL '18 months'
    GROUP BY ab.patient_id, ab.provider_id
)
SELECT 
    'Step 1: ar_aging_from_int' as step,
    COUNT(*) as row_count,
    COUNT(DISTINCT patient_id) as unique_patients,
    COUNT(DISTINCT provider_id) as unique_providers
FROM ar_aging_from_int;

-- Step 2: ar_base (with dim_patient join)
WITH ar_aging_from_int AS (
    SELECT 
        ab.patient_id,
        ab.provider_id,
        SUM(ab.current_balance) as total_balance_from_procedures
    FROM raw_intermediate.int_ar_balance ab
    WHERE ab.include_in_ar = true
      AND ab.current_balance > 0
      AND ab.procedure_date >= CURRENT_DATE - INTERVAL '18 months'
    GROUP BY ab.patient_id, ab.provider_id
),
ar_base AS (
    SELECT 
        dp.patient_id,
        dp.primary_provider_id,
        dp.total_balance
    FROM raw_marts.dim_patient dp
    LEFT JOIN ar_aging_from_int aai_provider
        ON dp.patient_id = aai_provider.patient_id
        AND dp.primary_provider_id = aai_provider.provider_id
    WHERE dp.patient_status IN ('Patient', 'Inactive')
      AND dp.total_balance > 0
)
SELECT 
    'Step 2: ar_base' as step,
    COUNT(*) as row_count,
    COUNT(DISTINCT patient_id) as unique_patients
FROM ar_base;

-- Step 3: Check if final join with dim_date and dim_provider would work
WITH ar_base AS (
    SELECT 
        dp.patient_id,
        dp.primary_provider_id,
        dp.total_balance
    FROM raw_marts.dim_patient dp
    WHERE dp.patient_status IN ('Patient', 'Inactive')
      AND dp.total_balance > 0
)
SELECT 
    'Step 3: Final Join Check' as step,
    COUNT(*) as ar_base_count,
    COUNT(*) FILTER (WHERE dd.date_day = CURRENT_DATE) as has_today_date,
    COUNT(*) FILTER (WHERE prov.provider_id IS NOT NULL) as has_provider_match
FROM ar_base ab
CROSS JOIN raw_marts.dim_date dd
LEFT JOIN raw_marts.dim_provider prov
    ON ab.primary_provider_id = prov.provider_id
WHERE dd.date_day = CURRENT_DATE;
