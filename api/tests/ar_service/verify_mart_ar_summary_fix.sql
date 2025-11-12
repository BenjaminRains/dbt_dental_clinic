-- Verification queries for mart_ar_summary fix
-- Run these after rebuilding mart_ar_summary to verify the aging bucket fix worked

-- =============================================================================
-- VERIFICATION 1: Are aging buckets no longer all zeros?
-- =============================================================================
SELECT 
    'VERIFICATION 1: Aging buckets check' as verification,
    COUNT(*) as total_records,
    COUNT(CASE WHEN total_balance > 0 THEN 1 END) as records_with_balance,
    COUNT(CASE WHEN balance_0_30_days > 0 THEN 1 END) as records_with_0_30_balance,
    COUNT(CASE WHEN balance_31_60_days > 0 THEN 1 END) as records_with_31_60_balance,
    COUNT(CASE WHEN balance_61_90_days > 0 THEN 1 END) as records_with_61_90_balance,
    COUNT(CASE WHEN balance_over_90_days > 0 THEN 1 END) as records_with_over_90_balance,
    SUM(CASE WHEN balance_0_30_days = 0 AND total_balance > 0 THEN 1 ELSE 0 END) as records_with_zero_0_30,
    SUM(CASE WHEN balance_over_90_days = 0 AND total_balance > 0 THEN 1 ELSE 0 END) as records_with_zero_over_90
FROM raw_marts.mart_ar_summary;

-- Expected: 
-- - records_with_0_30_balance should be > 0 (not all zeros)
-- - records_with_over_90_balance should be > 0 (not all zeros)
-- - records_with_zero_0_30 should be < records_with_balance (some should have values)
-- - records_with_zero_over_90 should be < records_with_balance (some should have values)

-- =============================================================================
-- VERIFICATION 2: Do aging buckets sum to total balance?
-- =============================================================================
SELECT 
    'VERIFICATION 2: Aging bucket sum check' as verification,
    COUNT(*) as total_records,
    COUNT(CASE 
        WHEN total_balance > 0 
        AND ABS(total_balance - (balance_0_30_days + balance_31_60_days + balance_61_90_days + balance_over_90_days)) <= 0.01
        THEN 1 
    END) as records_with_matching_buckets,
    COUNT(CASE 
        WHEN total_balance > 0 
        AND ABS(total_balance - (balance_0_30_days + balance_31_60_days + balance_61_90_days + balance_over_90_days)) > 0.01
        THEN 1 
    END) as records_with_mismatched_buckets,
    SUM(CASE 
        WHEN total_balance > 0 
        THEN ABS(total_balance - (balance_0_30_days + balance_31_60_days + balance_61_90_days + balance_over_90_days))
        ELSE 0
    END) as total_difference,
    MAX(CASE 
        WHEN total_balance > 0 
        THEN ABS(total_balance - (balance_0_30_days + balance_31_60_days + balance_61_90_days + balance_over_90_days))
        ELSE 0
    END) as max_difference
FROM raw_marts.mart_ar_summary;

-- Expected:
-- - records_with_mismatched_buckets should be 0 or very small (< 5% of records)
-- - total_difference should be close to 0
-- - max_difference should be < 1.00 (rounding errors only)

-- =============================================================================
-- VERIFICATION 3: Are insurance estimates calculated (not all zeros)?
-- =============================================================================
SELECT 
    'VERIFICATION 3: Insurance estimate check' as verification,
    COUNT(*) as total_records,
    COUNT(CASE WHEN total_balance > 0 THEN 1 END) as records_with_balance,
    COUNT(CASE WHEN insurance_estimate > 0 THEN 1 END) as records_with_insurance_estimate,
    COUNT(CASE WHEN insurance_estimate = 0 AND total_balance > 0 THEN 1 END) as records_with_zero_insurance,
    SUM(insurance_estimate) as total_insurance_estimate,
    SUM(total_balance) as total_ar,
    CASE 
        WHEN SUM(total_balance) > 0 
        THEN (SUM(insurance_estimate) / SUM(total_balance)) * 100
        ELSE 0
    END as insurance_percentage_of_total
FROM raw_marts.mart_ar_summary;

-- Expected:
-- - records_with_insurance_estimate should be > 0 (not all zeros)
-- - total_insurance_estimate should be > 0
-- - insurance_percentage_of_total should be reasonable (e.g., 20-50% for dental practice)

-- =============================================================================
-- VERIFICATION 4: Patient + Insurance = Total AR?
-- =============================================================================
SELECT 
    'VERIFICATION 4: Patient/Insurance split check' as verification,
    COUNT(*) as total_records,
    COUNT(CASE 
        WHEN total_balance > 0 
        AND ABS(total_balance - (patient_responsibility + insurance_estimate)) <= 0.01
        THEN 1 
    END) as records_with_matching_split,
    COUNT(CASE 
        WHEN total_balance > 0 
        AND ABS(total_balance - (patient_responsibility + insurance_estimate)) > 0.01
        THEN 1 
    END) as records_with_mismatched_split,
    SUM(CASE 
        WHEN total_balance > 0 
        THEN ABS(total_balance - (patient_responsibility + insurance_estimate))
        ELSE 0
    END) as total_split_difference,
    MAX(CASE 
        WHEN total_balance > 0 
        THEN ABS(total_balance - (patient_responsibility + insurance_estimate))
        ELSE 0
    END) as max_split_difference
FROM raw_marts.mart_ar_summary;

-- Expected:
-- - records_with_mismatched_split should be 0 or very small
-- - total_split_difference should be close to 0
-- - max_split_difference should be < 1.00

-- =============================================================================
-- VERIFICATION 5: Reconciliation with int_ar_balance
-- =============================================================================
WITH mart_totals AS (
    SELECT 
        COUNT(DISTINCT patient_id) as patient_count,
        SUM(total_balance) as total_ar,
        SUM(balance_0_30_days) as bucket_0_30,
        SUM(balance_31_60_days) as bucket_31_60,
        SUM(balance_61_90_days) as bucket_61_90,
        SUM(balance_over_90_days) as bucket_over_90,
        SUM(patient_responsibility) as patient_ar,
        SUM(insurance_estimate) as insurance_ar
    FROM raw_marts.mart_ar_summary
    WHERE total_balance > 0
),
int_totals AS (
    SELECT 
        COUNT(DISTINCT patient_id) as patient_count,
        SUM(current_balance) as total_ar,
        SUM(CASE WHEN aging_bucket = '0-30' THEN current_balance ELSE 0 END) as bucket_0_30,
        SUM(CASE WHEN aging_bucket = '31-60' THEN current_balance ELSE 0 END) as bucket_31_60,
        SUM(CASE WHEN aging_bucket = '61-90' THEN current_balance ELSE 0 END) as bucket_61_90,
        SUM(CASE WHEN aging_bucket = '90+' THEN current_balance ELSE 0 END) as bucket_over_90
    FROM raw_intermediate.int_ar_balance
    WHERE include_in_ar = TRUE
        AND current_balance > 0
        AND procedure_date >= CURRENT_DATE - INTERVAL '24 months'
)
SELECT 
    'VERIFICATION 5: Reconciliation check' as verification,
    m.patient_count as mart_patient_count,
    i.patient_count as int_patient_count,
    ABS(m.patient_count - i.patient_count) as patient_count_diff,
    m.total_ar as mart_total_ar,
    i.total_ar as int_total_ar,
    ABS(m.total_ar - i.total_ar) as total_ar_diff,
    CASE 
        WHEN m.total_ar > 0 
        THEN (ABS(m.total_ar - i.total_ar) / m.total_ar) * 100
        ELSE 0
    END as total_ar_diff_percent,
    m.bucket_0_30 as mart_bucket_0_30,
    i.bucket_0_30 as int_bucket_0_30,
    ABS(m.bucket_0_30 - i.bucket_0_30) as bucket_0_30_diff,
    m.bucket_over_90 as mart_bucket_over_90,
    i.bucket_over_90 as int_bucket_over_90,
    ABS(m.bucket_over_90 - i.bucket_over_90) as bucket_over_90_diff
FROM mart_totals m
CROSS JOIN int_totals i;

-- Expected:
-- - total_ar_diff_percent should be < 5% (allowing for rounding and different date ranges)
-- - patient_count_diff should be small (< 10 patients)
-- - Individual bucket differences should be reasonable

-- =============================================================================
-- VERIFICATION 6: Sample records check (look at actual data)
-- =============================================================================
SELECT 
    'VERIFICATION 6: Sample records' as verification,
    patient_id,
    provider_id,
    total_balance,
    balance_0_30_days,
    balance_31_60_days,
    balance_61_90_days,
    balance_over_90_days,
    (balance_0_30_days + balance_31_60_days + balance_61_90_days + balance_over_90_days) as sum_of_buckets,
    ABS(total_balance - (balance_0_30_days + balance_31_60_days + balance_61_90_days + balance_over_90_days)) as bucket_difference,
    patient_responsibility,
    insurance_estimate,
    (patient_responsibility + insurance_estimate) as sum_of_split,
    ABS(total_balance - (patient_responsibility + insurance_estimate)) as split_difference,
    snapshot_date
FROM raw_marts.mart_ar_summary
WHERE total_balance > 0
ORDER BY total_balance DESC
LIMIT 20;

-- Expected:
-- - All bucket_difference values should be < 0.01 (rounding only)
-- - All split_difference values should be < 0.01
-- - At least some records should have non-zero values in aging buckets
-- - At least some records should have non-zero insurance_estimate

-- =============================================================================
-- VERIFICATION 7: Percentage calculations check
-- =============================================================================
SELECT 
    'VERIFICATION 7: Percentage calculations' as verification,
    COUNT(*) as total_records,
    COUNT(CASE 
        WHEN total_balance > 0 
        AND ABS(pct_current - (balance_0_30_days / NULLIF(total_balance, 0) * 100)) > 0.01
        THEN 1 
    END) as records_with_pct_current_mismatch,
    COUNT(CASE 
        WHEN total_balance > 0 
        AND ABS(pct_over_90 - (balance_over_90_days / NULLIF(total_balance, 0) * 100)) > 0.01
        THEN 1 
    END) as records_with_pct_over_90_mismatch,
    MAX(CASE 
        WHEN total_balance > 0 
        THEN ABS(pct_current - (balance_0_30_days / NULLIF(total_balance, 0) * 100))
        ELSE 0
    END) as max_pct_current_diff,
    MAX(CASE 
        WHEN total_balance > 0 
        THEN ABS(pct_over_90 - (balance_over_90_days / NULLIF(total_balance, 0) * 100))
        ELSE 0
    END) as max_pct_over_90_diff
FROM raw_marts.mart_ar_summary;

-- Expected:
-- - records_with_pct_current_mismatch should be 0
-- - records_with_pct_over_90_mismatch should be 0
-- - max differences should be < 0.01

