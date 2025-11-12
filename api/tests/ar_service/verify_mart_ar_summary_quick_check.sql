-- Quick verification check for mart_ar_summary fix
-- Run this first to get a quick overview

SELECT 
    'Quick Verification Summary' as check_type,
    -- Totals
    COUNT(*) as total_records,
    COUNT(CASE WHEN total_balance > 0 THEN 1 END) as records_with_balance,
    
    -- Aging buckets status
    COUNT(CASE WHEN balance_0_30_days > 0 THEN 1 END) as has_0_30_balance,
    COUNT(CASE WHEN balance_over_90_days > 0 THEN 1 END) as has_over_90_balance,
    
    -- Sum checks
    COUNT(CASE 
        WHEN total_balance > 0 
        AND ABS(total_balance - (balance_0_30_days + balance_31_60_days + balance_61_90_days + balance_over_90_days)) <= 0.01
        THEN 1 
    END) as buckets_match_total,
    COUNT(CASE 
        WHEN total_balance > 0 
        AND ABS(total_balance - (patient_responsibility + insurance_estimate)) <= 0.01
        THEN 1 
    END) as split_matches_total,
    
    -- Amounts
    SUM(total_balance) as total_ar,
    SUM(balance_0_30_days) as total_0_30,
    SUM(balance_over_90_days) as total_over_90,
    SUM(insurance_estimate) as total_insurance,
    
    -- Percentages
    CASE 
        WHEN SUM(total_balance) > 0 
        THEN (SUM(balance_0_30_days) / SUM(total_balance)) * 100
        ELSE 0
    END as pct_current,
    CASE 
        WHEN SUM(total_balance) > 0 
        THEN (SUM(balance_over_90_days) / SUM(total_balance)) * 100
        ELSE 0
    END as pct_over_90,
    CASE 
        WHEN SUM(total_balance) > 0 
        THEN (SUM(insurance_estimate) / SUM(total_balance)) * 100
        ELSE 0
    END as pct_insurance
    
FROM raw_marts.mart_ar_summary
WHERE total_balance > 0;

