-- Investigation Queries for Hygiene Retention Category Mappings
-- Date: 2026-01-23
-- Purpose: Investigate hygiene_status, retention_category, and patient_risk_category
--          to verify all scenarios are properly handled
-- Related: validation/VALIDATION_TEMPLATE.md

-- ============================================================================
-- Query 1: Hygiene Status Analysis
-- ============================================================================

-- Query 1.1: Hygiene Status distribution
SELECT 
    'Hygiene Status - Distribution' as investigation_type,
    hygiene_status,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients,
    COUNT(DISTINCT date_id) as distinct_dates,
    MIN(date_id) as earliest_date,
    MAX(date_id) as latest_date
FROM marts.mart_hygiene_retention
GROUP BY hygiene_status
ORDER BY record_count DESC;

-- Query 1.2: Hygiene Status breakdown by last_hygiene_date and days_since_last_hygiene
SELECT 
    'Hygiene Status - Date Breakdown' as analysis,
    hygiene_status,
    COUNT(*) as record_count,
    COUNT(*) FILTER (WHERE last_hygiene_date IS NULL) as null_last_hygiene_date,
    COUNT(*) FILTER (WHERE last_hygiene_date IS NOT NULL) as non_null_last_hygiene_date,
    AVG(days_since_last_hygiene) as avg_days_since_hygiene,
    MIN(days_since_last_hygiene) as min_days_since_hygiene,
    MAX(days_since_last_hygiene) as max_days_since_hygiene,
    COUNT(*) FILTER (WHERE days_since_last_hygiene <= 180) as days_0_180,
    COUNT(*) FILTER (WHERE days_since_last_hygiene BETWEEN 181 AND 270) as days_181_270,
    COUNT(*) FILTER (WHERE days_since_last_hygiene BETWEEN 271 AND 365) as days_271_365,
    COUNT(*) FILTER (WHERE days_since_last_hygiene > 365) as days_over_365
FROM marts.mart_hygiene_retention
GROUP BY hygiene_status
ORDER BY hygiene_status;

-- Query 1.3: 'Lapsed' category detailed analysis
SELECT 
    'Hygiene Status - Lapsed Category Analysis' as investigation_type,
    COUNT(*) as total_lapsed_records,
    COUNT(DISTINCT patient_id) as distinct_lapsed_patients,
    COUNT(*) FILTER (WHERE last_hygiene_date IS NULL) as null_last_hygiene_date,
    COUNT(*) FILTER (WHERE last_hygiene_date IS NOT NULL) as non_null_last_hygiene_date,
    AVG(days_since_last_hygiene) as avg_days_since_hygiene,
    MIN(days_since_last_hygiene) as min_days_since_hygiene,
    MAX(days_since_last_hygiene) as max_days_since_hygiene,
    COUNT(*) FILTER (WHERE days_since_last_hygiene <= 365) as days_365_or_less,
    COUNT(*) FILTER (WHERE days_since_last_hygiene > 365) as days_over_365,
    COUNT(*) FILTER (WHERE last_hygiene_date >= current_date - interval '12 months') as last_hygiene_within_12m
FROM marts.mart_hygiene_retention
WHERE hygiene_status = 'Lapsed';

-- Query 1.4: Sample records for 'Lapsed' category
SELECT 
    'Hygiene Status - Lapsed Samples' as analysis,
    patient_id,
    date_id,
    hygiene_status,
    last_hygiene_date,
    days_since_last_hygiene,
    total_hygiene_appointments,
    hygiene_visits_last_year,
    CASE 
        WHEN last_hygiene_date IS NULL THEN 'NULL Last Hygiene Date'
        WHEN last_hygiene_date >= current_date - interval '12 months' THEN 'Last Hygiene Within 12 Months'
        WHEN days_since_last_hygiene <= 365 THEN 'Days <= 365'
        ELSE 'Days > 365'
    END as issue_type
FROM marts.mart_hygiene_retention
WHERE hygiene_status = 'Lapsed'
ORDER BY days_since_last_hygiene DESC NULLS LAST, patient_id
LIMIT 30;

-- Query 1.5: Edge cases - potential misclassifications
SELECT 
    'Hygiene Status - Potential Misclassifications' as analysis,
    hygiene_status,
    CASE 
        WHEN last_hygiene_date >= current_date - interval '6 months' AND hygiene_status != 'Current' THEN 'Last hygiene within 6m but not Current'
        WHEN last_hygiene_date >= current_date - interval '9 months' AND last_hygiene_date < current_date - interval '6 months' AND hygiene_status != 'Due' THEN 'Last hygiene 6-9m but not Due'
        WHEN last_hygiene_date >= current_date - interval '12 months' AND last_hygiene_date < current_date - interval '9 months' AND hygiene_status != 'Overdue' THEN 'Last hygiene 9-12m but not Overdue'
        WHEN last_hygiene_date < current_date - interval '12 months' AND hygiene_status != 'Lapsed' THEN 'Last hygiene >12m but not Lapsed'
        WHEN last_hygiene_date IS NULL AND hygiene_status != 'Lapsed' THEN 'NULL last hygiene but not Lapsed'
        ELSE 'OK'
    END as potential_issue,
    COUNT(*) as record_count
FROM marts.mart_hygiene_retention
WHERE (
    (last_hygiene_date >= current_date - interval '6 months' AND hygiene_status != 'Current')
    OR (last_hygiene_date >= current_date - interval '9 months' AND last_hygiene_date < current_date - interval '6 months' AND hygiene_status != 'Due')
    OR (last_hygiene_date >= current_date - interval '12 months' AND last_hygiene_date < current_date - interval '9 months' AND hygiene_status != 'Overdue')
    OR (last_hygiene_date < current_date - interval '12 months' AND hygiene_status != 'Lapsed')
    OR (last_hygiene_date IS NULL AND hygiene_status != 'Lapsed')
)
GROUP BY hygiene_status,
    CASE 
        WHEN last_hygiene_date >= current_date - interval '6 months' AND hygiene_status != 'Current' THEN 'Last hygiene within 6m but not Current'
        WHEN last_hygiene_date >= current_date - interval '9 months' AND last_hygiene_date < current_date - interval '6 months' AND hygiene_status != 'Due' THEN 'Last hygiene 6-9m but not Due'
        WHEN last_hygiene_date >= current_date - interval '12 months' AND last_hygiene_date < current_date - interval '9 months' AND hygiene_status != 'Overdue' THEN 'Last hygiene 9-12m but not Overdue'
        WHEN last_hygiene_date < current_date - interval '12 months' AND hygiene_status != 'Lapsed' THEN 'Last hygiene >12m but not Lapsed'
        WHEN last_hygiene_date IS NULL AND hygiene_status != 'Lapsed' THEN 'NULL last hygiene but not Lapsed'
        ELSE 'OK'
    END
ORDER BY record_count DESC;

-- ============================================================================
-- Query 2: Retention Category Analysis
-- ============================================================================

-- Query 2.1: Retention Category distribution
SELECT 
    'Retention Category - Distribution' as investigation_type,
    retention_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients,
    AVG(hygiene_visits_last_year) as avg_visits_last_year,
    AVG(regular_interval_percentage) as avg_regular_interval_pct,
    MIN(hygiene_visits_last_year) as min_visits_last_year,
    MAX(hygiene_visits_last_year) as max_visits_last_year
FROM marts.mart_hygiene_retention
GROUP BY retention_category
ORDER BY record_count DESC;

-- Query 2.2: Retention Category breakdown by key factors
SELECT 
    'Retention Category - Factor Breakdown' as analysis,
    retention_category,
    COUNT(*) as record_count,
    AVG(hygiene_visits_last_year) as avg_visits_last_year,
    COUNT(*) FILTER (WHERE hygiene_visits_last_year >= 2) as visits_2_or_more,
    COUNT(*) FILTER (WHERE hygiene_visits_last_year = 1) as visits_1,
    COUNT(*) FILTER (WHERE hygiene_visits_last_year = 0) as visits_0,
    AVG(regular_interval_percentage) as avg_regular_interval_pct,
    COUNT(*) FILTER (WHERE regular_interval_percentage >= 80) as interval_pct_80_plus,
    COUNT(*) FILTER (WHERE regular_interval_percentage >= 60 AND regular_interval_percentage < 80) as interval_pct_60_79,
    COUNT(*) FILTER (WHERE regular_interval_percentage < 60) as interval_pct_under_60,
    COUNT(*) FILTER (WHERE regular_interval_percentage IS NULL) as null_interval_pct
FROM marts.mart_hygiene_retention
GROUP BY retention_category
ORDER BY retention_category;

-- Query 2.3: Retention Category edge cases
SELECT 
    'Retention Category - Edge Cases' as investigation_type,
    retention_category,
    CASE 
        WHEN hygiene_visits_last_year >= 2 AND regular_interval_percentage >= 80 AND retention_category != 'Excellent' THEN '2+ visits + 80%+ interval but not Excellent'
        WHEN hygiene_visits_last_year >= 2 AND regular_interval_percentage >= 60 AND regular_interval_percentage < 80 AND retention_category != 'Good' THEN '2+ visits + 60-79% interval but not Good'
        WHEN hygiene_visits_last_year >= 1 AND retention_category NOT IN ('Excellent', 'Good', 'Fair') THEN '1+ visits but not Excellent/Good/Fair'
        WHEN hygiene_visits_last_year < 1 AND retention_category != 'Poor' THEN '<1 visits but not Poor'
        ELSE 'OK'
    END as potential_issue,
    COUNT(*) as record_count
FROM marts.mart_hygiene_retention
WHERE (
    (hygiene_visits_last_year >= 2 AND regular_interval_percentage >= 80 AND retention_category != 'Excellent')
    OR (hygiene_visits_last_year >= 2 AND regular_interval_percentage >= 60 AND regular_interval_percentage < 80 AND retention_category != 'Good')
    OR (hygiene_visits_last_year >= 1 AND retention_category NOT IN ('Excellent', 'Good', 'Fair'))
    OR (hygiene_visits_last_year < 1 AND retention_category != 'Poor')
)
GROUP BY retention_category,
    CASE 
        WHEN hygiene_visits_last_year >= 2 AND regular_interval_percentage >= 80 AND retention_category != 'Excellent' THEN '2+ visits + 80%+ interval but not Excellent'
        WHEN hygiene_visits_last_year >= 2 AND regular_interval_percentage >= 60 AND regular_interval_percentage < 80 AND retention_category != 'Good' THEN '2+ visits + 60-79% interval but not Good'
        WHEN hygiene_visits_last_year >= 1 AND retention_category NOT IN ('Excellent', 'Good', 'Fair') THEN '1+ visits but not Excellent/Good/Fair'
        WHEN hygiene_visits_last_year < 1 AND retention_category != 'Poor' THEN '<1 visits but not Poor'
        ELSE 'OK'
    END
ORDER BY record_count DESC;

-- Query 2.4: Sample records for each retention category
SELECT 
    'Retention Category - Samples' as analysis,
    patient_id,
    date_id,
    retention_category,
    hygiene_visits_last_year,
    regular_interval_percentage,
    total_hygiene_appointments,
    last_hygiene_date
FROM marts.mart_hygiene_retention
WHERE retention_category = 'Poor'
ORDER BY hygiene_visits_last_year, regular_interval_percentage NULLS LAST, patient_id
LIMIT 30;

-- ============================================================================
-- Query 3: Patient Risk Category Analysis
-- ============================================================================

-- Query 3.1: Patient Risk Category distribution
SELECT 
    'Patient Risk Category - Distribution' as investigation_type,
    patient_risk_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients,
    AVG(days_since_last_hygiene) as avg_days_since_hygiene,
    AVG(hygiene_no_show_rate) as avg_no_show_rate,
    AVG(avg_hygiene_interval_days) as avg_interval_days
FROM marts.mart_hygiene_retention
GROUP BY patient_risk_category
ORDER BY record_count DESC;

-- Query 3.2: Patient Risk Category breakdown by key factors
SELECT 
    'Patient Risk Category - Factor Breakdown' as analysis,
    patient_risk_category,
    COUNT(*) as record_count,
    COUNT(*) FILTER (WHERE last_hygiene_date < current_date - interval '12 months') as last_hygiene_over_12m,
    COUNT(*) FILTER (WHERE hygiene_no_show_rate > 20) as no_show_rate_over_20,
    COUNT(*) FILTER (WHERE avg_hygiene_interval_days > 240) as interval_over_240,
    AVG(days_since_last_hygiene) as avg_days_since_hygiene,
    AVG(hygiene_no_show_rate) as avg_no_show_rate,
    AVG(avg_hygiene_interval_days) as avg_interval_days,
    COUNT(*) FILTER (WHERE last_hygiene_date IS NULL) as null_last_hygiene_date
FROM marts.mart_hygiene_retention
GROUP BY patient_risk_category
ORDER BY patient_risk_category;

-- Query 3.3: Patient Risk Category edge cases
SELECT 
    'Patient Risk Category - Edge Cases' as investigation_type,
    patient_risk_category,
    CASE 
        WHEN last_hygiene_date < current_date - interval '12 months' AND patient_risk_category != 'High Risk' THEN 'Last hygiene >12m but not High Risk'
        WHEN hygiene_no_show_rate > 20 AND patient_risk_category != 'Medium Risk' AND last_hygiene_date >= current_date - interval '12 months' THEN 'No-show >20% but not Medium Risk'
        WHEN avg_hygiene_interval_days > 240 AND patient_risk_category != 'Medium Risk' AND last_hygiene_date >= current_date - interval '12 months' THEN 'Interval >240 but not Medium Risk'
        WHEN last_hygiene_date >= current_date - interval '12 months' AND hygiene_no_show_rate <= 20 AND avg_hygiene_interval_days <= 240 AND patient_risk_category != 'Low Risk' THEN 'Good metrics but not Low Risk'
        ELSE 'OK'
    END as potential_issue,
    COUNT(*) as record_count
FROM marts.mart_hygiene_retention
WHERE (
    (last_hygiene_date < current_date - interval '12 months' AND patient_risk_category != 'High Risk')
    OR (hygiene_no_show_rate > 20 AND patient_risk_category != 'Medium Risk' AND last_hygiene_date >= current_date - interval '12 months')
    OR (avg_hygiene_interval_days > 240 AND patient_risk_category != 'Medium Risk' AND last_hygiene_date >= current_date - interval '12 months')
    OR (last_hygiene_date >= current_date - interval '12 months' AND hygiene_no_show_rate <= 20 AND avg_hygiene_interval_days <= 240 AND patient_risk_category != 'Low Risk')
)
GROUP BY patient_risk_category,
    CASE 
        WHEN last_hygiene_date < current_date - interval '12 months' AND patient_risk_category != 'High Risk' THEN 'Last hygiene >12m but not High Risk'
        WHEN hygiene_no_show_rate > 20 AND patient_risk_category != 'Medium Risk' AND last_hygiene_date >= current_date - interval '12 months' THEN 'No-show >20% but not Medium Risk'
        WHEN avg_hygiene_interval_days > 240 AND patient_risk_category != 'Medium Risk' AND last_hygiene_date >= current_date - interval '12 months' THEN 'Interval >240 but not Medium Risk'
        WHEN last_hygiene_date >= current_date - interval '12 months' AND hygiene_no_show_rate <= 20 AND avg_hygiene_interval_days <= 240 AND patient_risk_category != 'Low Risk' THEN 'Good metrics but not Low Risk'
        ELSE 'OK'
    END
ORDER BY record_count DESC;

-- Query 3.4: Sample records for each patient risk category
SELECT 
    'Patient Risk Category - Samples' as analysis,
    patient_id,
    date_id,
    patient_risk_category,
    last_hygiene_date,
    days_since_last_hygiene,
    hygiene_no_show_rate,
    avg_hygiene_interval_days
FROM marts.mart_hygiene_retention
WHERE patient_risk_category = 'High Risk'
ORDER BY days_since_last_hygiene DESC NULLS LAST, patient_id
LIMIT 30;

-- ============================================================================
-- Query 4: Combined Analysis
-- ============================================================================

-- Query 4.1: Cross-tabulation of hygiene_status and retention_category
SELECT 
    'Combined - Hygiene Status vs Retention Category' as analysis,
    hygiene_status,
    retention_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM marts.mart_hygiene_retention
GROUP BY hygiene_status, retention_category
ORDER BY hygiene_status, retention_category;

-- Query 4.2: Cross-tabulation of hygiene_status and patient_risk_category
SELECT 
    'Combined - Hygiene Status vs Patient Risk' as analysis,
    hygiene_status,
    patient_risk_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM marts.mart_hygiene_retention
GROUP BY hygiene_status, patient_risk_category
ORDER BY hygiene_status, patient_risk_category;

-- Query 4.3: Patients with unexpected combinations
SELECT 
    'Combined - Unexpected Combinations' as analysis,
    hygiene_status,
    retention_category,
    patient_risk_category,
    COUNT(*) as record_count
FROM marts.mart_hygiene_retention
WHERE (
    (hygiene_status = 'Current' AND patient_risk_category = 'High Risk')
    OR (hygiene_status = 'Lapsed' AND patient_risk_category = 'Low Risk')
    OR (hygiene_status = 'Current' AND retention_category = 'Poor')
)
GROUP BY hygiene_status, retention_category, patient_risk_category
ORDER BY record_count DESC;

-- ============================================================================
-- Query 5: Summary Statistics
-- ============================================================================

-- Query 5.1: Overall summary for all three fields
SELECT 
    'Hygiene Categories - Summary Statistics' as analysis,
    COUNT(*) as total_records,
    COUNT(DISTINCT patient_id) as distinct_patients,
    COUNT(DISTINCT date_id) as distinct_dates,
    COUNT(*) FILTER (WHERE hygiene_status = 'Lapsed') as lapsed_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE hygiene_status = 'Lapsed') / COUNT(*), 2) as lapsed_percentage,
    COUNT(*) FILTER (WHERE retention_category = 'Poor') as poor_retention_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE retention_category = 'Poor') / COUNT(*), 2) as poor_retention_percentage,
    COUNT(*) FILTER (WHERE patient_risk_category = 'High Risk') as high_risk_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE patient_risk_category = 'High Risk') / COUNT(*), 2) as high_risk_percentage
FROM marts.mart_hygiene_retention;

-- Query 5.2: NULL value check for key fields
SELECT 
    'Hygiene Categories - NULL Value Check' as analysis,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE hygiene_status IS NULL) as null_hygiene_status,
    COUNT(*) FILTER (WHERE retention_category IS NULL) as null_retention_category,
    COUNT(*) FILTER (WHERE patient_risk_category IS NULL) as null_patient_risk,
    COUNT(*) FILTER (WHERE last_hygiene_date IS NULL) as null_last_hygiene_date,
    COUNT(*) FILTER (WHERE hygiene_visits_last_year IS NULL) as null_visits_last_year,
    COUNT(*) FILTER (WHERE regular_interval_percentage IS NULL) as null_interval_percentage,
    COUNT(*) FILTER (WHERE hygiene_no_show_rate IS NULL) as null_no_show_rate,
    COUNT(*) FILTER (WHERE avg_hygiene_interval_days IS NULL) as null_avg_interval
FROM marts.mart_hygiene_retention;
