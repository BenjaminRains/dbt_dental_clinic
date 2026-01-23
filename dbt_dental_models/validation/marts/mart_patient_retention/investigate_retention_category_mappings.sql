-- Investigation Queries for Patient Retention Category Mappings
-- Date: 2026-01-23
-- Purpose: Investigate retention_status, churn_risk_category, and patient_value_category
--          to verify all scenarios are properly handled
-- Related: validation/VALIDATION_TEMPLATE.md

-- ============================================================================
-- Query 1: Retention Status Analysis
-- ============================================================================

-- Query 1.1: Retention Status distribution
SELECT 
    'Retention Status - Distribution' as investigation_type,
    retention_status,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients,
    COUNT(DISTINCT date_id) as distinct_dates,
    MIN(date_id) as earliest_date,
    MAX(date_id) as latest_date
FROM marts.mart_patient_retention
GROUP BY retention_status
ORDER BY record_count DESC;

-- Query 1.2: Retention Status breakdown by appointment activity fields
SELECT 
    'Retention Status - Activity Breakdown' as analysis,
    retention_status,
    COUNT(*) as record_count,
    AVG(appointments_last_30_days) as avg_appts_30d,
    AVG(appointments_last_90_days) as avg_appts_90d,
    AVG(appointments_last_6_months) as avg_appts_6m,
    AVG(appointments_last_year) as avg_appts_1y,
    AVG(appointments_last_2_years) as avg_appts_2y,
    COUNT(*) FILTER (WHERE last_appointment_date > current_date) as has_future_appt,
    COUNT(*) FILTER (WHERE last_appointment_date IS NULL) as null_last_appt_date
FROM marts.mart_patient_retention
GROUP BY retention_status
ORDER BY retention_status;

-- Query 1.3: 'Lost' category detailed analysis
SELECT 
    'Retention Status - Lost Category Analysis' as investigation_type,
    COUNT(*) as total_lost_records,
    COUNT(DISTINCT patient_id) as distinct_lost_patients,
    COUNT(*) FILTER (WHERE appointments_last_30_days > 0) as has_appts_30d,
    COUNT(*) FILTER (WHERE appointments_last_90_days > 0) as has_appts_90d,
    COUNT(*) FILTER (WHERE appointments_last_6_months > 0) as has_appts_6m,
    COUNT(*) FILTER (WHERE appointments_last_year > 0) as has_appts_1y,
    COUNT(*) FILTER (WHERE appointments_last_2_years > 0) as has_appts_2y,
    COUNT(*) FILTER (WHERE last_appointment_date > current_date) as has_future_appt,
    COUNT(*) FILTER (WHERE last_appointment_date IS NULL) as null_last_appt_date,
    AVG(days_since_last_visit) as avg_days_since_visit,
    MIN(days_since_last_visit) as min_days_since_visit,
    MAX(days_since_last_visit) as max_days_since_visit
FROM marts.mart_patient_retention
WHERE retention_status = 'Lost';

-- Query 1.4: Sample records for 'Lost' category
SELECT 
    'Retention Status - Lost Samples' as analysis,
    patient_id,
    date_id,
    retention_status,
    appointments_last_30_days,
    appointments_last_90_days,
    appointments_last_6_months,
    appointments_last_year,
    appointments_last_2_years,
    last_appointment_date,
    days_since_last_visit,
    CASE 
        WHEN last_appointment_date > current_date THEN 'Future Appointment'
        WHEN last_appointment_date IS NULL THEN 'NULL Last Appointment'
        WHEN appointments_last_2_years > 0 THEN 'Has Appointments in 2 Years'
        ELSE 'No Recent Activity'
    END as issue_type
FROM marts.mart_patient_retention
WHERE retention_status = 'Lost'
ORDER BY days_since_last_visit DESC NULLS LAST, patient_id
LIMIT 30;

-- Query 1.5: Edge cases - patients that might be misclassified
SELECT 
    'Retention Status - Potential Misclassifications' as analysis,
    retention_status,
    CASE 
        WHEN appointments_last_30_days > 0 AND retention_status != 'Active' THEN 'Has 30d appts but not Active'
        WHEN appointments_last_90_days > 0 AND retention_status NOT IN ('Active', 'Recent') THEN 'Has 90d appts but not Active/Recent'
        WHEN appointments_last_6_months > 0 AND retention_status NOT IN ('Active', 'Recent', 'Moderate') THEN 'Has 6m appts but not Active/Recent/Moderate'
        WHEN appointments_last_year > 0 AND retention_status NOT IN ('Active', 'Recent', 'Moderate', 'Dormant') THEN 'Has 1y appts but not Active/Recent/Moderate/Dormant'
        WHEN appointments_last_2_years > 0 AND retention_status NOT IN ('Active', 'Recent', 'Moderate', 'Dormant', 'Inactive') THEN 'Has 2y appts but not Active/Recent/Moderate/Dormant/Inactive'
        WHEN last_appointment_date > current_date AND retention_status != 'Scheduled' THEN 'Has future appt but not Scheduled'
        ELSE 'OK'
    END as potential_issue,
    COUNT(*) as record_count
FROM marts.mart_patient_retention
WHERE (
    (appointments_last_30_days > 0 AND retention_status != 'Active')
    OR (appointments_last_90_days > 0 AND retention_status NOT IN ('Active', 'Recent'))
    OR (appointments_last_6_months > 0 AND retention_status NOT IN ('Active', 'Recent', 'Moderate'))
    OR (appointments_last_year > 0 AND retention_status NOT IN ('Active', 'Recent', 'Moderate', 'Dormant'))
    OR (appointments_last_2_years > 0 AND retention_status NOT IN ('Active', 'Recent', 'Moderate', 'Dormant', 'Inactive'))
    OR (last_appointment_date > current_date AND retention_status != 'Scheduled')
)
GROUP BY retention_status,
    CASE 
        WHEN appointments_last_30_days > 0 AND retention_status != 'Active' THEN 'Has 30d appts but not Active'
        WHEN appointments_last_90_days > 0 AND retention_status NOT IN ('Active', 'Recent') THEN 'Has 90d appts but not Active/Recent'
        WHEN appointments_last_6_months > 0 AND retention_status NOT IN ('Active', 'Recent', 'Moderate') THEN 'Has 6m appts but not Active/Recent/Moderate'
        WHEN appointments_last_year > 0 AND retention_status NOT IN ('Active', 'Recent', 'Moderate', 'Dormant') THEN 'Has 1y appts but not Active/Recent/Moderate/Dormant'
        WHEN appointments_last_2_years > 0 AND retention_status NOT IN ('Active', 'Recent', 'Moderate', 'Dormant', 'Inactive') THEN 'Has 2y appts but not Active/Recent/Moderate/Dormant/Inactive'
        WHEN last_appointment_date > current_date AND retention_status != 'Scheduled' THEN 'Has future appt but not Scheduled'
        ELSE 'OK'
    END
ORDER BY record_count DESC;

-- ============================================================================
-- Query 2: Churn Risk Category Analysis
-- ============================================================================

-- Query 2.1: Churn Risk Category distribution
SELECT 
    'Churn Risk Category - Distribution' as investigation_type,
    churn_risk_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients,
    AVG(churn_risk_score) as avg_churn_score,
    MIN(churn_risk_score) as min_churn_score,
    MAX(churn_risk_score) as max_churn_score
FROM marts.mart_patient_retention
GROUP BY churn_risk_category
ORDER BY record_count DESC;

-- Query 2.2: Churn Risk Category breakdown by key factors
SELECT 
    'Churn Risk Category - Factor Breakdown' as analysis,
    churn_risk_category,
    COUNT(*) as record_count,
    AVG(days_since_last_visit) as avg_days_since_visit,
    COUNT(*) FILTER (WHERE days_since_last_visit IS NULL) as null_days_since_visit,
    COUNT(*) FILTER (WHERE days_since_last_visit > 365) as days_over_365,
    COUNT(*) FILTER (WHERE days_since_last_visit > 180) as days_over_180,
    COUNT(*) FILTER (WHERE days_since_last_visit > 120) as days_over_120,
    COUNT(*) FILTER (WHERE days_since_last_visit > 90) as days_over_90,
    AVG(appointments_last_year) as avg_appts_last_year,
    COUNT(*) FILTER (WHERE appointments_last_year = 0) as zero_appts_last_year,
    COUNT(*) FILTER (WHERE appointments_last_year < 2) as appts_less_than_2,
    AVG(no_show_appointments::numeric / NULLIF(total_appointments, 0)) as avg_no_show_rate,
    COUNT(*) FILTER (WHERE last_appointment_date > current_date) as has_future_appt
FROM marts.mart_patient_retention
GROUP BY churn_risk_category
ORDER BY churn_risk_category;

-- Query 2.3: Churn Risk Category edge cases
SELECT 
    'Churn Risk Category - Edge Cases' as investigation_type,
    churn_risk_category,
    CASE 
        WHEN days_since_last_visit IS NULL AND last_appointment_date > current_date AND churn_risk_category != 'Low Risk' THEN 'NULL days + future appt but not Low Risk'
        WHEN days_since_last_visit > 365 AND churn_risk_category != 'High Risk' THEN 'Days > 365 but not High Risk'
        WHEN days_since_last_visit > 180 AND appointments_last_year = 0 AND churn_risk_category != 'High Risk' THEN 'Days > 180 + no appts but not High Risk'
        WHEN days_since_last_visit > 120 AND no_show_appointments::numeric / NULLIF(total_appointments, 0) > 0.3 AND churn_risk_category != 'Medium Risk' THEN 'Days > 120 + high no-show but not Medium Risk'
        WHEN days_since_last_visit > 90 AND churn_risk_category NOT IN ('Medium Risk', 'High Risk') THEN 'Days > 90 but not Medium/High Risk'
        WHEN appointments_last_year < 2 AND avg_days_between_appointments > 180 AND churn_risk_category != 'Medium Risk' THEN 'Low appts + long interval but not Medium Risk'
        ELSE 'OK'
    END as potential_issue,
    COUNT(*) as record_count
FROM marts.mart_patient_retention
WHERE (
    (days_since_last_visit IS NULL AND last_appointment_date > current_date AND churn_risk_category != 'Low Risk')
    OR (days_since_last_visit > 365 AND churn_risk_category != 'High Risk')
    OR (days_since_last_visit > 180 AND appointments_last_year = 0 AND churn_risk_category != 'High Risk')
    OR (days_since_last_visit > 120 AND no_show_appointments::numeric / NULLIF(total_appointments, 0) > 0.3 AND churn_risk_category != 'Medium Risk')
    OR (days_since_last_visit > 90 AND churn_risk_category NOT IN ('Medium Risk', 'High Risk'))
    OR (appointments_last_year < 2 AND avg_days_between_appointments > 180 AND churn_risk_category != 'Medium Risk')
)
GROUP BY churn_risk_category,
    CASE 
        WHEN days_since_last_visit IS NULL AND last_appointment_date > current_date AND churn_risk_category != 'Low Risk' THEN 'NULL days + future appt but not Low Risk'
        WHEN days_since_last_visit > 365 AND churn_risk_category != 'High Risk' THEN 'Days > 365 but not High Risk'
        WHEN days_since_last_visit > 180 AND appointments_last_year = 0 AND churn_risk_category != 'High Risk' THEN 'Days > 180 + no appts but not High Risk'
        WHEN days_since_last_visit > 120 AND no_show_appointments::numeric / NULLIF(total_appointments, 0) > 0.3 AND churn_risk_category != 'Medium Risk' THEN 'Days > 120 + high no-show but not Medium Risk'
        WHEN days_since_last_visit > 90 AND churn_risk_category NOT IN ('Medium Risk', 'High Risk') THEN 'Days > 90 but not Medium/High Risk'
        WHEN appointments_last_year < 2 AND avg_days_between_appointments > 180 AND churn_risk_category != 'Medium Risk' THEN 'Low appts + long interval but not Medium Risk'
        ELSE 'OK'
    END
ORDER BY record_count DESC;

-- Query 2.4: Sample records for each churn risk category
SELECT 
    'Churn Risk Category - Samples' as analysis,
    patient_id,
    date_id,
    churn_risk_category,
    churn_risk_score,
    days_since_last_visit,
    appointments_last_year,
    no_show_appointments,
    total_appointments,
    CASE 
        WHEN total_appointments > 0 THEN ROUND((no_show_appointments::numeric / total_appointments * 100), 2)
        ELSE NULL
    END as no_show_rate_pct,
    avg_days_between_appointments,
    last_appointment_date
FROM marts.mart_patient_retention
WHERE churn_risk_category = 'High Risk'
ORDER BY days_since_last_visit DESC NULLS LAST, patient_id
LIMIT 30;

-- ============================================================================
-- Query 3: Patient Value Category Analysis
-- ============================================================================

-- Query 3.1: Patient Value Category distribution
SELECT 
    'Patient Value Category - Distribution' as investigation_type,
    patient_value_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients,
    AVG(lifetime_production) as avg_lifetime_production,
    MIN(lifetime_production) as min_lifetime_production,
    MAX(lifetime_production) as max_lifetime_production,
    COUNT(*) FILTER (WHERE lifetime_production IS NULL) as null_lifetime_production
FROM marts.mart_patient_retention
GROUP BY patient_value_category
ORDER BY record_count DESC;

-- Query 3.2: Patient Value Category breakdown by lifetime_production ranges
SELECT 
    'Patient Value Category - Production Range Breakdown' as analysis,
    patient_value_category,
    CASE 
        WHEN lifetime_production IS NULL THEN 'NULL'
        WHEN lifetime_production = 0 THEN 'Zero'
        WHEN lifetime_production < 500 THEN '0-499'
        WHEN lifetime_production < 2000 THEN '500-1999'
        WHEN lifetime_production < 5000 THEN '2000-4999'
        WHEN lifetime_production >= 5000 THEN '5000+'
        ELSE 'Other'
    END as production_range,
    COUNT(*) as record_count,
    AVG(lifetime_production) as avg_production,
    MIN(lifetime_production) as min_production,
    MAX(lifetime_production) as max_production
FROM marts.mart_patient_retention
GROUP BY patient_value_category,
    CASE 
        WHEN lifetime_production IS NULL THEN 'NULL'
        WHEN lifetime_production = 0 THEN 'Zero'
        WHEN lifetime_production < 500 THEN '0-499'
        WHEN lifetime_production < 2000 THEN '500-1999'
        WHEN lifetime_production < 5000 THEN '2000-4999'
        WHEN lifetime_production >= 5000 THEN '5000+'
        ELSE 'Other'
    END
ORDER BY patient_value_category, production_range;

-- Query 3.3: Patient Value Category edge cases
SELECT 
    'Patient Value Category - Edge Cases' as investigation_type,
    patient_value_category,
    CASE 
        WHEN lifetime_production IS NULL AND patient_value_category != 'No Production' THEN 'NULL production but not No Production'
        WHEN lifetime_production = 0 AND patient_value_category != 'No Production' THEN 'Zero production but not No Production'
        WHEN lifetime_production > 0 AND lifetime_production < 500 AND patient_value_category != 'Low Value' THEN '0-499 but not Low Value'
        WHEN lifetime_production >= 500 AND lifetime_production < 2000 AND patient_value_category != 'Medium Value' THEN '500-1999 but not Medium Value'
        WHEN lifetime_production >= 2000 AND lifetime_production < 5000 AND patient_value_category != 'High Value' THEN '2000-4999 but not High Value'
        WHEN lifetime_production >= 5000 AND patient_value_category != 'VIP' THEN '5000+ but not VIP'
        ELSE 'OK'
    END as potential_issue,
    COUNT(*) as record_count
FROM marts.mart_patient_retention
WHERE (
    (lifetime_production IS NULL AND patient_value_category != 'No Production')
    OR (lifetime_production = 0 AND patient_value_category != 'No Production')
    OR (lifetime_production > 0 AND lifetime_production < 500 AND patient_value_category != 'Low Value')
    OR (lifetime_production >= 500 AND lifetime_production < 2000 AND patient_value_category != 'Medium Value')
    OR (lifetime_production >= 2000 AND lifetime_production < 5000 AND patient_value_category != 'High Value')
    OR (lifetime_production >= 5000 AND patient_value_category != 'VIP')
)
GROUP BY patient_value_category,
    CASE 
        WHEN lifetime_production IS NULL AND patient_value_category != 'No Production' THEN 'NULL production but not No Production'
        WHEN lifetime_production = 0 AND patient_value_category != 'No Production' THEN 'Zero production but not No Production'
        WHEN lifetime_production > 0 AND lifetime_production < 500 AND patient_value_category != 'Low Value' THEN '0-499 but not Low Value'
        WHEN lifetime_production >= 500 AND lifetime_production < 2000 AND patient_value_category != 'Medium Value' THEN '500-1999 but not Medium Value'
        WHEN lifetime_production >= 2000 AND lifetime_production < 5000 AND patient_value_category != 'High Value' THEN '2000-4999 but not High Value'
        WHEN lifetime_production >= 5000 AND patient_value_category != 'VIP' THEN '5000+ but not VIP'
        ELSE 'OK'
    END
ORDER BY record_count DESC;

-- Query 3.4: Sample records for each patient value category
SELECT 
    'Patient Value Category - Samples' as analysis,
    patient_id,
    date_id,
    patient_value_category,
    lifetime_production,
    annual_production_value,
    total_visits
FROM marts.mart_patient_retention
WHERE patient_value_category = 'VIP'
ORDER BY lifetime_production DESC, patient_id
LIMIT 30;

-- ============================================================================
-- Query 4: Combined Analysis
-- ============================================================================

-- Query 4.1: Cross-tabulation of retention_status and churn_risk_category
SELECT 
    'Combined - Retention Status vs Churn Risk' as analysis,
    retention_status,
    churn_risk_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM marts.mart_patient_retention
GROUP BY retention_status, churn_risk_category
ORDER BY retention_status, churn_risk_category;

-- Query 4.2: Cross-tabulation of retention_status and patient_value_category
SELECT 
    'Combined - Retention Status vs Patient Value' as analysis,
    retention_status,
    patient_value_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM marts.mart_patient_retention
GROUP BY retention_status, patient_value_category
ORDER BY retention_status, patient_value_category;

-- Query 4.3: Patients with unexpected combinations
SELECT 
    'Combined - Unexpected Combinations' as analysis,
    retention_status,
    churn_risk_category,
    patient_value_category,
    COUNT(*) as record_count
FROM marts.mart_patient_retention
WHERE (
    (retention_status = 'Active' AND churn_risk_category = 'High Risk')
    OR (retention_status = 'Lost' AND churn_risk_category = 'Low Risk')
    OR (retention_status = 'Scheduled' AND churn_risk_category = 'High Risk')
)
GROUP BY retention_status, churn_risk_category, patient_value_category
ORDER BY record_count DESC;

-- ============================================================================
-- Query 5: Summary Statistics
-- ============================================================================

-- Query 5.1: Overall summary for all three fields
SELECT 
    'Retention Categories - Summary Statistics' as analysis,
    COUNT(*) as total_records,
    COUNT(DISTINCT patient_id) as distinct_patients,
    COUNT(DISTINCT date_id) as distinct_dates,
    COUNT(*) FILTER (WHERE retention_status = 'Lost') as lost_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE retention_status = 'Lost') / COUNT(*), 2) as lost_percentage,
    COUNT(*) FILTER (WHERE churn_risk_category = 'High Risk') as high_risk_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE churn_risk_category = 'High Risk') / COUNT(*), 2) as high_risk_percentage,
    COUNT(*) FILTER (WHERE patient_value_category = 'VIP') as vip_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE patient_value_category = 'VIP') / COUNT(*), 2) as vip_percentage
FROM marts.mart_patient_retention;

-- Query 5.2: NULL value check for key fields
SELECT 
    'Retention Categories - NULL Value Check' as analysis,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE retention_status IS NULL) as null_retention_status,
    COUNT(*) FILTER (WHERE churn_risk_category IS NULL) as null_churn_risk,
    COUNT(*) FILTER (WHERE patient_value_category IS NULL) as null_patient_value,
    COUNT(*) FILTER (WHERE days_since_last_visit IS NULL) as null_days_since_visit,
    COUNT(*) FILTER (WHERE lifetime_production IS NULL) as null_lifetime_production,
    COUNT(*) FILTER (WHERE last_appointment_date IS NULL) as null_last_appointment_date
FROM marts.mart_patient_retention;
