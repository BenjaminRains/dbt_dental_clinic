-- Investigation Queries for Patient Category Mappings
-- Date: 2026-01-23
-- Purpose: Investigate age_category, preferred_confirmation_method, and preferred_contact_method
--          to verify all source values are properly mapped
-- Related: validation/VALIDATION_TEMPLATE.md

-- ============================================================================
-- Query 1: Age Category Analysis
-- ============================================================================

-- Query 1.1: Age distribution and NULL check
SELECT 
    'Age Category - Age Distribution' as investigation_type,
    COUNT(*) as total_patients,
    COUNT(*) FILTER (WHERE age IS NULL) as null_age_count,
    COUNT(*) FILTER (WHERE age IS NOT NULL) as non_null_age_count,
    COUNT(*) FILTER (WHERE birth_date IS NULL) as null_birth_date_count,
    COUNT(*) FILTER (WHERE birth_date IS NOT NULL) as non_null_birth_date_count,
    MIN(age) as min_age,
    MAX(age) as max_age,
    AVG(age) as avg_age
FROM marts.dim_patient;

-- Query 1.2: Age Category distribution
SELECT 
    'Age Category - Distribution' as investigation_type,
    age_category,
    COUNT(*) as patient_count,
    COUNT(*) FILTER (WHERE age IS NULL) as null_age_count,
    COUNT(*) FILTER (WHERE age IS NOT NULL) as non_null_age_count,
    MIN(age) as min_age,
    MAX(age) as max_age,
    AVG(age) as avg_age
FROM marts.dim_patient
GROUP BY age_category
ORDER BY patient_count DESC;

-- Query 1.3: Age Category breakdown by age ranges
SELECT 
    'Age Category - Age Range Breakdown' as analysis,
    age_category,
    CASE 
        WHEN age IS NULL THEN 'NULL'
        WHEN age < 0 THEN 'Negative'
        WHEN age < 18 THEN '0-17 (Minor)'
        WHEN age BETWEEN 18 AND 64 THEN '18-64 (Adult)'
        WHEN age >= 65 THEN '65+ (Senior)'
        ELSE 'Other'
    END as age_range,
    COUNT(*) as patient_count,
    MIN(age) as min_age,
    MAX(age) as max_age
FROM marts.dim_patient
GROUP BY age_category,
    CASE 
        WHEN age IS NULL THEN 'NULL'
        WHEN age < 0 THEN 'Negative'
        WHEN age < 18 THEN '0-17 (Minor)'
        WHEN age BETWEEN 18 AND 64 THEN '18-64 (Adult)'
        WHEN age >= 65 THEN '65+ (Senior)'
        ELSE 'Other'
    END
ORDER BY age_category, age_range;

-- Query 1.4: Sample records for Unknown age_category
SELECT 
    'Age Category - Unknown Samples' as analysis,
    patient_id,
    age,
    birth_date,
    age_category,
    CASE 
        WHEN age IS NULL AND birth_date IS NULL THEN 'Both NULL'
        WHEN age IS NULL AND birth_date IS NOT NULL THEN 'Age NULL, Birth Date exists'
        WHEN age IS NOT NULL AND birth_date IS NULL THEN 'Age exists, Birth Date NULL'
        WHEN age < 0 THEN 'Negative Age'
        ELSE 'Other'
    END as issue_type
FROM marts.dim_patient
WHERE age_category = 'Unknown'
ORDER BY patient_id
LIMIT 30;

-- ============================================================================
-- Query 2: Preferred Confirmation Method Analysis
-- ============================================================================

-- Query 2.1: Preferred Confirmation Method distribution in source (staging)
SELECT 
    'Preferred Confirmation Method - Source Distribution' as investigation_type,
    preferred_confirmation_method,
    COUNT(*) as patient_count,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM staging.stg_opendental__patient
GROUP BY preferred_confirmation_method
ORDER BY preferred_confirmation_method;

-- Query 2.2: Preferred Confirmation Method distribution in mart
SELECT 
    'Preferred Confirmation Method - Mart Distribution' as investigation_type,
    preferred_confirmation_method,
    COUNT(*) as patient_count,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM marts.dim_patient
GROUP BY preferred_confirmation_method
ORDER BY patient_count DESC;

-- Query 2.3: Breakdown by source value and mapped category
SELECT 
    'Preferred Confirmation Method - Mapping Breakdown' as analysis,
    s.preferred_confirmation_method as source_value,
    d.preferred_confirmation_method as mapped_category,
    COUNT(*) as patient_count
FROM staging.stg_opendental__patient s
LEFT JOIN marts.dim_patient d ON s.patient_id = d.patient_id
GROUP BY s.preferred_confirmation_method, d.preferred_confirmation_method
ORDER BY s.preferred_confirmation_method, d.preferred_confirmation_method;

-- Query 2.4: Unknown preferred_confirmation_method analysis
SELECT 
    'Preferred Confirmation Method - Unknown Analysis' as investigation_type,
    COUNT(*) as total_unknown_records,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM marts.dim_patient
WHERE preferred_confirmation_method = 'Unknown';

-- Query 2.5: Breakdown of Unknown by source values
SELECT 
    'Preferred Confirmation Method - Unknown by Source Value' as analysis,
    s.preferred_confirmation_method as source_value,
    COUNT(*) as patient_count
FROM staging.stg_opendental__patient s
INNER JOIN marts.dim_patient d ON s.patient_id = d.patient_id
WHERE d.preferred_confirmation_method = 'Unknown'
GROUP BY s.preferred_confirmation_method
ORDER BY patient_count DESC;

-- Query 2.6: Sample records for Unknown preferred_confirmation_method
SELECT 
    'Preferred Confirmation Method - Unknown Samples' as analysis,
    d.patient_id,
    s.preferred_confirmation_method as source_value,
    d.preferred_confirmation_method as mapped_category
FROM marts.dim_patient d
LEFT JOIN staging.stg_opendental__patient s ON d.patient_id = s.patient_id
WHERE d.preferred_confirmation_method = 'Unknown'
ORDER BY d.patient_id
LIMIT 30;

-- ============================================================================
-- Query 3: Preferred Contact Method Analysis
-- ============================================================================

-- Query 3.1: Preferred Contact Method distribution in source (staging)
SELECT 
    'Preferred Contact Method - Source Distribution' as investigation_type,
    preferred_contact_method,
    COUNT(*) as patient_count,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM staging.stg_opendental__patient
GROUP BY preferred_contact_method
ORDER BY preferred_contact_method;

-- Query 3.2: Preferred Contact Method distribution in mart
SELECT 
    'Preferred Contact Method - Mart Distribution' as investigation_type,
    preferred_contact_method,
    COUNT(*) as patient_count,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM marts.dim_patient
GROUP BY preferred_contact_method
ORDER BY patient_count DESC;

-- Query 3.3: Breakdown by source value and mapped category
SELECT 
    'Preferred Contact Method - Mapping Breakdown' as analysis,
    s.preferred_contact_method as source_value,
    d.preferred_contact_method as mapped_category,
    COUNT(*) as patient_count
FROM staging.stg_opendental__patient s
LEFT JOIN marts.dim_patient d ON s.patient_id = d.patient_id
GROUP BY s.preferred_contact_method, d.preferred_contact_method
ORDER BY s.preferred_contact_method, d.preferred_contact_method;

-- Query 3.4: Unknown preferred_contact_method analysis
SELECT 
    'Preferred Contact Method - Unknown Analysis' as investigation_type,
    COUNT(*) as total_unknown_records,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM marts.dim_patient
WHERE preferred_contact_method = 'Unknown';

-- Query 3.5: Breakdown of Unknown by source values
SELECT 
    'Preferred Contact Method - Unknown by Source Value' as analysis,
    s.preferred_contact_method as source_value,
    COUNT(*) as patient_count
FROM staging.stg_opendental__patient s
INNER JOIN marts.dim_patient d ON s.patient_id = d.patient_id
WHERE d.preferred_contact_method = 'Unknown'
GROUP BY s.preferred_contact_method
ORDER BY patient_count DESC;

-- Query 3.6: Sample records for Unknown preferred_contact_method
SELECT 
    'Preferred Contact Method - Unknown Samples' as analysis,
    d.patient_id,
    s.preferred_contact_method as source_value,
    d.preferred_contact_method as mapped_category
FROM marts.dim_patient d
LEFT JOIN staging.stg_opendental__patient s ON d.patient_id = s.patient_id
WHERE d.preferred_contact_method = 'Unknown'
ORDER BY d.patient_id
LIMIT 30;

-- ============================================================================
-- Query 4: Combined Analysis
-- ============================================================================

-- Query 4.1: Patients with multiple Unknown categories
SELECT 
    'Combined - Multiple Unknown Categories' as analysis,
    COUNT(*) as patient_count,
    COUNT(*) FILTER (WHERE age_category = 'Unknown') as unknown_age,
    COUNT(*) FILTER (WHERE preferred_confirmation_method = 'Unknown') as unknown_confirmation,
    COUNT(*) FILTER (WHERE preferred_contact_method = 'Unknown') as unknown_contact,
    COUNT(*) FILTER (WHERE age_category = 'Unknown' AND preferred_confirmation_method = 'Unknown' AND preferred_contact_method = 'Unknown') as all_unknown
FROM marts.dim_patient;

-- Query 4.2: Cross-tabulation of Unknown categories
SELECT 
    'Combined - Unknown Categories Cross Tab' as analysis,
    age_category,
    preferred_confirmation_method,
    preferred_contact_method,
    COUNT(*) as patient_count
FROM marts.dim_patient
WHERE age_category = 'Unknown'
   OR preferred_confirmation_method = 'Unknown'
   OR preferred_contact_method = 'Unknown'
GROUP BY age_category, preferred_confirmation_method, preferred_contact_method
ORDER BY patient_count DESC
LIMIT 20;

-- ============================================================================
-- Query 5: Summary Statistics
-- ============================================================================

-- Query 5.1: Overall summary for all three fields
SELECT 
    'Patient Categories - Summary Statistics' as analysis,
    COUNT(*) as total_patients,
    COUNT(*) FILTER (WHERE age_category = 'Unknown') as unknown_age_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE age_category = 'Unknown') / COUNT(*), 2) as unknown_age_percentage,
    COUNT(*) FILTER (WHERE preferred_confirmation_method = 'Unknown') as unknown_confirmation_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE preferred_confirmation_method = 'Unknown') / COUNT(*), 2) as unknown_confirmation_percentage,
    COUNT(*) FILTER (WHERE preferred_contact_method = 'Unknown') as unknown_contact_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE preferred_contact_method = 'Unknown') / COUNT(*), 2) as unknown_contact_percentage
FROM marts.dim_patient;

-- Query 5.2: Distinct source values summary
SELECT 
    'Source Values - Distinct Counts' as analysis,
    COUNT(DISTINCT preferred_confirmation_method) as distinct_confirmation_methods,
    COUNT(DISTINCT preferred_contact_method) as distinct_contact_methods,
    STRING_AGG(DISTINCT preferred_confirmation_method::text, ', ' ORDER BY preferred_confirmation_method::text) FILTER (WHERE preferred_confirmation_method IS NOT NULL) as confirmation_method_values,
    STRING_AGG(DISTINCT preferred_contact_method::text, ', ' ORDER BY preferred_contact_method::text) FILTER (WHERE preferred_contact_method IS NOT NULL) as contact_method_values
FROM staging.stg_opendental__patient;
