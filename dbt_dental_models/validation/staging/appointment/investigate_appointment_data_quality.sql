-- Investigation Queries for Appointment Data Quality Issues
-- Date: 2026-01-23
-- Purpose: Investigate failing appointment tests to understand root causes

-- ============================================================================
-- Query 1: Broken/Missed Appointments Without Procedure Descriptions
-- ============================================================================
-- Test: appt_broken_wo_procs
-- Issue: 83 broken/missed appointments missing procedure descriptions

-- Query 1.1: Summary of broken appointments without procedure descriptions
SELECT 
    'Broken Appointments - Missing Procedure Descriptions' as investigation_type,
    COUNT(*) as total_records,
    COUNT(DISTINCT patient_id) as distinct_patients,
    COUNT(DISTINCT provider_id) as distinct_providers,
    MIN(appointment_datetime) as earliest_appointment,
    MAX(appointment_datetime) as latest_appointment
FROM staging.stg_opendental__appointment
WHERE appointment_status = 5  -- Broken/Missed
  AND (procedure_description IS NULL OR TRIM(procedure_description) = '')
  AND is_hygiene = false
  AND patient_id IS NOT NULL;

-- Query 1.2: Breakdown by appointment characteristics
SELECT 
    'Broken Appointments - Breakdown' as analysis,
    is_hygiene,
    CASE 
        WHEN procedure_description IS NULL THEN 'NULL'
        WHEN TRIM(procedure_description) = '' THEN 'Empty String'
        ELSE 'Has Description'
    END as procedure_description_status,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients,
    COUNT(DISTINCT provider_id) as distinct_providers
FROM staging.stg_opendental__appointment
WHERE appointment_status = 5  -- Broken/Missed
  AND is_hygiene = false
  AND patient_id IS NOT NULL
GROUP BY is_hygiene, 
    CASE 
        WHEN procedure_description IS NULL THEN 'NULL'
        WHEN TRIM(procedure_description) = '' THEN 'Empty String'
        ELSE 'Has Description'
    END
ORDER BY record_count DESC;

-- Query 1.3: Sample records for investigation
SELECT 
    'Broken Appointments - Sample Records' as analysis,
    appointment_id,
    patient_id,
    provider_id,
    appointment_datetime,
    appointment_status,
    procedure_description,
    is_hygiene,
    note,
    entered_by_user_id
FROM staging.stg_opendental__appointment
WHERE appointment_status = 5  -- Broken/Missed
  AND (procedure_description IS NULL OR TRIM(procedure_description) = '')
  AND is_hygiene = false
  AND patient_id IS NOT NULL
ORDER BY appointment_datetime DESC
LIMIT 20;

-- Query 1.4: Check if these appointments have procedures in procedurelog
SELECT 
    'Broken Appointments - Procedure Link Check' as analysis,
    COUNT(DISTINCT a.appointment_id) as appointments_with_procedures,
    COUNT(DISTINCT a.appointment_id) FILTER (WHERE p.procedure_id IS NOT NULL) as has_linked_procedures,
    COUNT(DISTINCT a.appointment_id) FILTER (WHERE p.procedure_id IS NULL) as no_linked_procedures
FROM staging.stg_opendental__appointment a
LEFT JOIN staging.stg_opendental__procedurelog p
    ON p.appointment_id = a.appointment_id
WHERE a.appointment_status = 5  -- Broken/Missed
  AND (a.procedure_description IS NULL OR TRIM(a.procedure_description) = '')
  AND a.is_hygiene = false
  AND a.patient_id IS NOT NULL;

-- Query 1.5: Distribution by date range
SELECT 
    'Broken Appointments - Date Distribution' as analysis,
    DATE_TRUNC('month', appointment_datetime) as appointment_month,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM staging.stg_opendental__appointment
WHERE appointment_status = 5  -- Broken/Missed
  AND (procedure_description IS NULL OR TRIM(procedure_description) = '')
  AND is_hygiene = false
  AND patient_id IS NOT NULL
GROUP BY DATE_TRUNC('month', appointment_datetime)
ORDER BY appointment_month DESC;

-- ============================================================================
-- Query 2: Past Appointments Still Marked as Scheduled
-- ============================================================================
-- Test: appt_past_scheduled
-- Issue: 8 past appointments still marked as scheduled (status = 1)

-- Query 2.1: Summary of past scheduled appointments
SELECT 
    'Past Scheduled Appointments - Summary' as investigation_type,
    COUNT(*) as total_records,
    COUNT(DISTINCT patient_id) as distinct_patients,
    COUNT(DISTINCT provider_id) as distinct_providers,
    MIN(appointment_datetime) as earliest_appointment,
    MAX(appointment_datetime) as latest_appointment,
    AVG(current_date - appointment_datetime) as avg_days_overdue,
    MAX(current_date - appointment_datetime) as max_days_overdue
FROM staging.stg_opendental__appointment
WHERE appointment_status = 1  -- Scheduled
  AND appointment_datetime < current_date
  AND appointment_datetime < '2025-01-01'
  AND patient_id IS NOT NULL;

-- Query 2.2: Sample records for investigation
SELECT 
    'Past Scheduled Appointments - Sample Records' as analysis,
    appointment_id,
    patient_id,
    provider_id,
    appointment_datetime,
    appointment_status,
    current_date as today,
    current_date - appointment_datetime as days_overdue,
    note,
    is_hygiene,
    procedure_description,
    entered_by_user_id
FROM staging.stg_opendental__appointment
WHERE appointment_status = 1  -- Scheduled
  AND appointment_datetime < current_date
  AND appointment_datetime < '2025-01-01'
  AND patient_id IS NOT NULL
ORDER BY appointment_datetime DESC;

-- Query 2.3: Check appointment status distribution for past appointments
SELECT 
    'Past Appointments - Status Distribution' as analysis,
    appointment_status,
    CASE 
        WHEN appointment_status = 1 THEN 'Scheduled (Should be Completed)'
        WHEN appointment_status = 2 THEN 'Completed (Correct)'
        WHEN appointment_status = 5 THEN 'Broken/Missed'
        ELSE 'Other'
    END as status_description,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM staging.stg_opendental__appointment
WHERE appointment_datetime < current_date
  AND appointment_datetime < '2025-01-01'
  AND patient_id IS NOT NULL
GROUP BY appointment_status
ORDER BY record_count DESC;

-- Query 2.4: Check if these appointments have completion indicators
SELECT 
    'Past Scheduled - Completion Indicators' as analysis,
    COUNT(*) as total_past_scheduled,
    COUNT(*) FILTER (WHERE dismissed_datetime IS NOT NULL) as has_dismissed_datetime,
    COUNT(*) FILTER (WHERE arrival_datetime IS NOT NULL) as has_arrival_datetime,
    COUNT(*) FILTER (WHERE seated_datetime IS NOT NULL) as has_seated_datetime,
    COUNT(*) FILTER (WHERE note IS NOT NULL AND TRIM(note) != '') as has_note
FROM staging.stg_opendental__appointment
WHERE appointment_status = 1  -- Scheduled
  AND appointment_datetime < current_date
  AND appointment_datetime < '2025-01-01'
  AND patient_id IS NOT NULL;

-- Query 2.5: Distribution by how many days overdue
SELECT 
    'Past Scheduled - Days Overdue Distribution' as analysis,
    CASE 
        WHEN (current_date - appointment_datetime)::integer <= 7 THEN '1-7 days'
        WHEN (current_date - appointment_datetime)::integer <= 30 THEN '8-30 days'
        WHEN (current_date - appointment_datetime)::integer <= 90 THEN '31-90 days'
        WHEN (current_date - appointment_datetime)::integer <= 180 THEN '91-180 days'
        ELSE '180+ days'
    END as days_overdue_range,
    COUNT(*) as record_count,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM staging.stg_opendental__appointment
WHERE appointment_status = 1  -- Scheduled
  AND appointment_datetime < current_date
  AND appointment_datetime < '2025-01-01'
  AND patient_id IS NOT NULL
GROUP BY 
    CASE 
        WHEN (current_date - appointment_datetime)::integer <= 7 THEN '1-7 days'
        WHEN (current_date - appointment_datetime)::integer <= 30 THEN '8-30 days'
        WHEN (current_date - appointment_datetime)::integer <= 90 THEN '31-90 days'
        WHEN (current_date - appointment_datetime)::integer <= 180 THEN '91-180 days'
        ELSE '180+ days'
    END
ORDER BY MIN((current_date - appointment_datetime)::integer);

-- ============================================================================
-- Query 3: Combined Analysis - Data Quality Patterns
-- ============================================================================

-- Query 3.1: Check if past scheduled appointments are also missing procedure descriptions
SELECT 
    'Past Scheduled - Procedure Description Check' as analysis,
    COUNT(*) as total_past_scheduled,
    COUNT(*) FILTER (WHERE procedure_description IS NULL OR TRIM(procedure_description) = '') as missing_procedure_description,
    COUNT(*) FILTER (WHERE procedure_description IS NOT NULL AND TRIM(procedure_description) != '') as has_procedure_description
FROM staging.stg_opendental__appointment
WHERE appointment_status = 1  -- Scheduled
  AND appointment_datetime < current_date
  AND appointment_datetime < '2025-01-01'
  AND patient_id IS NOT NULL;

-- Query 3.2: Check appointment type distribution for both issues
SELECT 
    'Appointment Type Distribution' as analysis,
    'Broken Without Procedures' as issue_type,
    appointment_type_id,
    COUNT(*) as record_count
FROM staging.stg_opendental__appointment
WHERE appointment_status = 5  -- Broken/Missed
  AND (procedure_description IS NULL OR TRIM(procedure_description) = '')
  AND is_hygiene = false
  AND patient_id IS NOT NULL
GROUP BY appointment_type_id

UNION ALL

SELECT 
    'Appointment Type Distribution' as analysis,
    'Past Still Scheduled' as issue_type,
    appointment_type_id,
    COUNT(*) as record_count
FROM staging.stg_opendental__appointment
WHERE appointment_status = 1  -- Scheduled
  AND appointment_datetime < current_date
  AND appointment_datetime < '2025-01-01'
  AND patient_id IS NOT NULL
GROUP BY appointment_type_id
ORDER BY issue_type, record_count DESC;
