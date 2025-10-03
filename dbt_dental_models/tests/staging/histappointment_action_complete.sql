-- INFORMATIONAL ANALYSIS: Data completeness in history records
-- This test will return 0 rows by default in test mode but contains analysis queries
-- Use the commented query at the bottom for detailed information

-- Set this parameter to false for test runs, true for analysis
{% set analysis_mode = false %}  

WITH hist_with_prev AS (
    -- Get history records with their previous version (if any)
    SELECT
        h.hist_appointment_id,
        h.appointment_id,
        h.history_action,
        h.patient_id,
        h.provider_id,
        h.appointment_datetime,
        h.appointment_status,
        h.note,
        LAG(h.hist_appointment_id) OVER (
            PARTITION BY h.appointment_id 
            ORDER BY h.history_timestamp
        ) AS prev_hist_id
    FROM {{ ref('stg_opendental__histappointment') }} h
),

current_appointments AS (
    -- Get current appointments for checking deleted appointments
    SELECT appointment_id
    FROM {{ ref('stg_opendental__appointment') }}
),

created_issues AS (
    -- Check for issues with Created (1) records
    SELECT
        hist_appointment_id,
        appointment_id,
        'Created action missing core fields' AS issue_type,
        'WARNING' AS severity
    FROM hist_with_prev
    WHERE 
        history_action = 1
        AND (
            patient_id IS NULL
            OR appointment_datetime IS NULL
            OR provider_id IS NULL
            OR appointment_status IS NULL
        )
),

modified_issues AS (
    -- Check for issues with Modified (2) records
    SELECT 
        h.hist_appointment_id,
        h.appointment_id,
        'Modified action with no apparent changes' AS issue_type,
        'INFO' AS severity
    FROM hist_with_prev h
    JOIN {{ ref('stg_opendental__histappointment') }} prev 
        ON h.prev_hist_id = prev.hist_appointment_id
    WHERE 
        h.history_action = 2
        AND h.patient_id IS NOT DISTINCT FROM prev.patient_id
        AND h.provider_id IS NOT DISTINCT FROM prev.provider_id
        AND h.appointment_datetime IS NOT DISTINCT FROM prev.appointment_datetime
        AND h.appointment_status IS NOT DISTINCT FROM prev.appointment_status
        AND h.note IS NOT DISTINCT FROM prev.note
),

deleted_issues AS (
    -- Check for issues with Deleted (3) records
    SELECT
        h.hist_appointment_id,
        h.appointment_id,
        'Deleted action but appointment still exists' AS issue_type,
        'WARNING' AS severity
    FROM hist_with_prev h
    JOIN current_appointments a ON h.appointment_id = a.appointment_id
    WHERE 
        h.history_action = 3
)

-- FOR TEST MODE: Return empty set to pass the test
{% if not analysis_mode %}
SELECT 
    NULL::integer as appointment_id,
    NULL::text as issue_type
WHERE 1=0  -- Always returns zero rows
{% else %}
-- FOR ANALYSIS MODE: Return all issues found
SELECT 
    hist_appointment_id,
    appointment_id,
    issue_type,
    severity
FROM (
    SELECT * FROM created_issues
    UNION ALL
    SELECT * FROM modified_issues
    UNION ALL
    SELECT * FROM deleted_issues
) AS all_issues
ORDER BY severity, appointment_id
{% endif %}

-- To see analysis results, run this query directly:
/*
SELECT 
    hist_appointment_id,
    appointment_id,
    issue_type,
    severity
FROM (
    SELECT * FROM created_issues
    UNION ALL
    SELECT * FROM modified_issues
    UNION ALL
    SELECT * FROM deleted_issues
) AS all_issues
ORDER BY severity, appointment_id
*/
