-- Test to verify that significant appointment status changes have corresponding history records
-- Returns appointments that should have history records but are missing appropriate coverage

WITH current_appointments AS (
    -- Get current appointments with significant statuses
    SELECT
        appointment_id,
        appointment_status,
        appointment_datetime,
        patient_id
    FROM {{ ref('stg_opendental__appointment') }}
    WHERE 
        -- Focus on completed and broken/missed appointments
        appointment_status IN (2, 5)
        -- Only consider appointments within our history tracking period
        AND appointment_datetime >= '2023-01-01'
),

last_history_per_appointment AS (
    -- Get the most recent history record for each appointment
    SELECT DISTINCT ON (appointment_id)
        appointment_id,
        hist_appointment_id,
        appointment_status AS history_status,
        history_action,
        history_timestamp
    FROM {{ ref('stg_opendental__histappointment') }}
    ORDER BY 
        appointment_id, 
        history_timestamp DESC
),

history_counts AS (
    -- Count history records per appointment
    SELECT
        appointment_id,
        COUNT(*) AS history_record_count
    FROM {{ ref('stg_opendental__histappointment') }}
    GROUP BY appointment_id
),

missing_history AS (
    -- Appointments with no history records at all
    SELECT
        a.appointment_id,
        a.appointment_status,
        a.appointment_datetime,
        'No history records found' AS issue_type
    FROM current_appointments a
    LEFT JOIN history_counts h ON a.appointment_id = h.appointment_id
    WHERE h.history_record_count IS NULL OR h.history_record_count = 0
),

status_mismatch AS (
    -- Appointments where current status doesn't match last history record
    SELECT
        a.appointment_id,
        a.appointment_status,
        lh.history_status,
        a.appointment_datetime,
        'Current status does not match history' AS issue_type
    FROM current_appointments a
    JOIN last_history_per_appointment lh ON a.appointment_id = lh.appointment_id
    WHERE a.appointment_status != lh.history_status
),

status_change_without_history AS (
    -- Completed or broken appointments with insufficient history records
    -- (Should have at least 2 records - one for creation, one for status change)
    SELECT
        a.appointment_id,
        a.appointment_status,
        a.appointment_datetime,
        h.history_record_count,
        'Status change without sufficient history tracking' AS issue_type
    FROM current_appointments a
    JOIN history_counts h ON a.appointment_id = h.appointment_id
    WHERE 
        -- For completed/broken appointments, we expect at least 2 history records
        -- (one for creation, one for status change)
        h.history_record_count < 2
)

-- Combine all issues
SELECT 
    appointment_id,
    appointment_status,
    appointment_datetime,
    issue_type
FROM missing_history

UNION ALL

SELECT 
    appointment_id,
    appointment_status,
    appointment_datetime,
    issue_type
FROM status_mismatch

UNION ALL

SELECT 
    appointment_id,
    appointment_status,
    appointment_datetime,
    issue_type
FROM status_change_without_history

ORDER BY appointment_id
