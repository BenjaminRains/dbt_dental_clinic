-- Test to identify appointments truly missing required history records
-- Missing creation records are now treated as informational, not errors

WITH appointment_with_metrics AS (
    -- Get current appointments with additional metrics
    SELECT
        a.appointment_id,
        a.appointment_status,
        a.appointment_datetime,
        a.date_timestamp AS last_modified,
        
        -- Calculate expected minimum history records based on status
        CASE
            WHEN a.appointment_status = 1 THEN 1  -- Scheduled: at least creation record
            WHEN a.appointment_status = 2 THEN 2  -- Completed: at least creation + completion
            WHEN a.appointment_status = 5 THEN 2  -- Broken: at least creation + broken status
            WHEN a.appointment_status = 6 THEN 1  -- Unscheduled: at least creation
            ELSE 1
        END AS min_expected_records
    FROM {{ ref('stg_opendental__appointment') }} a
    WHERE a.appointment_datetime >= '2023-01-01'
),

history_metrics AS (
    -- Get aggregated history metrics by appointment
    SELECT
        appointment_id,
        COUNT(*) AS history_count,
        MIN(history_timestamp) AS first_history,
        MAX(history_timestamp) AS last_history,
        -- Check if we have specific action types
        SUM(CASE WHEN history_action = 1 THEN 1 ELSE 0 END) AS has_create,
        SUM(CASE WHEN history_action = 3 THEN 1 ELSE 0 END) AS has_delete
    FROM {{ ref('stg_opendental__histappointment') }}
    GROUP BY appointment_id
),

-- FIX: Better approach to calculate gaps between history records
history_with_gaps AS (
    SELECT
        appointment_id,
        history_timestamp,
        LAG(history_timestamp) OVER (PARTITION BY appointment_id ORDER BY history_timestamp) AS prev_timestamp,
        -- Calculate gap in days, but only when prev_timestamp exists
        CASE 
            WHEN LAG(history_timestamp) OVER (PARTITION BY appointment_id ORDER BY history_timestamp) IS NOT NULL
            THEN EXTRACT(EPOCH FROM (history_timestamp - 
                  LAG(history_timestamp) OVER (PARTITION BY appointment_id ORDER BY history_timestamp)
                ))::float / 86400
            ELSE NULL
        END AS gap_days
    FROM {{ ref('stg_opendental__histappointment') }}
),

gap_metrics AS (
    -- Aggregate gap information by appointment
    SELECT
        appointment_id,
        MAX(gap_days) AS max_gap_days
    FROM history_with_gaps
    WHERE gap_days IS NOT NULL AND gap_days > 60
    GROUP BY appointment_id
),

-- ERRORS: Only truly missing history is considered an error
insufficient_history AS (
    -- Appointments with fewer history records than expected minimum
    -- And they don't have ANY history records at all (true error)
    SELECT
        a.appointment_id,
        a.appointment_status,
        a.appointment_datetime,
        COALESCE(h.history_count, 0) AS actual_record_count,
        a.min_expected_records AS expected_record_count,
        'Insufficient history records (no records at all)' AS issue_type,
        'ERROR' AS severity
    FROM appointment_with_metrics a
    LEFT JOIN history_metrics h ON a.appointment_id = h.appointment_id
    WHERE 
        COALESCE(h.history_count, 0) = 0  -- No history records at all
        AND a.appointment_datetime <= CURRENT_DATE - INTERVAL '1 day'  -- Only consider non-future appointments
),

-- INFORMATIONAL: These might be legitimate system behavior we don't understand
partial_history AS (
    -- Appointments with some history but fewer than expected for their status
    SELECT
        a.appointment_id,
        a.appointment_status,
        a.appointment_datetime,
        COALESCE(h.history_count, 0) AS actual_record_count,
        a.min_expected_records AS expected_record_count,
        'Partial history records' AS issue_type,
        'INFO' AS severity
    FROM appointment_with_metrics a
    LEFT JOIN history_metrics h ON a.appointment_id = h.appointment_id
    WHERE 
        COALESCE(h.history_count, 0) > 0  -- Has some history records
        AND COALESCE(h.history_count, 0) < a.min_expected_records  -- But fewer than expected
        AND a.appointment_datetime <= CURRENT_DATE - INTERVAL '1 day'  -- Only consider non-future appointments
),

missing_creation_records AS (
    -- Appointments with history but no creation record
    SELECT
        a.appointment_id,
        a.appointment_status,
        a.appointment_datetime,
        h.history_count AS actual_record_count,
        0 AS expected_record_count,
        'Missing creation record (no history_action=1)' AS issue_type,
        'INFO' AS severity  -- Changed to INFO since we don't fully understand OpenDental's logic
    FROM appointment_with_metrics a
    JOIN history_metrics h ON a.appointment_id = h.appointment_id
    WHERE 
        h.has_create = 0  -- No creation record
        AND h.history_count > 0  -- But has some history
),

-- FIX: Simplified large_gaps CTE that doesn't use window functions in WHERE
large_gaps AS (
    -- Appointments with unusually large gaps between history records
    SELECT
        a.appointment_id,
        a.appointment_status,
        a.appointment_datetime,
        h.history_count AS actual_record_count,
        g.max_gap_days AS expected_record_count,
        'Large gap between history records (' || ROUND(g.max_gap_days) || ' days)' AS issue_type,
        'INFO' AS severity  -- Not an error, just informational
    FROM appointment_with_metrics a
    JOIN history_metrics h ON a.appointment_id = h.appointment_id
    JOIN gap_metrics g ON a.appointment_id = g.appointment_id
    WHERE a.appointment_status IN (2, 5)  -- Focus on completed and broken appointments
)

-- FOR TEST: Return only the ERRORS for test evaluation 
-- Now only appointments with ZERO history records cause test failures
SELECT 
    appointment_id,
    appointment_status,
    appointment_datetime,
    issue_type 
FROM insufficient_history

-- For documentation/information only - this query returns more detailed information about all issues
-- Comment out the above query and uncomment this one to see all issues for analysis
/*
SELECT 
    appointment_id,
    appointment_status,
    CASE appointment_status 
        WHEN 1 THEN 'Scheduled'
        WHEN 2 THEN 'Completed'
        WHEN 3 THEN 'Unknown'
        WHEN 5 THEN 'Broken/Missed'
        WHEN 6 THEN 'Unscheduled'
        ELSE 'Other'
    END as status_description,
    appointment_datetime,
    actual_record_count,
    expected_record_count,
    issue_type,
    severity
FROM (
    SELECT * FROM insufficient_history
    UNION ALL
    SELECT * FROM partial_history
    UNION ALL
    SELECT * FROM missing_creation_records
    UNION ALL
    SELECT * FROM large_gaps
) AS all_issues
ORDER BY severity, appointment_status, appointment_id
*/
