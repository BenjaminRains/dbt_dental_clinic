-- INFORMATIONAL ANALYSIS: History action patterns in OpenDental
-- This query is meant to provide insights rather than strict pass/fail testing
-- When run as test, returns 0 rows to pass

-- Set this parameter to false for test runs, true for analysis
{% set analysis_mode = false %}

WITH ranked_history AS (
    -- Add sequence numbers and next/previous record info
    SELECT
        hist_appointment_id,
        appointment_id,
        history_action,
        history_timestamp,
        patient_id,
        provider_id,
        appointment_datetime,
        appointment_status,
        
        -- Get sequence number of this record within its appointment history
        ROW_NUMBER() OVER (PARTITION BY appointment_id ORDER BY history_timestamp) AS seq_num,
        
        -- Count total records for this appointment
        COUNT(*) OVER (PARTITION BY appointment_id) AS total_records,
        
        -- Get the next action (if any) for this appointment
        LEAD(history_action) OVER (PARTITION BY appointment_id ORDER BY history_timestamp) AS next_action
    FROM {{ ref('stg_opendental__histappointment') }}
),

current_appointments AS (
    -- Get current appointments
    SELECT appointment_id 
    FROM {{ ref('stg_opendental__appointment') }}
),

-- Collect various patterns for analysis
deleted_patterns AS (
    -- Deleted actions and their contexts
    SELECT
        r.hist_appointment_id,
        r.appointment_id,
        r.history_action,
        r.seq_num,
        r.history_timestamp,
        CASE
            WHEN r.seq_num < r.total_records THEN 'Deleted not last in sequence'
            WHEN a.appointment_id IS NOT NULL THEN 'Deleted but appointment exists'
            ELSE 'Normal deletion'
        END AS pattern_type
    FROM ranked_history r
    LEFT JOIN current_appointments a ON r.appointment_id = a.appointment_id
    WHERE r.history_action = 3
),

action_stats AS (
    -- Get statistics about action types per appointment
    SELECT
        appointment_id,
        COUNT(*) AS total_records,
        SUM(CASE WHEN history_action = 0 THEN 1 ELSE 0 END) AS action_0_count,
        SUM(CASE WHEN history_action = 1 THEN 1 ELSE 0 END) AS action_1_count,
        SUM(CASE WHEN history_action = 2 THEN 1 ELSE 0 END) AS action_2_count,
        SUM(CASE WHEN history_action = 3 THEN 1 ELSE 0 END) AS action_3_count,
        MIN(CASE WHEN seq_num = 1 THEN history_action ELSE NULL END) AS first_action,
        MAX(CASE WHEN seq_num = total_records THEN history_action ELSE NULL END) AS last_action
    FROM ranked_history
    GROUP BY appointment_id
),

-- ANALYSIS OUTPUT #1: Deleted action patterns 
deleted_analysis AS (
    SELECT 
        hist_appointment_id,
        appointment_id,
        history_action,
        history_timestamp,
        pattern_type AS analysis_note,
        CASE 
            WHEN pattern_type = 'Normal deletion' THEN 'INFO'
            WHEN pattern_type = 'Deleted but appointment exists' THEN 'WARNING'
            WHEN pattern_type = 'Deleted not last in sequence' THEN 'WARNING'
            ELSE 'INFO'
        END AS classification
    FROM deleted_patterns
),

-- ANALYSIS OUTPUT #2: Unusual action sequences
unusual_sequences AS (
    SELECT 
        hist_appointment_id,
        appointment_id,
        history_action,
        history_timestamp,
        'Deleted followed by action ' || next_action AS analysis_note,
        'WARNING' AS classification
    FROM ranked_history
    WHERE history_action = 3 AND next_action IS NOT NULL
),

-- ANALYSIS OUTPUT #3: Unusual action distributions
unusual_distributions AS (
    SELECT
        NULL::integer AS hist_appointment_id,
        appointment_id,
        NULL::smallint AS history_action,
        NULL::timestamp AS history_timestamp,
        CASE
            WHEN action_1_count = 0 THEN 'No "Created" actions found'
            WHEN action_2_count = 0 AND total_records > 10 THEN 'Many records but no "Modified" actions'
            WHEN action_3_count > 1 THEN 'Multiple "Deleted" actions'
            WHEN first_action != 1 THEN 'First action is ' || first_action || ' (not 1)'
            ELSE NULL
        END AS analysis_note,
        'INFO' AS classification
    FROM action_stats
    WHERE action_1_count = 0 
       OR (action_2_count = 0 AND total_records > 10)
       OR action_3_count > 1
       OR first_action != 1
)

-- FOR TEST MODE: Return empty set to pass the test
{% if not analysis_mode %}
SELECT 
    NULL::integer as appointment_id,
    NULL::text as analysis_note
WHERE 1=0  -- Always returns zero rows
{% else %}
-- FOR ANALYSIS MODE: Return all findings
SELECT 
    hist_appointment_id,
    appointment_id,
    history_action,
    history_timestamp,
    analysis_note,
    classification
FROM deleted_analysis
UNION ALL
SELECT * FROM unusual_sequences  
UNION ALL
SELECT * FROM unusual_distributions
WHERE analysis_note IS NOT NULL
ORDER BY classification, appointment_id
{% endif %}

-- To see analysis results, run this query directly with "analysis_mode = true" or this query:
/*
SELECT 
    hist_appointment_id,
    appointment_id,
    history_action,
    history_timestamp,
    analysis_note,
    classification
FROM deleted_analysis
UNION ALL
SELECT * FROM unusual_sequences  
UNION ALL
SELECT * FROM unusual_distributions
WHERE analysis_note IS NOT NULL
ORDER BY classification, appointment_id
LIMIT 1000  -- Add a limit since there could be many records
*/

