{{ config(
    materialized='incremental',
    
    unique_key='metric_id'
) }}

/*
    Intermediate model for communication metrics
    Part of System F: Communications
    
    This model:
    1. Aggregates communication data from int_patient_communications_base
    2. Generates metrics for reporting on communication activities
    3. Supports analysis of communication effectiveness
    4. Tracks performance by user, type, and category
*/

WITH daily_communications AS (
    SELECT
        communication_datetime::date AS date,
        user_id,
        communication_type,
        direction AS communication_direction,
        communication_category,
        COUNT(*) AS total_count,
        SUM(CASE WHEN outcome IN ('confirmed', 'completed', 'rescheduled') THEN 1 ELSE 0 END) AS successful_count,
        SUM(CASE WHEN outcome IN ('cancelled', 'no_answer') THEN 1 ELSE 0 END) AS failed_count,
        -- Calculate average duration only for completed phone calls with valid timestamps
        AVG(
            CASE 
                WHEN communication_mode = 2  -- Only phone calls
                AND communication_datetime <= CURRENT_TIMESTAMP  -- Not future scheduled
                AND communication_end_datetime > '0001-01-01 00:00:00'  -- Has valid end time
                AND communication_end_datetime > communication_datetime  -- End time after start time
                THEN EXTRACT(EPOCH FROM (communication_end_datetime - communication_datetime))/60 
                ELSE NULL 
            END
        ) AS average_duration_minutes,
        -- Add diagnostic columns
        MIN(communication_datetime) as min_start_time,
        MAX(communication_datetime) as max_start_time,
        MIN(CASE WHEN communication_end_datetime > '0001-01-01 00:00:00' THEN communication_end_datetime END) as min_end_time,
        MAX(CASE WHEN communication_end_datetime > '0001-01-01 00:00:00' THEN communication_end_datetime END) as max_end_time,
        COUNT(CASE WHEN communication_end_datetime > '0001-01-01 00:00:00' THEN 1 END) as records_with_end_time,
        COUNT(*) as total_records,
        COUNT(CASE 
            WHEN communication_mode = 2  -- Only phone calls
            AND communication_datetime <= CURRENT_TIMESTAMP  -- Not future scheduled
            AND communication_end_datetime > '0001-01-01 00:00:00'  -- Has valid end time
            AND communication_end_datetime > communication_datetime  -- End time after start time
            THEN 1 
        END) as valid_durations,
        -- Add future date diagnostics
        COUNT(CASE WHEN communication_datetime > CURRENT_TIMESTAMP THEN 1 END) as future_dates_count,
        MIN(CASE WHEN communication_datetime > CURRENT_TIMESTAMP THEN communication_datetime END) as earliest_future_date,
        MAX(CASE WHEN communication_datetime > CURRENT_TIMESTAMP THEN communication_datetime END) as latest_future_date,
        -- Add mode-specific counts
        COUNT(CASE WHEN communication_mode = 1 THEN 1 END) as email_count,
        COUNT(CASE WHEN communication_mode = 2 THEN 1 END) as phone_count,
        COUNT(CASE WHEN communication_mode = 3 THEN 1 END) as mail_count,
        COUNT(CASE WHEN communication_mode = 5 THEN 1 END) as text_count,
        -- Add program-specific counts
        COUNT(CASE WHEN program_id = 0 THEN 1 END) as system_default_count,
        COUNT(CASE WHEN program_id = 95 THEN 1 END) as legacy_system_count
    FROM {{ ref('int_patient_communications_base') }}
    
    {% if is_incremental() %}
    WHERE communication_datetime::date >= (
        SELECT MAX(date) + INTERVAL '1 day' FROM {{ this }}
    )
    {% endif %}
    
    GROUP BY 
        communication_datetime::date,
        user_id,
        communication_type,
        direction,
        communication_category
),

-- Calculate response rates for outbound communications
response_metrics AS (
    SELECT
        communication_datetime::date AS date,
        user_id,
        communication_type,
        direction AS communication_direction,
        communication_category,
        COUNT(*) AS total_count,
        COUNT(CASE WHEN outcome IN ('confirmed', 'completed', 'rescheduled') THEN 1 ELSE NULL END) AS successful_count,
        CASE
            WHEN COUNT(*) > 0 THEN (COUNT(CASE WHEN outcome IN ('confirmed', 'completed', 'rescheduled') THEN 1 ELSE NULL END)::float / COUNT(*))
            ELSE 0
        END AS response_rate,
        -- For conversion rate, we'll count only those that have a specific positive outcome
        -- This is just an example implementation - would need refinement based on business definitions
        CASE
            WHEN COUNT(*) > 0 THEN
                (COUNT(CASE WHEN communication_category = 'appointment' AND outcome = 'confirmed' THEN 1 ELSE NULL END)::float / COUNT(*))
            ELSE 0
        END AS conversion_rate
    FROM {{ ref('int_patient_communications_base') }}

    {% if is_incremental() %}
    WHERE communication_datetime::date >= (
        SELECT MAX(date) + INTERVAL '1 day' FROM {{ this }}
    )
    {% endif %}

    GROUP BY
        communication_datetime::date,
        user_id,
        communication_type,
        direction,
        communication_category
)

SELECT
    -- Generate a unique ID for each metrics record
    {{ dbt_utils.generate_surrogate_key(['dc.date', 'dc.user_id', 'dc.communication_type', 'dc.communication_direction', 'dc.communication_category']) }} AS metric_id,
    dc.date,
    dc.user_id,
    dc.communication_type,
    dc.communication_direction,
    dc.communication_category,
    dc.total_count,
    dc.successful_count,
    dc.failed_count,
    dc.average_duration_minutes AS average_duration,
    rm.response_rate,
    rm.conversion_rate,
    -- Add all diagnostic columns
    dc.min_start_time,
    dc.max_start_time,
    dc.min_end_time,
    dc.max_end_time,
    dc.records_with_end_time,
    dc.valid_durations,
    dc.future_dates_count,
    dc.earliest_future_date,
    dc.latest_future_date,
    -- Add mode-specific counts
    dc.email_count,
    dc.phone_count,
    dc.mail_count,
    dc.text_count,
    -- Add program-specific counts
    dc.system_default_count,
    dc.legacy_system_count,
    -- Add standardized metadata
    {{ standardize_intermediate_metadata(primary_source_alias='dc', preserve_source_metadata=false) }}
FROM daily_communications dc
LEFT JOIN response_metrics rm
    ON dc.date = rm.date
    AND dc.user_id = rm.user_id
    AND dc.communication_type = rm.communication_type
    AND dc.communication_direction = rm.communication_direction
    AND dc.communication_category = rm.communication_category
