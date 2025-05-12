{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='metric_id'
) }}

/*
    Intermediate model for communication metrics
    Part of System F: Communications
    
    This model:
    1. Aggregates communication data from int_patient_communications
    2. Generates metrics for reporting on communication activities
    3. Supports analysis of communication effectiveness
    4. Tracks performance by user, type, and category
*/

WITH DailyCommunications AS (
    SELECT
        communication_datetime::date AS date,
        user_id,
        communication_type,
        direction AS communication_direction,
        communication_category,
        COUNT(*) AS total_count,
        SUM(CASE WHEN outcome IN ('confirmed', 'completed', 'rescheduled') THEN 1 ELSE 0 END) AS successful_count,
        SUM(CASE WHEN outcome IN ('cancelled', 'no_answer') THEN 1 ELSE 0 END) AS failed_count,
        -- Calculate average duration when end datetime is available
        AVG(
            CASE 
                WHEN communication_end_datetime IS NOT NULL 
                THEN EXTRACT(EPOCH FROM (communication_end_datetime - communication_datetime))/60 
                ELSE NULL 
            END
        ) AS average_duration_minutes
    FROM {{ ref('int_patient_communications') }}
    
    {% if is_incremental() %}
    WHERE communication_datetime >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    
    GROUP BY 
        communication_datetime::date,
        user_id,
        communication_type,
        direction,
        communication_category
),

-- Calculate response rates for outbound communications
ResponseMetrics AS (
    SELECT
        date,
        user_id,
        communication_type,
        communication_direction,
        communication_category,
        CASE 
            WHEN total_count > 0 THEN (successful_count::float / total_count) 
            ELSE 0 
        END AS response_rate,
        -- For conversion rate, we'll count only those that have a specific positive outcome
        -- This is just an example implementation - would need refinement based on business definitions
        CASE 
            WHEN total_count > 0 THEN 
                (SUM(CASE WHEN communication_category = 'appointment' AND outcome = 'confirmed' THEN 1 ELSE 0 END)::float / total_count)
            ELSE 0
        END AS conversion_rate
    FROM {{ ref('int_patient_communications') }}
    
    {% if is_incremental() %}
    WHERE communication_datetime >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    
    GROUP BY 
        date,
        user_id,
        communication_type,
        communication_direction,
        communication_category,
        total_count,
        successful_count
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
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM DailyCommunications dc
LEFT JOIN ResponseMetrics rm
    ON dc.date = rm.date
    AND dc.user_id = rm.user_id
    AND dc.communication_type = rm.communication_type
    AND dc.communication_direction = rm.communication_direction
    AND dc.communication_category = rm.communication_category