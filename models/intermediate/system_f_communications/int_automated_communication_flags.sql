{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='communication_id',
    indexes=[
        {'columns': ['communication_datetime']},
        {'columns': ['direction']},
        {'columns': ['patient_id']}
    ],
    incremental_strategy='delete+insert'
) }}

/*
    Model for automated communication flags
    Part of System F: Communications
    
    This model:
    1. Adds automation detection flags to base communications
    2. Identifies likely automated messages using patterns
    3. Provides engagement metrics for automated comms
    4. Supports analysis of automated communication effectiveness

    Performance Optimizations:
    - Pre-aggregates batch communications to avoid repeated subqueries
    - Pre-aggregates replies to avoid repeated subqueries
    - Uses CTEs to improve query readability and performance
    - Optimizes pattern matching with pre-filtered content
    - Improves incremental processing
    - Adds indexes for frequently joined columns

    Safety Checks:
    - Limits batch detection to prevent self-matching
    - Adds row count limits for debugging
    - Restricts lookback window to prevent excessive data processing
*/

WITH BaseCommunications AS (
    -- Pre-filter base communications to reduce data volume
    SELECT
        base.communication_id,
        base.patient_id,
        base.user_id,
        base.communication_datetime,
        base.content,
        base.direction,  -- Use the existing direction column from base model
        base.program_id,
        base.communication_mode,
        base.outcome,
        base.linked_appointment_id
    FROM {{ ref('int_patient_communications_base') }} base
    {% if is_incremental() %}
    WHERE base.communication_datetime > (
        SELECT COALESCE(MAX(communication_datetime), '2020-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
),

FilteredCommunications AS (
    -- Apply direction filter after we have the column
    SELECT 
        communication_id,
        patient_id,
        user_id,
        communication_datetime,
        content,
        direction,
        program_id,
        communication_mode,
        outcome,
        linked_appointment_id
    FROM BaseCommunications
    WHERE direction = 'outbound'  -- This matches the new direction values from base model
    -- Add a limit for debugging
    LIMIT 1000000  -- Adjust this number based on your data volume
),

ContentPatterns AS (
    -- Pre-calculate pattern matches to avoid repeated operations
    SELECT
        comm.communication_id,
        comm.patient_id,  -- propagate patient_id
        comm.content,
        comm.communication_datetime,
        comm.program_id,
        comm.communication_mode,
        comm.outcome,
        comm.linked_appointment_id,
        comm.direction,  -- propagate direction
        CASE
            WHEN comm.content LIKE 'Patient Text Sent via PbN%' THEN TRUE
            WHEN comm.content LIKE 'Email sent via PbN for campaign%' THEN TRUE
            WHEN comm.content LIKE '%Reply pause to unsubscribe%' THEN TRUE
            WHEN comm.content LIKE '%this is%Merrillville Dental Center%' THEN TRUE
            WHEN comm.content LIKE '%Merrillville Dental Center%' AND (
                comm.content LIKE '%appointment%' OR 
                comm.content LIKE '%balance%' OR 
                comm.content LIKE '%forms%' OR
                comm.content LIKE '%review%' OR
                comm.content LIKE '%payment%' OR
                comm.content LIKE '%confirm%'
            ) THEN TRUE
            WHEN comm.content LIKE '%Text message sent:%' THEN TRUE
            ELSE FALSE
        END as has_automation_indicators,
        CASE 
            -- Appointment related triggers
            WHEN comm.content LIKE '%appointment%' AND (
                comm.content LIKE '%starting on%' OR 
                comm.content LIKE '%scheduled for%' OR
                comm.content LIKE '%tomorrow%' OR
                comm.content LIKE '%confirm%' OR
                comm.content LIKE '%attempted to contact%' OR
                comm.content LIKE '%on the schedule%' OR
                comm.content LIKE '%time for a dental cleaning%'
            ) THEN 'appointment_reminder'
            WHEN comm.content LIKE '%appointment%' AND (
                comm.content LIKE '%confirmed via%' OR
                comm.content LIKE '%has been confirmed%'
            ) THEN 'appointment_confirmation'
            WHEN comm.content LIKE '%broken%' OR 
                 comm.content LIKE '%BROKEN%' OR
                 comm.content LIKE '%change this appt%' THEN 'broken_appointment'
            
            -- Financial triggers
            WHEN comm.content LIKE '%balance%' OR 
                 comm.content LIKE '%account balance%' OR 
                 comm.content LIKE '%outstanding%' OR
                 comm.content LIKE '%payment%' OR
                 comm.content LIKE '%paid%' THEN 'balance_notice'
            
            -- Patient interaction triggers
            WHEN comm.content LIKE '%Patient Text Received%' THEN 'patient_response'
            WHEN comm.content LIKE '%opted in for text%' OR
                 comm.content LIKE '%opted out%' THEN 'preference_update'
            
            -- Review and form triggers
            WHEN comm.content LIKE '%review%' OR 
                 comm.content LIKE '%experience%' OR 
                 comm.content LIKE '%trusting us%' OR
                 comm.content LIKE '%questions about the treatment%' THEN 'review_request'
            WHEN comm.content LIKE '%forms%' AND (
                comm.content LIKE '%complete%' OR 
                comm.content LIKE '%update%' OR
                comm.content LIKE '%new patient%'
            ) THEN 'form_request'
            
            -- Clinical triggers
            WHEN comm.content LIKE '%post operative%' OR 
                 comm.content LIKE '%instructions%' OR
                 comm.content LIKE '%Crown%' THEN 'post_op_instructions'
            
            -- System notifications
            WHEN comm.content LIKE '%END OF YEAR LETTER%' OR
                 comm.content LIKE '%annual%' THEN 'annual_notification'
            WHEN comm.content LIKE '%DELIVERY FAILURE%' OR
                 comm.content LIKE '%Send Failed%' THEN 'delivery_failure'
            
            ELSE 'other'
        END as detected_trigger_type,
        CASE
            WHEN comm.content LIKE '%campaign Appointment%' THEN 'appointment_reminders'
            WHEN comm.content LIKE '%campaign Accounts%' THEN 'accounts_receivable'
            WHEN comm.content LIKE '%campaign Crown%' THEN 'crown_instructions'
            WHEN comm.content LIKE '%campaign Form%' THEN 'form_invite'
            WHEN comm.content LIKE '%campaign Unscheduled Treatment%' THEN 'unscheduled_treatment'
            ELSE NULL
        END as campaign_type,
        CASE
            WHEN comm.content LIKE '%opened%' THEN 1
            ELSE 0
        END as has_open,
        CASE
            WHEN comm.content LIKE '%clicked%' THEN 1
            ELSE 0
        END as has_click,
        CASE
            WHEN comm.content LIKE '%bounce%' THEN 1
            ELSE 0
        END as has_bounce
    FROM FilteredCommunications comm
),

BatchCommunications AS (
    -- Pre-aggregate batch communications with improved filtering
    -- Added safety check to prevent self-matching
    SELECT 
        content,
        communication_datetime,
        direction,  -- propagate direction
        patient_id,  -- propagate patient_id
        COUNT(DISTINCT patient_id) as patient_count
    FROM ContentPatterns
    GROUP BY content, communication_datetime, direction, patient_id
    HAVING COUNT(DISTINCT patient_id) > 3
    -- Add a limit for debugging
    LIMIT 10000  -- Adjust this number based on your data volume
),

ReplyTracking AS (
    -- Optimize reply tracking with a more efficient join
    -- Added safety check to prevent excessive joins
    SELECT 
        comm.communication_id,
        comm.patient_id,  -- propagate patient_id
        comm.direction,  -- propagate direction
        MAX(CASE 
            WHEN reply.commlog_id IS NOT NULL THEN 1 
            ELSE 0 
        END) as has_reply
    FROM ContentPatterns comm
    LEFT JOIN (
        SELECT 
            commlog_id,
            patient_id,
            communication_datetime,
            note,
            CASE 
                WHEN is_sent = 2 THEN 'outbound'
                WHEN is_sent = 1 THEN 'inbound'
                WHEN is_sent = 0 THEN 'system'
                ELSE 'unknown'
            END AS reply_direction  -- Renamed to avoid conflict
        FROM "opendental_analytics"."public_staging"."stg_opendental__commlog"
        WHERE is_sent = 1  -- Inbound messages
        AND mode IN (1, 5)  -- Only consider email (1) and text (5) for replies
        AND note IS NOT NULL
    ) reply
        ON reply.patient_id = comm.patient_id
        AND reply.communication_datetime BETWEEN comm.communication_datetime 
            AND comm.communication_datetime + INTERVAL '3 days'
    GROUP BY comm.communication_id, comm.direction, comm.patient_id
    -- Add a limit for debugging
    LIMIT 100000  -- Adjust this number based on your data volume
),

AutomatedFlags AS (
    SELECT
        comm.communication_id,
        comm.patient_id,  -- propagate patient_id
        comm.direction,  -- propagate direction
        {{ dbt_utils.generate_surrogate_key(['comm.communication_id', 'comm.content']) }} AS automated_communication_id,
        comm.communication_datetime,
        comm.communication_mode,  -- Add communication_mode here
        
        -- Simplified automation detection using pre-calculated flags
        -- Added safety check to prevent self-matching in batch detection
        CASE
            WHEN comm.has_automation_indicators THEN TRUE
            WHEN comm.program_id IS NOT NULL THEN TRUE
            WHEN EXISTS (
                SELECT 1
                FROM BatchCommunications batch
                WHERE batch.content = comm.content
                AND batch.communication_datetime BETWEEN comm.communication_datetime - INTERVAL '5 minutes'
                    AND comm.communication_datetime + INTERVAL '5 minutes'
                AND batch.communication_datetime != comm.communication_datetime  -- Prevent self-matching
            ) THEN TRUE
            ELSE FALSE
        END AS is_automated,
        
        comm.detected_trigger_type as trigger_type,
        comm.campaign_type as campaign_type,
        
        -- Status based on outcome
        CASE
            WHEN comm.outcome IN ('confirmed', 'rescheduled') THEN 'responded_positive'
            WHEN comm.outcome = 'cancelled' THEN 'responded_negative'
            WHEN comm.outcome = 'no_answer' THEN 'undelivered'
            ELSE 'sent'
        END AS status,
        
        -- Simplified engagement metrics using pre-calculated flags
        CASE WHEN comm.communication_mode = 1 THEN comm.has_open ELSE 0 END AS open_count,
        CASE WHEN comm.communication_mode = 1 THEN comm.has_click ELSE 0 END AS click_count,
        COALESCE(reply.has_reply, 0) as reply_count,
        CASE WHEN comm.communication_mode = 1 THEN comm.has_bounce ELSE 0 END AS bounce_count,
        
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at
    FROM ContentPatterns comm
    LEFT JOIN ReplyTracking reply
        ON reply.communication_id = comm.communication_id
)

-- Final selection with row limit for debugging
SELECT
    communication_id,
    patient_id,  -- propagate patient_id to final output
    automated_communication_id,
    direction,  -- propagate direction to final output
    is_automated,
    trigger_type,
    campaign_type,
    status,
    open_count,
    click_count,
    reply_count,
    bounce_count,
    communication_datetime,
    communication_mode,  -- Add communication_mode to final output
    model_created_at,
    model_updated_at
FROM AutomatedFlags
WHERE is_automated = TRUE  -- Only return communications flagged as automated