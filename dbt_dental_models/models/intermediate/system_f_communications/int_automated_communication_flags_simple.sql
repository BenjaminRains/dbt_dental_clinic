{{ config(
    materialized='incremental',
    unique_key='communication_id',
    indexes=[
        {'columns': ['communication_datetime']},
        {'columns': ['direction']},
        {'columns': ['patient_id']}
    ],
    incremental_strategy='delete+insert'
) }}

/*
    Simplified model for automated communication flags
    Part of System F: Communications
    
    This model provides a streamlined approach to automated communication detection
    with optimized performance and comprehensive feature coverage.
    
    This model:
    1. Adds automation detection flags to communications using pattern matching
    2. Identifies likely automated messages using content patterns and batch detection
    3. Provides engagement metrics including open, click, reply, and bounce tracking
    4. Supports analysis of automated communication effectiveness
    5. Includes campaign type classification for better analytics
    
    Performance Features:
    - Optimized batch detection to identify mass communications
    - Efficient reply tracking with time-windowed matching
    - Incremental processing for large datasets
    - Indexed columns for fast querying
*/

WITH BaseCommunications AS (
    -- Get base communications with basic filtering
    SELECT
        base.communication_id,
        base.patient_id,
        base.communication_datetime,
        base.content,
        base.direction,
        base.program_id,
        base.communication_mode,
        base.outcome
    FROM {{ ref('int_patient_communications_base') }} base
    WHERE base.direction = 'outbound'
    {% if is_incremental() %}
    AND base.communication_datetime > (
        SELECT COALESCE(MAX(communication_datetime), '2020-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
    -- No limit - process all outbound communications
),

BatchDetection AS (
    -- Basic batch detection - find content sent to multiple patients
    SELECT 
        content,
        communication_datetime,
        COUNT(DISTINCT patient_id) as patient_count
    FROM BaseCommunications
    GROUP BY content, communication_datetime
    HAVING COUNT(DISTINCT patient_id) > 3  -- Content sent to 4+ patients is likely automated
    -- No limit - detect all batch communications
),

ReplyTracking AS (
    -- Basic reply tracking - find patient responses to outbound communications
    SELECT 
        comm.communication_id,
        comm.patient_id,
        MAX(CASE 
            WHEN reply.commlog_id IS NOT NULL THEN 1 
            ELSE 0 
        END) as has_reply
    FROM BaseCommunications comm
    LEFT JOIN (
        SELECT 
            commlog_id,
            patient_id,
            communication_datetime,
            note
        FROM {{ ref('stg_opendental__commlog') }}
        WHERE sent_or_received_raw = 1  -- Inbound messages only
        AND mode IN (1, 5)  -- Only email (1) and text (5) for replies
        AND note IS NOT NULL
        AND communication_datetime >= '2020-01-01'  -- Limit lookback for performance
    ) reply
        ON reply.patient_id = comm.patient_id
        AND reply.communication_datetime BETWEEN comm.communication_datetime 
            AND comm.communication_datetime + INTERVAL '7 days'  -- 7-day response window
    GROUP BY comm.communication_id, comm.patient_id
    -- No limit - track all replies
),

SimpleFlags AS (
    -- Enhanced version with batch detection
    SELECT
        comm.communication_id,
        comm.patient_id,
        comm.communication_datetime,
        comm.content,
        comm.direction,
        comm.program_id,
        comm.communication_mode,
        comm.outcome,
        
        -- Enhanced automation detection with batch detection
        CASE
            WHEN comm.content LIKE 'Patient Text Sent via PbN%' THEN TRUE
            WHEN comm.content LIKE 'Email sent via PbN for campaign%' THEN TRUE
            WHEN comm.content LIKE '%Reply pause to unsubscribe%' THEN TRUE
            WHEN comm.content LIKE '%this is%Merrillville Dental Center%' THEN TRUE
            WHEN comm.content LIKE '%Text message sent:%' THEN TRUE
            WHEN comm.content LIKE '%Merrillville Dental Center%' AND (
                comm.content LIKE '%appointment%' OR 
                comm.content LIKE '%balance%' OR 
                comm.content LIKE '%forms%' OR
                comm.content LIKE '%review%' OR
                comm.content LIKE '%payment%' OR
                comm.content LIKE '%confirm%'
            ) THEN TRUE
            WHEN comm.program_id IS NOT NULL THEN TRUE
            WHEN EXISTS (
                SELECT 1
                FROM BatchDetection batch
                WHERE batch.content = comm.content
                AND batch.communication_datetime BETWEEN comm.communication_datetime - INTERVAL '5 minutes'
                    AND comm.communication_datetime + INTERVAL '5 minutes'
            ) THEN TRUE
            ELSE FALSE
        END AS is_automated,
        
        -- Enhanced trigger type detection
        CASE 
            -- Appointment related triggers
            WHEN comm.content LIKE '%appointment%' AND (
                comm.content LIKE '%starting on%' OR 
                comm.content LIKE '%scheduled for%' OR
                comm.content LIKE '%tomorrow%' OR
                comm.content LIKE '%confirm%' OR
                comm.content LIKE '%attempted to contact%'
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
                 comm.content LIKE '%trusting us%' THEN 'review_request'
            WHEN comm.content LIKE '%forms%' AND (
                comm.content LIKE '%complete%' OR 
                comm.content LIKE '%update%' OR
                comm.content LIKE '%new patient%'
            ) THEN 'form_request'
            
            -- Clinical triggers
            WHEN comm.content LIKE '%post operative%' OR 
                 comm.content LIKE '%instructions%' OR
                 comm.content LIKE '%Crown%' THEN 'post_op_instructions'
            
            ELSE 'other'
        END as trigger_type,
        
        -- Simple status
        CASE
            WHEN comm.outcome IN ('confirmed', 'rescheduled') THEN 'responded_positive'
            WHEN comm.outcome = 'cancelled' THEN 'responded_negative'
            WHEN comm.outcome = 'no_answer' THEN 'undelivered'
            ELSE 'sent'
        END AS status,
        
        -- Enhanced engagement metrics with reply tracking
        CASE
            WHEN comm.content LIKE '%opened%' THEN 1
            ELSE 0
        END AS open_count,
        CASE
            WHEN comm.content LIKE '%clicked%' THEN 1
            ELSE 0
        END AS click_count,
        COALESCE(reply.has_reply, 0) AS reply_count,
        CASE
            WHEN comm.content LIKE '%bounce%' THEN 1
            ELSE 0
        END AS bounce_count,
        
        -- Generate simple ID
        comm.communication_id::varchar AS automated_communication_id,
        
        -- Campaign type detection
        CASE
            WHEN comm.content LIKE '%appointment%' THEN 'appointment_campaign'
            WHEN comm.content LIKE '%balance%' OR comm.content LIKE '%payment%' THEN 'financial_campaign'
            WHEN comm.content LIKE '%review%' OR comm.content LIKE '%experience%' THEN 'review_campaign'
            WHEN comm.content LIKE '%forms%' THEN 'form_campaign'
            WHEN comm.content LIKE '%post operative%' OR comm.content LIKE '%instructions%' THEN 'clinical_campaign'
            WHEN comm.program_id IS NOT NULL THEN 'program_campaign'
            ELSE 'general_campaign'
        END AS campaign_type,
        
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at
    FROM BaseCommunications comm
    LEFT JOIN ReplyTracking reply
        ON reply.communication_id = comm.communication_id
)

-- Final selection
SELECT
    communication_id,
    patient_id,
    automated_communication_id,
    direction,
    is_automated,
    trigger_type,
    campaign_type,
    status,
    open_count,
    click_count,
    reply_count,
    bounce_count,
    communication_datetime,
    communication_mode,
    model_created_at,
    model_updated_at
FROM SimpleFlags
WHERE is_automated = TRUE
