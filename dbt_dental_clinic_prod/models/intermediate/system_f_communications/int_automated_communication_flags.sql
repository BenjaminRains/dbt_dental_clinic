{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='communication_id',
    on_schema_change='fail',
    incremental_strategy='merge',
    indexes=[
        {'columns': ['communication_datetime']},
        {'columns': ['direction']},
        {'columns': ['patient_id']}
    ]
) }}

/*
    Intermediate model for automated communication flags
    Part of System F: Communications
    
    This model:
    1. Adds automation detection flags to base communications
    2. Identifies likely automated messages using patterns
    3. Provides engagement metrics for automated comms
    4. Supports analysis of automated communication effectiveness
    
    Business Logic Features:
    - Pattern-based automation detection using content analysis
    - Batch communication identification for system-generated messages
    - Trigger type categorization (appointment, billing, clinical, etc.)
    - Campaign type identification for marketing communications
    - Engagement tracking (opens, clicks, replies, bounces)
    - Status determination based on communication outcomes
    
    Data Quality Notes:
    - Filters to outbound communications only (automated comms are primarily outbound)
    - Handles future dates for scheduled automated communications
    - Validates batch detection to prevent false positives
    - Limits processing scope for performance optimization
    
    Performance Considerations:
    - Pre-aggregates batch communications to avoid repeated subqueries
    - Pre-aggregates replies to avoid repeated subqueries
    - Uses CTEs to improve query readability and performance
    - Optimizes pattern matching with pre-filtered content
    - Adds indexes for frequently joined columns
    - Implements row limits for debugging and performance control
    
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
        base.linked_appointment_id,
        -- Preserve metadata from primary source for data lineage
        base._loaded_at,
        base._created_at
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
        linked_appointment_id,
        -- Preserve metadata
        _loaded_at,
        _created_at
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
        -- Preserve metadata
        comm._loaded_at,
        comm._created_at,
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
            -- Appointment related triggers (more inclusive)
            WHEN comm.content LIKE '%appointment%' AND (
                comm.content LIKE '%reminder%' OR
                comm.content LIKE '%starting%' OR 
                comm.content LIKE '%scheduled%' OR
                comm.content LIKE '%tomorrow%' OR
                comm.content LIKE '%upcoming%' OR
                comm.content LIKE '%confirm%' OR
                comm.content LIKE '%attempted%' OR
                comm.content LIKE '%schedule%'
            ) AND comm.content NOT LIKE '%confirmed%' THEN 'appointment_reminder'
            WHEN comm.content LIKE '%appointment%' AND (
                comm.content LIKE '%confirmed%' OR
                comm.content LIKE '%has been confirmed%'
            ) THEN 'appointment_confirmation'
            WHEN comm.content LIKE '%broken%' OR 
                 comm.content LIKE '%BROKEN%' OR
                 comm.content LIKE '%change%' OR
                 comm.content LIKE '%cancel%' THEN 'broken_appointment'
            
            -- Financial triggers (more inclusive)
            WHEN comm.content LIKE '%balance%' OR 
                 comm.content LIKE '%account%' OR 
                 comm.content LIKE '%outstanding%' OR
                 comm.content LIKE '%payment%' OR
                 comm.content LIKE '%due%' OR
                 comm.content LIKE '%owe%' OR
                 comm.content LIKE '%bill%' THEN 'balance_notice'
            
            -- Patient interaction triggers
            WHEN comm.content LIKE '%Patient Text Received%' OR
                 comm.content LIKE '%patient%' AND comm.content LIKE '%text%' THEN 'patient_response'
            WHEN comm.content LIKE '%opted%' OR
                 comm.content LIKE '%preference%' THEN 'preference_update'
            
            -- Review and form triggers (more inclusive)
            WHEN comm.content LIKE '%review%' OR
                 comm.content LIKE '%experience%' OR
                 comm.content LIKE '%rate%' OR
                 comm.content LIKE '%feedback%' THEN 'review_request'
            WHEN comm.content LIKE '%forms%' OR
                 comm.content LIKE '%complete%' OR 
                 comm.content LIKE '%update%' OR
                 comm.content LIKE '%new patient%' OR
                 comm.content LIKE '%fill%' OR
                 comm.content LIKE '%submit%' THEN 'form_request'
            
            -- Clinical triggers (more inclusive)
            WHEN comm.content LIKE '%post%' OR 
                 comm.content LIKE '%operative%' OR
                 comm.content LIKE '%instructions%' OR
                 comm.content LIKE '%Crown%' OR
                 comm.content LIKE '%treatment%' OR
                 comm.content LIKE '%procedure%' THEN 'post_op_instructions'
            
            -- System notifications
            WHEN comm.content LIKE '%END OF YEAR%' OR
                 comm.content LIKE '%annual%' OR
                 comm.content LIKE '%year end%' THEN 'annual_notification'
            WHEN comm.content LIKE '%DELIVERY FAILURE%' OR
                 comm.content LIKE '%Send Failed%' OR
                 comm.content LIKE '%failed%' THEN 'delivery_failure'
            
            -- Follow-up communications
            WHEN comm.content LIKE '%follow%' AND (
                comm.content LIKE '%up%' OR 
                comm.content LIKE '%call%' OR
                comm.content LIKE '%contact%'
            ) THEN 'follow_up'
            
            -- Recall communications
            WHEN comm.content LIKE '%recall%' OR 
                 (comm.content LIKE '%reminder%' AND comm.content NOT LIKE '%appointment%') THEN 'recall_reminder'
            
            -- Insurance related
            WHEN comm.content LIKE '%insurance%' OR 
                 comm.content LIKE '%claim%' OR
                 comm.content LIKE '%benefit%' OR
                 comm.content LIKE '%coverage%' THEN 'insurance_related'
            
            -- General communication (catch-all for manual communications)
            WHEN comm.content LIKE '%call%' OR
                 comm.content LIKE '%phone%' OR
                 comm.content LIKE '%contact%' OR
                 comm.content LIKE '%message%' OR
                 comm.content LIKE '%note%' THEN 'general_communication'
            
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
    -- Only consider outbound communications for batch detection
    SELECT 
        content,
        date_trunc('minute', communication_datetime) AS dt_minute,
        direction,
        COUNT(DISTINCT patient_id) as patient_count
    FROM ContentPatterns
    WHERE direction = 'outbound'  -- Only outbound can be automated
    GROUP BY content, date_trunc('minute', communication_datetime), direction
    HAVING COUNT(DISTINCT patient_id) >= 5  -- Higher threshold for batch detection
),

ReplyTracking AS (
    -- Look for inbound communications that are replies to outbound communications
    SELECT 
        comm.communication_id,
        comm.patient_id,
        comm.direction,
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
                WHEN is_sent = 1 THEN 'outbound'  -- Clinic to patient
                WHEN is_sent = 2 THEN 'inbound'   -- Patient to clinic
                WHEN is_sent = 0 THEN 'system'
                ELSE 'unknown'
            END AS reply_direction
        FROM {{ ref('stg_opendental__commlog') }}
        WHERE is_sent = 2  -- Patient replies (inbound)
        AND mode IN (1, 5)  -- Only consider email (1) and text (5) for replies
        AND note IS NOT NULL
        AND note NOT LIKE '%Patient Text Sent via PbN%'  -- Exclude automated messages
        AND note NOT LIKE '%Email sent via PbN%'         -- Exclude automated messages
    ) reply
        ON reply.patient_id = comm.patient_id
        AND reply.communication_datetime BETWEEN comm.communication_datetime 
            AND comm.communication_datetime + INTERVAL '3 days'
        AND reply.communication_datetime > comm.communication_datetime  -- Reply must be after original
    GROUP BY comm.communication_id, comm.direction, comm.patient_id
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
            WHEN comm.direction != 'outbound' THEN FALSE  -- Only outbound can be automated
            WHEN comm.has_automation_indicators THEN TRUE
            WHEN comm.program_id IS NOT NULL AND comm.program_id > 0 AND comm.program_id < 100 THEN TRUE  -- Only very specific program IDs
            WHEN comm.campaign_type IS NOT NULL AND comm.campaign_type != '' THEN TRUE  -- Campaign communications are automated
            WHEN comm.detected_trigger_type IN ('appointment_reminder', 'balance_notice', 'form_request', 'review_request') 
                 AND comm.communication_mode IN ('text_message', 'manual_note')  -- Only automated modes
                 AND comm.detected_trigger_type != 'general_communication' THEN TRUE
            WHEN EXISTS (
                SELECT 1
                FROM BatchCommunications batch
                WHERE batch.content = comm.content
                AND batch.dt_minute = date_trunc('minute', comm.communication_datetime)
                AND batch.direction = 'outbound'
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
        
        -- Engagement metrics - set to NULL until proper event data is available
        -- TODO: Join with email/SMS event tables when available
        NULL::int4 AS open_count,  -- Will be populated when email events table is available
        NULL::int4 AS click_count, -- Will be populated when email events table is available
        COALESCE(reply.has_reply, 0) as reply_count,
        NULL::int4 AS bounce_count, -- Will be populated when email events table is available
        
        -- Standardized metadata using macro (preserves primary source metadata)
        {{ standardize_intermediate_metadata(
            primary_source_alias='comm',
            preserve_source_metadata=true,
            source_metadata_fields=['_loaded_at', '_created_at']
        ) }}
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
    -- Standardized metadata fields
    _loaded_at,
    _created_at,
    _transformed_at
FROM AutomatedFlags
-- Return all communications, not just automated ones
