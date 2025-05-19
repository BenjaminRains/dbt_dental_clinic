{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='automated_communication_id'
) }}

/*
    DEPRECATION NOTICE:
    This model is deprecated and will be removed in a future release.
    
    Reason for deprecation:
    - The circular dependency this model was created to solve has been properly resolved
    - The main int_automated_communications model now provides better functionality
    - The int_automated_communication_flags model handles automation detection more robustly
    
    Please use int_automated_communications instead, which provides:
    - Better data quality checks and documentation
    - Proper relationships and tests
    - More comprehensive functionality
    - Better template matching
    - Improved automation detection
    
    Migration path:
    - Replace references to this model with int_automated_communications
    - Update any dependent models to use the new structure
    - Contact the data engineering team if you need assistance with migration
*/

/*
    Simplified version of automated communications model
    Part of System F: Communications
    
    This model:
    1. Tracks communications sent through automated systems
    2. Provides metrics on communication engagement
    3. Supports analysis of automated communication effectiveness
    
    IMPORTANT: This is a simplified version that breaks the circular dependency
    between int_patient_communications → int_communication_templates → int_automated_communications
*/

WITH AutomatedComms AS (
    -- Identify likely automated communications from communication logs directly
    -- without relying on templates for identification
    SELECT
        -- Generate a surrogate key as the primary identifier
        {{ dbt_utils.generate_surrogate_key(['comm_id', 'content_signature']) }} AS automated_communication_id,
        comm_id AS communication_id,
        NULL AS template_id, -- No template linkage in this simplified model
        
        -- Categorize by content patterns
        CASE 
            WHEN content LIKE '%appointment reminder%' THEN 'appointment_reminder'
            WHEN content LIKE '%recall%' THEN 'recall_notice'
            WHEN content LIKE '%birthday%' THEN 'birthday_greeting'
            WHEN content LIKE '%balance%' THEN 'balance_reminder'
            WHEN content LIKE '%welcome%' THEN 'new_patient_welcome'
            ELSE 'general_notification'
        END AS trigger_type,
        
        -- Extract details about what triggered the communication
        CASE
            WHEN content LIKE '%appointment%' AND appt_id IS NOT NULL 
                THEN 'appointment_id=' || appt_id
            WHEN content LIKE '%statement%' AND content ~ '(?:statement|bill)\\s+#\\s*([0-9]{1,7})\\b' 
                THEN 'statement_id=' || REGEXP_REPLACE(SUBSTRING(content FROM '(?:statement|bill)\\s+#\\s*([0-9]{1,7})\\b'), '[^0-9]', '', 'g')
            WHEN content LIKE '%recall%' AND content ~ 'recall\\s+([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})' 
                THEN 'recall_date=' || SUBSTRING(content FROM 'recall\\s+([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})')
            ELSE NULL
        END AS trigger_details,
        
        -- Timestamps
        created_at AS scheduled_datetime,
        comm_datetime AS sent_datetime,
        
        -- Status - derive from patterns in content and outcomes
        CASE
            WHEN outcome IN ('confirmed', 'rescheduled') THEN 'responded_positive'
            WHEN outcome = 'cancelled' THEN 'responded_negative'
            WHEN outcome = 'no_answer' THEN 'undelivered'
            ELSE 'sent'
        END AS status,
        
        -- Metrics
        1 AS recipient_count, -- Default to 1 per message
        
        -- Email tracking metrics (mode = 1 is email)
        CASE WHEN comm_mode = 1 AND content LIKE '%opened%' THEN 1 ELSE 0 END AS open_count,
        CASE WHEN comm_mode = 1 AND content LIKE '%clicked%' THEN 1 ELSE 0 END AS click_count,
        
        -- Reply tracking - simplified
        CASE WHEN outcome IN ('confirmed', 'rescheduled', 'cancelled') THEN 1 ELSE 0 END AS reply_count,
        
        -- Bounce tracking for email
        CASE WHEN comm_mode = 1 AND content LIKE '%bounce%' THEN 1 ELSE 0 END AS bounce_count,
        
        -- Metadata fields
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at
    FROM (
        -- Subquery to get communication logs directly from staging
        SELECT
            cl.commlog_id AS comm_id,
            cl.patient_id,
            cl.communication_datetime AS comm_datetime,
            cl.communication_type AS comm_type,
            cl.mode AS comm_mode,
            cl.is_sent,
            cl.note AS content,
            -- Generate content signature for detecting duplicates
            LEFT(cl.note, 50) AS content_signature,
            -- For appointment linkage
            CASE
                WHEN cl.note ~* '(appt|appointment)\\s+#\\s*([0-9]{1,7})\\b'
                THEN REGEXP_REPLACE(SUBSTRING(cl.note FROM '(appt|appointment)\\s+#\\s*([0-9]{1,7})\\b'), '[^0-9]', '', 'g')::bigint
                ELSE NULL
            END AS appt_id,
            -- Communication outcome
            CASE
                WHEN cl.note LIKE '%confirmed%' THEN 'confirmed'
                WHEN cl.note LIKE '%rescheduled%' THEN 'rescheduled'
                WHEN cl.note LIKE '%cancelled%' THEN 'cancelled'
                WHEN cl.note LIKE '%no answer%' THEN 'no_answer'
                ELSE 'sent'
            END AS outcome,
            cl.created_at
        FROM {{ ref('stg_opendental__commlog') }} cl
        WHERE 
            cl.is_sent = 1 -- Outbound communications only
            AND (
                -- Communications with standard automated patterns
                cl.note ~* '(automated|auto-generated|do not reply|noreply)'
                OR cl.note LIKE '%reminder%'
                OR cl.note LIKE '%notification%'
                OR cl.note LIKE '%recall%'
                OR cl.note LIKE '%confirm%'
                -- Communications with template patterns
                OR cl.note ~ '\\{[A-Za-z0-9_]+\\}'
            )
    ) AS comm_data
    
    -- For incremental models, only process new records
    {% if is_incremental() %}
    WHERE comm_datetime > (SELECT MAX(sent_datetime) FROM {{ this }})
    {% endif %}
    
    -- Group similar automated communications to provide distribution stats
    GROUP BY
        comm_id,
        content_signature,
        patient_id,
        comm_datetime,
        comm_type,
        comm_mode,
        is_sent,
        content,
        appt_id,
        outcome,
        created_at
)

-- Final selection
SELECT
    automated_communication_id,
    communication_id,
    template_id,
    trigger_type,
    trigger_details,
    scheduled_datetime,
    sent_datetime,
    status,
    recipient_count,
    open_count,
    click_count,
    reply_count,
    bounce_count,
    model_created_at,
    model_updated_at
FROM AutomatedComms