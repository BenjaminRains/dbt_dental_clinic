{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='automated_communication_id'
) }}

/*
    Intermediate model for automated communications
    Part of System F: Communications
    
    This model:
    1. Tracks communications sent through automated systems
    2. Links to templates and base communication records
    3. Provides metrics on communication engagement
    4. Supports analysis of automated communication effectiveness
*/

WITH AutomatedComms AS (
    -- Identify likely automated communications from communication logs
    -- We'll look for patterns that suggest automated origin:
    -- 1. Communications sent in batches (multiple identical messages in short time)
    -- 2. Communications with template-like content
    -- 3. Communications linked to specific programs
    SELECT
        -- Generate a surrogate key as the primary identifier
        {{ dbt_utils.generate_surrogate_key(['comm.communication_id', 'tmpl.template_id']) }} AS automated_communication_id,
        comm.communication_id,
        tmpl.template_id,
        CASE 
            WHEN comm.content LIKE '%appointment reminder%' THEN 'appointment_reminder'
            WHEN comm.content LIKE '%recall%' THEN 'recall_notice'
            WHEN comm.content LIKE '%birthday%' THEN 'birthday_greeting'
            WHEN comm.content LIKE '%balance%' THEN 'balance_reminder'
            WHEN comm.content LIKE '%welcome%' THEN 'new_patient_welcome'
            ELSE 'general_notification'
        END AS trigger_type,
        
        -- Extract details about what triggered the communication
        CASE
            WHEN comm.content LIKE '%appointment%' AND comm.linked_appointment_id IS NOT NULL 
                THEN 'appointment_id=' || comm.linked_appointment_id
            WHEN comm.content LIKE '%statement%' AND comm.content ~ '(?:statement|bill)\\s+#\\s*([0-9]{1,7})\\b' 
                THEN 'statement_id=' || REGEXP_REPLACE(SUBSTRING(comm.content FROM '(?:statement|bill)\\s+#\\s*([0-9]{1,7})\\b'), '[^0-9]', '', 'g')
            WHEN comm.content LIKE '%recall%' AND comm.content ~ 'recall\\s+([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})' 
                THEN 'recall_date=' || SUBSTRING(comm.content FROM 'recall\\s+([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})')
            ELSE NULL
        END AS trigger_details,
        
        -- Scheduled time - default to creation time for records without explicit schedule
        comm.created_at AS scheduled_datetime,
        
        -- Sent time - communication_datetime from commlog
        comm.communication_datetime AS sent_datetime,
        
        -- Status - derive from patterns in content and outcome
        CASE
            WHEN comm.outcome IN ('confirmed', 'rescheduled') THEN 'responded_positive'
            WHEN comm.outcome = 'cancelled' THEN 'responded_negative'
            WHEN comm.outcome = 'no_answer' THEN 'undelivered'
            ELSE 'sent'
        END AS status,
        
        -- Metrics - these would ideally come from message tracking systems
        -- For now, we'll estimate based on patterns
        1 AS recipient_count, -- Default to 1 per message, would aggregate batch sends in production
        
        -- Email tracking metrics (placeholder implementation)
        CASE WHEN comm.communication_mode = 'email' AND comm.content LIKE '%opened%' THEN 1 ELSE 0 END AS open_count,
        CASE WHEN comm.communication_mode = 'email' AND comm.content LIKE '%clicked%' THEN 1 ELSE 0 END AS click_count,
        
        -- Reply tracking for all modes
        CASE WHEN EXISTS (
            SELECT 1 FROM {{ ref('int_patient_communications') }} reply
            WHERE reply.patient_id = comm.patient_id
              AND reply.direction = 'inbound'
              AND reply.communication_datetime BETWEEN comm.communication_datetime 
                  AND comm.communication_datetime + INTERVAL '3 days'
        ) THEN 1 ELSE 0 END AS reply_count,
        
        -- Bounce tracking for email
        CASE WHEN comm.communication_mode = 'email' AND comm.content LIKE '%bounce%' THEN 1 ELSE 0 END AS bounce_count,
        
        -- Metadata fields
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at
    FROM {{ ref('int_patient_communications') }} comm
    JOIN {{ ref('int_communication_templates') }} tmpl
        ON (
            -- Match based on content similarity
            comm.content LIKE '%' || LEFT(tmpl.content, 20) || '%'
            -- Or match based on category and type (with explicit type cast for mode)
            OR (comm.communication_category = tmpl.category AND comm.communication_mode::text = tmpl.template_type)
        )
    WHERE 
        -- Filter for likely automated communications
        comm.direction = 'outbound'
        AND (
            -- Communications sent through a program
            comm.program_id IS NOT NULL
            -- Messages with template-like content (checking for template variables with pattern {VARIABLE_NAME})
            OR comm.content ~ '\{[A-Za-z0-9_]+\}'
            OR (comm.content = tmpl.content)
            -- Messages sent in batches (look for identical content sent to multiple patients)
            OR EXISTS (
                SELECT 1
                FROM {{ ref('int_patient_communications') }} batch
                WHERE batch.content = comm.content
                AND batch.communication_datetime BETWEEN comm.communication_datetime - INTERVAL '5 minutes'
                    AND comm.communication_datetime + INTERVAL '5 minutes'
                GROUP BY batch.content, batch.communication_datetime
                HAVING COUNT(DISTINCT batch.patient_id) > 3
            )
        )
    
    -- For incremental models, only process new records
    {% if is_incremental() %}
    AND comm.communication_datetime > (SELECT MAX(sent_datetime) FROM {{ this }})
    {% endif %}
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