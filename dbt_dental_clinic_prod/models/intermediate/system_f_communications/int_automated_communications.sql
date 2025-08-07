{{ config(
    materialized='incremental',
    
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
    SELECT
        -- Use the automated_communication_id from flags
        flags.automated_communication_id,
        
        -- Base communication data
        base.communication_id,
        base.patient_id,
        base.user_id,
        base.communication_datetime,
        base.communication_type,
        base.communication_mode,
        base.content,
        base.linked_appointment_id,
        base.linked_claim_id,
        base.linked_procedure_id,
        base.contact_phone_number,
        base.communication_category,
        base.outcome,
        base.program_id,
        
        -- Patient context
        base.patient_name,
        base.patient_status,
        base.birth_date,
        
        -- User context
        base.user_name,
        base.provider_id,
        
        -- Automation flags and metrics
        flags.trigger_type,
        flags.status,
        flags.open_count,
        flags.click_count,
        flags.reply_count,
        flags.bounce_count,
        
        -- Template information
        tmpl.template_id,
        tmpl.template_name,
        tmpl.template_type,
        tmpl.category AS template_category,
        tmpl.subject AS template_subject,
        
        -- Metadata
        base.created_at,
        base.updated_at,
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at,
        
        -- Add a row number to handle multiple template matches
        ROW_NUMBER() OVER (
            PARTITION BY base.communication_id 
            ORDER BY 
                -- Prioritize content similarity matches
                CASE 
                    WHEN {% if target.type == 'postgres' %}
                         similarity(base.content, tmpl.content) > 0.4
                    {% else %}
                         base.content LIKE '%' || LEFT(tmpl.content, 20) || '%'
                    {% endif %}
                    THEN 1
                    ELSE 2
                END,
                -- Then by template creation date (newer templates preferred)
                tmpl.created_at DESC
        ) as template_rank
    FROM {{ ref('int_automated_communication_flags') }} flags
    INNER JOIN {{ ref('int_patient_communications_base') }} base
        ON flags.communication_id = base.communication_id
    LEFT JOIN {{ ref('int_communication_templates') }} tmpl
        ON (
            -- Match based on content similarity using trigram
            {% if target.type == 'postgres' %}
            similarity(base.content, tmpl.content) > 0.4
            {% else %}
            -- Fallback to LIKE matching for non-PostgreSQL databases
            base.content LIKE '%' || LEFT(tmpl.content, 20) || '%'
            {% endif %}
            -- Only match active templates
            AND tmpl.is_active = TRUE
        )
        OR (
            -- Fallback to category/mode matching if no content match
            base.communication_category = tmpl.category 
            AND tmpl.is_active = TRUE
            AND (CASE
                WHEN tmpl.template_type = 'email' THEN 1
                WHEN tmpl.template_type = 'SMS' THEN 5
                WHEN tmpl.template_type = 'letter' THEN 3
                ELSE NULL
            END) = base.communication_mode
        )
    
    -- For incremental models, only process new records
    {% if is_incremental() %}
    WHERE base.communication_datetime > (SELECT MAX(communication_datetime) FROM {{ this }})
    {% endif %}
)

-- Final selection
SELECT
    automated_communication_id,
    communication_id,
    patient_id,
    user_id,
    communication_datetime,
    communication_type,
    communication_mode,
    content,
    linked_appointment_id,
    linked_claim_id,
    linked_procedure_id,
    contact_phone_number,
    communication_category,
    outcome,
    program_id,
    patient_name,
    patient_status,
    birth_date,
    user_name,
    provider_id,
    trigger_type,
    status,
    open_count,
    click_count,
    reply_count,
    bounce_count,
    template_id,
    template_name,
    template_type,
    template_category,
    template_subject,
    created_at,
    updated_at,
    model_created_at,
    model_updated_at
FROM AutomatedComms
WHERE template_rank = 1  -- Only keep the best template match
