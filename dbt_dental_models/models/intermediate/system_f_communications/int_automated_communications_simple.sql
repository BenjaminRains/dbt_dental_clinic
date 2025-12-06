{{ config(
    materialized='incremental',
    unique_key='automated_communication_id',
    indexes=[
        {'columns': ['communication_datetime']},
        {'columns': ['patient_id']},
        {'columns': ['communication_id']}
    ],
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

/*
    Simplified intermediate model for automated communications
    Part of System F: Communications

    This model provides a streamlined approach to automated communication tracking
    with optimized performance and comprehensive feature coverage.

    This model:
    1. Tracks communications sent through automated systems
    2. Links to templates and base communication records using efficient matching
    3. Provides comprehensive metrics on communication engagement
    4. Supports analysis of automated communication effectiveness
    5. Includes patient and user context for better analytics
    
    Performance Features:
    - Optimized template matching using category and mode matching
    - Efficient JOINs with proper indexing
    - Incremental processing for large datasets
    - Streamlined query structure for fast execution
*/

WITH template_matches AS (
    -- Pre-select the best template match per communication to avoid Cartesian product
    SELECT 
        flags.communication_id,
        tmpl.template_id,
        tmpl.template_name,
        tmpl.template_type,
        tmpl.category AS template_category,
        tmpl.subject AS template_subject,
        ROW_NUMBER() OVER (
            PARTITION BY flags.communication_id 
            ORDER BY 
                -- Prioritize templates with more specific names
                CASE WHEN tmpl.template_name IS NOT NULL THEN 1 ELSE 2 END,
                -- Then by template ID for consistency
                tmpl.template_id
        ) as template_rank
    FROM {{ ref('int_automated_communication_flags_simple') }} flags
    INNER JOIN {{ ref('int_patient_communications_base') }} base
        ON flags.communication_id = base.communication_id
    LEFT JOIN {{ ref('int_communication_templates') }} tmpl
        ON (
            -- Match based on category and communication mode (faster than similarity)
            base.communication_category = tmpl.category 
            AND tmpl.is_active = TRUE
            AND (CASE
                WHEN tmpl.template_type = 'email' THEN 1
                WHEN tmpl.template_type = 'SMS' THEN 5
                WHEN tmpl.template_type = 'letter' THEN 3
                ELSE NULL
            END) = base.communication_mode
        )
    {% if is_incremental() %}
    WHERE base.communication_datetime > (SELECT MAX(communication_datetime) FROM {{ this }})
    {% endif %}
),

automated_comms AS (
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
        base.patient_status,
        base.birth_date,
        
        -- User context
        base.provider_id,
        
        -- Automation flags and metrics
        flags.trigger_type,
        flags.status,
        flags.open_count,
        flags.click_count,
        flags.reply_count,
        flags.bounce_count,
        
        -- Template information (one per communication)
        tmpl.template_id,
        tmpl.template_name,
        tmpl.template_type,
        tmpl.template_category,
        tmpl.template_subject,
        
        -- Metadata using standardize_intermediate_metadata macro
        {{ standardize_intermediate_metadata(
            primary_source_alias='base',
            preserve_source_metadata=true,
            source_metadata_fields=['_created_at', 'updated_at']
        ) }}
    FROM {{ ref('int_automated_communication_flags_simple') }} flags
    INNER JOIN {{ ref('int_patient_communications_base') }} base
        ON flags.communication_id = base.communication_id
    LEFT JOIN template_matches tmpl
        ON flags.communication_id = tmpl.communication_id
        AND tmpl.template_rank = 1  -- Only get the best template match
    
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
    patient_status,
    birth_date,
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
    _created_at,
    updated_at,
    _transformed_at
FROM automated_comms
