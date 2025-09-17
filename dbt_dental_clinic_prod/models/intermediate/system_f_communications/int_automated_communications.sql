{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='automated_communication_id',
    incremental_strategy='merge'
) }}

/*
    Simplified intermediate model for automated communications
    Part of System F: Communications

    This model:
    1. Tracks communications sent through automated systems
    2. Links to base communication records
    3. Provides metrics on communication engagement
    4. Supports analysis of automated communication effectiveness
    
    NOTE: This is a simplified version without template matching for initial build.
    Template matching will be added in a separate model to avoid performance issues.
    
    Business Logic Features:
    - Automated communication identification and categorization
    - Engagement metrics aggregation (opens, clicks, replies, bounces)
    - Metadata preservation for data lineage
    
    Data Quality Notes:
    - Filters to only automated communications (is_automated = true)
    - Preserves metadata from primary source for data lineage
    - Limited to recent data for performance
    
    Performance Considerations:
    - Simplified joins without complex template matching
    - Limited data scope for initial build
    - No expensive trigram similarity calculations
*/

-- Simplified version without template matching
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
    
    -- Primary source metadata (from base communications)
    base._loaded_at,
    base._created_at,
    
    -- Template fields (NULL in simplified version)
    NULL as template_id,
    NULL as template_name,
    NULL as template_type,
    NULL as template_category,
    NULL as template_subject,
    
    -- dbt intermediate model build timestamp (model-specific tracking)
    current_timestamp as _transformed_at
    
FROM {{ ref('int_automated_communication_flags') }} flags
INNER JOIN {{ ref('int_patient_communications_base') }} base
    ON flags.communication_id = base.communication_id
    {% if is_incremental() %}
        AND base.communication_datetime > 
            (SELECT COALESCE(MAX(communication_datetime), '2000-01-01'::timestamp) FROM {{ this }})
    {% else %}
        AND base.communication_datetime >= '2023-01-01'
    {% endif %}
