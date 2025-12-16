{{ config(
    materialized='table',
    unique_key='template_id'
) }}

{# 
    Note: GIN index with gin_trgm_ops removed due to extension compatibility issues.
    The index can be created manually if needed:
    CREATE INDEX int_communication_templates_content_gin_idx 
    ON intermediate.int_communication_templates 
    USING gin (content gin_trgm_ops);
#}

/*
    Note: The pg_trgm extension must be created at the database level before running this model.
    You can create it using:
    CREATE EXTENSION IF NOT EXISTS pg_trgm;
    
    This should be done by a database administrator or through your database migration process.
*/

/*
    Intermediate model for communication templates
    Part of System F: Communications
    
    This model:
    1. Extracts templates from Practice by Numbers (PbN) campaign messages
    2. Includes standardized text message templates
    3. Properly categorizes templates by type and purpose
    4. Supports template variables for personalization
*/

WITH pbn_templates AS (
    -- Extract templates from PbN campaign messages
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['campaign_name', 'subject']) }} AS template_id,
        campaign_name AS template_name,
        'email' AS template_type,
        CASE 
            WHEN campaign_name LIKE '%Appointment%' THEN 'appointment'
            WHEN campaign_name LIKE '%Accounts%' THEN 'billing'
            WHEN campaign_name LIKE '%Treatment%' THEN 'clinical'
            WHEN campaign_name LIKE '%Form%' THEN 'forms'
            WHEN campaign_name LIKE '%Recall%' THEN 'recall'
            ELSE 'general'
        END AS category,
        subject,
        NULL AS content,  -- Content would need to be extracted from a different source
        ARRAY['PATIENT_NAME', 'CLINIC_NAME', 'APPOINTMENT_DATE', 'PROVIDER_NAME']::text[] AS variables,
        TRUE AS is_active,
        NULL AS created_by,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM (
        SELECT DISTINCT
            SPLIT_PART(content, 'campaign ', 2) AS campaign_name,
            SPLIT_PART(SPLIT_PART(content, 'Subject:', 2), '¶', 1) AS subject
        FROM {{ ref('int_patient_communications_base') }}
        WHERE content LIKE '%Email sent via PbN for campaign%'
    ) campaign_data
),

text_templates AS (
    -- Define standard text message templates
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['template_type', 'content_pattern']) }} AS template_id,
        template_type AS template_name,
        'SMS' AS template_type,
        'appointment' AS category,
        NULL AS subject,
        content_pattern AS content,
        ARRAY['PHONE_NUMBER', 'CLINIC_NAME']::text[] AS variables,
        TRUE AS is_active,
        NULL AS created_by,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM (
        SELECT 
            'Appointment Reminder' AS template_type,
            'Hello {PATIENT_NAME}! It''s time for a dental cleaning and checkup. Please call us now at {PHONE_NUMBER} to schedule your appointment. {CLINIC_NAME}' AS content_pattern
        UNION ALL
        SELECT 
            'Missed Call Response' AS template_type,
            'We are sorry that we missed your phone call. We are currently helping other patients. We will call you back as soon as we are able to or you can send us a text message here. {CLINIC_NAME}' AS content_pattern
        UNION ALL
        SELECT 
            'Opt-in Confirmation' AS template_type,
            'Thank you for opting in to receive text messages from {CLINIC_NAME}. You will now receive appointment reminders and important updates via text.' AS content_pattern
        UNION ALL
        SELECT 
            'Opt-out Confirmation' AS template_type,
            'You have been unsubscribed from receiving text messages from {CLINIC_NAME}. You will no longer receive appointment reminders or updates via text.' AS content_pattern
    ) template_patterns
),

-- Additional templates derived from communication logs
communication_templates AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['communication_type', 'communication_category', 'content_pattern']) }} AS template_id,
        'Auto-detected ' || 
        CASE
            WHEN communication_category = 'appointment' THEN 'Appointment'
            WHEN communication_category = 'billing' THEN 'Billing'
            WHEN communication_category = 'clinical' THEN 'Clinical'
            WHEN communication_category = 'insurance' THEN 'Insurance'
            WHEN communication_category = 'follow_up' THEN 'Follow-up'
            ELSE 'General'
        END || ' Template' AS template_name,
        CASE 
            WHEN communication_mode = 1 THEN 'email'  -- Email communications
            WHEN communication_mode = 3 THEN 'letter' -- Phone calls
            WHEN communication_mode = 4 THEN 'SMS'    -- Text messages
            WHEN communication_mode = 5 THEN 'SMS'    -- Alternative text message mode
            ELSE NULL
        END AS template_type,
        communication_category::text AS category,
        'Auto-detected ' || communication_category || ' communication' AS subject,
        -- Use a representative content example
        MIN(content) AS content,
        -- Placeholder for variables detection
        ARRAY['DATE', 'PROVIDER']::text[] AS variables,
        TRUE::boolean AS is_active,
        -- Only set created_by if user_id exists in stg_opendental__userod
        CASE 
            WHEN MIN(user_id) IN (SELECT user_id FROM {{ ref('stg_opendental__userod') }}) THEN MIN(user_id)
            ELSE NULL
        END::integer AS created_by,
        MIN(created_at) AS created_at,
        MAX(updated_at) AS updated_at
    FROM (
        SELECT
            communication_type,
            communication_mode,
            communication_category,
            -- Group similar content patterns
            LEFT(content, 50) AS content_pattern,
            content,
            user_id,
            _created_at as created_at,
            updated_at,
            COUNT(*) AS frequency
        FROM {{ ref('int_patient_communications_base') }}
        WHERE content IS NOT NULL
        GROUP BY 
            communication_type,
            communication_mode,
            communication_category,
            LEFT(content, 50),
            content,
            user_id,
            _created_at,
            updated_at
        HAVING COUNT(*) > 3  -- Only include patterns used multiple times
    ) AS pattern_detection
    GROUP BY 
        communication_type,
        communication_mode,
        communication_category,
        content_pattern
)

-- Combine all template sources
SELECT
    template_id::text,
    template_name::text,
    template_type::text,
    category::text,
    subject::text,
    content::text,
    variables::text[],
    is_active::boolean,
    created_by::integer,
    created_at::timestamp,
    updated_at::timestamp,
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM pbn_templates

UNION ALL

SELECT
    template_id::text,
    template_name::text,
    template_type::text,
    category::text,
    subject::text,
    content::text,
    variables::text[],
    is_active::boolean,
    created_by::integer,
    created_at::timestamp,
    updated_at::timestamp,
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM text_templates

UNION ALL

SELECT
    template_id::text,
    template_name::text,
    template_type::text,
    category::text,
    subject::text,
    content::text,
    variables::text[],
    is_active::boolean,
    created_by::integer,
    created_at::timestamp,
    updated_at::timestamp,
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM communication_templates
