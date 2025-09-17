{{ config(
    materialized='table',
    unique_key='template_id'
) }}

WITH PbNTemplates AS (
    -- Extract templates from PbN campaign messages
    SELECT DISTINCT
        md5(cast(coalesce(cast(campaign_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS template_id,
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
        campaign_data.subject,
        NULL AS content,
        ARRAY['PATIENT_NAME', 'CLINIC_NAME', 'APPOINTMENT_DATE', 'PROVIDER_NAME']::text[] AS variables,
        TRUE AS is_active,
        NULL::integer AS created_by,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM (
        SELECT DISTINCT
            SPLIT_PART(content, 'campaign ', 2) AS campaign_name,
            SPLIT_PART(SPLIT_PART(content, 'Subject:', 2), '¶', 1) AS subject
        FROM {{ ref('int_patient_communications_base') }}
        WHERE content LIKE '%Email sent via PbN for campaign%'
        LIMIT 100  -- Limit for testing
    ) campaign_data
),

TextTemplates AS (
    -- Define standard text message templates
    SELECT DISTINCT
        md5(cast(coalesce(cast(template_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(content_pattern as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS template_id,
        template_type AS template_name,
        'SMS' AS template_type,
        'appointment' AS category,
        NULL AS subject,
        content_pattern AS content,
        ARRAY['PATIENT_NAME', 'PHONE_NUMBER', 'CLINIC_NAME']::text[] AS variables,
        TRUE AS is_active,
        NULL::integer AS created_by,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM (
        SELECT
            'Appointment Reminder' AS template_type,
            'Hello {PATIENT_NAME}! Appointment reminder.' AS content_pattern
        UNION ALL
        SELECT
            'Missed Call Response' AS template_type,
            'We missed your call. We will call back soon.' AS content_pattern
    ) template_patterns
)

-- Combine template sources (WITHOUT CommunicationTemplates for now)
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
    current_timestamp as _transformed_at
FROM (
    SELECT * FROM PbNTemplates
    UNION ALL
    SELECT * FROM TextTemplates
) templates