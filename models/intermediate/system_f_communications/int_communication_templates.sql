{{ config(
    materialized='table',
    schema='intermediate',
    unique_key='template_id'
) }}

/*
    Intermediate model for communication templates
    Part of System F: Communications
    
    This model:
    1. Consolidates templates used for patient communications
    2. Standardizes template structure and metadata
    3. Enables template management and tracking
    4. Supports communication automation
*/

WITH TemplateBase AS (
    -- Source data would typically come from a templates table
    -- Since we don't have a direct source, this is a placeholder implementation
    -- that creates templates based on common communication patterns
    
    SELECT
        -- Generate template IDs from program properties that might contain templates
        {{ dbt_utils.generate_surrogate_key(['pp.program_property_id', 'pp.property_desc']) }} AS template_id,
        CASE
            WHEN pp.property_desc LIKE '%appointment%' THEN 'Appointment Reminder'
            WHEN pp.property_desc LIKE '%confirm%' THEN 'Appointment Confirmation'
            WHEN pp.property_desc LIKE '%recall%' THEN 'Recall Notice'
            WHEN pp.property_desc LIKE '%balance%' OR pp.property_desc LIKE '%statement%' THEN 'Balance Reminder'
            WHEN pp.property_desc LIKE '%birthday%' THEN 'Birthday Greeting'
            WHEN pp.property_desc LIKE '%welcome%' THEN 'New Patient Welcome'
            ELSE 'General Template'
        END AS template_name,
        CASE
            WHEN pp.property_desc LIKE '%email%' THEN 'email'
            WHEN pp.property_desc LIKE '%text%' OR pp.property_desc LIKE '%sms%' THEN 'SMS'
            WHEN pp.property_desc LIKE '%letter%' THEN 'letter'
            ELSE 'email'
        END AS template_type,
        CASE
            WHEN pp.property_desc LIKE '%appointment%' OR pp.property_desc LIKE '%confirm%' THEN 'appointment'
            WHEN pp.property_desc LIKE '%balance%' OR pp.property_desc LIKE '%statement%' THEN 'billing'
            WHEN pp.property_desc LIKE '%treatment%' THEN 'clinical'
            WHEN pp.property_desc LIKE '%recall%' THEN 'recall'
            WHEN pp.property_desc LIKE '%birthday%' OR pp.property_desc LIKE '%welcome%' THEN 'marketing'
            ELSE 'general'
        END AS category,
        CASE
            WHEN pp.property_desc LIKE '%appointment%' THEN 'Your upcoming appointment'
            WHEN pp.property_desc LIKE '%confirm%' THEN 'Please confirm your appointment'
            WHEN pp.property_desc LIKE '%recall%' THEN 'Time for your dental checkup'
            WHEN pp.property_desc LIKE '%balance%' THEN 'Your account balance reminder'
            WHEN pp.property_desc LIKE '%statement%' THEN 'Your dental statement'
            WHEN pp.property_desc LIKE '%birthday%' THEN 'Happy Birthday from your dental team!'
            WHEN pp.property_desc LIKE '%welcome%' THEN 'Welcome to our dental practice'
            ELSE 'Notification from your dental practice'
        END AS subject,
        pp.property_value AS content,
        
        -- Extract template variables using common placeholders
        -- This extracts variables that appear in format: {VARIABLE_NAME}
        ARRAY(
            SELECT DISTINCT matches[1]
            FROM regexp_matches(pp.property_value, '\{([A-Za-z0-9_]+)\}', 'g') AS matches
        ) AS variables,
        
        TRUE AS is_active,
        pp.user_num AS created_by,
        pp.date_created AS created_at,
        pp.date_modified AS updated_at
    FROM {{ ref('stg_opendental__programproperty') }} pp
    WHERE 
        pp.property_value IS NOT NULL
        AND LENGTH(pp.property_value) > 20
        AND (
            pp.property_desc LIKE '%template%'
            OR pp.property_desc LIKE '%message%'
            OR pp.property_value LIKE '%' || '{' || '%' || '}' || '%' -- Contains placeholder variables
        )
),

-- Additional templates derived from communication logs
-- Looking for patterns that suggest templated content
CommunicationTemplates AS (
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
        communication_mode AS template_type,
        communication_category AS category,
        'Auto-detected ' || communication_category || ' communication' AS subject,
        -- Use a representative content example
        MIN(content) AS content,
        -- Placeholder for variables detection
        ARRAY['PATIENT_NAME', 'DATE', 'PROVIDER']::text[] AS variables,
        TRUE AS is_active,
        MIN(user_id) AS created_by,
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
            created_at,
            updated_at,
            COUNT(*) AS frequency
        FROM {{ ref('int_patient_communications') }}
        WHERE content IS NOT NULL
        GROUP BY 
            communication_type,
            communication_mode,
            communication_category,
            LEFT(content, 50),
            content,
            user_id,
            created_at,
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
    template_id,
    template_name,
    template_type,
    category,
    subject,
    content,
    variables,
    is_active,
    created_by,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM TemplateBase

UNION ALL

SELECT
    template_id,
    template_name,
    template_type,
    category,
    subject,
    content,
    variables,
    is_active,
    created_by,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM CommunicationTemplates