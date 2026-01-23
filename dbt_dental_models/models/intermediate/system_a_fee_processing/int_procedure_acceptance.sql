{{ config(
    materialized='incremental',
    schema='int',
    unique_key='procedure_id',
    on_schema_change='sync_all_columns',
    incremental_strategy='merge',
    indexes=[
        {'columns': ['procedure_id'], 'unique': true},
        {'columns': ['patient_id']},
        {'columns': ['procedure_date']},
        {'columns': ['_updated_at']}
    ]) }}

/*
    Intermediate model for procedure-based treatment acceptance
    Part of System A: Fee Processing & Verification
    
    This model:
    1. Tracks procedure-level treatment acceptance metrics
    2. Identifies exam procedures (D0 codes for diagnostic procedures)
    3. Determines if procedures were presented (recommended) vs accepted (completed/scheduled)
    4. Identifies same-day treatment (procedures completed on presentation date)
    5. Links procedures to appointments for scheduled procedures
    
    Business Logic:
    - Presented: Procedures with status 1 (Treatment Planned) OR status 6 (Ordered/Planned)
    - Accepted: Procedures with status 2 (Completed) OR status 1/6 with appointment scheduled
    - Same-Day Treatment: Procedures completed (status 2) on the same day they were presented
    - Exam Procedures: D0120, D0140, D0150, D0220, D0272, D0274, D0330 (diagnostic codes)
    
    Data Sources:
    - int_procedure_complete: Procedure data with fees and status
    - int_appointment_details: Appointment data for scheduled procedures
    - int_procedure_catalog: Procedure code definitions and categories
    
    Performance Considerations:
    - Incremental materialization based on procedure_date for efficient updates
    - Indexed on procedure_id, patient_id, and procedure_date for fast lookups
*/

-- 1. Source data retrieval
WITH source_procedures AS (
    SELECT *
    FROM {{ ref('int_procedure_complete') }}
    WHERE procedure_date IS NOT NULL  -- Filter out NULL procedure_date values
    {% if is_incremental() %}
        AND procedure_date >= (SELECT MAX(procedure_date) FROM {{ this }}) - INTERVAL '7 days'
    {% endif %}
),

-- 2. Appointment data for scheduled procedures
appointment_data AS (
    SELECT
        appointment_id,
        patient_id,
        provider_id AS appointment_provider_id,
        clinic_id AS appointment_clinic_id,
        DATE(appointment_datetime) AS appointment_date,
        appointment_datetime,
        appointment_status,
        appointment_status_desc,
        is_complete AS appointment_is_complete
    FROM {{ ref('int_appointment_details') }}
),

-- 3. Procedure code definitions
procedure_definitions AS (
    SELECT
        procedure_code_id,
        procedure_code,
        procedure_description,
        procedure_category,
        is_radiology
    FROM {{ ref('int_procedure_catalog') }}
),

-- 4. Exam procedure identification
exam_procedures AS (
    SELECT
        procedure_code_id,
        CASE
            WHEN procedure_code IN ('D0120', 'D0140', 'D0150', 'D0220', 'D0272', 'D0274', 'D0330') THEN TRUE
            WHEN procedure_code LIKE 'D0%' AND is_radiology = TRUE THEN TRUE
            ELSE FALSE
        END AS is_exam_procedure
    FROM procedure_definitions
),

-- 5. Procedure acceptance logic
procedure_acceptance AS (
    SELECT
        pc.procedure_id,
        pc.patient_id,
        pc.provider_id,
        pc.clinic_id,
        pc.procedure_code_id,
        pc.procedure_date,
        pc.procedure_status,
        pc.procedure_status_desc,
        pc.procedure_fee,
        pc.procedure_code,
        pd.procedure_description,
        pd.procedure_category,
        ep.is_exam_procedure,
        
        -- Appointment information
        pc.appointment_id,
        a.appointment_date,
        a.appointment_datetime,
        a.appointment_status,
        a.appointment_status_desc,
        a.appointment_is_complete,
        
        -- Acceptance logic
        -- Presented: Status 1 (Treatment Planned) OR Status 6 (Ordered/Planned)
        CASE
            WHEN pc.procedure_status IN (1, 6) THEN TRUE
            ELSE FALSE
        END AS is_presented,
        
        -- Accepted: Status 2 (Completed) OR Status 1/6 with appointment scheduled
        -- Note: Based on validation, procedures are NOT tracked through lifecycle (status 1/6 → 2)
        -- Most status 2 procedures never pass through status 1/6, so we count ALL status 2 as accepted
        -- This aligns with PBN definition: "accepted (completed or scheduled)"
        CASE
            WHEN pc.procedure_status = 2 THEN TRUE  -- Completed (always accepted)
            WHEN pc.procedure_status IN (1, 6) 
                AND pc.appointment_id IS NOT NULL 
                AND pc.appointment_id != 0
                AND a.appointment_status = 1  -- Scheduled
                AND a.appointment_date >= pc.procedure_date THEN TRUE  -- Scheduled for future
            ELSE FALSE
        END AS is_accepted,
        
        -- Same-day treatment: Status 2 (Completed) where procedure_date = appointment_date
        -- Note: This represents procedures completed on the same day they were scheduled
        CASE
            WHEN pc.procedure_status = 2 
                AND a.appointment_date IS NOT NULL
                AND DATE(pc.procedure_date) = a.appointment_date THEN TRUE
            ELSE FALSE
        END AS is_same_day_treatment,
        
        -- Metadata fields
        {{ standardize_intermediate_metadata(primary_source_alias='pc') }}
        
    FROM source_procedures pc
    LEFT JOIN procedure_definitions pd
        ON pc.procedure_code_id = pd.procedure_code_id
    LEFT JOIN exam_procedures ep
        ON pc.procedure_code_id = ep.procedure_code_id
    LEFT JOIN appointment_data a
        ON pc.appointment_id = a.appointment_id
        AND pc.appointment_id IS NOT NULL
        AND pc.appointment_id != 0
)

SELECT * FROM procedure_acceptance

