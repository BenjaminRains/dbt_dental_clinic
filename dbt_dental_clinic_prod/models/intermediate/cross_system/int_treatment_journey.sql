{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='procedure_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['procedure_id'], 'unique': true},
            {'columns': ['patient_id']},
            {'columns': ['treatment_journey_id']},
            {'columns': ['_updated_at']}
        ]
    )
}}

/*
    Intermediate model for patient treatment journey
    Part of Cross-System: Treatment Journey Analysis
    
    This model:
    1. Tracks complete patient treatment flow from appointment through procedure, claim, and payment
    2. Provides comprehensive journey analysis across multiple systems
    3. Enables treatment timeline and financial status tracking
    
    Business Logic Features:
    - Treatment Timeline: Tracks progression from appointment to payment completion
    - Financial Status: Monitors insurance claims, payments, and remaining balances
    - Journey Status: Categorizes treatment completion status
    - Cross-System Integration: Connects Scheduling, Fee Processing, Insurance, and Payment systems
    
    Data Quality Notes:
    - Only includes completed procedures (status 'C' or 'EC')
    - Handles missing appointment, claim, or payment data gracefully
    - Validates financial calculations and remaining balances
    
    Performance Considerations:
    - Uses table materialization for complex cross-system joins
    - Indexed on key lookup fields for efficient querying
    - Filters to completed procedures to reduce data volume
*/

-- 1. Source data retrieval
WITH source_procedures AS (
    SELECT
        procedure_id,
        patient_id,
        provider_id,
        procedure_date,
        procedure_code,
        procedure_description,
        procedure_status,
        procedure_fee AS fee,
        appointment_id,
        -- Metadata fields for standardize_intermediate_metadata macro
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by
    FROM {{ ref('int_procedure_complete') }}
    WHERE procedure_status = 2  -- Only completed procedures
),

source_patients AS (
    SELECT
        patient_id,
        preferred_name AS first_name
    FROM {{ ref('int_patient_profile') }}
),

-- 2. Lookup/reference data
source_appointments AS (
    SELECT
        appointment_id,
        appointment_datetime,
        appointment_status_desc
    FROM {{ ref('int_appointment_details') }}
    WHERE appointment_id IS NOT NULL
),

source_claims AS (
    SELECT
        procedure_id,
        MAX(claim_id) AS claim_id,
        MAX(claim_procedure_id) AS claimproc_id,
        MAX(claim_date) AS date_sent,
        MAX(claim_status) AS claim_status,
        SUM(billed_amount) AS insurance_estimate,
        SUM(paid_amount) AS insurance_payment,
        SUM(write_off_amount) AS write_off,
        MAX(claim_date) AS date_received,
        MAX(claim_status) AS tracking_status
    FROM {{ ref('int_claim_details') }}
    WHERE procedure_id IS NOT NULL
    GROUP BY procedure_id
),

source_payments AS (
    SELECT
        procedure_id,
        MAX(payment_id) AS payment_id,
        MAX(paysplit_id) AS paysplit_id,
        MAX(payment_date) AS payment_date,
        SUM(split_amount) AS split_amount,
        MAX(payment_type_id) AS payment_type,
        MAX(split_type) AS split_type_desc
    FROM {{ ref('int_payment_split') }}
    WHERE procedure_id IS NOT NULL 
        AND split_amount > 0  -- Only positive payments
    GROUP BY procedure_id
),

-- 3. Business logic transformation
journey_enhanced AS (
    SELECT
        -- Primary identification
        {{ dbt_utils.generate_surrogate_key(['sp.procedure_id', 'sp.patient_id', 'sp.procedure_date', 'sp.appointment_id']) }} AS treatment_journey_id,
        sp.procedure_id,
        sp.patient_id,
        sp.provider_id,
        sp.procedure_code,
        sp.procedure_description,
        sp.procedure_date,
        sp.fee AS procedure_fee,
        sp.appointment_id,
        
        -- Patient information
        pat.first_name AS patient_first_name,
        
        -- Appointment details
        app.appointment_datetime,
        app.appointment_status_desc,
        
        -- Claim details
        cl.claim_id,
        cl.claimproc_id,
        cl.date_sent AS claim_date_sent,
        cl.date_received AS claim_date_received,
        cl.claim_status,
        cl.insurance_estimate,
        cl.insurance_payment,
        cl.write_off,
        cl.tracking_status,
        
        -- Payment details
        pay.payment_id,
        pay.paysplit_id,
        pay.payment_date,
        pay.split_amount AS patient_payment,
        pay.payment_type,
        pay.split_type_desc,
        
        -- Timeline calculations
        (sp.procedure_date - app.appointment_datetime::date) AS days_appointment_to_procedure,
        (cl.date_sent - sp.procedure_date) AS days_procedure_to_claim,
        (cl.date_received - cl.date_sent) AS days_claim_to_payment,
        (pay.payment_date - sp.procedure_date) AS days_procedure_to_payment,
        
        -- Status flags (using standardized boolean pattern)
        CASE
            WHEN cl.claim_id IS NULL THEN FALSE
            ELSE TRUE
        END AS has_insurance_claim,
        
        CASE
            WHEN cl.insurance_payment > 0 THEN TRUE
            ELSE FALSE
        END AS has_insurance_payment,
        
        CASE
            WHEN pay.payment_id IS NULL THEN FALSE
            ELSE TRUE
        END AS has_patient_payment,
        
        -- Financial calculations
        sp.fee - COALESCE(cl.insurance_payment, 0) - COALESCE(cl.write_off, 0) - COALESCE(pay.split_amount, 0) AS remaining_balance,
        
        -- Journey timeline
        app.appointment_datetime::date AS journey_start_date,
        cl.date_sent AS claim_sent_date,
        cl.date_received AS claim_received_date,
        
        -- Journey status categorization
        CASE
            WHEN sp.fee - COALESCE(cl.insurance_payment, 0) - COALESCE(cl.write_off, 0) - COALESCE(pay.split_amount, 0) <= 0 
                THEN 'Complete'
            WHEN cl.claim_id IS NOT NULL AND cl.insurance_payment IS NULL 
                THEN 'Awaiting Insurance'
            WHEN cl.claim_id IS NOT NULL AND cl.insurance_payment > 0 AND pay.payment_id IS NULL 
                THEN 'Awaiting Patient Payment'
            WHEN cl.claim_id IS NULL AND pay.payment_id IS NULL 
                THEN 'Unbilled'
            ELSE 'In Progress'
        END AS journey_status,
        
        -- Standardized metadata fields
        {{ standardize_intermediate_metadata(primary_source_alias='sp') }}
        
    FROM source_procedures sp
    LEFT JOIN source_patients pat 
        ON sp.patient_id = pat.patient_id
    LEFT JOIN source_appointments app 
        ON sp.appointment_id = app.appointment_id
    LEFT JOIN source_claims cl 
        ON sp.procedure_id = cl.procedure_id
    LEFT JOIN source_payments pay 
        ON sp.procedure_id = pay.procedure_id
),

-- 4. Final integration with metadata
final AS (
    SELECT
        -- Core business fields
        treatment_journey_id,
        procedure_id,
        patient_id,
        provider_id,
        patient_first_name,
        procedure_code,
        procedure_description,
        procedure_date,
        procedure_fee,
        appointment_id,
        appointment_datetime,
        appointment_status_desc,
        claim_id,
        claimproc_id,
        claim_date_sent,
        claim_date_received,
        claim_status,
        insurance_estimate,
        insurance_payment,
        write_off,
        tracking_status,
        payment_id,
        paysplit_id,
        payment_date,
        patient_payment,
        payment_type,
        split_type_desc,
        days_appointment_to_procedure,
        days_procedure_to_claim,
        days_claim_to_payment,
        days_procedure_to_payment,
        has_insurance_claim,
        has_insurance_payment,
        has_patient_payment,
        remaining_balance,
        journey_start_date,
        claim_sent_date,
        claim_received_date,
        journey_status,
        
        -- Metadata fields from journey_enhanced
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by,
        _transformed_at
        
    FROM journey_enhanced
)

SELECT * FROM final