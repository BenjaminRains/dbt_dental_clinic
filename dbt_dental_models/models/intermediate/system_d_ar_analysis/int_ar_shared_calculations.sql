{{ config(
    materialized='table',
    indexes=[
        {'columns': ['patient_id']},
        {'columns': ['procedure_id']},
        {'columns': ['transaction_date']},
        {'columns': ['aging_bucket']},
        {'columns': ['days_outstanding']},
        {'columns': ['payment_date']},
        {'columns': ['adjustment_date']},
        {'columns': ['is_insurance_payment']},
        {'columns': ['is_patient_payment']},
        {'columns': ['payment_type']},
        {'columns': ['adjustment_type']},
        {'columns': ['patient_id', 'procedure_id']},
        {'columns': ['patient_id', 'transaction_date']},
        {'columns': ['patient_id', 'aging_bucket']},
        {'columns': ['procedure_id', 'transaction_date']},
        {'columns': ['transaction_date', 'aging_bucket']},
        {'columns': ['payment_date', 'is_insurance_payment']},
        {'columns': ['adjustment_date', 'adjustment_type']},
        {'columns': ['_loaded_at']},
        {'columns': ['_created_at']},
        {'columns': ['_updated_at']}
    ]
) }}

/*
    Shared calculations for AR analysis
    Part of System D: AR Analysis
    
    This model:
    1. Calculates common aging buckets
    2. Provides base payment and adjustment calculations
    3. Serves as a reference for other AR models
*/

WITH 
-- Deduplicate patient payments at the source
DedupedPatientPayments AS (
    SELECT DISTINCT
        patient_id,
        procedure_id,
        payment_date,
        split_amount AS payment_amount,
        CAST(payment_type_id AS TEXT) AS payment_type_id,
        CAST(payment_type_description AS TEXT) AS payment_type_description,
        payment_source,
        CAST(payment_status AS TEXT) AS payment_status,
        FALSE AS is_insurance_payment,
        TRUE AS is_patient_payment,
        CAST(payment_id AS TEXT) AS payment_id
    FROM {{ ref('int_patient_payment_allocated') }}
    WHERE include_in_ar = TRUE
),

-- Deduplicate insurance payments at the source
DedupedInsurancePayments AS (
    SELECT DISTINCT
        patient_id,
        procedure_id,
        payment_date,
        split_amount AS payment_amount,
        CAST(payment_type_id AS TEXT) AS payment_type_id,
        CAST(payment_type_description AS TEXT) AS payment_type_description,
        payment_source,
        CASE 
            WHEN status = 1 THEN 'COMPLETED'
            WHEN status = 3 THEN 'SUPPLEMENTAL'
            ELSE 'PENDING'
        END AS payment_status,
        TRUE AS is_insurance_payment,
        FALSE AS is_patient_payment,
        CAST(payment_id AS TEXT) AS payment_id
    FROM {{ ref('int_insurance_payment_allocated') }}
    WHERE include_in_ar = TRUE
),

BasePayments AS (
    -- Patient payments
    SELECT * FROM DedupedPatientPayments
    
    UNION ALL
    
    -- Insurance payments
    SELECT * FROM DedupedInsurancePayments
),

BaseAdjustments AS (
    SELECT DISTINCT
        patient_id,
        procedure_id,  -- This will be NULL for general account adjustments
        adjustment_date,
        adjustment_amount,
        adjustment_category,
        is_procedure_adjustment,
        is_retroactive_adjustment,
        CAST(adjustment_id AS TEXT) AS adjustment_id
    FROM {{ ref('int_adjustments') }}
),

BaseProcedures AS (
    SELECT DISTINCT
        procedure_id,
        patient_id,
        provider_id,
        procedure_date,
        procedure_fee,
        procedure_code,
        procedure_description,
        procedure_status_desc,
        -- Metadata fields (preserved from primary source)
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by
    FROM {{ ref('int_procedure_complete') }}
    WHERE procedure_status_desc IN ('Complete', 'Existing Current')
),

-- Make sure transactions are unique before union
TransactionsUnion AS (
    -- Procedures
    SELECT DISTINCT
        patient_id,
        procedure_id,
        procedure_date AS transaction_date,
        procedure_fee AS amount,
        'PROCEDURE' AS transaction_type,
        NULL AS payment_id,
        NULL AS adjustment_id
    FROM BaseProcedures
    
    UNION ALL
    
    -- Payments
    SELECT DISTINCT
        patient_id,
        procedure_id,
        payment_date AS transaction_date,
        payment_amount AS amount,
        CASE
            WHEN is_insurance_payment THEN 'INSURANCE_PAYMENT'
            ELSE 'PATIENT_PAYMENT'
        END AS transaction_type,
        payment_id,
        NULL AS adjustment_id
    FROM BasePayments
    
    UNION ALL
    
    -- Adjustments
    SELECT DISTINCT
        patient_id,
        procedure_id,
        adjustment_date AS transaction_date,
        adjustment_amount AS amount,
        CASE
            WHEN procedure_id = 0 OR procedure_id IS NULL THEN 'GENERAL_ADJUSTMENT'
            ELSE 'PROCEDURE_ADJUSTMENT'
        END AS transaction_type,
        NULL AS payment_id,
        adjustment_id
    FROM BaseAdjustments
),

AgingCalculations AS (
    SELECT
        t.patient_id,
        t.procedure_id,
        t.transaction_date,
        t.amount,
        t.transaction_type,
        t.payment_id,
        t.adjustment_id,
        CASE
            WHEN CURRENT_DATE - COALESCE(t.transaction_date, p.procedure_date) <= 30 THEN '0-30'
            WHEN CURRENT_DATE - COALESCE(t.transaction_date, p.procedure_date) <= 60 THEN '31-60'
            WHEN CURRENT_DATE - COALESCE(t.transaction_date, p.procedure_date) <= 90 THEN '61-90'
            ELSE '90+'
        END AS aging_bucket,
        CURRENT_DATE - COALESCE(t.transaction_date, p.procedure_date) AS days_outstanding
    FROM TransactionsUnion t
    LEFT JOIN BaseProcedures p
        ON t.patient_id = p.patient_id
        AND (t.procedure_id = p.procedure_id OR t.procedure_id IS NULL)
)

-- Final selection with proper join conditions to avoid cartesian products
SELECT DISTINCT
    ac.patient_id,
    ac.procedure_id,
    ac.transaction_date,
    ac.amount,
    ac.aging_bucket,
    ac.days_outstanding,
    
    -- Payment fields - only include when the transaction is a payment
    CASE WHEN ac.transaction_type IN ('INSURANCE_PAYMENT', 'PATIENT_PAYMENT') THEN bp.payment_date ELSE NULL END AS payment_date,
    CASE WHEN ac.transaction_type IN ('INSURANCE_PAYMENT', 'PATIENT_PAYMENT') THEN bp.payment_amount ELSE NULL END AS payment_amount,
    CASE 
        WHEN ac.transaction_type IN ('INSURANCE_PAYMENT', 'PATIENT_PAYMENT') THEN
            CASE
                WHEN bp.payment_type_description LIKE '%Check%' OR bp.payment_type_description = 'Insurance Check' THEN 'CHECK'
                WHEN bp.payment_type_description LIKE '%Credit%' OR bp.payment_type_description = 'Insurance Credit' THEN 'CREDIT_CARD'
                WHEN bp.payment_type_description LIKE '%Electronic%' OR bp.payment_type_description = 'Insurance Electronic Payment' THEN 'ELECTRONIC'
                WHEN bp.payment_type_description LIKE '%Cash%' THEN 'CASH'
                WHEN bp.payment_type_description IN ('Standard Payment', 'Regular Payment', 'High Value Payment', 'High Value', 'Very High Value', 'New Payment Type', 'New Type', 'Special Case') THEN 'STANDARD'
                WHEN bp.payment_type_description = 'Administrative' THEN 'ADMINISTRATIVE'
                WHEN bp.payment_type_description = 'Refund' THEN 'REFUND'
                ELSE 'OTHER'
            END
        ELSE NULL
    END AS payment_type,
    CASE WHEN ac.transaction_type IN ('INSURANCE_PAYMENT', 'PATIENT_PAYMENT') THEN bp.payment_source ELSE NULL END AS payment_source,
    CASE 
        WHEN ac.transaction_type IN ('INSURANCE_PAYMENT', 'PATIENT_PAYMENT') THEN
            CASE
                WHEN bp.payment_status = 'COMPLETED' THEN 'COMPLETED'
                WHEN bp.payment_status = 'SUPPLEMENTAL' THEN 'COMPLETED'
                ELSE 'PENDING'
            END
        ELSE NULL
    END AS payment_status,
    CASE WHEN ac.transaction_type IN ('INSURANCE_PAYMENT', 'PATIENT_PAYMENT') THEN bp.is_insurance_payment ELSE NULL END AS is_insurance_payment,
    CASE WHEN ac.transaction_type IN ('INSURANCE_PAYMENT', 'PATIENT_PAYMENT') THEN bp.is_patient_payment ELSE NULL END AS is_patient_payment,
    
    -- Adjustment fields - only include when the transaction is an adjustment
    CASE WHEN ac.transaction_type IN ('GENERAL_ADJUSTMENT', 'PROCEDURE_ADJUSTMENT') THEN ba.adjustment_date ELSE NULL END AS adjustment_date,
    CASE WHEN ac.transaction_type IN ('GENERAL_ADJUSTMENT', 'PROCEDURE_ADJUSTMENT') THEN ba.adjustment_amount ELSE NULL END AS adjustment_amount,
    CASE 
        WHEN ac.transaction_type IN ('GENERAL_ADJUSTMENT', 'PROCEDURE_ADJUSTMENT') THEN
            CASE
                WHEN ba.adjustment_category = 'insurance_writeoff' THEN 'WRITEOFF'
                WHEN ba.adjustment_category LIKE '%discount%' THEN 'DISCOUNT'
                WHEN ba.adjustment_category LIKE '%credit%' THEN 'CREDIT'
                WHEN ba.adjustment_category = 'admin_correction' THEN 'ADMIN_CORRECTION'
                WHEN ba.adjustment_category = 'admin_adjustment' THEN 'ADMIN_ADJUSTMENT'
                WHEN ba.adjustment_category = 'reallocation' THEN 'REALLOCATION'
                WHEN ba.adjustment_category = 'unearned_income' THEN 'UNEARNED_INCOME'
                ELSE 'OTHER'
            END
        ELSE NULL
    END AS adjustment_type,
    CASE WHEN ac.transaction_type IN ('GENERAL_ADJUSTMENT', 'PROCEDURE_ADJUSTMENT') THEN ba.adjustment_category ELSE NULL END AS adjustment_category,
    CASE WHEN ac.transaction_type IN ('GENERAL_ADJUSTMENT', 'PROCEDURE_ADJUSTMENT') THEN ba.is_procedure_adjustment ELSE NULL END AS is_procedure_adjustment,
    CASE WHEN ac.transaction_type IN ('GENERAL_ADJUSTMENT', 'PROCEDURE_ADJUSTMENT') THEN ba.is_retroactive_adjustment ELSE NULL END AS is_retroactive_adjustment,
    
    -- Standardized metadata from primary source (int_procedure_complete)
    {{ standardize_intermediate_metadata(primary_source_alias='pc') }}
    
FROM AgingCalculations ac
LEFT JOIN BaseProcedures pc
    ON ac.patient_id = pc.patient_id
    AND ac.procedure_id = pc.procedure_id
-- Use proper join keys and transaction type filters to avoid cartesian products
LEFT JOIN BasePayments bp
    ON ac.patient_id = bp.patient_id
    AND ac.procedure_id = bp.procedure_id
    AND ac.payment_id = bp.payment_id
    AND ac.transaction_type IN ('INSURANCE_PAYMENT', 'PATIENT_PAYMENT')
LEFT JOIN BaseAdjustments ba
    ON ac.patient_id = ba.patient_id
    AND ac.procedure_id = ba.procedure_id
    AND ac.adjustment_id = ba.adjustment_id
    AND ac.transaction_type IN ('GENERAL_ADJUSTMENT', 'PROCEDURE_ADJUSTMENT')
