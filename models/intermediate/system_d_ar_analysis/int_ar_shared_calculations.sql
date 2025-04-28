{{ config(
    materialized='table',
    schema='intermediate'
) }}

/*
    Shared calculations for AR analysis
    Part of System D: AR Analysis
    
    This model:
    1. Calculates common aging buckets
    2. Provides base payment and adjustment calculations
    3. Serves as a reference for other AR models
*/

WITH BasePayments AS (
    -- Patient payments
    SELECT
        patient_id,
        procedure_id,
        payment_date,
        split_amount AS payment_amount,
        CAST(payment_type_id AS TEXT) AS payment_type_id,
        CAST(payment_type_description AS TEXT) AS payment_type_description,
        CAST(payment_source AS TEXT) AS payment_source,
        CAST(payment_status AS TEXT) AS payment_status,
        FALSE AS is_insurance_payment,
        TRUE AS is_patient_payment,
        CAST(payment_id AS TEXT) AS payment_id
    FROM {{ ref('int_patient_payment_allocated') }}
    WHERE include_in_ar = TRUE
    
    UNION ALL
    
    -- Insurance payments
    SELECT
        patient_id,
        procedure_id,
        payment_date,
        split_amount AS payment_amount,
        CAST(payment_type_id AS TEXT) AS payment_type_id,
        CAST(payment_type_description AS TEXT) AS payment_type_description,
        'INSURANCE' AS payment_source,
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

BaseAdjustments AS (
    SELECT
        patient_id,
        procedure_id,
        adjustment_date,
        adjustment_amount,
        adjustment_type_id AS adjustment_type_id_raw,
        adjustment_type_name,
        adjustment_category_type AS adjustment_category,
        is_procedure_adjustment,
        is_retroactive_adjustment,
        CAST(adjustment_id AS TEXT) AS adjustment_id
    FROM {{ ref('int_adjustments') }}
),

BaseProcedures AS (
    SELECT
        procedure_id,
        patient_id,
        provider_id,
        procedure_date,
        procedure_fee,
        procedure_code,
        procedure_description,
        procedure_status_desc
    FROM {{ ref('int_procedure_complete') }}
    WHERE procedure_status_desc IN ('Complete', 'Existing Current')
),

TransactionsUnion AS (
    -- Procedures
    SELECT
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
    SELECT
        patient_id,
        procedure_id,
        payment_date AS transaction_date,
        -payment_amount AS amount, -- Negative for payments
        CASE
            WHEN is_insurance_payment THEN 'INSURANCE_PAYMENT'
            ELSE 'PATIENT_PAYMENT'
        END AS transaction_type,
        payment_id,
        NULL AS adjustment_id
    FROM BasePayments
    
    UNION ALL
    
    -- Adjustments
    SELECT
        patient_id,
        procedure_id,
        adjustment_date AS transaction_date,
        adjustment_amount AS amount,
        'ADJUSTMENT' AS transaction_type,
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
        AND t.procedure_id = p.procedure_id
)

SELECT
    ac.patient_id,
    ac.procedure_id,
    ac.transaction_date,
    ac.amount,
    ac.aging_bucket,
    ac.days_outstanding,
    
    -- Payment fields
    bp.payment_date,
    bp.payment_amount,
    CASE
        WHEN bp.payment_type_description LIKE '%Check%' THEN 'CHECK'
        WHEN bp.payment_type_description LIKE '%Credit%' THEN 'CREDIT_CARD'
        WHEN bp.payment_type_description LIKE '%Electronic%' THEN 'ELECTRONIC'
        WHEN bp.payment_type_description LIKE '%Cash%' THEN 'CASH'
        ELSE 'OTHER'
    END AS payment_type,
    bp.payment_source,
    CASE
        WHEN bp.payment_status = 'COMPLETED' THEN 'COMPLETED'
        WHEN bp.payment_status = 'SUPPLEMENTAL' THEN 'COMPLETED'
        ELSE 'PENDING'
    END AS payment_status,
    bp.is_insurance_payment,
    bp.is_patient_payment,
    
    -- Adjustment fields
    ba.adjustment_date,
    ba.adjustment_amount,
    CASE
        WHEN ba.adjustment_type_name LIKE '%Write%' THEN 'WRITEOFF'
        WHEN ba.adjustment_type_name LIKE '%Discount%' THEN 'DISCOUNT'
        WHEN ba.adjustment_type_name LIKE '%Credit%' THEN 'CREDIT'
        ELSE 'OTHER'
    END AS adjustment_type,
    ba.adjustment_category,
    ba.is_procedure_adjustment,
    ba.is_retroactive_adjustment
    
FROM AgingCalculations ac
LEFT JOIN BasePayments bp
    ON ac.patient_id = bp.patient_id
    AND ac.procedure_id = bp.procedure_id
    AND ac.payment_id = bp.payment_id -- Connect the specific payment
LEFT JOIN BaseAdjustments ba
    ON ac.patient_id = ba.patient_id
    AND ac.procedure_id = ba.procedure_id
    AND ac.adjustment_id = ba.adjustment_id -- Connect the specific adjustment 