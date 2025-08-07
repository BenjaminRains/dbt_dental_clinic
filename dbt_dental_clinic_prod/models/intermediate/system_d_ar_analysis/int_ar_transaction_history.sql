{{ config(
    materialized='incremental',
    unique_key='ar_transaction_id',
    
) }}

/*
    Intermediate model for AR transaction history
    Part of System D: AR Analysis
    
    This model:
    1. Creates a chronological transaction history
    2. Uses shared calculations for consistency
    3. Tracks all AR-related transactions
    4. Supports incremental loading
*/

{% if is_incremental() %}
    {% set min_transaction_date = "SELECT DATEADD(day, -7, MAX(transaction_date)) FROM " ~ this %}
{% endif %}

WITH TransactionBase AS (
    SELECT
        patient_id,
        procedure_id,
        transaction_date,
        amount,
        aging_bucket,
        days_outstanding,
        payment_date,
        payment_amount,
        payment_type,
        payment_source,
        payment_status,
        is_insurance_payment,
        is_patient_payment,
        adjustment_date,
        adjustment_amount,
        adjustment_type,
        adjustment_category,
        is_procedure_adjustment,
        is_retroactive_adjustment
    FROM {{ ref('int_ar_shared_calculations') }}
    {% if is_incremental() %}
    WHERE transaction_date >= {{ min_transaction_date }}
    {% endif %}
),

TransactionDetails AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY transaction_date, patient_id, procedure_id) AS ar_transaction_id,
        patient_id,
        procedure_id,
        transaction_date,
        amount,
        CASE
            WHEN payment_date IS NOT NULL AND is_insurance_payment THEN 'INSURANCE_PAYMENT'
            WHEN payment_date IS NOT NULL AND is_patient_payment THEN 'PATIENT_PAYMENT'
            WHEN adjustment_date IS NOT NULL THEN 'ADJUSTMENT'
            ELSE 'PROCEDURE'
        END AS transaction_type,
        CASE
            WHEN amount < 0 THEN 'DECREASE'
            ELSE 'INCREASE'
        END AS balance_impact,
        CASE
            WHEN payment_date IS NOT NULL AND is_insurance_payment THEN payment_type
            WHEN payment_date IS NOT NULL AND is_patient_payment THEN payment_type
            WHEN adjustment_date IS NOT NULL THEN adjustment_type
            ELSE 'PROCEDURE_FEE'
        END AS transaction_category,
        CASE
            WHEN payment_date IS NOT NULL AND is_insurance_payment THEN TRUE
            ELSE FALSE
        END AS insurance_transaction_flag,
        CASE
            WHEN payment_date IS NOT NULL THEN payment_source
            ELSE NULL
        END AS payment_source_type,
        CASE
            WHEN payment_date IS NOT NULL THEN payment_type
            ELSE NULL
        END AS payment_method,
        CASE
            WHEN adjustment_date IS NOT NULL THEN adjustment_type
            ELSE NULL
        END AS adjustment_type,
        aging_bucket,
        days_outstanding,
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at
    FROM TransactionBase
    WHERE amount <> 0  -- Exclude zero-amount transactions
)

SELECT * FROM TransactionDetails
