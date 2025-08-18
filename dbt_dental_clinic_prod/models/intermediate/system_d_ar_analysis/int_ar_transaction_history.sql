{{ config(
    materialized='table',
    unique_key='ar_transaction_id',
    indexes=[
        {'columns': ['patient_id']},
        {'columns': ['procedure_id']},
        {'columns': ['transaction_date']},
        {'columns': ['transaction_type']},
        {'columns': ['balance_impact']},
        {'columns': ['transaction_category']},
        {'columns': ['aging_bucket']},
        {'columns': ['days_outstanding']},
        {'columns': ['insurance_transaction_flag']},
        {'columns': ['payment_source_type']},
        {'columns': ['payment_method']},
        {'columns': ['adjustment_type']},
        {'columns': ['patient_id', 'transaction_date']},
        {'columns': ['patient_id', 'transaction_type']},
        {'columns': ['patient_id', 'aging_bucket']},
        {'columns': ['procedure_id', 'transaction_date']},
        {'columns': ['transaction_date', 'transaction_type']},
        {'columns': ['transaction_date', 'balance_impact']},
        {'columns': ['transaction_type', 'balance_impact']},
        {'columns': ['aging_bucket', 'balance_impact']},
        {'columns': ['_loaded_at']},
        {'columns': ['_created_at']},
        {'columns': ['_updated_at']}
    ]
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
        is_retroactive_adjustment,
        -- Metadata fields (preserved from primary source)
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by
    FROM {{ ref('int_ar_shared_calculations') }}
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
        
        -- Standardized metadata from primary source
        {{ standardize_intermediate_metadata(primary_source_alias='tb') }}
    FROM TransactionBase tb
    WHERE amount <> 0  -- Exclude zero-amount transactions
)

SELECT * FROM TransactionDetails
