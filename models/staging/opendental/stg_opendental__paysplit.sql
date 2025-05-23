{{ config(
    materialized='incremental',
    unique_key='paysplit_id'
) }}

WITH Source AS (
    SELECT * 
    FROM {{ source('opendental', 'paysplit') }}
    WHERE "DatePay" >= '2023-01-01'::date
        AND "DatePay" <= CURRENT_DATE
        AND "DatePay" > '2000-01-01'::date
    {% if is_incremental() %}
        AND "DatePay" > (SELECT max(payment_date) FROM {{ this }})
    {% endif %}
),

Renamed AS (
    SELECT
        -- Primary key
        "SplitNum" AS paysplit_id,
        
        -- Relationships
        "PayNum" AS payment_id,
        "PatNum" AS patient_id,
        "ClinicNum" AS clinic_id,
        "ProvNum" AS provider_id,
        "ProcNum" AS procedure_id,
        "AdjNum" AS adjustment_id,
        "PayPlanNum" AS payplan_id,
        "PayPlanChargeNum" AS payplan_charge_id,
        "FSplitNum" AS forward_split_id,
        "SecUserNumEntry" AS created_by_user_id,

        -- Split details
        "SplitAmt" AS split_amount,
        "DatePay" AS payment_date,
        "ProcDate" AS procedure_date,
        
        -- Flags and types
        CASE 
            WHEN "IsDiscount" = 1 THEN TRUE 
            ELSE FALSE 
        END AS is_discount_flag,
        "DiscountType" AS discount_type,
        "UnearnedType" AS unearned_type,
        "PayPlanDebitType" AS payplan_debit_type,

        -- Dates and metadata
        "DateEntry" AS entry_date,
        "SecDateTEdit" AS updated_at,
        
        -- Required metadata columns
        "DateEntry" AS _created_at,
        "SecDateTEdit" AS _updated_at,
        current_timestamp AS _loaded_at
    FROM Source
)

SELECT * FROM Renamed
