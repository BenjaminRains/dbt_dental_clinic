{{ config(
    materialized='incremental',
    unique_key='payment_allocation_id',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_payment_id_idx ON {{ this }} (payment_id)",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_patient_id_idx ON {{ this }} (patient_id)",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_payment_date_idx ON {{ this }} (payment_date)",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_patient_payment_date_idx ON {{ this }} (patient_id, payment_date)",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_provider_id_idx ON {{ this }} (provider_id)",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_payment_type_id_idx ON {{ this }} (payment_type_id)",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_include_in_ar_idx ON {{ this }} (include_in_ar) WHERE include_in_ar = true"
    ]
) }}

/*
    Intermediate model for patient payment allocations
    Part of System C: Payment Allocation & Reconciliation
    
    This model handles:
    1. Patient payments and their allocations
    2. Transfer pair validation
    3. Payment type definitions
    4. Provider attribution via procedurelog
    5. AR inclusion logic for payments
*/

-- First, identify problematic transfers to exclude them
WITH ProblematicTransfers AS (
    SELECT DISTINCT
        p.payment_id
    FROM {{ ref('stg_opendental__payment') }} p
    JOIN {{ ref('stg_opendental__paysplit') }} ps
        ON p.payment_id = ps.payment_id
    WHERE p.payment_type_id = 0
    AND p.payment_notes LIKE '%INCOME TRANSFER%'
    AND (
        -- Invalid unearned_type combinations
        (ps.unearned_type = 0 AND ps.split_amount < 0) OR
        (ps.unearned_type = 288 AND ps.split_amount > 0) OR
        -- Treatment plan validation
        (ps.unearned_type = 439 AND NOT EXISTS (
            SELECT 1 
            FROM {{ ref('stg_opendental__procedurelog') }} proc
            WHERE proc.procedure_id = ps.procedure_id
            AND proc.procedure_status = 1
        ))
    )
),

-- Identify unbalanced transfers
UnbalancedTransfers AS (
    SELECT DISTINCT
        p.payment_id
    FROM {{ ref('stg_opendental__payment') }} p
    JOIN {{ ref('stg_opendental__paysplit') }} ps
        ON p.payment_id = ps.payment_id
    WHERE p.payment_type_id = 0
    AND p.payment_notes LIKE '%INCOME TRANSFER%'
    AND ps.unearned_type IN (0, 288)
    GROUP BY p.payment_id, ABS(ps.split_amount), ps.unearned_type
    HAVING 
        (ps.unearned_type = 0 AND 
         COUNT(CASE WHEN ps.split_amount > 0 THEN 1 END) != 
         COUNT(CASE WHEN ps.split_amount < 0 THEN 1 END))
        OR
        (ps.unearned_type = 288 AND 
         COUNT(CASE WHEN ps.split_amount > 0 THEN 1 END) > 0)
),

-- Get all valid transfer pairs in a more efficient way
TransferPairs AS (
    SELECT DISTINCT
        p1.payment_id,
        p1.patient_id,
        p1.payment_date,
        p1.payment_amount,
        ps1.unearned_type,
        ps1.procedure_id,
        ps1.split_amount,
        ROW_NUMBER() OVER (
            PARTITION BY p1.payment_id, p1.patient_id, p1.payment_date, ps1.split_amount, ps1.unearned_type, ps1.procedure_id
            ORDER BY p1.entry_date, ps1.entry_date
        ) as rn
    FROM {{ ref('stg_opendental__payment') }} p1
    JOIN {{ ref('stg_opendental__paysplit') }} ps1
        ON p1.payment_id = ps1.payment_id
    WHERE p1.payment_type_id = 0
    AND p1.payment_notes LIKE '%INCOME TRANSFER%'
    AND ps1.unearned_type IN (0, 288)
    AND p1.payment_date >= '2023-01-01'
    -- Exclude problematic transfers
    AND p1.payment_id NOT IN (SELECT payment_id FROM ProblematicTransfers)
    AND p1.payment_id NOT IN (SELECT payment_id FROM UnbalancedTransfers)
),

-- Then get matching pairs with improved validation
ValidTransfers AS (
    SELECT 
        tp1.payment_id,
        tp1.procedure_id,
        tp1.split_amount,
        tp1.unearned_type,
        TRUE as has_matching_pair
    FROM TransferPairs tp1
    JOIN TransferPairs tp2
        ON tp2.patient_id = tp1.patient_id
        AND tp2.payment_date = tp1.payment_date
        AND tp2.split_amount = -tp1.split_amount
        AND tp2.unearned_type = CASE 
            WHEN tp1.unearned_type = 0 THEN 288 
            ELSE 0 
        END
    WHERE tp1.rn = 1 AND tp2.rn = 1
),

-- Get payment definitions with materialization
PaymentDefinitions AS MATERIALIZED (
    SELECT 
        definition_id,
        item_name,
        item_value,
        category_id
    FROM {{ ref('stg_opendental__definition') }}
    WHERE category_id = 1

    UNION ALL

    SELECT 
        payment_type_id as definition_id,
        CASE 
            WHEN payment_type_id = 71 THEN 'Standard Payment'
            WHEN payment_type_id = 0 THEN 'Administrative'
            WHEN payment_type_id = 69 THEN 'High Value Payment'
            WHEN payment_type_id = 70 THEN 'Regular Payment'
            WHEN payment_type_id = 391 THEN 'High Value'
            WHEN payment_type_id = 412 THEN 'New Payment Type'
            WHEN payment_type_id = 72 THEN 'Refund'
            WHEN payment_type_id = 634 THEN 'New Type'
            WHEN payment_type_id = 574 THEN 'Very High Value'
            WHEN payment_type_id = 417 THEN 'Special Case'
        END as item_name,
        NULL as item_value,
        1 as category_id
    FROM (VALUES 
        (71), (0), (69), (70), (391), (412), (72), (634), (574), (417)
    ) as t(payment_type_id)
),

-- Get patient payments with materialization and additional filtering
PatientPayments AS MATERIALIZED (
    SELECT 
        p.payment_id,
        p.patient_id,
        p.payment_date,
        CASE 
            WHEN ps.unearned_type = 288 THEN -ps.split_amount  -- Unearned income type 288 needs to be negated
            ELSE ps.split_amount  -- Regular splits keep their sign
        END as payment_amount,
        p.payment_type_id,
        p.check_number,
        p.bank_branch,
        p.payment_source,
        p.is_split,
        p.is_recurring_cc,
        p.payment_status,
        p.process_status,
        p.merchant_fee,
        p.payment_notes,
        p._created_by as created_by_user_id,
        p.entry_date,
        p._updated_at as updated_at,
        p.deposit_id,
        p.external_id,
        p.is_cc_completed,
        p.recurring_charge_date,
        p.receipt_text,
        ps.procedure_id,
        ps.procedure_date,
        proc.provider_id,
        ps.adjustment_id,
        ps.is_discount,
        ps.discount_type,
        ps.unearned_type,
        -- Metadata fields for standardize_intermediate_metadata macro
        p._loaded_at,
        p._transformed_at,
        p._created_at,
        p._updated_at,
        p._created_by,
        ROW_NUMBER() OVER (
            PARTITION BY p.payment_id, ps.procedure_id, ps.split_amount, ps.unearned_type
            ORDER BY p.entry_date, ps.entry_date
        ) as rn
    FROM {{ ref('stg_opendental__payment') }} p
    INNER JOIN {{ ref('stg_opendental__paysplit') }} ps
        ON p.payment_id = ps.payment_id
    LEFT JOIN {{ ref('stg_opendental__procedurelog') }} proc
        ON ps.procedure_id = proc.procedure_id
    WHERE p.payment_date >= '2023-01-01'
    -- Exclude problematic transfers
    AND (
        p.payment_type_id != 0 
        OR p.payment_notes NOT LIKE '%INCOME TRANSFER%'
        OR (
            p.payment_id NOT IN (SELECT payment_id FROM ProblematicTransfers)
            AND p.payment_id NOT IN (SELECT payment_id FROM UnbalancedTransfers)
        )
    )
    -- Exclude future-dated payments
    AND p.payment_date <= CURRENT_DATE
    -- Validate refund payments (type 72)
    AND NOT (p.payment_type_id = 72 AND p.is_cc_completed = true)
    -- Validate recurring payments
    AND NOT (p.is_cc_completed = true AND p.recurring_charge_date IS NULL)
)

-- Final select with patient payment allocations
SELECT 
    ROW_NUMBER() OVER (ORDER BY pp.payment_id, pp.procedure_id) AS payment_allocation_id,
    'PATIENT' AS payment_source_type,
    pp.payment_id,
    pp.patient_id,
    pp.provider_id,
    pp.procedure_id,
    pp.adjustment_id,
    NULL::INTEGER AS payplan_id,
    NULL::INTEGER AS payplan_charge_id,
    pp.payment_amount AS split_amount,
    pp.payment_date,
    pp.procedure_date,
    pp.payment_type_id,
    pp.payment_source::TEXT,
    pp.payment_status,
    pp.process_status,
    pp.is_discount,
    pp.discount_type,
    pp.unearned_type,
    NULL::INTEGER AS payplan_debit_type,
    pp.merchant_fee,
    pp.payment_notes,
    pp.check_number,
    pp.bank_branch,
    pp.created_by_user_id,
    pp.entry_date,
    pp.updated_at,
    pp.deposit_id,
    pp.external_id,
    pp.is_cc_completed,
    pp.recurring_charge_date,
    pp.receipt_text,
    pd.item_name AS payment_type_description,
    CASE
        WHEN pp.payment_type_id = 0 THEN FALSE  -- Always exclude Type 0 (administrative) payments
        WHEN pp.payment_date <= CURRENT_DATE THEN TRUE  -- Include non-Type 0 payments on or before current date
        ELSE FALSE  -- Exclude future-dated payments
    END AS include_in_ar,
    
    -- Standardized metadata from primary source (patient payments)
    {{ standardize_intermediate_metadata(primary_source_alias='pp') }}
    
FROM PatientPayments pp
LEFT JOIN PaymentDefinitions pd
    ON pp.payment_type_id = pd.definition_id
LEFT JOIN ValidTransfers vt
    ON pp.payment_id = vt.payment_id
    AND pp.procedure_id = vt.procedure_id
    AND pp.payment_amount = vt.split_amount
    AND pp.unearned_type = vt.unearned_type
WHERE 
    (pp.payment_type_id != 0  -- Exclude Type 0 payments
    OR pp.payment_notes NOT LIKE '%INCOME TRANSFER%'  -- Allow non-transfer Type 0 payments
    OR vt.has_matching_pair = TRUE)  -- Allow valid transfer pairs
    AND pp.rn = 1  -- Only take the first occurrence of each unique combination
