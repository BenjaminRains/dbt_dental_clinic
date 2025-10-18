{{ config(
    materialized='incremental',
    unique_key='paysplit_id',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_payment_id_idx ON {{ this }} (payment_id)",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_patient_id_idx ON {{ this }} (patient_id)",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_payment_date_idx ON {{ this }} (payment_date)",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_patient_payment_date_idx ON {{ this }} (patient_id, payment_date)"
    ]
) }}

/*
    Intermediate model for payment splits
    Focuses on split categorization and validation
    Part of System C: Payment Allocation & Reconciliation
    
    This model:
    1. Categorizes splits by type (normal, discount, unearned)
    2. Validates split business rules
    3. Provides split analytics and metadata
    4. Maintains relationships with procedures and adjustments
    5. Handles high-split payments efficiently
    6. Implements transfer payment rules
*/

WITH split_definitions AS (
    SELECT DISTINCT
        definition_id,
        item_name,
        item_value,
        category_id
    FROM {{ ref('stg_opendental__definition') }}
),

-- Get payment information for split validation
payment_info AS MATERIALIZED (
    SELECT DISTINCT
        p.payment_id,
        p.payment_type_id,
        p.payment_date,
        p.payment_notes,
        p.patient_id,
        p.payment_amount,
        p.is_split,
        p.is_recurring_cc,
        p.payment_status,
        p.process_status,
        p.merchant_fee,
        p._created_by as created_by_user_id,
        p.entry_date,
        p._updated_at as updated_at,
        p.deposit_id,
        p.external_id,
        p.is_cc_completed,
        p.recurring_charge_date,
        p.receipt_text,
        -- Metadata fields for standardize_intermediate_metadata macro
        p._loaded_at,
        p._transformed_at,
        p._created_at,
        p._updated_at,
        p._created_by
    FROM {{ ref('stg_opendental__payment') }} p
    WHERE p.payment_date >= '2023-01-01'
),

-- Get split counts for validation
split_counts AS MATERIALIZED (
    SELECT 
        ps.payment_id,
        COUNT(*) as total_splits,
        COUNT(DISTINCT ps.procedure_id) as unique_procedures,
        COUNT(DISTINCT ps.provider_id) as unique_providers,
        COUNT(CASE WHEN ps.unearned_type IN (0, 288) THEN 1 END) as transfer_splits,
        COUNT(CASE WHEN ps.unearned_type = 439 THEN 1 END) as treatment_plan_splits,
        COUNT(CASE WHEN pc.procedure_discount > 0 OR adj.adjustment_type_id IN (186, 472, 474, 475, 486, 9) THEN 1 END) as discount_splits
    FROM {{ ref('stg_opendental__paysplit') }} ps
    LEFT JOIN {{ ref('int_procedure_complete') }} pc
        ON ps.procedure_id = pc.procedure_id
    LEFT JOIN {{ ref('int_adjustments') }} adj
        ON ps.adjustment_id = adj.adjustment_id
    GROUP BY ps.payment_id
),

base_splits AS (
    SELECT
        -- Primary key and relationships
        ps.paysplit_id,
        ps.payment_id,
        ps.patient_id,
        ps.clinic_id,
        ps.provider_id,
        ps.procedure_id,
        ps.adjustment_id,
        ps.forward_split_id,
        
        -- Split details
        ps.split_amount,
        ps.payment_date,
        ps.procedure_date,
        
        -- Flags and types
        ps.is_discount,
        ps.discount_type,
        ps.unearned_type,
        
        -- Metadata fields for standardize_intermediate_metadata macro
        ps._loaded_at,
        ps._created_at,
        ps._updated_at,
        ps._created_by,
        
        -- Legacy metadata fields (for compatibility)
        ps.entry_date,
        ps._updated_at as updated_at,
        ps._created_by as created_by_user_id,
        
        -- Link to procedure data if available
        pc.procedure_code,
        pc.procedure_description,
        pc.procedure_fee,
        pc.procedure_status,
        pc.procedure_discount,
        
        -- Link to adjustment data if available
        adj.adjustment_amount,
        adj.adjustment_type_name,
        adj.adjustment_category_type,
        adj.adjustment_type_id,
        
        -- Payment info
        p.payment_type_id,
        p.payment_notes,
        p.is_split,
        p.is_recurring_cc,
        p.payment_status,
        p.process_status,
        p.merchant_fee,
        p.is_cc_completed,
        p.recurring_charge_date,
        
        -- Split counts
        sc.total_splits,
        sc.unique_procedures,
        sc.unique_providers,
        sc.transfer_splits,
        sc.treatment_plan_splits,
        sc.discount_splits
        
    FROM {{ ref('stg_opendental__paysplit') }} ps
    LEFT JOIN payment_info p
        ON ps.payment_id = p.payment_id
    LEFT JOIN {{ ref('int_procedure_complete') }} pc
        ON ps.procedure_id = pc.procedure_id
    LEFT JOIN {{ ref('int_adjustments') }} adj
        ON ps.adjustment_id = adj.adjustment_id
    LEFT JOIN split_counts sc
        ON ps.payment_id = sc.payment_id
    WHERE p.payment_date >= '2023-01-01'
),

split_categorization AS (
    SELECT
        *,
        
        -- Split type categorization
        CASE
            WHEN procedure_discount > 0 OR adjustment_type_id IN (186, 472, 474, 475, 486, 9) THEN 'DISCOUNT'
            WHEN unearned_type = 288 THEN 'UNEARNED_REVENUE'
            WHEN unearned_type = 439 THEN 'TREATMENT_PLAN_PREPAYMENT'
            WHEN unearned_type = 0 AND payment_notes LIKE '%INCOME TRANSFER%' THEN 'INCOME_TRANSFER'
            ELSE 'NORMAL_PAYMENT'
        END AS split_type,
        
        -- Split validation flags
        CASE
            WHEN split_amount = 0 AND 
                 procedure_discount = 0 AND 
                 adjustment_type_id NOT IN (186, 472, 474, 475, 486, 9) AND
                 unearned_type != 288 AND 
                 procedure_id IS NULL THEN FALSE
            ELSE TRUE
        END AS is_valid_zero_amount,
        
        CASE
            WHEN unearned_type IN (288, 439) AND provider_id IS NULL THEN FALSE
            ELSE TRUE
        END AS is_valid_unearned_type,
        
        CASE
            WHEN procedure_id IS NULL AND 
                 adjustment_id IS NULL THEN FALSE
            ELSE TRUE
        END AS is_valid_allocation,
        
        -- Transfer validation
        CASE
            WHEN payment_type_id = 0 AND 
                 payment_notes LIKE '%INCOME TRANSFER%' AND
                 total_splits > 20 THEN FALSE
            ELSE TRUE
        END AS is_valid_split_count,
        
        CASE
            WHEN payment_type_id = 0 AND 
                 payment_notes LIKE '%INCOME TRANSFER%' AND
                 unearned_type = 439 AND
                 procedure_status != 1 THEN FALSE
            ELSE TRUE
        END AS is_valid_treatment_plan,
        
        -- Split impact classification
        CASE
            WHEN ABS(split_amount) / NULLIF(procedure_fee, 0) > 0.5 THEN 'major'
            WHEN ABS(split_amount) / NULLIF(procedure_fee, 0) > 0.1 THEN 'moderate'
            ELSE 'minor'
        END AS split_impact,
        
        -- Split analytics
        CASE
            WHEN split_amount < 0 THEN 'REFUND'
            WHEN split_amount = 0 THEN 'ZERO'
            WHEN split_amount <= 100 THEN 'SMALL'
            WHEN split_amount <= 1000 THEN 'MEDIUM'
            WHEN split_amount <= 5000 THEN 'LARGE'
            ELSE 'VERY_LARGE'
        END AS amount_category,
        
        -- High split payment flag
        CASE
            WHEN total_splits > 20 THEN TRUE
            ELSE FALSE
        END AS is_high_split_payment,
        
        -- New discount fields
        COALESCE(procedure_discount, 0) + 
        COALESCE(CASE 
            WHEN adjustment_type_id IN (186, 472, 474, 475, 486, 9) 
            THEN adjustment_amount 
            ELSE 0 
        END, 0) as combined_discount_amount,
        
        CASE
            WHEN procedure_discount > 0 AND adjustment_amount < 0 THEN 'COMBINED_DISCOUNT'
            WHEN procedure_discount > 0 THEN 'PROCEDURE_DISCOUNT'
            WHEN adjustment_amount < 0 THEN 'ADJUSTMENT_DISCOUNT'
            ELSE 'NO_DISCOUNT'
        END as discount_category,
        
        CASE
            WHEN adjustment_type_id = 186 THEN 'SENIOR'
            WHEN adjustment_type_id IN (472, 474, 475) THEN 'PROVIDER'
            WHEN adjustment_type_id = 486 THEN 'FAMILY'
            WHEN adjustment_type_id = 9 THEN 'CASH'
            WHEN procedure_discount > 0 THEN 'PROCEDURE'
            WHEN procedure_discount > 0 AND adjustment_amount < 0 THEN 'COMBINED'
            ELSE NULL
        END as discount_source_type
        
    FROM base_splits
)

SELECT 
    *,
    -- dbt intermediate model build timestamp (model-specific tracking)
    current_timestamp as _transformed_at
    
FROM split_categorization
WHERE 
    -- Exclude invalid high-split payments
    (NOT is_high_split_payment OR is_valid_split_count)
    -- Exclude invalid treatment plan payments
    AND is_valid_treatment_plan
    -- Exclude invalid zero amount payments
    AND is_valid_zero_amount
    -- Exclude invalid unearned type payments
    AND is_valid_unearned_type
    -- Exclude invalid allocations
    AND is_valid_allocation
