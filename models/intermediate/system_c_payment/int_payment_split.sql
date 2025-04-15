{{
    config(
        materialized='incremental',
        unique_key='paysplit_id',
        schema='intermediate'
    )
}}

/*
    Intermediate model for payment splits
    Focuses on split categorization and validation
    Part of System C: Payment Allocation & Reconciliation
    
    This model:
    1. Categorizes splits by type (normal, discount, unearned)
    2. Validates split business rules
    3. Provides split analytics and metadata
    4. Maintains relationships with procedures and adjustments
*/

WITH SplitDefinitions AS (
    SELECT 
        definition_id,
        item_name,
        item_value,
        category_id
    FROM {{ ref('stg_opendental__definition') }}
),

BaseSplits AS (
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
        ps.is_discount_flag,
        ps.discount_type,
        ps.unearned_type,
        
        -- Metadata
        ps.entry_date,
        ps.updated_at,
        ps.created_by_user_id,
        
        -- Link to procedure data if available
        pc.procedure_code,
        pc.procedure_description,
        pc.procedure_fee,
        
        -- Link to adjustment data if available
        adj.adjustment_amount,
        adj.adjustment_type_name,
        adj.adjustment_category_type
        
    FROM {{ ref('stg_opendental__paysplit') }} ps
    LEFT JOIN {{ ref('int_procedure_complete') }} pc
        ON ps.procedure_id = pc.procedure_id
    LEFT JOIN {{ ref('int_adjustments') }} adj
        ON ps.adjustment_id = adj.adjustment_id
),

SplitCategorization AS (
    SELECT
        *,
        
        -- Split type categorization
        CASE
            WHEN is_discount_flag THEN 'DISCOUNT'
            WHEN unearned_type = 288 THEN 'UNEARNED_REVENUE'
            WHEN unearned_type = 439 THEN 'TREATMENT_PLAN_PREPAYMENT'
            ELSE 'NORMAL_PAYMENT'
        END AS split_type,
        
        -- Split validation flags
        CASE
            WHEN split_amount = 0 AND 
                 NOT is_discount_flag AND 
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
        
        -- Tracking fields
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at
        
    FROM BaseSplits
)

SELECT * FROM SplitCategorization
