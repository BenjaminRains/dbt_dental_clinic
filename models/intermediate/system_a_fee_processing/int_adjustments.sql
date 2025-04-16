{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='adjustment_id'
    )
}}

-- int_procedure_adjustments.sql
WITH AdjustmentDefinitions AS (
    SELECT 
        definition_id,
        item_name,
        item_value,
        category_id
    FROM {{ ref('stg_opendental__definition') }}
),

ProcedureAdjustments AS (
    SELECT
        -- Adjustment fields
        a.adjustment_id,
        a.patient_id,
        a.procedure_id,
        a.provider_id,
        a.clinic_id,
        a.adjustment_amount,
        a.adjustment_date,
        a.procedure_date,
        a.adjustment_category,
        a.adjustment_size,
        a.is_procedure_adjustment,
        a.is_retroactive_adjustment,
        a.is_provider_discretion,
        a.is_employee_discount,
        a.is_military_discount,
        a.is_courtesy_adjustment,
        a.adjustment_type_id,
        
        -- Definition linkage
        def.item_name as adjustment_type_name,
        def.item_value as adjustment_type_value,
        def.category_id as adjustment_category_type,
        
        -- Link to procedure data
        pc.procedure_code,
        pc.procedure_description,
        pc.procedure_fee,
        pc.fee_schedule_id,
        pc.standard_fee,
        
        -- Calculate adjusted fee
        pc.procedure_fee + COALESCE(a.adjustment_amount, 0) AS adjusted_fee,
        
        -- Adjustment impact flag
        CASE
            WHEN ABS(a.adjustment_amount) / NULLIF(pc.procedure_fee, 0) > 0.5 THEN 'major'
            WHEN ABS(a.adjustment_amount) / NULLIF(pc.procedure_fee, 0) > 0.1 THEN 'moderate'
            ELSE 'minor'
        END AS adjustment_impact
        
    FROM {{ ref('stg_opendental__adjustment') }} a
    LEFT JOIN {{ ref('int_procedure_complete') }} pc
        ON a.procedure_id = pc.procedure_id
    LEFT JOIN AdjustmentDefinitions def
        ON a.adjustment_type_id = def.definition_id
)

SELECT * FROM ProcedureAdjustments