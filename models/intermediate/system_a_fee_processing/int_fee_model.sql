{{
    config(
        materialized='incremental',
        unique_key='procedure_id'
    )
}}

WITH FeeSchedules AS (
    SELECT
        fee_schedule_id,
        fee_schedule_description,
        fee_schedule_type_id,
        is_hidden,
        is_global_flag
    FROM {{ ref('stg_opendental__feesched') }}
),

StandardFees AS (
    SELECT DISTINCT ON (procedure_code_id)
        fee_id AS standard_fee_id,
        procedure_code_id,
        fee_schedule_id,
        fee_amount AS standard_fee
    FROM {{ ref('stg_opendental__fee') }}
    ORDER BY procedure_code_id, is_default_fee DESC, created_at DESC
),

FeeStats AS (
    SELECT
        procedure_code_id,
        COUNT(DISTINCT fee_schedule_id) AS available_fee_options,
        MIN(fee_amount) AS min_available_fee,
        MAX(fee_amount) AS max_available_fee,
        AVG(fee_amount) AS avg_fee_amount
    FROM {{ ref('stg_opendental__fee') }}
    GROUP BY procedure_code_id
),

ProcedureFees AS (
    SELECT
        pl.procedure_id,
        pl.patient_id,
        pl.provider_id,
        pl.clinic_id,
        pl.procedure_code_id,
        pl.procedure_date,
        pl.procedure_status,
        pl.procedure_fee AS applied_fee,
        sf.standard_fee_id,
        sf.standard_fee,
        fs.fee_schedule_id,
        fs.fee_schedule_description,
        fs.fee_schedule_type_id,
        fs.is_hidden,
        fs.is_global_flag,
        fstats.available_fee_options,
        fstats.min_available_fee,
        fstats.max_available_fee,
        fstats.avg_fee_amount,
        pc.procedure_code,
        pc.description AS procedure_description,
        pc.abbreviated_description,
        pc.is_hygiene_flag,
        pc.is_prosthetic_flag,
        pc.is_multi_visit_flag
    FROM {{ ref('stg_opendental__procedurelog') }} pl
    LEFT JOIN StandardFees sf
        ON pl.procedure_code_id = sf.procedure_code_id
    LEFT JOIN FeeSchedules fs
        ON sf.fee_schedule_id = fs.fee_schedule_id
    LEFT JOIN FeeStats fstats
        ON pl.procedure_code_id = fstats.procedure_code_id
    LEFT JOIN {{ ref('stg_opendental__procedurecode') }} pc
        ON pl.procedure_code_id = pc.procedure_code_id
    WHERE pl.procedure_date >= '2023-01-01'  -- Filter for procedures from 2023 onwards
),

FeeAdjustments AS (
    SELECT
        a.adjustment_id,
        a.patient_id,
        a.procedure_id,
        a.provider_id,
        a.clinic_id,
        a.adjustment_amount,
        a.adjustment_date,
        p.procedure_date,
        d.item_name AS adjustment_type_name,
        d.item_value AS adjustment_type_value,
        d.category_id AS adjustment_category_type,
        CASE 
            WHEN a.adjustment_amount < 0 AND ABS(a.adjustment_amount) >= p.applied_fee * 0.5 THEN 'major'
            WHEN a.adjustment_amount < 0 AND ABS(a.adjustment_amount) >= p.applied_fee * 0.25 THEN 'moderate'
            ELSE 'minor'
        END AS adjustment_impact,
        CASE WHEN a.procedure_id IS NOT NULL THEN true ELSE false END AS is_procedure_adjustment,
        CASE WHEN a.adjustment_date > p.procedure_date THEN true ELSE false END AS is_retroactive_adjustment,
        CASE WHEN d.category_id = 0 AND d.item_value = 'Provider Discretion' THEN true ELSE false END AS is_provider_discretion,
        CASE WHEN d.category_id = 15 AND d.item_value LIKE '%Employee%' THEN true ELSE false END AS is_employee_discount,
        CASE WHEN d.category_id = 15 AND d.item_value LIKE '%Military%' THEN true ELSE false END AS is_military_discount,
        CASE WHEN d.category_id = 15 AND d.item_value LIKE '%Courtesy%' THEN true ELSE false END AS is_courtesy_adjustment
    FROM {{ ref('stg_opendental__adjustment') }} a
    LEFT JOIN ProcedureFees p
        ON a.procedure_id = p.procedure_id
    LEFT JOIN {{ ref('stg_opendental__definition') }} d
        ON a.adjustment_type_id = d.definition_id
),

FeeComparison AS (
    SELECT
        p.*,
        CASE WHEN p.standard_fee IS NOT NULL THEN true ELSE false END AS has_standard_fee,
        CASE WHEN p.applied_fee = p.standard_fee THEN true ELSE false END AS fee_matches_standard,
        CASE 
            WHEN p.applied_fee > p.standard_fee THEN 'above_standard'
            WHEN p.applied_fee < p.standard_fee THEN 'below_standard'
            ELSE 'matches_standard'
        END AS fee_relationship,
        CASE 
            WHEN p.standard_fee > 0 THEN ((p.applied_fee - p.standard_fee) / p.standard_fee) * 100
            ELSE 0
        END AS fee_variance_pct,
        COALESCE(SUM(a.adjustment_amount), 0) AS total_adjustments,
        COUNT(a.adjustment_id) AS adjustment_count,
        STRING_AGG(DISTINCT a.adjustment_type_name, ', ') AS adjustment_types,
        MIN(a.adjustment_date) AS first_adjustment_date,
        MAX(a.adjustment_date) AS last_adjustment_date,
        p.applied_fee + COALESCE(SUM(a.adjustment_amount), 0) AS effective_fee,
        CASE 
            WHEN COALESCE(SUM(a.adjustment_amount), 0) < 0 AND ABS(COALESCE(SUM(a.adjustment_amount), 0)) >= p.applied_fee * 0.5 THEN 'major'
            WHEN COALESCE(SUM(a.adjustment_amount), 0) < 0 AND ABS(COALESCE(SUM(a.adjustment_amount), 0)) >= p.applied_fee * 0.25 THEN 'moderate'
            ELSE 'minor'
        END AS adjustment_impact
    FROM ProcedureFees p
    LEFT JOIN FeeAdjustments a
        ON p.procedure_id = a.procedure_id
    GROUP BY 
        p.procedure_id,
        p.patient_id,
        p.provider_id,
        p.clinic_id,
        p.procedure_code_id,
        p.procedure_date,
        p.procedure_status,
        p.applied_fee,
        p.standard_fee_id,
        p.standard_fee,
        p.fee_schedule_id,
        p.fee_schedule_description,
        p.fee_schedule_type_id,
        p.is_hidden,
        p.is_global_flag,
        p.available_fee_options,
        p.min_available_fee,
        p.max_available_fee,
        p.avg_fee_amount,
        p.procedure_code,
        p.procedure_description,
        p.abbreviated_description,
        p.is_hygiene_flag,
        p.is_prosthetic_flag,
        p.is_multi_visit_flag
)

SELECT DISTINCT ON (procedure_id)
    *,
    CURRENT_TIMESTAMP AS _loaded_at
FROM FeeComparison
ORDER BY procedure_id, procedure_date DESC 