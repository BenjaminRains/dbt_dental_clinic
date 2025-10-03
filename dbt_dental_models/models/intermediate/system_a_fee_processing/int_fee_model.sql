{{ config(        materialized='incremental',
        schema='intermediate',
        unique_key='procedure_id',
        on_schema_change='fail',
        incremental_strategy='merge',
        indexes=[
            {'columns': ['procedure_id'], 'unique': true},
            {'columns': ['patient_id']},
            {'columns': ['provider_id']},
            {'columns': ['procedure_date']},
            {'columns': ['_updated_at']}
        ]) }}

/*
    Intermediate model for Fee Processing and Analysis
    Part of System A: Fee Processing & Verification
    
    This model:
    1. Standardizes fee schedules across different providers and clinics
    2. Calculates fee variances and adjustment impacts
    3. Tracks provider discretion and discount patterns
    
    Business Logic Features:
    - Fee Standardization: Identifies standard fees per procedure code
    - Variance Analysis: Calculates fee differences from standard rates
    - Adjustment Categorization: Classifies adjustments by impact level
    - Discount Tracking: Identifies employee, military, and courtesy adjustments
    
    Data Quality Notes:
    - Fee amounts may have historical inconsistencies pre-2023
    - Some procedures may lack standard fee assignments
    - Adjustment types depend on proper definition table maintenance
    
    Performance Considerations:
    - Incremental processing based on procedure dates
    - Indexes on key join columns for optimal performance
*/

-- 1. Source CTEs (multiple sources)
with source_procedures as (
    select * from {{ ref('stg_opendental__procedurelog') }}
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

source_fees as (
    select * from {{ ref('stg_opendental__fee') }}
),

source_fee_schedules as (
    select * from {{ ref('stg_opendental__feesched') }}
),

source_adjustments as (
    select * from {{ ref('stg_opendental__adjustment') }}
),

source_procedure_codes as (
    select * from {{ ref('stg_opendental__procedurecode') }}
),

-- 2. Lookup/Definition CTEs
adjustment_definitions as (
    select 
        definition_id,
        item_name,
        item_value,
        category_id
    from {{ ref('stg_opendental__definition') }}
    where category_id in (0, 1, 15) -- Adjustment-related categories
),

-- 3. Calculation/Aggregation CTEs
standard_fees as (
    select distinct on (procedure_code_id)
        fee_id as standard_fee_id,
        procedure_code_id,
        fee_schedule_id,
        fee_amount as standard_fee
    from source_fees
    order by procedure_code_id, is_default_fee desc, _created_at desc
),

fee_statistics as (
    select
        procedure_code_id,
        count(distinct fee_schedule_id) as available_fee_options,
        min(fee_amount) as min_available_fee,
        max(fee_amount) as max_available_fee,
        avg(fee_amount) as avg_fee_amount
    from source_fees
    group by procedure_code_id
),

-- 4. Business Logic CTEs
procedure_fees_enhanced as (
    select
        -- Primary identification
        pl.procedure_id,
        pl.patient_id,
        pl.provider_id,
        pl.clinic_id,
        pl.procedure_code_id,
        
        -- Procedure details
        pl.procedure_date,
        pl.procedure_status,
        pl.procedure_fee as applied_fee,
        
        -- Fee schedule information
        sf.standard_fee_id,
        sf.standard_fee,
        fs.fee_schedule_id,
        fs.fee_schedule_description,
        fs.fee_schedule_type_id,
        fs.is_hidden as is_fee_schedule_hidden,
        fs.is_global_flag as is_global_fee_schedule,
        
        -- Fee statistics
        fstats.available_fee_options,
        fstats.min_available_fee,
        fstats.max_available_fee,
        fstats.avg_fee_amount,
        
        -- Procedure code details
        pc.procedure_code,
        pc.description as procedure_description,
        pc.abbreviated_description,
        pc.is_hygiene as is_hygiene_procedure,
        pc.is_prosthetic as is_prosthetic_procedure,
        pc.is_multi_visit as is_multi_visit_procedure,
        
        -- Metadata
        pl._loaded_at,
        pl._created_at,
        pl._updated_at
        
    from source_procedures pl
    left join standard_fees sf
        on pl.procedure_code_id = sf.procedure_code_id
    left join source_fee_schedules fs
        on sf.fee_schedule_id = fs.fee_schedule_id
    left join fee_statistics fstats
        on pl.procedure_code_id = fstats.procedure_code_id
    left join source_procedure_codes pc
        on pl.procedure_code_id = pc.procedure_code_id
    where pl.procedure_date >= '2023-01-01'  -- Business rule: Focus on recent data
),

fee_adjustments_categorized as (
    select
        a.adjustment_id,
        a.patient_id,
        a.procedure_id,
        a.provider_id,
        a.clinic_id,
        a.adjustment_amount,
        a.adjustment_date,
        p.procedure_date,
        
        -- Adjustment type details
        d.item_name as adjustment_type_name,
        d.item_value as adjustment_type_value,
        d.category_id as adjustment_category_type,
        
        -- Adjustment impact categorization
        case 
            when a.adjustment_amount < 0 and abs(a.adjustment_amount) >= p.applied_fee * 0.5 then 'major'
            when a.adjustment_amount < 0 and abs(a.adjustment_amount) >= p.applied_fee * 0.25 then 'moderate'
            else 'minor'
        end as adjustment_impact,
        
        -- Adjustment flags
        case when a.procedure_id is not null then true else false end as is_procedure_adjustment,
        case when a.adjustment_date > p.procedure_date then true else false end as is_retroactive_adjustment,
        case when d.category_id = 0 and d.item_value = 'Provider Discretion' then true else false end as is_provider_discretion,
        case when d.category_id = 15 and d.item_value like '%Employee%' then true else false end as is_employee_discount,
        case when d.category_id = 15 and d.item_value like '%Military%' then true else false end as is_military_discount,
        case when d.category_id = 15 and d.item_value like '%Courtesy%' then true else false end as is_courtesy_adjustment
        
    from source_adjustments a
    left join procedure_fees_enhanced p
        on a.procedure_id = p.procedure_id
    left join adjustment_definitions d
        on a.adjustment_type_id = d.definition_id
),

-- 5. Integration CTE (joins everything together)
fee_model_integrated as (
    select
        -- Core fields
        p.procedure_id,
        p.patient_id,
        p.provider_id,
        p.clinic_id,
        p.procedure_code_id,
        p.procedure_code,
        p.procedure_description,
        p.abbreviated_description,
        
        -- Procedure details
        p.procedure_date,
        p.procedure_status,
        p.applied_fee,
        
        -- Fee schedule information
        p.standard_fee_id,
        p.standard_fee,
        p.fee_schedule_id,
        p.fee_schedule_description,
        p.fee_schedule_type_id,
        p.is_fee_schedule_hidden,
        p.is_global_fee_schedule,
        
        -- Fee analysis
        case when p.standard_fee is not null then true else false end as has_standard_fee,
        case when p.applied_fee = p.standard_fee then true else false end as fee_matches_standard,
        case 
            when p.applied_fee > p.standard_fee then 'above_standard'
            when p.applied_fee < p.standard_fee then 'below_standard'
            else 'matches_standard'
        end as fee_relationship,
        case 
            when p.standard_fee > 0 then ((p.applied_fee - p.standard_fee) / p.standard_fee) * 100
            else 0
        end as fee_variance_pct,
        
        -- Fee statistics
        p.available_fee_options,
        p.min_available_fee,
        p.max_available_fee,
        p.avg_fee_amount,
        
        -- Adjustment summary
        coalesce(sum(a.adjustment_amount), 0) as total_adjustments,
        count(distinct a.adjustment_id) as adjustment_count,
        string_agg(distinct a.adjustment_type_name, ', ' order by a.adjustment_type_name) as adjustment_types,
        min(a.adjustment_date) as first_adjustment_date,
        max(a.adjustment_date) as last_adjustment_date,
        
        -- Effective fee calculation
        p.applied_fee + coalesce(sum(a.adjustment_amount), 0) as effective_fee,
        
        -- Overall adjustment impact
        case 
            when coalesce(sum(a.adjustment_amount), 0) < 0 and abs(coalesce(sum(a.adjustment_amount), 0)) >= p.applied_fee * 0.5 then 'major'
            when coalesce(sum(a.adjustment_amount), 0) < 0 and abs(coalesce(sum(a.adjustment_amount), 0)) >= p.applied_fee * 0.25 then 'moderate'
            else 'minor'
        end as overall_adjustment_impact,
        
        -- Adjustment type flags
        case when bool_or(a.is_provider_discretion) then true else false end as has_provider_discretion,
        case when bool_or(a.is_employee_discount) then true else false end as has_employee_discount,
        case when bool_or(a.is_military_discount) then true else false end as has_military_discount,
        case when bool_or(a.is_courtesy_adjustment) then true else false end as has_courtesy_adjustment,
        
        -- Additional adjustment details (from original model)
        max(a.adjustment_type_name) as adjustment_type_name,
        max(a.adjustment_category_type) as adjustment_category_type,
        max(a.adjustment_id) as adjustment_id,
        
        -- Procedure type flags
        p.is_hygiene_procedure,
        p.is_prosthetic_procedure,
        p.is_multi_visit_procedure,
        
        -- Metadata (using standardized macro - only fields that exist in source)
        {{ standardize_intermediate_metadata(primary_source_alias='p', source_metadata_fields=['_loaded_at', '_created_at', '_updated_at']) }}
        
    from procedure_fees_enhanced p
    left join fee_adjustments_categorized a
        on p.procedure_id = a.procedure_id
    group by 
        p.procedure_id,
        p.patient_id,
        p.provider_id,
        p.clinic_id,
        p.procedure_code_id,
        p.procedure_code,
        p.procedure_description,
        p.abbreviated_description,
        p.procedure_date,
        p.procedure_status,
        p.applied_fee,
        p.standard_fee_id,
        p.standard_fee,
        p.fee_schedule_id,
        p.fee_schedule_description,
        p.fee_schedule_type_id,
        p.is_fee_schedule_hidden,
        p.is_global_fee_schedule,
        p.available_fee_options,
        p.min_available_fee,
        p.max_available_fee,
        p.avg_fee_amount,
        p.is_hygiene_procedure,
        p.is_prosthetic_procedure,
        p.is_multi_visit_procedure,
        p._loaded_at,
        p._created_at,
        p._updated_at,
        a.adjustment_type_name,
        a.adjustment_category_type
),

-- 6. Final filtering/validation with distinct on (matching original pattern)
final as (
    select distinct on (procedure_id)
        *
    from fee_model_integrated
    where procedure_id is not null  -- Data quality filter
    order by procedure_id, procedure_date desc
)

select * from final
