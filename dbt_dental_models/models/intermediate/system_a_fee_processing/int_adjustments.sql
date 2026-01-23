{{ config(
    materialized='table',
    schema='int',
    unique_key='adjustment_id',
    on_schema_change='fail',
    indexes=[
        {'columns': ['adjustment_id'], 'unique': true},
        {'columns': ['patient_id']},
        {'columns': ['_updated_at']}
    ]) }}

/*
    Intermediate model for adjustments
    Part of System A: Fee Processing & Verification
    
    This model:
    1. Enhances staging adjustment data with business categorization
    2. Provides comprehensive adjustment classification and analysis
    3. Links adjustments to procedures and unearned income tracking
    
    Business Logic Features:
    - Adjustment categorization: Maps OpenDental adjustment types to business categories
    - Note-based flags: Extracts business context from adjustment notes
    - Financial analysis: Calculates adjustment impact and sizing
    - Unearned income: Links to paysplit unearned income tracking
    
    Data Quality Notes:
    - Unearned income: Linked through paysplit table when adjustment_id exists
    - Adjustment impact: Calculated as percentage of original procedure fee
    - Provider discretion: Identified through type codes and note patterns
    
    Performance Considerations:
    - Uses table materialization for complex business logic
    - Indexed on adjustment_id (unique), patient_id, and _updated_at
*/

-- 1. Source CTEs
with source_adjustments as (
    select * from {{ ref('stg_opendental__adjustment') }}
),

source_procedures as (
    select * from {{ ref('int_procedure_complete') }}
),

-- 2. Lookup/Definition CTEs
adjustment_definitions as (
    select 
        definition_id,
        item_name,
        item_value,
        category_id
    from {{ ref('stg_opendental__definition') }}
),

paysplit_unearned as (
    select distinct
        adjustment_id,
        unearned_type
    from {{ ref('stg_opendental__paysplit') }}
    where adjustment_id is not null
        and unearned_type in (288, 439)
),

-- 3. Business Logic CTEs
adjustment_categorization as (
    select
        a.*,
        case
            when a.adjustment_type_id = 188 then 'insurance_writeoff'
            when a.adjustment_type_id = 474 then 'provider_discount'
            when a.adjustment_type_id = 186 then 'senior_discount'
            when a.adjustment_type_id = 235 then 'reallocation'
            when a.adjustment_type_id = 472 then 'employee_discount'
            when a.adjustment_type_id = 475 then 'provider_discount'
            when a.adjustment_type_id in (9, 185) then 'cash_discount'
            when a.adjustment_type_id in (18, 337) then 'patient_refund'
            when a.adjustment_type_id = 483 then 'referral_credit'
            when a.adjustment_type_id = 537 then 'new_patient_discount'
            when a.adjustment_type_id = 485 then 'employee_discount'
            when a.adjustment_type_id = 549 then 'admin_correction'
            when a.adjustment_type_id = 550 then 'admin_adjustment'
            when pu.adjustment_id is not null then 'unearned_income'
            else 'other'
        end as adjustment_category,
        
        case
            when abs(a.adjustment_amount) >= 1000 then 'large'
            when abs(a.adjustment_amount) >= 500 then 'medium'
            when abs(a.adjustment_amount) >= 100 then 'small'
            else 'minimal'
        end as adjustment_size
    from source_adjustments a
    left join paysplit_unearned pu
        on a.adjustment_id = pu.adjustment_id
),

adjustment_flags as (
    select
        ac.*,
        -- Note-based flags
        case 
            when lower(ac.adjustment_note) like '%n/c%' 
              or lower(ac.adjustment_note) like '%nc %'
              or lower(ac.adjustment_note) like '%no charge%' then true
            else false
        end as is_no_charge,

        case
            when lower(ac.adjustment_note) like '%military%' then true
            else false
        end as is_military_discount,

        case
            when lower(ac.adjustment_note) like '%warranty%' 
              or lower(ac.adjustment_note) like '%courtesy%' then true
            else false
        end as is_courtesy_adjustment,

        case
            when ac.adjustment_type_id in (474, 475) 
              or lower(ac.adjustment_note) like '%per dr%'
              or lower(ac.adjustment_note) like '%dr.%' then true
            else false
        end as is_provider_discretion,
        
        -- Discount type flags
        case 
            when ac.adjustment_type_id in (472, 485, 655) then true
            else false
        end as is_employee_discount,
        
        case
            when ac.adjustment_type_id in (482, 486) then true
            else false
        end as is_family_discount,
        
        case
            when ac.adjustment_type_id in (474, 475, 601) then true
            else false
        end as is_provider_discount,
        
        -- Financial analysis flags
        case 
            when ac.adjustment_type_id in (486, 474) and ac.adjustment_amount < -1000 then true
            else false
        end as is_large_adjustment,
        
        case
            when ac.adjustment_type_id in (186, 9) and ac.adjustment_amount > -50 then true
            else false
        end as is_minor_adjustment,
        
        case 
            when ac.adjustment_type_id in (288, 439) then true
            else false
        end as is_unearned_income
    from adjustment_categorization ac
),

-- 4. Integration CTE (joins everything together)
adjustment_integrated as (
    select
        -- Core adjustment fields
        af.adjustment_id,
        af.patient_id,
        af.procedure_id,
        af.provider_id,
        af.clinic_id,
        af.adjustment_amount,
        af.adjustment_date,
        af.procedure_date,
        af.adjustment_type_id,
        af.adjustment_note,
        af.date_entry,
        af.statement_id,
        af.tax_transaction_id,
        
        -- Basic calculated fields from staging
        af.adjustment_direction,
        af.is_procedure_adjustment,
        af.is_retroactive_adjustment,
        
        -- Enhanced categorization
        af.adjustment_category,
        af.adjustment_size,
        
        -- Flag fields
        af.is_no_charge,
        af.is_military_discount,
        af.is_courtesy_adjustment,
        af.is_provider_discretion,
        af.is_employee_discount,
        af.is_family_discount,
        af.is_provider_discount,
        af.is_large_adjustment,
        af.is_minor_adjustment,
        af.is_unearned_income,
        
        -- Unearned type from paysplit lookup
        pu.unearned_type as unearned_type_id,
        
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
        pc.procedure_fee + coalesce(af.adjustment_amount, 0) as adjusted_fee,
        
        -- Adjustment impact calculation
        case
            when abs(af.adjustment_amount) / nullif(pc.procedure_fee, 0) > 0.5 then 'major'
            when abs(af.adjustment_amount) / nullif(pc.procedure_fee, 0) > 0.1 then 'moderate'
            else 'minor'
        end as adjustment_impact,
        
        -- Metadata fields (standardized pattern)
        {{ standardize_intermediate_metadata(primary_source_alias='af') }}
        
    from adjustment_flags af
    left join source_procedures pc
        on af.procedure_id = pc.procedure_id
    left join adjustment_definitions def
        on af.adjustment_type_id = def.definition_id
    left join paysplit_unearned pu
        on af.adjustment_id = pu.adjustment_id
)

select * from adjustment_integrated
where adjustment_id is not null
