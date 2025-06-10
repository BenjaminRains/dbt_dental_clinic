{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='adjustment_id'
    )
}}

with adjustment_definitions as (
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

adjustment_enhanced as (
    select
        -- Core adjustment fields from staging
        a.adjustment_id,
        a.patient_id,
        a.procedure_id,
        a.provider_id,
        a.clinic_id,
        a.adjustment_amount,
        a.adjustment_date,
        a.procedure_date,
        a.adjustment_type_id,
        a.adjustment_note,
        a.entry_date,
        a.statement_id,
        a.tax_transaction_id,
        
        -- Basic calculated fields from staging
        a.adjustment_direction,
        a.is_procedure_adjustment,
        a.is_retroactive_adjustment,
        
        -- Enhanced adjustment categorization (business logic from staging)
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

        -- Note-based flags (business logic from staging)
        case 
            when lower(a.adjustment_note) like '%n/c%' 
              or lower(a.adjustment_note) like '%nc %'
              or lower(a.adjustment_note) like '%no charge%' then true
            else false
        end as is_no_charge,

        case
            when lower(a.adjustment_note) like '%military%' then true
            else false
        end as is_military_discount,

        case
            when lower(a.adjustment_note) like '%warranty%' 
              or lower(a.adjustment_note) like '%courtesy%' then true
            else false
        end as is_courtesy_adjustment,

        case
            when a.adjustment_type_id in (474, 475) 
              or lower(a.adjustment_note) like '%per dr%'
              or lower(a.adjustment_note) like '%dr.%' then true
            else false
        end as is_provider_discretion,

        -- Amount-based classifications (business logic from staging)
        case
            when abs(a.adjustment_amount) >= 1000 then 'large'
            when abs(a.adjustment_amount) >= 500 then 'medium'
            when abs(a.adjustment_amount) >= 100 then 'small'
            else 'minimal'
        end as adjustment_size,

        -- Discount type flags (business logic from staging)
        case 
            when a.adjustment_type_id in (472, 485, 655) then true
            else false
        end as is_employee_discount,
        
        case
            when a.adjustment_type_id in (482, 486) then true
            else false
        end as is_family_discount,
        
        case
            when a.adjustment_type_id in (474, 475, 601) then true
            else false
        end as is_provider_discount,
        
        -- Financial analysis flags (business logic from staging)
        case 
            when a.adjustment_type_id in (486, 474) and a.adjustment_amount < -1000 then true
            else false
        end as is_large_adjustment,
        
        case
            when a.adjustment_type_id in (186, 9) and a.adjustment_amount > -50 then true
            else false
        end as is_minor_adjustment,
        
        case 
            when a.adjustment_type_id in (288, 439) then true
            else false
        end as is_unearned_income,
        
        -- Unearned type from paysplit lookup (business logic from staging)
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
        pc.procedure_fee + coalesce(a.adjustment_amount, 0) as adjusted_fee,
        
        -- Adjustment impact flag
        case
            when abs(a.adjustment_amount) / nullif(pc.procedure_fee, 0) > 0.5 then 'major'
            when abs(a.adjustment_amount) / nullif(pc.procedure_fee, 0) > 0.1 then 'moderate'
            else 'minor'
        end as adjustment_impact,
        
        -- Metadata fields
        a._loaded_at,
        a._created_at,
        a._updated_at,
        a._created_by_user_id
        
    from {{ ref('stg_opendental__adjustment') }} a
    left join {{ ref('int_procedure_complete') }} pc
        on a.procedure_id = pc.procedure_id
    left join adjustment_definitions def
        on a.adjustment_type_id = def.definition_id
    left join paysplit_unearned pu
        on a.adjustment_id = pu.adjustment_id
)

select * from adjustment_enhanced