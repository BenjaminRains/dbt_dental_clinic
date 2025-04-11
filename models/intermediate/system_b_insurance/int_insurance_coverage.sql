{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

with Source as (
    select * from {{ ref('stg_opendental__insplan') }}
),

InsurancePlan as (
    select
        -- Primary Key
        insurance_plan_id,
        
        -- Foreign Keys
        carrier_id,
        
        -- Plan Details
        group_name,
        group_number,
        plan_type,
        
        -- Flags
        is_medical,
        is_hidden,
        show_base_units,
        code_subst_none,
        hide_from_verify_list,
        has_ppo_subst_writeoffs,
        is_blue_book_enabled,
        
        -- Meta Fields
        created_at,
        updated_at
    from Source
),

Carrier as (
    select
        carrier_id,
        carrier_name
    from {{ ref('stg_opendental__carrier') }}
),

Subscriber as (
    select
        inssub_id as subscriber_id,
        subscriber_external_id,
        entry_date as subscriber_created_at,
        last_modified_at as subscriber_updated_at
    from {{ ref('stg_opendental__inssub') }}
),

PatientPlan as (
    select
        patient_id,
        patplan_id,
        insurance_subscriber_id,
        ordinal,
        is_pending,
        relationship as patient_relationship,
        created_at as patient_plan_created_at,
        updated_at as patient_plan_updated_at
    from {{ ref('stg_opendental__patplan') }}
),

Verification as (
    select
        foreign_key_id as insurance_plan_id,
        last_verified_date,
        entry_timestamp,
        last_modified_at
    from {{ ref('stg_opendental__insverify') }}
    where verify_type = 1  -- Only get insurance subscriber verifications
),

Benefits as (
    select
        insurance_plan_id,
        json_agg(
            json_build_object(
                'benefit_id', benefit_id,
                'coverage_category_id', coverage_category_id,
                'procedure_code_id', procedure_code_id,
                'code_group_id', code_group_id,
                'benefit_type', benefit_type,
                'coverage_percent', coverage_percent,
                'monetary_amount', monetary_amount,
                'time_period', time_period,
                'quantity_qualifier', quantity_qualifier,
                'quantity', quantity,
                'coverage_level', coverage_level,
                'treatment_area', treatment_area
            )
        ) as benefit_details
    from {{ ref('stg_opendental__benefit') }}
    where patient_plan_id = 0  -- Only get template benefits
    group by insurance_plan_id
),

Final as (
    select
        -- Primary Key
        pp.patplan_id as insurance_plan_id,

        -- Foreign Keys
        pp.patient_id,
        ip.carrier_id,
        pp.insurance_subscriber_id as subscriber_id,

        -- Plan Details
        ip.plan_type,
        ip.group_number,
        ip.group_name,
        c.carrier_name,

        -- Verification Status
        v.last_verified_date as verification_date,

        -- Benefit Details
        b.benefit_details,

        -- Status Flags
        case
            when pp.is_pending = 1 then false
            when v.last_verified_date is not null then true
            else false
        end as is_active,

        -- Dates
        pp.patient_plan_created_at as effective_date,
        case
            when pp.is_pending = 1 then pp.patient_plan_updated_at
            else null
        end as termination_date,

        -- Meta Fields
        greatest(
            ip.created_at,
            pp.patient_plan_created_at,
            v.entry_timestamp
        ) as created_at,
        greatest(
            ip.updated_at,
            pp.patient_plan_updated_at,
            v.last_modified_at
        ) as updated_at

    from PatientPlan pp
    left join InsurancePlan ip
        on pp.patplan_id = ip.insurance_plan_id
    left join Carrier c
        on ip.carrier_id = c.carrier_id
    left join Subscriber s
        on pp.insurance_subscriber_id = s.subscriber_id
    left join Verification v
        on pp.patplan_id = v.insurance_plan_id
    left join Benefits b
        on pp.patplan_id = b.insurance_plan_id
)

select * from Final 