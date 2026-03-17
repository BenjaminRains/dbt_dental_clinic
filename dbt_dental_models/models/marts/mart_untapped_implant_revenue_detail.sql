{{ config(
    materialized='table',
    schema='marts',
    unique_key='patient_id',
    on_schema_change='fail',
    indexes=[
        {'columns': ['patient_id']},
        {'columns': ['first_extraction_bone_graft_date']},
        {'columns': ['estimated_balance']},
        {'columns': ['has_insurance_flag']}
    ]
) }}

/*
Enriched list of untapped implant patients: extraction + bone graft same day, no D6010.
Joins mart_untapped_implant_revenue to dim_patient and primary insurance for outreach and reporting.

Grain: One row per patient. Use for "who are these patients?" with balances, insurance, contact prefs.
*/

with cohort as (
    select
        patient_id,
        first_extraction_bone_graft_date,
        last_extraction_bone_graft_date,
        qualifying_visit_count,
        provider_id as treating_provider_id
    from {{ ref('mart_untapped_implant_revenue') }}
),

patient_info as (
    select
        patient_id,
        first_name,
        last_name,
        gender,
        age,
        age_category,
        patient_status,
        estimated_balance,
        total_balance,
        balance_status,
        has_insurance_flag,
        first_visit_date,
        primary_provider_id,
        clinic_id,
        preferred_contact_method,
        preferred_confirmation_method,
        text_messaging_consent
    from {{ ref('dim_patient') }}
),

-- One row per patient: primary insurance (ordinal = 1)
primary_insurance as (
    select
        patient_id,
        primary_carrier_name,
        primary_group_name,
        primary_plan_type
    from (
        select
            patient_id,
            carrier_name as primary_carrier_name,
            group_name as primary_group_name,
            plan_type as primary_plan_type,
            row_number() over (partition by patient_id order by insurance_plan_id) as rn
        from {{ ref('int_insurance_coverage') }}
        where ordinal = 1
          and (carrier_name is not null and carrier_name != '')
    ) sub
    where rn = 1
)

select
    c.patient_id,

    -- Name and demographics
    p.first_name,
    p.last_name,
    c.first_extraction_bone_graft_date,
    c.last_extraction_bone_graft_date,
    c.qualifying_visit_count,
    c.treating_provider_id,

    p.gender,
    p.age,
    p.age_category,
    p.patient_status,

    -- Financial
    coalesce(p.estimated_balance, 0) as estimated_balance,
    coalesce(p.total_balance, 0) as total_balance,
    p.balance_status,
    p.has_insurance_flag,

    -- Insurance (primary)
    pi.primary_carrier_name,
    pi.primary_group_name,
    pi.primary_plan_type,

    -- Practice context
    p.first_visit_date,
    p.primary_provider_id,
    p.clinic_id,

    -- Contact preferences (for outreach)
    p.preferred_contact_method,
    p.preferred_confirmation_method,
    p.text_messaging_consent,

    {{ standardize_mart_metadata(preserve_source_metadata=false) }}
from cohort c
inner join patient_info p on c.patient_id = p.patient_id
left join primary_insurance pi on c.patient_id = pi.patient_id
