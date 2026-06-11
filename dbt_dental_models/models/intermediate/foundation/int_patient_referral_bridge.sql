{{ config(
    materialized='table',
    schema='int',
    unique_key=['patient_id', 'referral_id'],
    on_schema_change='fail',
    indexes=[
        {'columns': ['patient_id', 'referral_id'], 'unique': true},
        {'columns': ['referral_id']},
        {'columns': ['first_referral_date']}
    ],
    tags=['foundation', 'weekly']
) }}

/*
    One row per (patient_id, referral_id) from OpenDental refattach.
    Aggregates attach dates for attribution and KPI marts.
*/

with refattach as (
    select
        patient_id,
        referral_id,
        referral_date,
        _loaded_at,
        _created_at,
        _updated_at
    from {{ ref('stg_opendental__refattach') }}
    where patient_id is not null
        and referral_id is not null
        and referral_date is not null
),

aggregated as (
    select
        patient_id,
        referral_id,
        min(referral_date) as first_referral_date,
        max(referral_date) as latest_referral_date,
        count(*) as referral_attach_count,
        max(_loaded_at) as _loaded_at,
        max(_created_at) as _created_at,
        max(_updated_at) as _updated_at
    from refattach
    group by patient_id, referral_id
),

final as (
    select
        a.patient_id,
        a.referral_id,
        a.first_referral_date,
        a.latest_referral_date,
        a.referral_attach_count,
        {{ standardize_intermediate_metadata(
            primary_source_alias='a',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at']
        ) }}
    from aggregated a
)

select * from final
