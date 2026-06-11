{{
    config(
        materialized='table',
        schema='marts',
        unique_key='referral_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['referral_id'], 'unique': true},
            {'columns': ['is_doctor']},
            {'columns': ['is_hidden']},
            {'columns': ['last_name_norm']}
        ]
    )
}}

/*
    Referral dimension (OpenDental referral table).
    Links to stg_opendental__refattach and communication facts via referral_id.
*/

with source_referral as (
    select * from {{ ref('stg_opendental__referral') }}
),

final as (
    select
        referral_id,

        last_name,
        first_name,
        middle_name,
        business_name,
        title,
        national_provider_id,

        case
            when business_name is not null and trim(business_name) != '' then business_name
            else trim(
                concat_ws(
                    ' ',
                    nullif(trim(title), ''),
                    nullif(trim(first_name), ''),
                    nullif(trim(last_name), '')
                )
            )
        end as display_name,

        lower(trim(coalesce(last_name, ''))) as last_name_norm,
        lower(
            regexp_replace(
                trim(
                    coalesce(business_name, '')
                    || coalesce(first_name, '')
                    || coalesce(last_name, '')
                ),
                '\s+',
                '',
                'g'
            )
        ) as referral_search_key,

        is_doctor,
        is_preferred,
        is_trusted_direct,
        is_hidden,
        not_person,
        is_using_tin,

        city,
        state,
        zip_code,
        telephone,
        email,

        {{ standardize_mart_metadata(
            primary_source_alias='r',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at']
        ) }}

    from source_referral r
)

select * from final
