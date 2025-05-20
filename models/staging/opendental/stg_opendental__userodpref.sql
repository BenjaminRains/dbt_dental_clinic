{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'userodpref') }}
),

renamed as (
    select
        "UserOdPrefNum" as user_od_pref_id,
        "UserNum" as user_id,
        "Fkey" as fkey,
        "FkeyType" as fkey_type,
        "ValueString" as value_string,
        "ClinicNum" as clinic_id
    from source
)

select * from renamed
