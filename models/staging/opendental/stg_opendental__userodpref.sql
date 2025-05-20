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
        "ClinicNum" as clinic_id,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at
    from source
)

select * from renamed
