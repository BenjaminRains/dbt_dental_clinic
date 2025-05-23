{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'userodpref') }}
),

renamed as (
    select
        -- Primary Key
        "UserOdPrefNum" as user_od_pref_id,
        
        -- Foreign Keys
        "UserNum" as user_id,
        "ClinicNum" as clinic_id,
        
        -- Business Columns
        "Fkey" as fkey,
        "FkeyType" as fkey_type,
        "ValueString" as value_string,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data into our warehouse
        current_timestamp as _created_at, -- When the preference was created (using current_timestamp as source has no creation date)
        current_timestamp as _updated_at  -- When the preference was last updated (using current_timestamp as source has no update date)
    from source
)

select * from renamed
