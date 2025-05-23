{{ config(
    materialized='view'
) }}

with source as (
    select * 
    from {{ source('opendental', 'insverifyhist') }}
    where "SecDateTEdit" >= '2023-01-01'
),

renamed as (
    select
        -- Primary Key
        "InsVerifyHistNum" as insurance_verify_history_id,
        
        -- Foreign Keys
        "InsVerifyNum" as insurance_verify_id,
        "UserNum" as user_id,
        "VerifyUserNum" as verify_user_id,
        "FKey" as foreign_key_id,
        "DefNum" as definition_id,
        
        -- Dates and Timestamps
        "DateLastVerified" as last_verified_date,
        "DateLastAssigned" as last_assigned_date,
        "DateTimeEntry" as entry_timestamp,
        "SecDateTEdit" as last_modified_at,
        
        -- Attributes
        "VerifyType" as verify_type,
        "Note" as note,
        "HoursAvailableForVerification" as hours_available_for_verification,

        -- Required metadata columns
        current_timestamp as _loaded_at,
        "DateTimeEntry" as _created_at,
        "SecDateTEdit" as _updated_at

    from source
)

select * from renamed
