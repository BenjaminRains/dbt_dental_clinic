{{ config(
    materialized='incremental',
    unique_key='claim_tracking_id'
) }}

with source as (
    select * from {{ source('opendental', 'claimtracking') }}
    where "DateTimeEntry" >= '2023-01-01'
    {% if is_incremental() %}
        and "DateTimeEntry" > (select max(entry_timestamp) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "ClaimTrackingNum" as claim_tracking_id,
        
        -- Foreign keys
        "ClaimNum" as claim_id,
        "UserNum" as user_id,
        "TrackingDefNum" as tracking_definition_id,
        "TrackingErrorDefNum" as tracking_error_definition_id,
        
        -- Date/timestamp fields
        "DateTimeEntry" as entry_timestamp,
        
        -- Text fields
        "TrackingType" as tracking_type,
        "Note" as note,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        "DateTimeEntry" as _created_at,
        "DateTimeEntry" as _updated_at  -- Using DateTimeEntry since this is a tracking/audit table
        
    from source
)

select * from renamed
