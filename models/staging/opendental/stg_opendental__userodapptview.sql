{{ config(
    materialized='view'
) }}

with Source as (
    select * from {{ source('opendental', 'userodapptview') }}
),

Renamed as (
    select
        -- Primary Key
        "UserodApptViewNum" as userod_appt_view_id,
        
        -- Foreign Keys
        "UserNum" as user_id,
        "ClinicNum" as clinic_id,
        "ApptViewNum" as appt_view_id,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data into our warehouse
        current_timestamp as _created_at, -- When the view preference was created (using current_timestamp as source has no creation date)
        current_timestamp as _updated_at  -- When the view preference was last updated (using current_timestamp as source has no update date)

    from Source
)

select * from Renamed
