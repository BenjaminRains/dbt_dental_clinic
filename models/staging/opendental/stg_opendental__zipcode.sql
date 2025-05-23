{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'zipcode') }}
),

renamed as (
    select
        -- Primary Key
        "ZipCodeNum" as zipcode_id,
        
        -- Business Columns
        "ZipCodeDigits" as zipcode,
        "City" as city,
        "State" as state,
        "IsFrequent" as is_frequent,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data into our warehouse
        current_timestamp as _created_at, -- When the zipcode was created (using current_timestamp as source has no creation date)
        current_timestamp as _updated_at  -- When the zipcode was last updated (using current_timestamp as source has no update date)
    from source
)

select * from renamed 