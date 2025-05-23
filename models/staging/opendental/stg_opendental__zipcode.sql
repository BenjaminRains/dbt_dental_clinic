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
        -- Clean zipcode: ensure it's exactly 5 digits, pad with leading zeros if needed
        LPAD(REGEXP_REPLACE("ZipCodeDigits", '[^0-9]', '', 'g'), 5, '0') as zipcode,
        "City" as city,
        -- Clean state: ensure it's exactly 2 uppercase letters
        UPPER(REGEXP_REPLACE("State", '[^A-Za-z]', '', 'g')) as state,
        "IsFrequent" as is_frequent,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data into our warehouse
        current_timestamp as _created_at, -- When the zipcode was created (using current_timestamp as source has no creation date)
        current_timestamp as _updated_at  -- When the zipcode was last updated (using current_timestamp as source has no update date)
    from source
)

select * from renamed 