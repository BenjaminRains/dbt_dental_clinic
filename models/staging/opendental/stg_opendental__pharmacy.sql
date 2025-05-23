{{
    config(
        materialized='table'
    )
}}

with source as (
    select * from {{ source('opendental', 'pharmacy') }}
),

renamed as (
    select
        -- Primary Key
        "PharmacyNum" as pharmacy_id,
        
        -- Identifiers
        "PharmID" as pharm_id,
        
        -- Attributes
        "StoreName" as store_name,
        "Phone" as phone,
        "Fax" as fax,
        "Address" as address,
        "Address2" as address2,
        "City" as city,
        "State" as state,
        "Zip" as zip,
        "Note" as note,
        
        -- Timestamps
        "DateTStamp" as date_tstamp,

        -- Required metadata columns
        current_timestamp as _loaded_at,
        "DateTStamp" as _created_at,  -- Using DateTStamp as creation timestamp
        "DateTStamp" as _updated_at   -- Using DateTStamp as update timestamp
    from source
)

select * from renamed
