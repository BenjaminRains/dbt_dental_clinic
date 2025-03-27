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
        "DateTStamp" as date_tstamp
    from source
)

select * from renamed
