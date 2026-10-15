with source as (
    select * from {{ source('opendental', 'pharmacy') }}
),

renamed as (
    select
        -- Primary key
        PharmacyNum as pharmacy_id,

        -- Identification
        PharmID as pharmacy_external_id,
        StoreName as store_name,

        -- Contact information
        Phone as phone_number,
        Fax as fax_number,

        -- Address details
        Address as address_line_1,
        Address2 as address_line_2,
        City as city,
        State as state,
        Zip as postal_code,

        -- Additional information
        Note as pharmacy_notes,

        -- Metadata
        DateTStamp as updated_at
    from source
)

select * from renamed