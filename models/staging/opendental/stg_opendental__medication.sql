with source as (
    select * from {{ source('opendental', 'medication') }}
),

renamed as (
    select
        -- Primary Key
        "MedicationNum" as medication_id,
        
        -- Attributes
        "MedName" as medication_name,
        "GenericNum" as generic_id,
        "Notes" as notes,
        "RxCui" as rxcui,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        "DateTStamp" as _created_at,     -- Rename source creation timestamp
        "DateTStamp" as _updated_at      -- Rename source update timestamp

    from source
)

select * from renamed
