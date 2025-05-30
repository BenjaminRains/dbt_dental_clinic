with source as (
    select * from {{ source('opendental', 'allergydef') }}
),

renamed as (
    select
        -- Primary Key
        "AllergyDefNum" as allergydef_id,
        
        -- Attributes
        "Description" as allergydef_description,
        "IsHidden" as is_hidden,
        "DateTStamp" as date_timestamp,
        "SnomedType" as snomed_type,
        "MedicationNum" as medication_id,
        "UniiCode" as unii_code,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        "DateTStamp" as _created_at,     -- Rename source creation timestamp
        "DateTStamp" as _updated_at      -- Rename source update timestamp

    from source
)

select * from renamed
