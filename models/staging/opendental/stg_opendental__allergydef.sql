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
        
        -- Meta Fields
        current_timestamp as _loaded_at

    from source
)

select * from renamed
