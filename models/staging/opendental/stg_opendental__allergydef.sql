with source_data as (
    select * from {{ source('opendental', 'allergydef') }}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"AllergyDefNum"', 'target': 'allergydef_id'},
            {'source': '"MedicationNum"', 'target': 'medication_id'}
        ]) }},
        
        -- Attributes
        "Description" as allergydef_description,
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ clean_opendental_date('"DateTStamp"') }} as date_timestamp,
        "SnomedType" as snomed_type,
        "UniiCode" as unii_code,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
