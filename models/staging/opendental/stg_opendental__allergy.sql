with source_data as (
    select * from {{ source('opendental', 'allergy') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"AllergyNum"', 'target': 'allergy_id'},
            {'source': '"AllergyDefNum"', 'target': 'allergy_def_id'},
            {'source': '"PatNum"', 'target': 'patient_id'}
        ]) }},
        
        -- Clinical information
        "Reaction" as reaction,
        "SnomedReaction" as snomed_reaction,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"StatusIsActive"') }} as is_active,
        
        -- Date fields using macro
        {{ clean_opendental_date('"DateAdverseReaction"') }} as adverse_reaction_date,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
