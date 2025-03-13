with source as (
    select * from {{ source('opendental', 'diseasedef') }}
),

renamed as (
    select
        -- Primary key
        DiseaseDefNum as disease_definition_id,

        -- Disease details
        DiseaseName as disease_name,
        ICD9Code as icd9_code,
        SnomedCode as snomed_code,
        Icd10Code as icd10_code,

        -- Display settings
        ItemOrder as display_order,
        IsHidden as is_hidden_flag,

        -- Metadata
        DateTStamp as updated_at
    from source
)

select * from renamed
