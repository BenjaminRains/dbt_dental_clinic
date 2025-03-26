with source as (

    select * from {{ source('opendental', 'diseasedef') }}

),

renamed as (

    select
        -- Primary Key
        "DiseaseDefNum" as disease_def_num,
        
        -- Attributes
        "DiseaseName" as disease_name,
        "ItemOrder" as item_order,
        "IsHidden" as is_hidden,
        "DateTStamp" as date_tstamp,
        "ICD9Code" as icd9_code,
        "SnomedCode" as snomed_code,
        "Icd10Code" as icd10_code

    from source

)

select * from renamed
