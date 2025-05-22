with source as (

    select * from {{ source('opendental', 'diseasedef') }}

),

renamed as (

    select
        -- Primary Key
        "DiseaseDefNum" as disease_def_id,
        
        -- Attributes
        "DiseaseName" as disease_name,
        "ItemOrder" as item_order,
        CASE 
            WHEN "IsHidden" = 1 THEN true
            WHEN "IsHidden" = 0 THEN false
            ELSE null 
        END as is_hidden,
        "DateTStamp" as date_tstamp,
        "ICD9Code" as icd9_code,
        "SnomedCode" as snomed_code,
        "Icd10Code" as icd10_code,

        -- Required metadata columns
        current_timestamp as _loaded_at,
        "DateTStamp" as _created_at,
        "DateTStamp" as _updated_at

    from source

)

select * from renamed
