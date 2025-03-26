with source as (
    select * from {{ source('opendental', 'allergy') }}
),

renamed as (
    select
        "AllergyNum" as allergy_num,
        "AllergyDefNum" as allergy_def_num,
        "PatNum" as patient_num,
        "Reaction" as reaction,
        "StatusIsActive" as is_active,
        "DateTStamp" as date_timestamp,
        "DateAdverseReaction" as adverse_reaction_date,
        "SnomedReaction" as snomed_reaction
    from source
)

select * from renamed
