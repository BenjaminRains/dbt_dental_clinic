with source as (
    select * from {{ source('opendental', 'allergy') }}
),

renamed as (
    select
        "AllergyNum" as allergy_id,
        "AllergyDefNum" as allergy_def_id,
        "PatNum" as patient_id,
        "Reaction" as reaction,
        "StatusIsActive" as is_active,
        "DateTStamp" as date_timestamp,
        "DateAdverseReaction" as adverse_reaction_date,
        "SnomedReaction" as snomed_reaction
    from source
)

select * from renamed
