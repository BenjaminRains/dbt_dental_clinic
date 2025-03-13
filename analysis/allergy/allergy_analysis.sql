with source as (
    select * from opendental_analytics_opendentalbackup_02_28_2025.allergy
),

-- Filter out financial entries
filtered_source as (
    select *
    from source
    where 
        -- Exclude known financial definition IDs
        AllergyDefNum not in (2,3,4,5,6,7,8,9)
        -- Only include records that either:
        -- 1. Have a reaction description
        -- 2. Have an allergy definition that isn't clearly financial
        or Reaction != ''
),

valid_dates as (
    select *,
        CASE 
            WHEN DateAdverseReaction = '0001-01-01' THEN NULL
            WHEN DateAdverseReaction > CURRENT_DATE THEN NULL
            ELSE DateAdverseReaction 
        END as valid_adverse_reaction_date
    from filtered_source
),

-- Join with correct definition category for allergies
allergies_with_definitions as (
    select 
        vd.*,
        d.ItemName as allergy_name,
        d.Category as definition_category
    from valid_dates vd
    left join definition d 
        on d.DefNum = vd.AllergyDefNum
),

renamed as (
    select
        -- Primary key
        AllergyNum as allergy_id,

        -- Relationships
        AllergyDefNum as allergy_definition_id,
        PatNum as patient_id,

        -- Allergy details
        NULLIF(Reaction, '') as reaction_description,
        NULLIF(SnomedReaction, '') as snomed_reaction_code,
        StatusIsActive as is_active_flag,
        allergy_name,
        definition_category,

        -- Enhanced data quality flags
        CASE 
            WHEN SnomedReaction = '' OR SnomedReaction IS NULL THEN 1
            ELSE 0
        END as is_missing_snomed,

        CASE 
            WHEN DateAdverseReaction = '0001-01-01' THEN 1
            ELSE 0
        END as is_default_date,

        CASE 
            WHEN allergy_name IS NULL THEN 1
            ELSE 0
        END as is_missing_definition,

        -- Dates
        valid_adverse_reaction_date as adverse_reaction_date,
        DateTStamp as updated_at
    from allergies_with_definitions
)

select * from renamed
