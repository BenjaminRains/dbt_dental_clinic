with source as (
    select * from {{ source('opendental', 'allergydef') }}
),

categorized as (
    select *,
        CASE 
            -- Medications
            WHEN MedicationNum > 0 THEN 'MEDICATION'
            -- Materials
            WHEN Description IN ('Acrylic', 'Latex', 'Metal', 'Betadine', 'Iodine') THEN 'MATERIAL'
            -- Environmental
            WHEN Description IN ('Bees', 'Animals', 'Seasonal') THEN 'ENVIRONMENTAL'
            -- Food
            WHEN Description = 'Food' OR SnomedType = 4 THEN 'FOOD'
            -- Medical Categories
            WHEN Description IN ('Local Anesthetics', 'Barbiturates / Sedatives') THEN 'MEDICAL_CATEGORY'
            -- Other
            ELSE 'OTHER'
        END as allergen_category,
        
        -- Flag common allergens
        CASE 
            WHEN Description IN ('Penicillin or Other Antibiotics', 'Latex', 'Local Anesthetics') THEN 1
            ELSE 0
        END as is_common_dental_allergen
    from source
),

renamed as (
    select
        -- Primary key
        AllergyDefNum as allergy_definition_id,

        -- Relationships
        MedicationNum as medication_id,

        -- Details
        Description as allergy_description,
        allergen_category,
        is_common_dental_allergen,
        UniiCode as unii_code,
        SnomedType as snomed_type,

        -- Status
        IsHidden as is_hidden_flag,

        -- Metadata
        DateTStamp as updated_at
    from categorized
)

select * from renamed
