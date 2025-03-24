with source as (
    select * from {{ source('opendental', 'patientnote') }}
),

renamed as (
    select
        "PatNum" as patient_id,
        "FamFinancial" as family_financial,
        "ApptPhone" as appointment_phone,
        "Medical" as medical,
        "Service" as service,
        "MedicalComp" as medical_comp,
        "Treatment" as treatment,
        "ICEName" as ice_name,
        "ICEPhone" as ice_phone,
        "OrthoMonthsTreatOverride" as ortho_months_treat_override,
        "DateOrthoPlacementOverride" as date_ortho_placement_override,
        "SecDateTEntry" as created_at,
        "SecDateTEdit" as updated_at,
        "Consent" as consent,
        "UserNumOrthoLocked" as user_num_ortho_locked,
        "Pronoun" as pronoun
    from source
    where "SecDateTEntry" >= '2023-01-01' -- Filter to include only notes from 2023 onwards
)

select * from renamed
