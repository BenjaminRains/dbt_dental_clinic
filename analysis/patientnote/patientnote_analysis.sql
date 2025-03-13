with source as (
    select * from {{ source('opendental', 'patientnote') }}
),

renamed as (
    select
        -- Primary key
        PatNum as patient_id,

        -- Emergency contact information
        ICEName as emergency_contact_name,
        ICEPhone as emergency_contact_phone,

        -- Patient preferences
        Consent as consent_flag,
        Pronoun as pronoun_preference,

        -- Orthodontic information
        OrthoMonthsTreatOverride as ortho_months_treatment_override,
        DateOrthoPlacementOverride as ortho_placement_date_override,
        UserNumOrthoLocked as ortho_locked_by_user_id,

        -- Notes (all may contain PHI)
        FamFinancial as family_financial_notes,
        ApptPhone as appointment_phone_notes,
        Medical as medical_notes,
        Service as service_notes,
        MedicalComp as medical_compliance_notes,
        Treatment as treatment_notes,

        -- Metadata
        SecDateTEntry as created_at,
        SecDateTEdit as updated_at
    from source
)

select * from renamed