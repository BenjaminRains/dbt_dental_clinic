with source as (
    select * from {{ source('opendental', 'treatplan') }}
),

renamed as (
    select
        -- Primary key
        TreatPlanNum as treatment_plan_id,

        -- Relationships
        PatNum as patient_id,
        ResponsParty as responsible_party_id,
        DocNum as document_id,
        SecUserNumEntry as created_by_user_id,
        UserNumPresenter as presenter_user_id,
        MobileAppDeviceNum as mobile_device_id,

        -- Plan details
        TPType as treatment_plan_type,
        TPStatus as treatment_plan_status,

        -- Description (may contain PHI)
        Heading as plan_heading,
        Note as plan_notes,

        -- Signatures
        Signature as patient_signature,
        SignatureText as patient_signature_text,
        SigIsTopaz as is_topaz_signature_flag,
        SignaturePractice as practice_signature,
        SignaturePracticeText as practice_signature_text,

        -- Dates
        DateTP as treatment_plan_date,
        DateTSigned as patient_signed_datetime,
        DateTPracticeSigned as practice_signed_datetime,
        SecDateEntry as created_at,
        SecDateTEdit as updated_at
    from source
)

select * from renamed