with source as (
    select * from {{ source('opendental', 'refattach') }}
),

renamed as (
    select
        -- Primary key
        RefAttachNum as referral_attachment_id,

        -- Relationships
        ReferralNum as referral_id,
        PatNum as patient_id,
        ProcNum as procedure_id,
        ProvNum as provider_id,

        -- Referral details
        RefType as referral_type,
        RefToStatus as referral_to_status,
        IsTransitionOfCare as is_transition_of_care_flag,

        -- Ordering
        ItemOrder as display_order,

        -- Dates
        RefDate as referral_date,
        DateProcComplete as procedure_completion_date,
        DateTStamp as updated_at,

        -- Notes (may contain PHI)
        Note as referral_notes
    from source
)

select * from renamed