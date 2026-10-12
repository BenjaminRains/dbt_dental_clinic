with source as (
    select * from {{ source('opendental', 'inssub') }}
),

renamed as (
    select
        -- Primary key
        InsSubNum as insurance_subscriber_id,

        -- Relationships
        PlanNum as insurance_plan_id,
        Subscriber as subscriber_patient_id,

        -- Subscriber identifiers
        SubscriberID as subscriber_id_number,

        -- Coverage dates
        DateEffective as effective_date,
        DateTerm as termination_date,

        -- Authorization flags
        ReleaseInfo as release_information,
        AssignBen as assign_benefits,

        -- Metadata
        SecUserNumEntry as created_by_user_id,
        SecDateEntry as created_at,
        SecDateTEdit as updated_at

        -- Excluded fields with potential PHI:
        -- BenefitNotes (may contain patient-specific coverage details)
        -- SubscNote (may contain notes about the subscriber)
    from source
)

select * from renamed