with source as (
    select * from {{ source('opendental', 'patplan') }}
),

renamed as (
    select
        -- Primary key
        PatPlanNum as patient_plan_id,

        -- Relationships
        PatNum as patient_id,
        InsSubNum as insurance_subscriber_id,

        -- Plan details
        Ordinal as plan_order,
        IsPending as is_pending_flag,
        Relationship as relationship_type,
        PatID as patient_insurance_id,

        -- Orthodontic settings
        OrthoAutoFeeBilledOverride as ortho_auto_fee_billed_override,
        OrthoAutoNextClaimDate as ortho_next_claim_date,

        -- Metadata
        SecDateTEntry as created_at,
        SecDateTEdit as updated_at
    from source
)

select * from renamed