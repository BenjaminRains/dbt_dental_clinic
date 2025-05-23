{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'patplan') }}
    where "SecDateTEdit" >= '2023-01-01'  -- Following pattern from other staging models
),

renamed as (
    select
        -- Primary Key
        "PatPlanNum" as patplan_id,
        
        -- Foreign Keys
        "PatNum" as patient_id,
        "InsSubNum" as insurance_subscriber_id,
        
        -- Additional Attributes
        "Ordinal" as ordinal,
        "IsPending" as is_pending,
        "Relationship" as relationship,
        "PatID" as patient_external_id,
        "OrthoAutoFeeBilledOverride" as ortho_auto_fee_billed_override,
        "OrthoAutoNextClaimDate" as ortho_auto_next_claim_date,
        
        -- Metadata
        current_timestamp as _loaded_at,
        COALESCE("SecDateTEntry", "SecDateTEdit") as _created_at,
        "SecDateTEdit" as _updated_at

    from source
)

select * from renamed
