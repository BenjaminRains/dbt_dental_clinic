{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key=['claim_id', 'claim_tracking_id', 'tracking_type', 'entry_timestamp']
    )
}}

with ClaimTracking as (
    select
        claim_id,
        claim_tracking_id,
        tracking_type,
        entry_timestamp,
        note as tracking_note
    from {{ ref('stg_opendental__claimtracking') }}
),

Final as (
    select
        -- Primary Key
        ct.claim_id,
        ct.claim_tracking_id,

        -- Tracking Information
        ct.tracking_type,
        ct.entry_timestamp,
        ct.tracking_note,

        -- Meta Fields
        ct.entry_timestamp as created_at,
        ct.entry_timestamp as updated_at

    from ClaimTracking ct
    -- Ensure the claim exists in int_claim_details
    inner join {{ ref('int_claim_details') }} cd
        on ct.claim_id = cd.claim_id
)

select * from Final 