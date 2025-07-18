{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='claim_tracking_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['claim_tracking_id'], 'unique': true},
            {'columns': ['claim_id']},
            {'columns': ['_updated_at']}
        ]
    )
}}

/*
    Intermediate model for claim tracking entries
    Part of System B: Insurance & Claims Processing
    
    This model:
    1. Standardizes claim tracking and status update records from OpenDental
    2. Provides clean tracking history for claims processing workflows
    3. Ensures referential integrity with claim details
    
    Business Logic Features:
    - Tracking Type Classification: Categorizes different types of claim tracking events
    - Temporal Tracking: Maintains chronological order of claim status changes
    - Data Integrity: Validates tracking entries against existing claims
    
    Data Quality Notes:
    - All tracking entries must reference valid claims in int_claim_details
    - Entry timestamps are used for both created_at and updated_at metadata
    - Tracking notes may contain free-text information from users
    
    Performance Considerations:
    - Table materialization for stable tracking history
    - Indexed on claim_id for efficient claim-based queries
    - Indexed on _updated_at for metadata-based filtering
*/

with source_claim_tracking as (
    select * from {{ ref('stg_opendental__claimtracking') }}
),

claim_tracking_enhanced as (
    select
        -- Primary identification
        claim_tracking_id,
        claim_id,

        -- Tracking information
        tracking_type,
        entry_timestamp,
        note as tracking_note,

        -- Metadata fields (standardized pattern)
        _extracted_at,
        entry_timestamp as _created_at,
        entry_timestamp as _updated_at,
        current_timestamp as _transformed_at

    from source_claim_tracking
    -- Ensure the claim exists in int_claim_details
    inner join {{ ref('int_claim_details') }} cd
        on source_claim_tracking.claim_id = cd.claim_id
)

select * from claim_tracking_enhanced 