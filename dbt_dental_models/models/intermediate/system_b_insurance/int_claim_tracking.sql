{{ config(
        materialized='table',
        unique_key=['claim_tracking_id', 'claim_id', 'date_time_entry'],
        on_schema_change='fail',
        indexes=[
            {'columns': ['claim_tracking_id']},
            {'columns': ['claim_id']},
            {'columns': ['_created_at']}
        ]) }}

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
    - Deduplication logic handles potential duplicate tracking records in source data
    - Uses composite unique key (claim_tracking_id, claim_id, date_time_entry) for data integrity
    - Tracking notes may contain free-text information from users
    
    Performance Considerations:
    - Table materialization for stable tracking history
    - Indexed on claim_id for efficient claim-based queries
    - Indexed on _updated_at for metadata-based filtering
    
    Metadata Strategy:
    - Primary source: claimtracking (stg_opendental__claimtracking) - contains core tracking details
    - Preserves business timestamps from primary source for audit trail (_created_at only, no _updated_at available)
    - Maintains pipeline tracking with _loaded_at and _transformed_at for debugging
*/

with source_claim_tracking as (
    select * from {{ ref('stg_opendental__claimtracking') }}
),

deduplicated_claim_tracking as (
    select *,
        row_number() over(
            partition by claim_tracking_id, claim_id, date_time_entry
            order by _created_at desc
        ) as rn
    from source_claim_tracking
),

claim_tracking_enhanced as (
    select
        -- Primary identification
        source_claim_tracking.claim_tracking_id,
        source_claim_tracking.claim_id,

        -- Tracking information
        source_claim_tracking.tracking_type,
        source_claim_tracking.date_time_entry as entry_timestamp,
        source_claim_tracking.note as tracking_note,

        -- Primary source metadata using macro (only available fields)
        {{ standardize_intermediate_metadata(
            primary_source_alias='source_claim_tracking',
            source_metadata_fields=['_loaded_at', '_created_at']
        ) }}

    from deduplicated_claim_tracking source_claim_tracking
    -- Ensure the claim exists in int_claim_details
    inner join {{ ref('int_claim_details') }} cd
        on source_claim_tracking.claim_id = cd.claim_id
    where source_claim_tracking.rn = 1
)

select * from claim_tracking_enhanced 
