with source as (
    select * from {{ source('opendental', 'claimtracking') }}
),

renamed as (
    select
        -- Keys
        ClaimTrackingNum as claim_tracking_id,
        ClaimNum as claim_id,
        TrackingDefNum as tracking_definition_id,
        TrackingErrorDefNum as tracking_error_definition_id,
        UserNum as user_id,
        
        -- Tracking details
        TrackingType as tracking_type,
        Note as tracking_note,
        
        -- Metadata
        DateTimeEntry as entry_datetime
    from source
)

select * from renamed