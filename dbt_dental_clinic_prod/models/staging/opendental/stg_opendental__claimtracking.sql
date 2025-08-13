{{ config(
    materialized='incremental',
    unique_key='claim_tracking_id',
    
) }}

with source_data as (
    select * from {{ source('opendental', 'claimtracking') }}
    where "DateTimeEntry" >= '2023-01-01'
    {% if is_incremental() %}
        and {{ clean_opendental_date('"DateTimeEntry"') }} > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"ClaimTrackingNum"', 'target': 'claim_tracking_id'},
            {'source': '"ClaimNum"', 'target': 'claim_id'},
            {'source': '"UserNum"', 'target': 'user_id'},
            {'source': '"TrackingDefNum"', 'target': 'tracking_definition_id'},
            {'source': '"TrackingErrorDefNum"', 'target': 'tracking_error_definition_id'}
        ]) }},
        
        -- Attributes
        "TrackingType" as tracking_type,
        "Note" as note,
        
        -- Raw metadata columns (preserved from source)
        {{ clean_opendental_date('"DateTimeEntry"') }} as date_time_entry,
        
        -- Metadata columns
        {{ standardize_metadata_columns(created_at_column='"DateTimeEntry"') }}

    from source_data
)

select * from renamed_columns
