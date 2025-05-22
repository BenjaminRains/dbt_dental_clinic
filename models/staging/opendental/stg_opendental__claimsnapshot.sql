{{ config(
    materialized='incremental',
    unique_key='claim_snapshot_id'
) }}

with source as (
    select * from {{ source('opendental', 'claimsnapshot') }}
    where "DateTEntry" >= '2023-01-01'
    {% if is_incremental() %}
        and "DateTEntry" > (select max(entry_timestamp) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "ClaimSnapshotNum" as claim_snapshot_id,
        
        -- Foreign keys
        "ProcNum" as procedure_id,
        "ClaimProcNum" as claim_procedure_id,
        
        -- Claim details
        "ClaimType" as claim_type,
        "Writeoff" as write_off_amount,
        "InsPayEst" as insurance_payment_estimate,
        "Fee" as fee_amount,
        "DateTEntry" as entry_timestamp,
        "SnapshotTrigger" as snapshot_trigger,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        "DateTEntry" as _created_at,
        "DateTEntry" as _updated_at  -- Using DateTEntry since this is a snapshot table
        
    from source
)

select * from renamed
