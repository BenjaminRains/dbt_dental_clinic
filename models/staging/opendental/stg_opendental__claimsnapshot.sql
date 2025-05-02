with Source as (
    select * from {{ source('opendental', 'claimsnapshot') }}
),

Renamed as (
    select
        "ClaimSnapshotNum" as claim_snapshot_id,
        "ProcNum" as procedure_id,
        "ClaimType" as claim_type,
        "Writeoff" as write_off_amount,
        "InsPayEst" as insurance_payment_estimate,
        "Fee" as fee_amount,
        "DateTEntry" as entry_timestamp,
        "ClaimProcNum" as claim_procedure_id,
        "SnapshotTrigger" as snapshot_trigger
    from Source
),

Final as (
    select
        claim_snapshot_id,
        case when procedure_id = 0 then null else procedure_id end as procedure_id,
        claim_type,
        write_off_amount,
        insurance_payment_estimate,
        fee_amount,
        entry_timestamp,
        case when claim_procedure_id = 0 then null else claim_procedure_id end as claim_procedure_id,
        snapshot_trigger
    from Renamed
)

select * from Final
