{{ config(
    materialized='incremental',
    unique_key='claim_snapshot_id',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'claimsnapshot') }}
    where "DateTEntry" >= '2023-01-01'
    {% if is_incremental() %}
        and "DateTEntry" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"ClaimSnapshotNum"', 'target': 'claim_snapshot_id'},
            {'source': '"ProcNum"', 'target': 'procedure_id'},
            {'source': '"ClaimProcNum"', 'target': 'claim_procedure_id'}
        ]) }},
        
        -- Attributes
        "ClaimType" as claim_type,
        "Writeoff" as write_off_amount,
        "InsPayEst" as insurance_payment_estimate,
        "Fee" as fee_amount,
        {{ clean_opendental_date('"DateTEntry"') }} as entry_timestamp,
        "SnapshotTrigger" as snapshot_trigger,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTEntry"',
            updated_at_column='"DateTEntry"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
