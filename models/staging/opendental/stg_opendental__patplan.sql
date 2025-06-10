{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'patplan') }}
    where "SecDateTEdit" >= '2023-01-01'  -- Following pattern from other staging models
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"PatPlanNum"', 'target': 'patplan_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"InsSubNum"', 'target': 'insurance_subscriber_id'}
        ]) }},
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"IsPending"') }} as is_pending,
        
        -- Additional Attributes
        "Ordinal" as ordinal,
        "Relationship" as relationship,
        "PatID" as patient_external_id,
        "OrthoAutoFeeBilledOverride" as ortho_auto_fee_billed_override,
        
        -- Date Fields
        {{ clean_opendental_date('"OrthoAutoNextClaimDate"') }} as ortho_auto_next_claim_date,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"SecDateTEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
