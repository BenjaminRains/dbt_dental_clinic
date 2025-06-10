{{ config(
    materialized='incremental',
    unique_key='ref_attach_id',
    schema='staging'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'refattach') }}
    where {{ clean_opendental_date('"RefDate"') }} >= '2023-01-01'
    {% if is_incremental() %}
        AND "DateTStamp" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"RefAttachNum"', 'target': 'ref_attach_id'}
        ]) }},
        
        -- Foreign Keys
        {{ transform_id_columns([
            {'source': '"ReferralNum"', 'target': 'referral_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"ProcNum"', 'target': 'procedure_id'},
            {'source': '"ProvNum"', 'target': 'provider_id'}
        ]) }},
        
        -- Regular columns
        "ItemOrder" as item_order,
        {{ clean_opendental_date('"RefDate"') }} as referral_date,
        "RefType" as referral_type,
        "RefToStatus" as referral_to_status,
        "Note" as note,
        {{ convert_opendental_boolean('"IsTransitionOfCare"') }} as is_transition_of_care,
        {{ clean_opendental_date('"DateProcComplete"') }} as procedure_completion_date,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
