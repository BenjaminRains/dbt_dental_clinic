{{
    config(
        materialized='incremental',
        unique_key='commlog_id',
        incremental_strategy='delete+insert',
        schema='staging',
        on_schema_change='fail'
    )
}}

with source_data as (
    select * 
    from {{ source('opendental', 'commlog') }}
    where "CommDateTime" >= '2023-01-01'
    {% if is_incremental() %}
        and "CommDateTime" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key and Foreign Keys
        {{ transform_id_columns([
            {'source': '"CommlogNum"', 'target': 'commlog_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"UserNum"', 'target': 'user_id'},
            {'source': '"ProgramNum"', 'target': 'program_id'},
            {'source': '"ReferralNum"', 'target': 'referral_id'}
        ]) }},
        
        -- Date/Timestamp Fields
        {{ clean_opendental_date('"CommDateTime"') }} as communication_datetime,
        {{ clean_opendental_date('"DateTimeEnd"') }} as communication_end_datetime,
        {{ clean_opendental_date('"DateTEntry"') }} as entry_datetime,
        
        -- Attributes
        "CommType" as communication_type,
        "Note" as note,
        "Mode_" as mode,
        "Signature" as signature,
        "CommSource" as communication_source,
        "CommReferralBehavior" as referral_behavior,
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"SentOrReceived"') }} as is_sent,
        {{ convert_opendental_boolean('"SigIsTopaz"') }} as is_topaz_signature,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTEntry"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
