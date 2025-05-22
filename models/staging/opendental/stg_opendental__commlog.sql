{{
    config(
        materialized='incremental',
        unique_key='commlog_num',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

with source as (
    select * 
    from {{ source('opendental', 'commlog') }}
    where "CommDateTime" >= '2023-01-01'
    {% if is_incremental() %}
        and "CommDateTime" > (select max(comm_date_time) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary Key
        "CommlogNum" as commlog_id,
        
        -- Foreign Keys
        "PatNum" as patient_id,
        "UserNum" as user_id,
        "ProgramNum" as program_id,
        "ReferralNum" as referral_id,
        
        -- Timestamps
        "CommDateTime" as communication_datetime,
        "DateTimeEnd" as communication_end_datetime,
        "DateTEntry" as entry_datetime,
        "DateTStamp" as created_at,
        
        -- Regular fields
        "CommType" as communication_type,
        "Note" as note,
        "Mode_" as mode,
        "SentOrReceived" as is_sent,  -- assuming 1 for sent, 0 for received
        "Signature" as signature,
        "SigIsTopaz" as is_topaz_signature,
        "CommSource" as communication_source,
        "CommReferralBehavior" as referral_behavior,
        
        -- Metadata
        current_timestamp as _loaded_at,
        "DateTEntry" as _created_at,
        "DateTStamp" as _updated_at

    from source
)

select * from renamed
