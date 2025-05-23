{{ config(
    materialized='incremental',
    unique_key='ref_attach_id',
    schema='staging'
) }}

with Source as (
    select * 
    from {{ source('opendental', 'refattach') }}
    where "RefDate" >= '2023-01-01'
    {% if is_incremental() %}
        AND "DateTStamp" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

Renamed as (
    select
        -- Primary Key
        "RefAttachNum" as ref_attach_id,
        
        -- Foreign Keys
        "ReferralNum" as referral_id,
        "PatNum" as patient_id,
        "ProcNum" as procedure_id,
        "ProvNum" as provider_id,
        
        -- Regular columns
        "ItemOrder" as item_order,
        "RefDate" as referral_date,
        "RefType" as referral_type,
        "RefToStatus" as referral_to_status,
        "Note" as note,
        "IsTransitionOfCare" as is_transition_of_care,
        "DateProcComplete" as procedure_completion_date,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        "DateTStamp" as _created_at,
        "DateTStamp" as _updated_at

    from Source
)

select * from Renamed
