{{ config(
    materialized='incremental',
    unique_key='treatment_plan_id'
) }}

with source as (
    select * 
    from {{ source('opendental', 'treatplan') }}
    where "DateTP" >= '2023-01-01'
    {% if is_incremental() %}
        and "SecDateTEdit" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "TreatPlanNum" as treatment_plan_id,
        
        -- Foreign keys
        "PatNum" as patient_id,
        "ResponsParty" as responsible_party_id,
        "DocNum" as document_id,
        "SecUserNumEntry" as entry_user_id,
        "UserNumPresenter" as presenter_user_id,
        "MobileAppDeviceNum" as mobile_app_device_id,
        
        -- Timestamps and dates
        "DateTP" as treatment_plan_date,
        "SecDateEntry" as entry_date,
        "DateTSigned" as signed_timestamp,
        "DateTPracticeSigned" as practice_signed_timestamp,
        
        -- Descriptive fields
        "Heading" as heading,
        "Note" as note,
        "Signature" as signature,
        "SignatureText" as signature_text,
        "SignaturePractice" as signature_practice,
        "SignaturePracticeText" as signature_practice_text,
        
        -- Status and type flags
        "SigIsTopaz" as is_signature_topaz,
        "TPStatus" as treatment_plan_status,
        "TPType" as treatment_plan_type,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        "SecDateEntry" as _created_at,   -- When the treatment plan was created
        "SecDateTEdit" as _updated_at    -- When the treatment plan was last updated
    from source
)

select * from renamed
