{{
    config(
        materialized='incremental',
        unique_key='lab_case_id'
    )
}}

with source as (
    select * from {{ source('opendental', 'labcase') }}
    where "DateTimeCreated" >= '2023-01-01'
    {% if is_incremental() %}
        and "DateTimeCreated" > (select max(created_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "LabCaseNum" as lab_case_id,
        
        -- Foreign keys
        "PatNum" as patient_id,
        "LaboratoryNum" as laboratory_id,
        "AptNum" as appointment_id,
        "PlannedAptNum" as planned_appointment_id,
        "ProvNum" as provider_id,
        
        -- Timestamps
        "DateTimeDue" as due_at,
        "DateTimeCreated" as created_at,
        "DateTimeSent" as sent_at,
        "DateTimeRecd" as received_at,
        "DateTimeChecked" as checked_at,
        "DateTStamp" as updated_at,
        
        -- Additional attributes
        "Instructions" as instructions,
        "LabFee" as lab_fee,
        "InvoiceNum" as invoice_number,

        -- Required metadata columns
        current_timestamp as _loaded_at,
        "DateTimeCreated" as _created_at,
        coalesce("DateTStamp", "DateTimeCreated") as _updated_at
    from source
)

select * from renamed
