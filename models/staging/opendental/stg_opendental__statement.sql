{{ config(
    materialized='incremental',
    unique_key='statement_id',
    schema='staging'
) }}

with source as (
    select * 
    from {{ source('opendental', 'statement') }}
    where "DateSent" >= '2023-01-01'::date
        and "DateSent" <= current_date
        and "DateSent" > '2000-01-01'::date
    {% if is_incremental() %}
        and "DateSent" > (select max(date_sent) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary Key
        "StatementNum" as statement_id,
        
        -- Foreign Keys
        "PatNum" as patient_id,
        "DocNum" as document_id,
        "SuperFamily" as super_family_id,
        
        -- Timestamps and Dates
        "DateSent" as date_sent,
        "DateRangeFrom" as date_range_from,
        "DateRangeTo" as date_range_to,
        
        -- Text Fields
        "Note" as note,
        "NoteBold" as note_bold,
        "EmailSubject" as email_subject,
        "EmailBody" as email_body,
        "StatementType" as statement_type,
        "ShortGUID" as short_guid,
        "StatementShortURL" as statement_short_url,
        "StatementURL" as statement_url,
        
        -- Numeric Fields
        "Mode_" as mode,
        "InsEst" as insurance_estimate,
        "BalTotal" as balance_total,
        
        -- Boolean/Status Fields
        "HidePayment"::boolean as is_payment_hidden,
        "SinglePatient"::boolean as is_single_patient,
        "Intermingled"::boolean as is_intermingled,
        "IsSent"::boolean as is_sent,
        "IsReceipt"::smallint as is_receipt,
        "IsInvoice"::smallint as is_invoice,
        "IsInvoiceCopy"::smallint as is_invoice_copy,
        "IsBalValid"::smallint as is_balance_valid,
        "SmsSendStatus"::smallint as sms_send_status,
        "LimitedCustomFamily"::smallint as limited_custom_family,
        
        -- Metadata
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        "DateTStamp" as _created_at,     -- When the record was created in source
        "DateTStamp" as _updated_at      -- Last update timestamp

    from source
)

select * from renamed
