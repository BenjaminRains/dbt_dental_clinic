{{ config(
    materialized='incremental',
    unique_key='statement_id'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'statement') }}
    where {{ clean_opendental_date('"DateSent"') }} >= '2023-01-01'::date
        and {{ clean_opendental_date('"DateSent"') }} <= current_date
        and {{ clean_opendental_date('"DateSent"') }} > '2000-01-01'::date
    {% if is_incremental() %}
        and "DateTStamp" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"StatementNum"', 'target': 'statement_id'}
        ]) }},
        
        -- Foreign Keys
        {{ transform_id_columns([
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"DocNum"', 'target': 'document_id'},
            {'source': '"SuperFamily"', 'target': 'super_family_id'}
        ]) }},
        
        -- Timestamps and Dates
        {{ clean_opendental_date('"DateSent"') }} as date_sent,
        {{ clean_opendental_date('"DateRangeFrom"') }} as date_range_from,
        {{ clean_opendental_date('"DateRangeTo"') }} as date_range_to,
        
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
        {{ convert_opendental_boolean('"HidePayment"') }} as is_payment_hidden,
        {{ convert_opendental_boolean('"SinglePatient"') }} as is_single_patient,
        {{ convert_opendental_boolean('"Intermingled"') }} as is_intermingled,
        {{ convert_opendental_boolean('"IsSent"') }} as is_sent,
        {{ convert_opendental_boolean('"IsReceipt"') }} as is_receipt,
        {{ convert_opendental_boolean('"IsInvoice"') }} as is_invoice,
        {{ convert_opendental_boolean('"IsInvoiceCopy"') }} as is_invoice_copy,
        {{ convert_opendental_boolean('"IsBalValid"') }} as is_balance_valid,
        "SmsSendStatus"::smallint as sms_send_status,
        {{ convert_opendental_boolean('"LimitedCustomFamily"') }} as limited_custom_family,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
