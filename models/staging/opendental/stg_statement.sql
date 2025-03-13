with source as (
    select * from {{ source('opendental', 'statement') }}
),

renamed as (
    select
        -- Primary key
        StatementNum as statement_id,

        -- Relationships
        PatNum as patient_id,
        DocNum as document_id,
        SuperFamily as super_family_id,

        -- Dates
        DateSent as sent_date,
        DateRangeFrom as date_range_start,
        DateRangeTo as date_range_end,

        -- Statement configuration
        Mode_ as statement_mode,
        StatementType as statement_type,
        HidePayment as hide_payment_flag,
        SinglePatient as single_patient_flag,
        Intermingled as is_intermingled_flag,
        IsSent as is_sent_flag,
        IsReceipt as is_receipt_flag,
        IsInvoice as is_invoice_flag,
        IsInvoiceCopy as is_invoice_copy_flag,
        LimitedCustomFamily as limited_custom_family_flag,

        -- Financial information
        InsEst as insurance_estimate,
        BalTotal as total_balance,
        IsBalValid as is_balance_valid_flag,

        -- Communication details
        EmailSubject as email_subject,
        EmailBody as email_body, -- May contain PHI
        SmsSendStatus as sms_send_status,

        -- Web access
        ShortGUID as short_guid,
        StatementShortURL as short_url,
        StatementURL as full_url,

        -- Notes (may contain PHI)
        Note as statement_notes,
        NoteBold as bold_notes,

        -- Metadata
        DateTStamp as updated_at
    from source
)

select * from renamed