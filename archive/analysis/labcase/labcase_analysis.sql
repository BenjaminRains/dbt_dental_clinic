with source as (
    select * from {{ source('opendental', 'labcase') }}
),

renamed as (
    select
        -- Primary key
        LabCaseNum as lab_case_id,

        -- Relationships
        PatNum as patient_id,
        LaboratoryNum as laboratory_id,
        AptNum as appointment_id,
        PlannedAptNum as planned_appointment_id,
        ProvNum as provider_id,

        -- Lab case details
        LabFee as lab_fee,
        InvoiceNum as invoice_number,

        -- Dates and timing
        DateTimeDue as due_datetime,
        DateTimeCreated as created_datetime,
        DateTimeSent as sent_datetime,
        DateTimeRecd as received_datetime,
        DateTimeChecked as checked_datetime,

        -- Metadata
        DateTStamp as updated_at,

        -- Instructions may contain PHI but including based on pattern with other staging models
        Instructions as instructions
    from source
)

select * from renamed