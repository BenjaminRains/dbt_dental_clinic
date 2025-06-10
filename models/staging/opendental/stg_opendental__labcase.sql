{{
    config(
        materialized='incremental',
        unique_key='lab_case_id',
        schema='staging'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'labcase') }}
    where "DateTimeCreated" >= '2023-01-01'
    {% if is_incremental() %}
        and "DateTimeCreated" > (select max(_created_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"LabCaseNum"', 'target': 'lab_case_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"LaboratoryNum"', 'target': 'laboratory_id'},
            {'source': '"AptNum"', 'target': 'appointment_id'},
            {'source': '"PlannedAptNum"', 'target': 'planned_appointment_id'},
            {'source': '"ProvNum"', 'target': 'provider_id'}
        ]) }},
        
        -- Date/Timestamp Fields
        {{ clean_opendental_date('"DateTimeDue"') }} as due_at,
        {{ clean_opendental_date('"DateTimeCreated"') }} as created_at,
        {{ clean_opendental_date('"DateTimeSent"') }} as sent_at,
        {{ clean_opendental_date('"DateTimeRecd"') }} as received_at,
        {{ clean_opendental_date('"DateTimeChecked"') }} as checked_at,
        {{ clean_opendental_date('"DateTStamp"') }} as updated_at,
        
        -- Additional Attributes
        "Instructions" as instructions,
        "LabFee" as lab_fee,
        "InvoiceNum" as invoice_number,

        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTimeCreated"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}
    from source_data
)

select * from renamed_columns
