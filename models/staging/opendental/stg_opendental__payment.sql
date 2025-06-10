{{ config(
    materialized='incremental',
    unique_key='payment_id',
    schema='staging'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'payment') }}
    where "PayDate" >= '2023-01-01'::date  
        and "PayDate" <= current_date
        and "PayDate" > '2000-01-01'::date  
    {% if is_incremental() %}
        and "PayDate" > (select max(payment_date) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"PayNum"', 'target': 'payment_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': 'NULLIF("ClinicNum", 0)', 'target': 'clinic_id'},
            {'source': '"PayType"', 'target': 'payment_type_id'},
            {'source': 'NULLIF("DepositNum", 0)', 'target': 'deposit_id'}
        ]) }},

        -- Payment details
        "PayDate" as payment_date,
        "PayAmt"::double precision as payment_amount,
        coalesce("MerchantFee", 0.0)::double precision as merchant_fee,
        nullif(trim("CheckNum"), '') as check_number,
        nullif(trim("BankBranch"), '') as bank_branch,
        nullif(trim("ExternalId"), '') as external_id,
        nullif(trim("PayNote"), '') as payment_notes,
        nullif(trim("Receipt"), '') as receipt_text,

        -- Status and type fields
        "PaymentStatus"::smallint as payment_status,
        "ProcessStatus"::smallint as process_status,
        "PaymentSource"::smallint as payment_source,

        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsSplit"') }} as is_split,
        {{ convert_opendental_boolean('"IsRecurringCC"') }} as is_recurring_cc,
        {{ convert_opendental_boolean('"IsCcCompleted"') }} as is_cc_completed,

        -- Date fields using macro
        {{ clean_opendental_date('"RecurringChargeDate"') }} as recurring_charge_date,
        {{ clean_opendental_date('"DateEntry"') }} as entry_date,

        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column='"SecUserNumEntry"'
        ) }}

    from source_data
)

select * from renamed_columns