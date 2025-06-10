{{ config(
    materialized='incremental',
    unique_key='paysplit_id',
    schema='staging'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'paysplit') }}
    where "DatePay" >= '2023-01-01'::date
        and "DatePay" <= current_date
        and "DatePay" > '2000-01-01'::date
    {% if is_incremental() %}
        and "DatePay" > (select max(payment_date) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"SplitNum"', 'target': 'paysplit_id'},
            {'source': '"PayNum"', 'target': 'payment_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': 'NULLIF("ClinicNum", 0)', 'target': 'clinic_id'},
            {'source': 'NULLIF("ProvNum", 0)', 'target': 'provider_id'},
            {'source': 'NULLIF("ProcNum", 0)', 'target': 'procedure_id'},
            {'source': 'NULLIF("AdjNum", 0)', 'target': 'adjustment_id'},
            {'source': 'NULLIF("PayPlanNum", 0)', 'target': 'payplan_id'},
            {'source': 'NULLIF("PayPlanChargeNum", 0)', 'target': 'payplan_charge_id'},
            {'source': 'NULLIF("FSplitNum", 0)', 'target': 'forward_split_id'}
        ]) }},

        -- Split details
        "SplitAmt"::double precision as split_amount,
        "DatePay" as payment_date,
        "ProcDate" as procedure_date,
        
        -- Type and category fields
        "DiscountType" as discount_type,
        "UnearnedType" as unearned_type,
        "PayPlanDebitType" as payplan_debit_type,

        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsDiscount"') }} as is_discount,

        -- Date fields using macro
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
