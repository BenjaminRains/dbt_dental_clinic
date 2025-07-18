{{ config(
    materialized='incremental',
    unique_key=['claim_payment_id', '_updated_at'],
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'claimpayment') }}
    where {{ clean_opendental_date('"CheckDate"') }} >= '2023-01-01'
    {% if is_incremental() %}
        and {{ clean_opendental_date('"SecDateTEdit"') }} > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"ClaimPaymentNum"', 'target': 'claim_payment_id'}
        ]) }},
        
        -- Foreign Keys
        {{ transform_id_columns([
            {'source': '"ClinicNum"', 'target': 'clinic_id'},
            {'source': '"DepositNum"', 'target': 'deposit_id'},
            {'source': '"PayType"', 'target': 'payment_type_id'},
            {'source': '"PayGroup"', 'target': 'payment_group_id'}
        ]) }},
        
        -- Date Fields
        {{ clean_opendental_date('"CheckDate"') }} as check_date,
        {{ clean_opendental_date('"DateIssued"') }} as date_issued,
        
        -- Amount and Identification Fields
        "CheckAmt" as check_amount,
        "CheckNum" as check_number,
        "BankBranch" as bank_branch,
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"IsPartial"') }} as is_partial,
        
        -- General Attributes
        "Note" as note,
        "CarrierName" as carrier_name,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column='"SecUserNumEntry"'
        ) }}
    
    from source_data
)

select * from renamed_columns
