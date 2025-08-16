{{ config(
    materialized='incremental',
    unique_key='claim_payment_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'claimpayment') }}
    where {{ clean_opendental_date('"CheckDate"') }} >= '2023-01-01'
    {% if is_incremental() %}
        and {{ clean_opendental_date('"SecDateTEdit"') }} > (select max(_loaded_at) from {{ this }})
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
        
        -- Raw metadata columns (preserved from source)
        {{ clean_opendental_date('"SecDateEntry"') }} as sec_date_entry,
        {{ clean_opendental_date('"SecDateTEdit"') }} as sec_date_t_edit,
        
        -- User ID column (using transform_id_columns for proper type conversion)
        {{ transform_id_columns([
            {'source': '"SecUserNumEntry"', 'target': 'sec_user_num_entry'}
        ]) }},
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"SecDateTEdit"'
        ) }},
        
        -- User ID column for compatibility with existing table structure
        {{ transform_id_columns([
            {'source': '"SecUserNumEntry"', 'target': '_created_by'}
        ]) }}
    
    from source_data
)

select * from renamed_columns
