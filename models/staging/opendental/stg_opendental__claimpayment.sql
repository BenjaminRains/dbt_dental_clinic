{{ config(
    materialized='incremental',
    unique_key=['claim_payment_id', '_updated_at']
) }}

with source as (
    select * from {{ source('opendental', 'claimpayment') }}
    where "CheckDate" >= '2023-01-01'
    {% if is_incremental() %}
        and "SecDateTEdit" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "ClaimPaymentNum" as claim_payment_id,
        
        -- Date fields
        "CheckDate" as check_date,
        "DateIssued" as date_issued,
        
        -- Amount and identification fields
        "CheckAmt" as check_amount,
        "CheckNum" as check_number,
        "BankBranch" as bank_branch,
        
        -- General attributes
        "Note" as note,
        "ClinicNum" as clinic_id,
        "DepositNum" as deposit_id,
        "CarrierName" as carrier_name,
        ("IsPartial" = 1)::boolean as is_partial,
        "PayType" as payment_type_id,
        "PayGroup" as payment_group_id,
        
        -- Metadata fields
        "SecUserNumEntry" as created_by_user_id,
        "SecDateEntry" as _created_at,
        "SecDateTEdit" as _updated_at,
        current_timestamp as _loaded_at
    
    from source
)

select * from renamed
