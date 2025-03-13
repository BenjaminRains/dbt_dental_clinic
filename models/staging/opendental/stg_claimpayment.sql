with source as (
    select * from {{ source('opendental', 'claimpayment') }}
),

renamed as (
    select
        -- Keys
        ClaimPaymentNum as claim_payment_id,
        ClinicNum as clinic_id,
        DepositNum as deposit_id,
        PayType as payment_type_id,
        PayGroup as payment_group_id,
        
        -- Payment details
        CheckNum as check_number,
        CheckAmt as check_amount,
        CheckDate as check_date,
        BankBranch as bank_branch,
        CarrierName as carrier_name,
        DateIssued as date_issued,
        
        -- Classification and status
        IsPartial as is_partial_payment,
        
        -- Derived fields
        CASE
            WHEN IsPartial = 1 THEN 'Partial'
            ELSE 'Complete'
        END as payment_completion_status,
        
        -- Notes (non-PHI administrative notes)
        Note as payment_note,
        
        -- Metadata
        SecUserNumEntry as created_by_user_id,
        SecDateEntry as created_at,
        SecDateTEdit as updated_at
    from source
)

select * from renamed