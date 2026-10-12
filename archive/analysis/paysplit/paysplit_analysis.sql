with source as (
    select * from {{ source('opendental', 'paysplit') }}
),

renamed as (
    select
        -- Primary key
        SplitNum as payment_split_id,

        -- Related entities
        PayNum as payment_id,
        PatNum as patient_id,
        ProvNum as provider_id,
        ClinicNum as clinic_id,
        ProcNum as procedure_id,
        PayPlanNum as payment_plan_id,
        PayPlanChargeNum as payment_plan_charge_id,
        AdjNum as adjustment_id,
        FSplitNum as prepayment_split_id,
        SecUserNumEntry as created_by_user_id,

        -- Financial details
        SplitAmt as split_amount,
        IsDiscount as is_discount_flag,
        DiscountType as discount_type,
        PayPlanDebitType as payment_plan_debit_type,
        UnearnedType as unearned_type,

        -- Dates
        ProcDate as procedure_date,
        DatePay as payment_date,
        DateEntry as entry_date,
        SecDateTEdit as updated_at,

        -- Security
        SecurityHash as security_hash
    from source
)

select * from renamed