with source as (
    select * from {{ source('opendental', 'paysplit') }}
    where UnearnedType IN (288, 439)
    and DatePay >= '2022-01-01'
    and DatePay <= CURRENT_DATE
),

renamed as (
    select
        -- Keys
        SplitNum as unearned_income_id,
        PayNum as payment_id,
        PatNum as patient_id,
        NULLIF(ProvNum, 0) as provider_id,
        NULLIF(ClinicNum, 0) as clinic_id,
        
        -- Amount and type
        UnearnedType as unearned_type_id,
        SplitAmt as amount,
        
        -- Dates
        DatePay as payment_date,
        
        -- Calculated fields
        CASE 
            WHEN UnearnedType = 288 THEN 'type_288_unearned'
            WHEN UnearnedType = 439 THEN 'type_439_unearned'
        END as unearned_category,
        
        CASE 
            WHEN SplitAmt > 0 THEN 'positive'
            WHEN SplitAmt < 0 THEN 'negative'
            ELSE 'zero'
        END as amount_direction,
        
        CASE
            WHEN ABS(SplitAmt) >= 5000 THEN 'very_large'
            WHEN ABS(SplitAmt) >= 1000 THEN 'large'
            WHEN ABS(SplitAmt) >= 100 THEN 'medium'
            ELSE 'small'
        END as amount_size,
        
        -- Additional flags
        CASE 
            WHEN ABS(SplitAmt) >= 5000 THEN 1
            ELSE 0
        END as is_large_transaction,
        
        CASE 
            WHEN SplitAmt < 0 THEN 1
            ELSE 0
        END as is_reversal
        
    from source
)
select * from renamed