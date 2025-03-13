/*
  PAYMENT VALIDATION RULES
  =======================
  
  Purpose:
  Validates payment data patterns and business rules discovered during analysis. this file 
  references the actual source table file 'stg_opendental__payment'. 
  
  Key Validation Rules:
  1. Payment Type Rules (2023-current):
     - Type 0: Must have $0 amount (Administrative entries)
     - Type 72: Must be negative (Refunds)
  
  Last Updated: 2024-03-14
*/

with PaymentData as (
    select * from {{ ref('stg_opendental__payment') }}
    where payment_date >= '2023-01-01'
),

ValidationFailures as (
    -- 1. Type 0 Validation (Administrative)
    select 
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        payment_notes,
        'Type 0 with non-zero amount' as failure_type
    from PaymentData
    where payment_type_id = 0 
    and payment_amount != 0

    union all

    -- 2. Type 72 Validation (Refunds)
    select 
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        payment_notes,
        'Type 72 with non-negative amount' as failure_type
    from PaymentData
    where payment_type_id = 72 
    and payment_amount >= 0
)

select *
from ValidationFailures