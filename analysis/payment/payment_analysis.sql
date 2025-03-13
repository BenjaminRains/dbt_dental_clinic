/*
  DEVELOPMENT VERSION FOR DBEAVER TESTING
  This file is used for initial analysis and validation of payment data.
  
  Changes needed for DBT:
  - Replace direct table references with {{ ref() }}
  - Remove _dev suffix from filename
  - follow the sql_naming_conventions.md file for naming conventions
*/
-- First, let's analyze payment types for current data
with CurrentPaymentTypes as (
    select 
        PayType as payment_type_id,
        count(*) as payment_count,
        round(avg(PayAmt), 2) as avg_amount,
        min(PayAmt) as min_amount,
        max(PayAmt) as max_amount,
        count(case when PayAmt < 0 then 1 end) as negative_count,
        min(PayDate) as first_seen,
        max(PayDate) as last_seen
    from opendental_analytics_opendentalbackup_02_28_2025.payment
    where PayDate >= '2023-01-01' 
        and PayDate <= current_date()
    group by PayType
    order by payment_count desc
)
select * from CurrentPaymentTypes;

-- Investigate validation failures by type
with PaymentData as (
    select 
        PayNum as payment_id,
        PatNum as patient_id,
        PayType as payment_type_id,
        PayAmt as payment_amount,
        PayDate as payment_date,
        PayNote as payment_notes
    from opendental_analytics_opendentalbackup_02_28_2025.payment
    where PayDate >= '2023-01-01' 
        and PayDate <= current_date()
),

ValidationFailures as (
    -- Type 0 validation failures
    select 
        'type_0_nonzero' as check_name,
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        payment_notes,
        'Error: Type 0 payment with non-zero amount' as validation_message
    from PaymentData
    where payment_type_id = 0 and payment_amount != 0

    union all

    -- Type 72 validation failures
    select 
        'type_72_positive' as check_name,
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        payment_notes,
        'Error: Type 72 payment with non-negative amount' as validation_message
    from PaymentData
    where payment_type_id = 72 and payment_amount >= 0

    union all

    -- High value payment warnings
    select 
        'high_value_payment' as check_name,
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        payment_notes,
        'Warning: Unusually high payment amount for type' as validation_message
    from PaymentData
    where (
        (payment_type_id = 69 and payment_amount > 5000) or
        (payment_type_id = 574 and payment_amount > 50000)
    )

    union all

    -- Unexpected negative amounts
    select 
        'unexpected_negative' as check_name,
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        payment_notes,
        'Warning: Negative amount needs review' as validation_message
    from PaymentData
    where payment_amount < 0 
    and payment_type_id != 72
    and payment_notes not like '%refund%'
    and payment_notes not like '%reversal%'
    and payment_notes not like '%given back%'
)

-- Summary of failures by type
select 
    check_name,
    validation_message,
    count(*) as failure_count,
    round(avg(payment_amount), 2) as avg_amount,
    min(payment_amount) as min_amount,
    max(payment_amount) as max_amount
from ValidationFailures
group by check_name, validation_message

-- Detailed failures
union all
select 
    check_name,
    validation_message,
    1 as failure_count,
    payment_amount as avg_amount,
    payment_amount as min_amount,
    payment_amount as max_amount
from ValidationFailures
order by check_name, payment_id;