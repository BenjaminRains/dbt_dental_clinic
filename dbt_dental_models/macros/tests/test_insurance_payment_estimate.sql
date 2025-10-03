{% test test_insurance_payment_estimate(model, column_name) %}

with validation as (
    select
        claim_procedure_id,
        status,
        fee_billed,
        insurance_payment_estimate,
        insurance_payment_amount,
        case
            -- Allow zero estimates for pre-authorizations and denied claims
            when status in (2, 4) then insurance_payment_estimate = 0
            -- For received claims with payments, estimate should be close to actual payment
            when status = 1 and insurance_payment_amount > 0 then 
                abs(insurance_payment_estimate - insurance_payment_amount) <= 1000
            -- For received claims without payments, estimate should be <= fee_billed
            when status = 1 and insurance_payment_amount = 0 then 
                insurance_payment_estimate <= fee_billed
            -- For pending claims, allow any non-negative estimate
            when status not in (1, 2, 4) then insurance_payment_estimate >= 0
            else true
        end as is_valid
    from {{ model }}
),

validation_errors as (
    select *
    from validation
    where not is_valid
)

select count(*)
from validation_errors

{% endtest %} 