{{
    config(
        materialized='table',
        schema='marts',
        unique_key='payment_date',
        on_schema_change='fail'
    )
}}

/*
Daily payments KPI

Grain:
- One row per payment_date (calendar date derived from fact_payment.payment_date).

Business logic assumptions:
- Uses all rows from marts.fact_payment (no additional filtering by payment_type beyond the flags below).
- patient_payment_amount / insurance_payment_amount use boolean flags from fact_payment
  (is_patient_payment, is_insurance_payment), which are derived from payment_type_id.
- income_amount / refund_amount are based on payment_direction from fact_payment:
  - 'Income' = positive payment_amount
  - 'Refund' = negative payment_amount
  - 'Zero'   = zero-amount records; these contribute to total_payment_amount but not to
    income_amount or refund_amount.
- net_collections_amount = income_amount + refund_amount (refunds are negative).
- All monetary fields are cast to numeric(18,2) and rounded to 2 decimals for financial reporting.

Payment source breakdown (patient / insurance / other):
- In principle, payments are either from the patient or from insurance. In this mart they are
  grouped using fact_payment’s is_patient_payment and is_insurance_payment (based on
  payment_type_id = 0 and 1 respectively). Any payment not classified as patient or insurance
  is counted and summed as "other".
- "Other" includes: Partial, PrePayment, Adjustment, Refund (as payment type), and any
  payment_type_id not mapped to Patient (0) or Insurance (1) in fact_payment. Many clinics
  use OpenDental payment type IDs such as 69, 70, 71, 391, 412, etc.; if those are not
  mapped to Patient/Insurance in fact_payment, they appear here as other_payment_count /
  other_payment_amount. So payment_count = patient_payment_count + insurance_payment_count
  + other_payment_count, and total_payment_amount = patient + insurance + other amounts.

Intended uses:
- Daily collections tracking and comparison to OpenDental daily deposit / payment logs.
- High-level patient vs insurance mix by day.
- Upstream input to dashboard KPIs and deposit-reconciliation views.
*/

with source_payments as (
    select *
    from {{ ref('fact_payment') }}
),

payments_clean as (
    select
        payment_date::date as payment_date,
        payment_amount::numeric(18, 2) as payment_amount,
        payment_direction,
        payment_type,
        is_patient_payment,
        is_insurance_payment
    from source_payments
),

daily_agg as (
    select
        payment_date,

        -- Raw sums
        round(sum(payment_amount), 2) as total_payment_amount,

        -- Patient vs insurance using boolean flags from fact_payment
        round(
            coalesce(sum(case when is_patient_payment then payment_amount end), 0),
            2
        ) as patient_payment_amount,
        round(
            coalesce(sum(case when is_insurance_payment then payment_amount end), 0),
            2
        ) as insurance_payment_amount,
        round(
            coalesce(sum(case when not is_patient_payment and not is_insurance_payment then payment_amount end), 0),
            2
        ) as other_payment_amount,

        -- Direction-based views
        round(
            coalesce(sum(case when payment_direction = 'Income' then payment_amount end), 0),
            2
        ) as income_amount,
        round(
            coalesce(sum(case when payment_direction = 'Refund' then payment_amount end), 0),
            2
        ) as refund_amount,

        -- Net (collections minus refunds)
        round(
            coalesce(sum(case when payment_direction = 'Income' then payment_amount end), 0) +
            coalesce(sum(case when payment_direction = 'Refund' then payment_amount end), 0),
            2
        ) as net_collections_amount,

        -- Volumetrics
        count(*) as payment_count,
        count(*) filter (where is_patient_payment)   as patient_payment_count,
        count(*) filter (where is_insurance_payment) as insurance_payment_count,
        count(*) filter (where not is_patient_payment and not is_insurance_payment) as other_payment_count

    from payments_clean
    group by payment_date
),

final as (
    select
        payment_date,
        total_payment_amount,
        patient_payment_amount,
        insurance_payment_amount,
        other_payment_amount,
        income_amount,
        refund_amount,
        net_collections_amount,
        payment_count,
        patient_payment_count,
        insurance_payment_count,
        other_payment_count
    from daily_agg
)

select *
from final

