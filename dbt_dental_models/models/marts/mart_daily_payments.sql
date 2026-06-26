{{
    config(
        materialized='table',
        schema='marts',
        unique_key='payment_date',
        on_schema_change='fail'
    )
}}

/*
Daily payments KPI — aligned with OpenDental Daily Payments report.

Grain:
- One row per calendar payment_date.

Sources (validated 2026-06-24 vs OD golden export):
- Patient / practice payments: stg_opendental__payment on PayDate (payment_date).
- Insurance claim payments (e.g. Metlife EFT): stg_opendental__claimpayment on CheckDate (check_date).

OD Daily Payments nets income-transfer sections to zero; non-zero patient payment headers plus
claim check totals match the report total. Zero-amount payment headers are excluded from net
collections and counts (they do not change dollar totals).

PayType mapping (clinic patient types on payment.PayType):
- Patient: 69, 70, 71, 391, 412, 417, 574, 634, 676 (Check, Cash, Credit Card, Cherry, etc.)
- Insurance on this mart comes from claimpayment, not payment.PayType 261/303/464/...
*/

with patient_payments as (
    select
        payment_date::date as payment_date,
        payment_type_id,
        payment_amount::numeric(18, 2) as payment_amount
    from {{ ref('stg_opendental__payment') }}
    where payment_date is not null
),

patient_daily as (
    select
        payment_date,

        round(coalesce(sum(payment_amount) filter (where payment_amount <> 0), 0), 2)
            as patient_payment_amount,

        round(coalesce(sum(payment_amount) filter (
            where payment_amount <> 0
              and payment_type_id not in (69, 70, 71, 391, 412, 417, 574, 634, 676)
        ), 0), 2) as patient_other_type_amount,

        round(coalesce(sum(payment_amount) filter (where payment_amount > 0), 0), 2)
            as patient_income_amount,

        round(coalesce(sum(payment_amount) filter (where payment_amount < 0), 0), 2)
            as patient_refund_amount,

        count(*) filter (where payment_amount <> 0) as patient_payment_count,
        count(*) filter (
            where payment_amount <> 0
              and payment_type_id not in (69, 70, 71, 391, 412, 417, 574, 634, 676)
        ) as patient_other_type_count

    from patient_payments
    group by payment_date
),

insurance_daily as (
    select
        check_date::date as payment_date,

        round(coalesce(sum(check_amount::numeric), 0), 2) as insurance_payment_amount,

        round(coalesce(sum(check_amount::numeric) filter (where check_amount > 0), 0), 2)
            as insurance_income_amount,

        round(coalesce(sum(check_amount::numeric) filter (where check_amount < 0), 0), 2)
            as insurance_refund_amount,

        count(*) as insurance_payment_count

    from {{ ref('stg_opendental__claimpayment') }}
    where check_date is not null
    group by check_date::date
),

all_dates as (
    select payment_date from patient_daily
    union
    select payment_date from insurance_daily
),

daily_agg as (
    select
        d.payment_date,

        round(
            coalesce(p.patient_payment_amount, 0) + coalesce(i.insurance_payment_amount, 0),
            2
        ) as total_payment_amount,

        coalesce(p.patient_payment_amount, 0)::numeric(18, 2) as patient_payment_amount,
        coalesce(i.insurance_payment_amount, 0)::numeric(18, 2) as insurance_payment_amount,
        coalesce(p.patient_other_type_amount, 0)::numeric(18, 2) as other_payment_amount,

        round(
            coalesce(p.patient_income_amount, 0) + coalesce(i.insurance_income_amount, 0),
            2
        ) as income_amount,

        round(
            coalesce(p.patient_refund_amount, 0) + coalesce(i.insurance_refund_amount, 0),
            2
        ) as refund_amount,

        round(
            coalesce(p.patient_payment_amount, 0) + coalesce(i.insurance_payment_amount, 0),
            2
        ) as net_collections_amount,

        coalesce(p.patient_payment_count, 0) + coalesce(i.insurance_payment_count, 0)
            as payment_count,
        coalesce(p.patient_payment_count, 0) as patient_payment_count,
        coalesce(i.insurance_payment_count, 0) as insurance_payment_count,
        coalesce(p.patient_other_type_count, 0) as other_payment_count

    from all_dates d
    left join patient_daily p on d.payment_date = p.payment_date
    left join insurance_daily i on d.payment_date = i.payment_date
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
        other_payment_count,

        {{ standardize_mart_metadata(preserve_source_metadata=false) }}

    from daily_agg
)

select *
from final
