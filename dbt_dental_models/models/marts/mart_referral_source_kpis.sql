{{
    config(
        materialized='table',
        schema='marts',
        unique_key=['reporting_month', 'referral_id', 'period_basis'],
        on_schema_change='fail',
        indexes=[
            {'columns': ['reporting_month', 'referral_id', 'period_basis'], 'unique': true},
            {'columns': ['referral_id']},
            {'columns': ['period_basis']},
            {'columns': ['referral_display_name']},
            {'columns': ['reporting_year', 'reporting_month_number']},
            {'columns': ['period_basis_sort_order']}
        ]
    )
}}

/*
    Monthly KPIs by referral source and attribution basis.
    period_basis:
      - referral_link: refattach referral_date in the reporting month.
      - new_patient_first_visit: patient has a link to the referral and first_visit_date in the month.
      - production_in_period: patient linked to referral and had completed production (fact_procedure) in the month.

    Value columns:
      - production_value_in_period: sum of fact_procedure.actual_fee (completed / existing-prior procedures) in the
        reporting month for cohort patients — production, not cash.
      - net_collections_in_period: cash view — sum of fact_payment.payment_amount for payments in the reporting
        month where payment_direction is Income or Refund (refunds negative), same net definition as mart_daily_payments.

    Referral / referring-source names come from dim_referral (OpenDental referral list — external referrers and doctors),
    not dim_provider (in-house treating providers).
*/

with refattach_filtered as (
    select
        ra.patient_id,
        ra.referral_id,
        ra.referral_date
    from {{ ref('stg_opendental__refattach') }} ra
    inner join {{ ref('dim_referral') }} dr
        on ra.referral_id = dr.referral_id
    where ra.patient_id is not null
        and ra.referral_id is not null
        and ra.referral_date is not null
        and not dr.is_hidden
),

-- fact_procedure exposes date_id only; align to calendar month via dim_date
procedure_with_month as (
    select
        proc_f.patient_id,
        proc_f.actual_fee,
        date_trunc('month', dd.date_day)::date as procedure_month
    from {{ ref('fact_procedure') }} proc_f
    inner join {{ ref('dim_date') }} dd on proc_f.date_id = dd.date_id
),

-- Collections by patient-month from fact_payment (aligned with mart_daily_payments net_collections logic)
payment_with_month as (
    select
        pay.patient_id,
        date_trunc('month', pay.payment_date)::date as payment_month,
        case
            when pay.payment_direction in ('Income', 'Refund') then pay.payment_amount::numeric(18, 2)
            else 0::numeric(18, 2)
        end as net_collections_amount
    from {{ ref('fact_payment') }} pay
),

procedure_month_totals as (
    select
        patient_id,
        procedure_month,
        sum(actual_fee)::numeric(18, 2) as production_total
    from procedure_with_month
    group by patient_id, procedure_month
),

payment_month_totals as (
    select
        patient_id,
        payment_month,
        sum(net_collections_amount)::numeric(18, 2) as net_collections_total
    from payment_with_month
    group by patient_id, payment_month
),

referral_link_cohorts as (
    select distinct
        date_trunc('month', referral_date)::date as reporting_month,
        referral_id,
        patient_id
    from refattach_filtered
),

new_patient_cohorts as (
    select distinct
        date_trunc('month', dp.first_visit_date)::date as reporting_month,
        rf.referral_id,
        rf.patient_id
    from refattach_filtered rf
    inner join {{ ref('dim_patient') }} dp
        on rf.patient_id = dp.patient_id
    where dp.first_visit_date is not null
),

production_cohorts as (
    select distinct
        pwm.procedure_month as reporting_month,
        rf.referral_id,
        pwm.patient_id
    from procedure_with_month pwm
    inner join refattach_filtered rf
        on pwm.patient_id = rf.patient_id
),

referral_link_metrics as (
    select
        c.reporting_month,
        c.referral_id,
        'referral_link' as period_basis,
        count(distinct c.patient_id) as distinct_patient_count,
        coalesce(sum(pt.production_total), 0)::numeric(18, 2) as production_value_in_period,
        coalesce(sum(pyt.net_collections_total), 0)::numeric(18, 2) as net_collections_in_period
    from referral_link_cohorts c
    left join procedure_month_totals pt
        on c.patient_id = pt.patient_id
        and pt.procedure_month = c.reporting_month
    left join payment_month_totals pyt
        on c.patient_id = pyt.patient_id
        and pyt.payment_month = c.reporting_month
    group by c.reporting_month, c.referral_id
),

new_patient_metrics as (
    select
        c.reporting_month,
        c.referral_id,
        'new_patient_first_visit' as period_basis,
        count(distinct c.patient_id) as distinct_patient_count,
        coalesce(sum(pt.production_total), 0)::numeric(18, 2) as production_value_in_period,
        coalesce(sum(pyt.net_collections_total), 0)::numeric(18, 2) as net_collections_in_period
    from new_patient_cohorts c
    left join procedure_month_totals pt
        on c.patient_id = pt.patient_id
        and pt.procedure_month = c.reporting_month
    left join payment_month_totals pyt
        on c.patient_id = pyt.patient_id
        and pyt.payment_month = c.reporting_month
    group by c.reporting_month, c.referral_id
),

production_metrics as (
    select
        c.reporting_month,
        c.referral_id,
        'production_in_period' as period_basis,
        count(distinct c.patient_id) as distinct_patient_count,
        coalesce(sum(pt.production_total), 0)::numeric(18, 2) as production_value_in_period,
        coalesce(sum(pyt.net_collections_total), 0)::numeric(18, 2) as net_collections_in_period
    from production_cohorts c
    left join procedure_month_totals pt
        on c.patient_id = pt.patient_id
        and pt.procedure_month = c.reporting_month
    left join payment_month_totals pyt
        on c.patient_id = pyt.patient_id
        and pyt.payment_month = c.reporting_month
    group by c.reporting_month, c.referral_id
),

unioned as (
    select * from referral_link_metrics
    union all
    select * from new_patient_metrics
    union all
    select * from production_metrics
),

final as (
    select
        u.reporting_month,
        extract(year from u.reporting_month)::integer as reporting_year,
        extract(month from u.reporting_month)::integer as reporting_month_number,
        to_char(u.reporting_month, 'YYYY-MM') as reporting_year_month,
        u.referral_id,
        dr.display_name as referral_display_name,
        dr.last_name as referral_last_name,
        dr.first_name as referral_first_name,
        dr.middle_name as referral_middle_name,
        dr.business_name as referral_business_name,
        dr.title as referral_title,
        dr.national_provider_id as referral_national_provider_id,
        dr.is_doctor as referral_is_doctor,
        dr.not_person as referral_not_person,
        case
            when dr.is_doctor then 'physician_or_clinical'
            when dr.not_person then 'organization'
            else 'individual'
        end as referral_source_segment,
        u.period_basis,
        case u.period_basis
            when 'referral_link' then 1
            when 'new_patient_first_visit' then 2
            when 'production_in_period' then 3
        end as period_basis_sort_order,
        case u.period_basis
            when 'referral_link' then 'Referral attached in month (RefDate)'
            when 'new_patient_first_visit' then 'First visit in month with referral link on file'
            when 'production_in_period' then 'Production in month (patient has referral link; not necessarily new)'
        end as period_basis_description,
        u.distinct_patient_count,
        u.production_value_in_period,
        u.net_collections_in_period,
        {{ standardize_mart_metadata(preserve_source_metadata=false) }}
    from unioned u
    inner join {{ ref('dim_referral') }} dr
        on u.referral_id = dr.referral_id
)

select * from final
