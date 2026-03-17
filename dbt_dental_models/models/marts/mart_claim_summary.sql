{{ config(
    materialized='table',
    schema='marts',
    unique_key=['date_id', 'provider_id'],
    on_schema_change='fail',
    indexes=[
        {'columns': ['date_id']},
        {'columns': ['provider_id']},
        {'columns': ['date_id', 'provider_id']}
    ]
) }}

/*
Summary Mart model for insurance claim analytics and revenue cycle metrics.
Part of System B: Insurance and Claims Management

This model:
1. Provides aggregated claim metrics by date and provider for insurance performance tracking
2. Supports reimbursement analysis, approval rates, and payment cycle time metrics
3. Enables insurance metrics dashboards and provider-level claims KPIs

Business Logic Features:
- Claim volume and status distribution (paid, denied, rejected, pending)
- Financial aggregation: billed, allowed, paid, write-off, patient responsibility
- Reimbursement rate and approval rate calculations
- Average days to payment for paid claims
- Safe division with nullif() for percentage metrics

Key Metrics:
- total_claim_procedures, total_claims (distinct claim count)
- claims_paid, claims_denied, claims_rejected, claims_pending
- total_billed_amount, total_paid_amount, total_write_off_amount
- reimbursement_rate (paid/billed), approval_rate (paid procedures / total)
- avg_payment_days_from_claim (for paid claims only)

Data Quality Notes:
- Excludes pre-auth/draft claims (claim_id = 0) from summary
- Excludes rows with null claim_date or null provider_id to maintain clean grain
- Uses nullif() for all rate calculations to avoid division by zero
- Payment_status_category from fact_claim: Paid, Denied, Rejected, Pending, Unknown, Pre-auth

Performance Considerations:
- Grain: one row per date_id + provider_id for optimal dashboard queries
- Indexed on date_id, provider_id, and composite for filters and joins
- Materialized as table for fast analytical queries

Dependencies:
- fact_claim: Primary source for claim transaction and payment data
- dim_date: Date dimension for temporal analysis
- dim_provider: Provider dimension for provider attributes
*/

with claim_base as (
    select
        fc.claim_date,
        fc.provider_id,
        fc.claim_id,
        fc.payment_status_category,
        fc.billed_amount,
        fc.allowed_amount,
        fc.paid_amount,
        fc.write_off_amount,
        fc.adjustment_write_off_amount,
        fc.patient_responsibility,
        fc.payment_days_from_claim,
        fc.patient_id
    from {{ ref('fact_claim') }} fc
    where fc.claim_date is not null
      and fc.provider_id is not null
      and fc.claim_id != 0  /* exclude pre-auth/draft claims */
),

claim_aggregated as (
    select
        claim_date,
        provider_id,

        -- Volume: procedure-level and claim-level
        count(*) as total_claim_procedures,
        count(distinct claim_id) as total_claims,
        count(distinct patient_id) as unique_patients,

        -- Status counts (payment_status_category)
        sum(case when payment_status_category = 'Paid' then 1 else 0 end) as claims_paid,
        sum(case when payment_status_category = 'Denied' then 1 else 0 end) as claims_denied,
        sum(case when payment_status_category = 'Rejected' then 1 else 0 end) as claims_rejected,
        sum(case when payment_status_category = 'Pending' then 1 else 0 end) as claims_pending,
        sum(case when payment_status_category not in ('Paid', 'Denied', 'Rejected', 'Pending') then 1 else 0 end) as claims_other_status,

        -- Financial amounts
        sum(billed_amount) as total_billed_amount,
        sum(allowed_amount) as total_allowed_amount,
        sum(paid_amount) as total_paid_amount,
        sum(write_off_amount) as total_write_off_amount,
        sum(adjustment_write_off_amount) as total_adjustment_write_off_amount,
        sum(patient_responsibility) as total_patient_responsibility,

        -- Total write-off (legacy + adjustment)
        sum(coalesce(write_off_amount, 0) + coalesce(adjustment_write_off_amount, 0)) as total_write_off_total,

        -- Payment timing (only for paid claims)
        avg(case when payment_status_category = 'Paid' and payment_days_from_claim is not null then payment_days_from_claim end) as avg_payment_days_from_claim,
        sum(case when payment_status_category = 'Paid' and payment_days_from_claim is not null then 1 else 0 end) as paid_claims_with_days

    from claim_base
    group by claim_date, provider_id
),

claim_rates as (
    select
        *,
        round(
            ((total_paid_amount::numeric / nullif(total_billed_amount, 0)) * 100)::numeric,
            2
        ) as reimbursement_rate_pct,
        round(
            ((claims_paid::numeric / nullif(total_claim_procedures, 0)) * 100)::numeric,
            2
        ) as approval_rate_pct
    from claim_aggregated
),

date_dimension as (
    select
        date_id,
        date_day as date_actual,
        day_name,
        is_weekend,
        is_holiday,
        month,
        quarter,
        year
    from {{ ref('dim_date') }}
),

provider_dimension as (
    select
        provider_id,
        provider_type_category as provider_type,
        specialty_description as specialty
    from {{ ref('dim_provider') }}
),

final as (
    select
        dd.date_id,
        cr.claim_date,
        cr.provider_id,
        pd.provider_type,
        pd.specialty,
        dd.day_name,
        dd.is_weekend,
        dd.is_holiday,
        dd.month,
        dd.quarter,
        dd.year,

        -- Volume metrics
        cr.total_claim_procedures,
        cr.total_claims,
        cr.unique_patients,
        cr.claims_paid,
        cr.claims_denied,
        cr.claims_rejected,
        cr.claims_pending,
        cr.claims_other_status,

        -- Financial metrics
        round(cr.total_billed_amount::numeric, 2) as total_billed_amount,
        round(cr.total_allowed_amount::numeric, 2) as total_allowed_amount,
        round(cr.total_paid_amount::numeric, 2) as total_paid_amount,
        round(cr.total_write_off_amount::numeric, 2) as total_write_off_amount,
        round(cr.total_adjustment_write_off_amount::numeric, 2) as total_adjustment_write_off_amount,
        round(cr.total_patient_responsibility::numeric, 2) as total_patient_responsibility,
        round(cr.total_write_off_total::numeric, 2) as total_write_off_total,

        -- Rate metrics (0-100)
        cr.reimbursement_rate_pct,
        cr.approval_rate_pct,

        -- Payment cycle
        round(cr.avg_payment_days_from_claim::numeric, 2) as avg_payment_days_from_claim,
        cr.paid_claims_with_days,

        -- Standardized metadata
        {{ standardize_mart_metadata(preserve_source_metadata=false) }}

    from claim_rates cr
    inner join date_dimension dd
        on cr.claim_date = dd.date_actual
    left join provider_dimension pd
        on cr.provider_id = pd.provider_id
)

select * from final
