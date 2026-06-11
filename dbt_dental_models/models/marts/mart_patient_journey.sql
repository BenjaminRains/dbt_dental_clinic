{{ config(
    materialized='table',
    schema='marts',
    unique_key='patient_id',
    on_schema_change='fail',
    indexes=[
        {'columns': ['patient_id'], 'unique': true},
        {'columns': ['journey_stage']},
        {'columns': ['primary_provider_id']},
        {'columns': ['clinic_id']},
        {'columns': ['acquisition_year', 'acquisition_month']},
        {'columns': ['_updated_at']}
    ]
) }}

/*
Patient Journey Mart - End-to-end patient lifecycle spine (current-state, one row per patient)
Part of System A / System G: Patient Management

This model:
1. Stitches the per-stage marts into a single patient-grain "journey spine"
2. Records the date a patient first reached each lifecycle milestone (immutable historical facts)
3. Derives stage-to-stage velocity (lags) and a current journey_stage classification

Grain: one row per patient (current state as of journey_as_of_date = current_date).
This mirrors dim_patient / mart_patient_retention so the three reconcile. The model is
"snapshot-ready": journey_as_of_date is carried as a column so a periodic snapshot grain
can be layered on later without restructuring.

Acceptance definition (DECISION: procedure-based only):
- "Presented"  = procedure status 1/6 (Treatment Planned / Ordered)  -> first_tx_presented_date
- "Accepted"   = procedure status 2 (Completed) OR status 1/6 with a scheduled appt -> first_tx_accepted_date
- "Treatment"  = procedure status 2 (Completed)                       -> first_treatment_date
This matches int_procedure_acceptance and mart_procedure_acceptance_summary (PBN-aligned).
The treatment-plan signature path (int_treatment_plan) is intentionally NOT used: the MDC
clinic barely uses treatment plans (~220 plans vs 40K+ patients), so signature milestones
would be ~empty and misleading.

Journey stages:
  Prospect            -> no completed visit yet, but a future appointment exists
  New Patient         -> first completed visit within last 90 days, no treatment presented yet
  Treatment Presented -> treatment presented (status 1/6) but not yet accepted
  In Treatment        -> treatment accepted, not yet established in the recall loop
  Active / Recall      -> treatment done and seen within 12 months or in the hygiene loop
  At Risk             -> 6-18 months since last visit, or recall >60 days overdue
  Reactivated         -> seen within 180 days after a prior gap of >365 days
  Lapsed / Lost       -> >18 months since last completed visit

Data Quality Notes:
- Eligible population mirrors mart_patient_retention filters (active statuses, valid first
  visit + DOB, excludes House position and known test patient 32974) for reconciliation.
- case_acceptance_pct uses procedure fees; it can exceed 100% because completed (status 2)
  procedures count as accepted even when never formally "presented" (see int_procedure_acceptance).
- Lag/tenure calculations exclude future dates to avoid negative durations.

Dependencies:
- dim_patient, dim_provider, dim_date, dim_referral
- fact_appointment, fact_claim, fact_payment
- int_procedure_acceptance, int_recall_management, int_patient_referral_bridge
*/

with
-- 0. Eligible patients: single filtered read of dim_patient (mirrors mart_patient_retention)
eligible_patients as (
    select *
    from {{ ref('dim_patient') }}
    where patient_status in ('Patient', 'Inactive')
      and first_visit_date is not null
      and first_visit_date <= current_date
      and first_visit_date >= '2000-01-01'
      and patient_id != 32974
      and birth_date not in ('0001-01-01', '1900-01-01')  -- OpenDental placeholders for unknown DOB
      and position_code != 'House'
),

-- 1. Appointment milestones and activity (eligible patients only)
appointment_milestones as (
    select
        fa.patient_id,
        min(fa.appointment_date) as first_appointment_date,
        min(case when fa.is_completed then fa.appointment_date end) as first_completed_visit_date,
        max(case when fa.is_completed then fa.appointment_date end) as last_completed_visit_date,
        max(fa.appointment_date) as last_appointment_date,
        count(*) as total_appointments,
        sum(case when fa.is_completed then 1 else 0 end) as total_completed_visits,
        sum(case when fa.is_no_show then 1 else 0 end) as no_show_count,
        sum(case when fa.is_hygiene_appointment then 1 else 0 end) as hygiene_visits,
        count(distinct fa.provider_id) as unique_providers_seen,
        max(case when fa.appointment_date > current_date then 1 else 0 end) as has_future_appointment
    from {{ ref('fact_appointment') }} fa
    inner join eligible_patients ep on fa.patient_id = ep.patient_id
    group by fa.patient_id
),

-- 1b. Largest gap between consecutive completed visits (for reactivation detection)
visit_gaps as (
    select
        patient_id,
        max(gap_days) as max_visit_gap_days
    from (
        select
            fa.patient_id,
            fa.appointment_date - lag(fa.appointment_date) over (
                partition by fa.patient_id order by fa.appointment_date
            ) as gap_days
        from {{ ref('fact_appointment') }} fa
        inner join eligible_patients ep on fa.patient_id = ep.patient_id
        where fa.is_completed
          and fa.appointment_date <= current_date
    ) g
    group by patient_id
),

-- 2. Treatment acceptance milestones (procedure-based, PBN-aligned)
acceptance_milestones as (
    select
        pa.patient_id,
        min(case when pa.is_presented then pa.procedure_date end) as first_tx_presented_date,
        min(case when pa.is_accepted then pa.procedure_date end) as first_tx_accepted_date,
        min(case when pa.procedure_status = 2 then pa.procedure_date end) as first_treatment_date,
        min(case when pa.is_exam_procedure and pa.procedure_status = 2 then pa.procedure_date end) as first_exam_date,
        count(distinct case when pa.is_presented then pa.procedure_id end) as procedures_presented,
        count(distinct case when pa.is_accepted then pa.procedure_id end) as procedures_accepted,
        sum(case when pa.is_presented then pa.procedure_fee else 0 end) as tx_presented_amount,
        sum(case when pa.is_accepted then pa.procedure_fee else 0 end) as tx_accepted_amount
    from {{ ref('int_procedure_acceptance') }} pa
    inner join eligible_patients ep on pa.patient_id = ep.patient_id
    where pa.procedure_date <= current_date
    group by pa.patient_id
),

-- 3. Production milestones (billing)
production_milestones as (
    select
        fc.patient_id,
        min(fc.claim_date) as first_production_date,
        sum(fc.billed_amount) as lifetime_production,
        sum(fc.paid_amount) as lifetime_collections
    from {{ ref('fact_claim') }} fc
    inner join eligible_patients ep on fc.patient_id = ep.patient_id
    group by fc.patient_id
),

-- 4. Payment milestones (patient out-of-pocket)
payment_milestones as (
    select
        fp.patient_id,
        min(case when fp.is_patient_payment then fp.payment_date end) as first_patient_payment_date,
        sum(case when fp.is_patient_payment then fp.payment_amount else 0 end) as lifetime_patient_payments
    from {{ ref('fact_payment') }} fp
    inner join eligible_patients ep on fp.patient_id = ep.patient_id
    group by fp.patient_id
),

-- 5. Recall / hygiene loop status (a patient can have multiple recalls -> aggregate)
recall_status as (
    select
        rm.patient_id,
        min(case when rm.recall_status_code = 0 then rm.date_due end) as next_recall_due_date,
        max(case when rm.is_overdue then rm.days_overdue end) as recall_overdue_days,
        bool_or(rm.recall_status_code = 0) as has_active_recall,
        bool_or(rm.is_overdue) as is_recall_overdue
    from {{ ref('int_recall_management') }} rm
    inner join eligible_patients ep on rm.patient_id = ep.patient_id
    group by rm.patient_id
),

-- 6. Acquisition source: earliest referral per patient
patient_first_referral as (
    select patient_id, referral_id, first_referral_date
    from (
        select
            prb.patient_id,
            prb.referral_id,
            prb.first_referral_date,
            row_number() over (
                partition by prb.patient_id
                order by prb.first_referral_date asc, prb.referral_id asc
            ) as rn
        from {{ ref('int_patient_referral_bridge') }} prb
        inner join eligible_patients ep on prb.patient_id = ep.patient_id
    ) ranked
    where rn = 1
),

-- 7. Final integration and journey-stage classification
final as (
    select
        -- Keys and snapshot context
        ep.patient_id,
        current_date as journey_as_of_date,

        -- Demographics
        ep.age,
        ep.gender,
        ep.age_category,
        ep.patient_status,
        ep.has_insurance_flag,
        ep.primary_provider_id,
        ep.clinic_id,
        prov.provider_type_category as primary_provider_type,
        prov.provider_specialty_category as primary_provider_specialty,

        -- Acquisition source
        pfr.referral_id,
        ref.display_name as referral_source_name,
        pfr.first_referral_date,

        -- Acquisition cohort (from first visit date)
        extract(year from ep.first_visit_date)::int as acquisition_year,
        extract(month from ep.first_visit_date)::int as acquisition_month,
        extract(quarter from ep.first_visit_date)::int as acquisition_quarter,

        -- ===== Milestone dates (the journey backbone) =====
        ep.first_visit_date as acquisition_date,
        am.first_appointment_date,
        am.first_completed_visit_date,
        acc.first_exam_date,
        acc.first_tx_presented_date,
        acc.first_tx_accepted_date,
        acc.first_treatment_date,
        prod.first_production_date,
        pay.first_patient_payment_date,
        am.last_completed_visit_date as last_visit_date,
        am.last_appointment_date,
        rc.next_recall_due_date,

        -- ===== Tenure =====
        case when ep.first_visit_date <= current_date
            then current_date - ep.first_visit_date end as days_as_patient,
        case when ep.first_visit_date <= current_date
            then round(((current_date - ep.first_visit_date) / 365.0)::numeric, 1) end as years_as_patient,
        case when am.last_completed_visit_date is not null and am.last_completed_visit_date <= current_date
            then current_date - am.last_completed_visit_date end as days_since_last_visit,

        -- ===== Stage-to-stage velocity (lags, in days) =====
        case when acc.first_tx_presented_date is not null and am.first_completed_visit_date is not null
            then acc.first_tx_presented_date - am.first_completed_visit_date end as days_acquisition_to_presented,
        case when acc.first_tx_accepted_date is not null and acc.first_tx_presented_date is not null
            then acc.first_tx_accepted_date - acc.first_tx_presented_date end as days_presented_to_accepted,
        case when acc.first_treatment_date is not null and acc.first_tx_accepted_date is not null
            then acc.first_treatment_date - acc.first_tx_accepted_date end as days_accepted_to_treatment,
        case when pay.first_patient_payment_date is not null and ep.first_visit_date is not null
            then pay.first_patient_payment_date - ep.first_visit_date end as days_acquisition_to_first_payment,
        vg.max_visit_gap_days,

        -- ===== Activity & value =====
        coalesce(am.total_appointments, 0) as total_appointments,
        coalesce(am.total_completed_visits, 0) as total_completed_visits,
        coalesce(am.no_show_count, 0) as no_show_count,
        round((coalesce(am.no_show_count, 0)::numeric
            / nullif(coalesce(am.total_appointments, 0), 0) * 100)::numeric, 2) as no_show_rate,
        coalesce(am.hygiene_visits, 0) as hygiene_visits,
        coalesce(am.unique_providers_seen, 0) as unique_providers_seen,
        coalesce(acc.procedures_presented, 0) as procedures_presented,
        coalesce(acc.procedures_accepted, 0) as procedures_accepted,
        coalesce(acc.tx_presented_amount, 0) as tx_presented_amount,
        coalesce(acc.tx_accepted_amount, 0) as tx_accepted_amount,
        -- Case acceptance % (fee-based). Can exceed 100% by design (see header note).
        case when coalesce(acc.tx_presented_amount, 0) > 0
            then round((coalesce(acc.tx_accepted_amount, 0)::numeric
                / nullif(acc.tx_presented_amount, 0) * 100)::numeric, 2) end as case_acceptance_pct,
        coalesce(prod.lifetime_production, 0) as lifetime_production,
        coalesce(prod.lifetime_collections, 0) as lifetime_collections,
        coalesce(pay.lifetime_patient_payments, 0) as lifetime_patient_payments,
        case
            when ep.first_visit_date > current_date then null
            when (current_date - ep.first_visit_date) / 365.0 <= 0 then null
            else round((coalesce(prod.lifetime_production, 0)
                / nullif((current_date - ep.first_visit_date) / 365.0, 0))::numeric, 2)
        end as annual_patient_value,

        -- ===== Recall loop =====
        coalesce(rc.has_active_recall, false) as has_active_recall,
        coalesce(rc.is_recall_overdue, false) as is_recall_overdue,
        rc.recall_overdue_days,
        case when coalesce(am.hygiene_visits, 0) >= 2 or coalesce(rc.has_active_recall, false)
            then true else false end as in_recall_loop,

        -- ===== Funnel stage flags =====
        case when am.first_completed_visit_date is not null then true else false end as reached_first_visit,
        case when acc.first_tx_presented_date is not null then true else false end as reached_presented,
        case when acc.first_tx_accepted_date is not null then true else false end as reached_acceptance,
        case when acc.first_treatment_date is not null then true else false end as reached_treatment,
        case when pay.first_patient_payment_date is not null then true else false end as reached_payment,
        case
            when am.last_completed_visit_date is not null
                and am.last_completed_visit_date <= current_date
                and (current_date - am.last_completed_visit_date) <= 180
                and coalesce(vg.max_visit_gap_days, 0) > 365
            then true else false
        end as is_reactivated,

        -- ===== Current journey stage =====
        case
            when am.first_completed_visit_date is null and coalesce(am.has_future_appointment, 0) = 1
                then 'Prospect'
            when am.first_completed_visit_date is null
                then 'Prospect'
            when (current_date - am.last_completed_visit_date) > 540
                then 'Lapsed / Lost'
            when (current_date - am.last_completed_visit_date) <= 180
                and coalesce(vg.max_visit_gap_days, 0) > 365
                then 'Reactivated'
            when (current_date - am.last_completed_visit_date) between 181 and 540
                or (coalesce(rc.is_recall_overdue, false) and coalesce(rc.recall_overdue_days, 0) > 60)
                then 'At Risk'
            when acc.first_treatment_date is not null
                and (
                    (coalesce(am.hygiene_visits, 0) >= 2 or coalesce(rc.has_active_recall, false))
                    or (current_date - am.last_completed_visit_date) <= 365
                )
                then 'Active / Recall'
            when acc.first_tx_accepted_date is not null
                then 'In Treatment'
            when acc.first_tx_presented_date is not null
                then 'Treatment Presented'
            when ep.first_visit_date >= current_date - 90
                then 'New Patient'
            else 'Active / Recall'
        end as journey_stage,

        -- Metadata
        {{ standardize_mart_metadata(
            primary_source_alias='ep',
            source_metadata_fields=['_loaded_at', '_updated_at']
        ) }}

    from eligible_patients ep
    left join {{ ref('dim_provider') }} prov
        on ep.primary_provider_id = prov.provider_id
    left join appointment_milestones am
        on ep.patient_id = am.patient_id
    left join visit_gaps vg
        on ep.patient_id = vg.patient_id
    left join acceptance_milestones acc
        on ep.patient_id = acc.patient_id
    left join production_milestones prod
        on ep.patient_id = prod.patient_id
    left join payment_milestones pay
        on ep.patient_id = pay.patient_id
    left join recall_status rc
        on ep.patient_id = rc.patient_id
    left join patient_first_referral pfr
        on ep.patient_id = pfr.patient_id
    left join {{ ref('dim_referral') }} ref
        on pfr.referral_id = ref.referral_id
)

select * from final
