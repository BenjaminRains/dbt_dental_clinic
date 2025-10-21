{{ config(
    materialized='table',
    schema='marts',
    unique_key=['date_id', 'patient_id'],
    on_schema_change='fail',
    indexes=[
        {'columns': ['date_id']},
        {'columns': ['patient_id']},
        {'columns': ['primary_provider_id']},
        {'columns': ['clinic_id']}
    ]
) }}

/*
Summary Mart model for New Patient Analytics
Part of System A: Patient Management

This model:
1. Tracks new patient acquisition patterns and onboarding success
2. Analyzes early patient journey metrics and retention indicators
3. Provides comprehensive new patient analytics for marketing optimization

Business Logic Features:
- New patient identification and demographic categorization
- First visit success tracking and completion rate analysis
- Early engagement metrics (90-day window)
- Patient value categorization and referral generation tracking
- Onboarding success scoring (0-100 scale)
- Acquisition performance tier classification

Key Metrics:
- New patient acquisition patterns by demographics and time periods
- First visit completion rates and no-show tracking
- Early patient engagement and retention indicators
- Patient value categorization and lifetime value estimation
- Referral generation and family acquisition tracking
- Onboarding success scoring and performance tier classification

Data Quality Notes:
- New patients identified by first_visit_date not null and patient_status = 'Patient'
- Production amounts calculated from actual procedure fees in procedurelog table
- Procedure codes retrieved from procedurecode table via procedurelog relationship
- Referral tracking assumes family relationships via guarantor_id
- 90-day engagement window calculated from first_visit_date
- Patient value categories based on realistic production thresholds ($100, $300, $600)

Performance Considerations:
- Indexed on date_id, patient_id, provider_id, and clinic_id for analytical queries
- Complex aggregations across multiple fact tables require careful join optimization
- Array aggregation for procedure codes may impact query performance

Dependencies:
- dim_patient: Primary patient demographic and status data
- fact_appointment: Appointment scheduling and completion data
- int_procedure_complete: Actual procedure fees and production data with codes
- fact_payment: Early payment and financial activity data
- dim_date: Date dimension for temporal analysis
- dim_provider: Provider information and categorization
*/

-- 1. Base patient data
with new_patient_base as (
    select 
        dp.patient_id,
        dp.first_visit_date,
        dp.admit_date,
        dp.birth_date,
        dp.age,
        dp.gender,
        dp.patient_status,
        dp.primary_provider_id,
        dp.clinic_id,
        dp.guarantor_id,
        dp.has_insurance_flag,
        dp.estimated_balance,
        -- Source metadata persisted from staging/dimensions
        dp._loaded_at,
        dp._created_at,
        dp._updated_at,
        dp.preferred_contact_method,
        dp.preferred_confirmation_method
    from {{ ref('dim_patient') }} dp
    where dp.first_visit_date is not null
        and dp.patient_status = 'Patient'
),

-- 2. First appointment analysis with procedure data from intermediate
first_appointments as (
    select 
        fa.patient_id,
        min(fa.appointment_date) as first_appointment_date,
        min(fa.appointment_datetime) as first_appointment_datetime,
        count(*) as total_first_day_appointments,
        sum(case when fa.is_completed then 1 else 0 end) as completed_first_appointments,
        sum(case when fa.is_no_show then 1 else 0 end) as no_show_first_appointments,
        -- Get actual production from procedures performed on first visit
        coalesce(sum(pc.procedure_fee), 0.00) as first_visit_scheduled_production,
        -- Get actual procedure codes from procedures performed
        array_agg(distinct pc.procedure_code) filter (where pc.procedure_code is not null) as first_visit_procedures
    from {{ ref('fact_appointment') }} fa
    inner join new_patient_base np
        on fa.patient_id = np.patient_id
        and fa.appointment_date = np.first_visit_date
    left join {{ ref('int_procedure_complete') }} pc
        on fa.appointment_id = pc.appointment_id
        and pc.procedure_date = fa.appointment_date
    group by fa.patient_id
),

-- 3. Early engagement analysis (90 days) with procedure data from intermediate
early_visits as (
    select 
        fa.patient_id,
        count(*) as appointments_first_90_days,
        sum(case when fa.is_completed then 1 else 0 end) as completed_appointments_90_days,
        -- Get actual production from procedures performed in first 90 days
        coalesce(sum(pc.procedure_fee), 0.00) as production_first_90_days,
        max(fa.appointment_date) as last_appointment_90_days,
        count(distinct fa.provider_id) as providers_seen_90_days
    from {{ ref('fact_appointment') }} fa
    inner join new_patient_base np
        on fa.patient_id = np.patient_id
    left join {{ ref('int_procedure_complete') }} pc
        on fa.appointment_id = pc.appointment_id
        and pc.procedure_date = fa.appointment_date
    where fa.appointment_date between np.first_visit_date 
        and np.first_visit_date + interval '90 days'
    group by fa.patient_id
),

-- 4. Early financial activity analysis
early_payments as (
    select 
        fp.patient_id,
        count(*) as payments_first_90_days,
        sum(case when fp.is_patient_payment then fp.payment_amount else 0 end) as patient_payments_90_days,
        sum(case when fp.is_insurance_payment then fp.payment_amount else 0 end) as insurance_payments_90_days,
        avg(fp.payment_amount) as avg_payment_amount_90_days
    from {{ ref('fact_payment') }} fp
    inner join new_patient_base np
        on fp.patient_id = np.patient_id
    where fp.payment_date between np.first_visit_date 
        and np.first_visit_date + interval '90 days'
    group by fp.patient_id
),

-- 5. Referral generation analysis
patient_referrals as (
    select 
        np.patient_id,
        count(*) as referrals_generated,
        array_agg(distinct ref_patient.patient_id::text) as referred_patient_ids
    from new_patient_base np
    inner join {{ ref('dim_patient') }} ref_patient
        on np.patient_id = ref_patient.guarantor_id  -- Assuming family referrals
        and ref_patient.patient_id != np.patient_id
        and ref_patient.first_visit_date > np.first_visit_date
        and ref_patient.first_visit_date <= np.first_visit_date + interval '365 days'
    group by np.patient_id
),

-- 6. Final integration and business logic enhancement
final as (
    select
        -- Keys and Dimensions
        dd.date_id,
        np.patient_id,
        np.first_visit_date,
        
        -- Patient Demographics
        np.age as age_at_first_visit,
        np.gender,
        np.primary_provider_id,
        np.clinic_id,
        
        -- Provider Information
        concat(prov.provider_first_name, ' ', prov.provider_last_name) as primary_provider_name,
        prov.provider_type_category as primary_provider_type,
        prov.specialty_description as primary_provider_specialty,
        
        -- Date Dimensions
        dd.year as acquisition_year,
        dd.month as acquisition_month,
        dd.quarter as acquisition_quarter,
        dd.day_name as acquisition_day,
        dd.is_weekend as acquired_on_weekend,
        
        -- Patient Characteristics
        case 
            when np.age < 13 then 'Child'
            when np.age < 18 then 'Teen'
            when np.age < 35 then 'Young Adult'
            when np.age < 50 then 'Adult'
            when np.age < 65 then 'Middle Age'
            else 'Senior'
        end as age_category,
        
        np.has_insurance_flag,
        np.estimated_balance as initial_balance,
        np.preferred_contact_method,
        np.preferred_confirmation_method,
        
        -- First Visit Metrics
        coalesce(fa.first_appointment_datetime, np.first_visit_date::timestamp) as first_appointment_datetime,
        coalesce(fa.total_first_day_appointments, 0) as total_first_day_appointments,
        coalesce(fa.completed_first_appointments, 0) as completed_first_appointments,
        coalesce(fa.no_show_first_appointments, 0) as no_show_first_appointments,
        coalesce(fa.first_visit_scheduled_production, 0.00) as first_visit_scheduled_production,
        coalesce(fa.first_visit_procedures, array[]::text[]) as first_visit_procedures,
        
        -- First Visit Success Indicators
        case when coalesce(fa.completed_first_appointments, 0) > 0 then true else false end as completed_first_visit,
        case when coalesce(fa.no_show_first_appointments, 0) > 0 then true else false end as no_show_first_visit,
        case 
            when coalesce(fa.total_first_day_appointments, 0) = 0 then 0.00
            else round(
                coalesce(fa.completed_first_appointments, 0)::numeric / 
                coalesce(fa.total_first_day_appointments, 0)::numeric * 100, 2
            )
        end as first_visit_completion_rate,
        
        -- Early Engagement (90 days)
        coalesce(ev.appointments_first_90_days, 1) as appointments_first_90_days,
        coalesce(ev.completed_appointments_90_days, 0) as completed_appointments_90_days,
        coalesce(ev.production_first_90_days, 0.00) as production_first_90_days,
        coalesce(ev.last_appointment_90_days, np.first_visit_date) as last_appointment_90_days,
        coalesce(ev.providers_seen_90_days, 1) as providers_seen_90_days,
        
        -- Early Financial Activity
        coalesce(ep.payments_first_90_days, 0) as payments_first_90_days,
        coalesce(ep.patient_payments_90_days, 0.00) as patient_payments_90_days,
        coalesce(ep.insurance_payments_90_days, 0.00) as insurance_payments_90_days,
        coalesce(ep.avg_payment_amount_90_days, 0.00) as avg_payment_amount_90_days,
        
        -- Retention Indicators
        case when ev.appointments_first_90_days > 1 then true else false end as returned_within_90_days,
        case when ev.last_appointment_90_days >= np.first_visit_date + interval '60 days' then true else false end as active_after_60_days,
        
        -- Patient Value Metrics
        round(
            coalesce(ev.production_first_90_days, 0)::numeric / 
            nullif(coalesce(ev.appointments_first_90_days, 0), 0)::numeric, 2
        ) as avg_production_per_visit_90_days,
        case 
            when coalesce(ev.production_first_90_days, 0) = 0 then 'No Production'
            when coalesce(ev.production_first_90_days, 0) < 100 then 'Low Value'
            when coalesce(ev.production_first_90_days, 0) < 300 then 'Medium Value'
            when coalesce(ev.production_first_90_days, 0) < 600 then 'High Value'
            else 'Very High Value'
        end as patient_value_category_90_days,
        
        -- Referral Generation
        coalesce(pr.referrals_generated, 0) as referrals_generated_1_year,
        coalesce(pr.referred_patient_ids, array[]::text[]) as referred_patient_ids,
        case when coalesce(pr.referrals_generated, 0) > 0 then true else false end as generated_referrals,
        
        -- Onboarding Success Score (0-100)
        round((
            case when coalesce(fa.completed_first_appointments, 0) > 0 then 25 else 0 end +
            case when coalesce(ev.appointments_first_90_days, 0) > 1 then 25 else 0 end +
            case when coalesce(ep.patient_payments_90_days, 0) > 0 then 25 else 0 end +
            case when coalesce(pr.referrals_generated, 0) > 0 then 25 else 0 end
        )::numeric, 0) as onboarding_success_score,
        
        -- Time to Next Appointment
        case 
            when coalesce(ev.appointments_first_90_days, 0) > 1 then 
                (ev.last_appointment_90_days - np.first_visit_date)
        end as days_to_last_appointment_90_days,
        
        -- Current Status (based on recent activity)
        case 
            when ev.last_appointment_90_days >= current_date - interval '30 days' then 'Active'
            when ev.last_appointment_90_days >= current_date - interval '90 days' then 'Recent'
            when ev.last_appointment_90_days >= current_date - interval '180 days' then 'Dormant'
            else 'Lost'
        end as current_patient_status,
        
        -- Acquisition Performance
        case 
            when coalesce(fa.completed_first_appointments, 0) > 0 and coalesce(ev.appointments_first_90_days, 0) > 1 and coalesce(ep.patient_payments_90_days, 0) > 0 then 'Excellent'
            when coalesce(fa.completed_first_appointments, 0) > 0 and coalesce(ev.appointments_first_90_days, 0) > 1 then 'Good'
            when coalesce(fa.completed_first_appointments, 0) > 0 then 'Fair'
            else 'Poor'
        end as acquisition_success,
        
        -- Metadata (with fallback for legacy patients)
        np._loaded_at,
        coalesce(np._created_at, np.first_visit_date) as _created_at,
        np._updated_at,
        current_timestamp as _transformed_at,
        current_timestamp as _mart_refreshed_at
        
    from new_patient_base np
    inner join {{ ref('dim_date') }} dd
        on np.first_visit_date = dd.date_day
    inner join {{ ref('dim_provider') }} prov
        on np.primary_provider_id = prov.provider_id
    left join first_appointments fa
        on np.patient_id = fa.patient_id
    left join early_visits ev
        on np.patient_id = ev.patient_id
    left join early_payments ep
        on np.patient_id = ep.patient_id
    left join patient_referrals pr
        on np.patient_id = pr.patient_id
)

select * from final 