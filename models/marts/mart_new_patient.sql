{{
    config(
        materialized='table',
        schema='marts',
        unique_key=['date_id', 'patient_id']
    )
}}

/*
New Patient Mart - New patient acquisition and onboarding analytics
This mart tracks new patient acquisition, sources, onboarding success,
and early patient journey metrics to optimize marketing and patient experience.

Key metrics:
- New patient acquisition patterns
- Source and referral tracking
- Onboarding appointment completion
- Early patient lifetime value
- Conversion and retention rates
*/

with NewPatients as (
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
        dp.preferred_contact_method,
        dp.preferred_confirmation_method
    from {{ ref('dim_patient') }} dp
    where dp.first_visit_date is not null
        and dp.patient_status = 'Patient'
),

FirstAppointments as (
    select 
        fa.patient_id,
        min(fa.appointment_date) as first_appointment_date,
        min(fa.appointment_datetime) as first_appointment_datetime,
        count(*) as total_first_day_appointments,
        sum(case when fa.is_completed then 1 else 0 end) as completed_first_appointments,
        sum(case when fa.is_no_show then 1 else 0 end) as no_show_first_appointments,
        sum(fa.scheduled_production_amount) as first_visit_scheduled_production,
        array_agg(fa.procedure_codes) as first_visit_procedures
    from {{ ref('fact_appointment') }} fa
    inner join NewPatients np
        on fa.patient_id = np.patient_id
        and fa.appointment_date = np.first_visit_date
    group by fa.patient_id
),

EarlyVisits as (
    select 
        fa.patient_id,
        count(*) as appointments_first_90_days,
        sum(case when fa.is_completed then 1 else 0 end) as completed_appointments_90_days,
        sum(fa.scheduled_production_amount) as production_first_90_days,
        max(fa.appointment_date) as last_appointment_90_days,
        count(distinct fa.provider_id) as providers_seen_90_days
    from {{ ref('fact_appointment') }} fa
    inner join NewPatients np
        on fa.patient_id = np.patient_id
    where fa.appointment_date between np.first_visit_date 
        and np.first_visit_date + interval '90 days'
    group by fa.patient_id
),

EarlyPayments as (
    select 
        fp.patient_id,
        count(*) as payments_first_90_days,
        sum(case when fp.is_patient_payment then fp.payment_amount else 0 end) as patient_payments_90_days,
        sum(case when fp.is_insurance_payment then fp.payment_amount else 0 end) as insurance_payments_90_days,
        avg(fp.payment_amount) as avg_payment_amount_90_days
    from {{ ref('fact_payment') }} fp
    inner join NewPatients np
        on fp.patient_id = np.patient_id
    where fp.payment_date between np.first_visit_date 
        and np.first_visit_date + interval '90 days'
    group by fp.patient_id
),

PatientReferrals as (
    select 
        np.patient_id,
        count(*) as referrals_generated,
        array_agg(distinct ref_patient.patient_id::text) as referred_patient_ids
    from NewPatients np
    inner join {{ ref('dim_patient') }} ref_patient
        on np.patient_id = ref_patient.guarantor_id  -- Assuming family referrals
        and ref_patient.patient_id != np.patient_id
        and ref_patient.first_visit_date > np.first_visit_date
        and ref_patient.first_visit_date <= np.first_visit_date + interval '365 days'
    group by np.patient_id
),

Final as (
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
        prov.provider_name as primary_provider_name,
        prov.provider_type as primary_provider_type,
        prov.specialty as primary_provider_specialty,
        
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
        fa.first_appointment_datetime,
        fa.total_first_day_appointments,
        fa.completed_first_appointments,
        fa.no_show_first_appointments,
        fa.first_visit_scheduled_production,
        fa.first_visit_procedures,
        
        -- First Visit Success Indicators
        case when fa.completed_first_appointments > 0 then true else false end as completed_first_visit,
        case when fa.no_show_first_appointments > 0 then true else false end as no_show_first_visit,
        round(fa.completed_first_appointments::numeric / nullif(fa.total_first_day_appointments, 0) * 100, 2) as first_visit_completion_rate,
        
        -- Early Engagement (90 days)
        ev.appointments_first_90_days,
        ev.completed_appointments_90_days,
        ev.production_first_90_days,
        ev.last_appointment_90_days,
        ev.providers_seen_90_days,
        
        -- Early Financial Activity
        ep.payments_first_90_days,
        ep.patient_payments_90_days,
        ep.insurance_payments_90_days,
        ep.avg_payment_amount_90_days,
        
        -- Retention Indicators
        case when ev.appointments_first_90_days > 1 then true else false end as returned_within_90_days,
        case when ev.last_appointment_90_days >= np.first_visit_date + interval '60 days' then true else false end as active_after_60_days,
        
        -- Patient Value Metrics
        round(ev.production_first_90_days / nullif(ev.appointments_first_90_days, 0), 2) as avg_production_per_visit_90_days,
        case 
            when ev.production_first_90_days = 0 then 'No Production'
            when ev.production_first_90_days < 200 then 'Low Value'
            when ev.production_first_90_days < 500 then 'Medium Value'
            when ev.production_first_90_days < 1000 then 'High Value'
            else 'Very High Value'
        end as patient_value_category_90_days,
        
        -- Referral Generation
        coalesce(pr.referrals_generated, 0) as referrals_generated_1_year,
        pr.referred_patient_ids,
        case when pr.referrals_generated > 0 then true else false end as generated_referrals,
        
        -- Onboarding Success Score (0-100)
        round((
            case when fa.completed_first_appointments > 0 then 25 else 0 end +
            case when ev.returned_within_90_days then 25 else 0 end +
            case when ep.patient_payments_90_days > 0 then 25 else 0 end +
            case when pr.referrals_generated > 0 then 25 else 0 end
        ), 0) as onboarding_success_score,
        
        -- Time to Next Appointment
        case 
            when ev.appointments_first_90_days > 1 then 
                extract(days from ev.last_appointment_90_days - np.first_visit_date)
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
            when fa.completed_first_visit and ev.returned_within_90_days and ep.patient_payments_90_days > 0 then 'Excellent'
            when fa.completed_first_visit and ev.returned_within_90_days then 'Good'
            when fa.completed_first_visit then 'Fair'
            else 'Poor'
        end as acquisition_success,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from NewPatients np
    inner join {{ ref('dim_date') }} dd
        on np.first_visit_date = dd.date_actual
    inner join {{ ref('dim_provider') }} prov
        on np.primary_provider_id = prov.provider_id
    left join FirstAppointments fa
        on np.patient_id = fa.patient_id
    left join EarlyVisits ev
        on np.patient_id = ev.patient_id
    left join EarlyPayments ep
        on np.patient_id = ep.patient_id
    left join PatientReferrals pr
        on np.patient_id = pr.patient_id
)

select * from Final 