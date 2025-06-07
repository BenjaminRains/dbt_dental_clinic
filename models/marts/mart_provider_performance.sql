{{
    config(
        materialized='table',
        schema='marts',
        unique_key=['date_id', 'provider_id']
    )
}}

/*
Provider Performance Mart - Comprehensive provider analytics and performance tracking
This mart provides detailed provider performance metrics to support practice management,
provider development, and performance evaluation.

Key metrics:
- Production and collection performance
- Patient volume and case complexity
- Appointment efficiency and utilization
- Quality indicators and patient outcomes
- Comparative performance analysis
*/

with ProviderProduction as (
    select 
        ps.production_date,
        ps.provider_id,
        ps.clinic_id,
        
        -- Aggregate across all clinics for provider
        sum(ps.actual_production) as total_production,
        sum(ps.collections) as total_collections,
        sum(ps.scheduled_production) as total_scheduled_production,
        sum(ps.completed_appointments) as total_completed_appointments,
        sum(ps.total_appointments) as total_scheduled_appointments,
        sum(ps.missed_appointments) as total_missed_appointments,
        sum(ps.hygiene_appointments) as total_hygiene_appointments,
        sum(ps.productive_hours) as total_productive_hours,
        sum(ps.total_procedures) as total_procedures,
        sum(ps.unique_patients) as total_unique_patients,
        avg(ps.collection_rate) as avg_collection_rate,
        avg(ps.completion_rate) as avg_completion_rate,
        avg(ps.time_utilization_rate) as avg_utilization_rate
        
    from {{ ref('mart_production_summary') }} ps
    group by ps.production_date, ps.provider_id, ps.clinic_id
),

ProviderAppointments as (
    select 
        aps.appointment_date,
        aps.provider_id,
        
        -- Appointment efficiency metrics
        sum(aps.total_appointments) as daily_appointments,
        avg(aps.avg_arrival_delay) as avg_patient_delay,
        avg(aps.avg_wait_time) as avg_patient_wait_time,
        avg(aps.avg_treatment_time) as avg_treatment_time,
        avg(aps.no_show_rate) as daily_no_show_rate,
        avg(aps.cancellation_rate) as daily_cancellation_rate,
        avg(aps.confirmation_rate) as daily_confirmation_rate,
        sum(aps.new_patient_appointments) as new_patient_count,
        max(aps.schedule_span_hours) as working_hours,
        avg(aps.appointments_per_hour) as appointments_per_hour
        
    from {{ ref('mart_appt_summary') }} aps
    group by aps.appointment_date, aps.provider_id
),

ProviderClaims as (
    select 
        fc.claim_date,
        fc.provider_id,
        
        -- Claims and billing metrics
        count(*) as total_claims,
        sum(fc.billed_amount) as total_billed,
        sum(fc.allowed_amount) as total_allowed,
        sum(fc.paid_amount) as total_insurance_payments,
        sum(fc.write_off_amount) as total_write_offs,
        avg(fc.billed_amount) as avg_claim_amount,
        
        -- Procedure complexity
        count(distinct fc.procedure_code) as unique_procedure_codes,
        sum(case when dp.is_hygiene_flag then 1 else 0 end) as hygiene_procedures,
        sum(case when dp.is_radiology_flag then 1 else 0 end) as radiology_procedures,
        sum(case when dp.is_prosthetic_flag then 1 else 0 end) as prosthetic_procedures
        
    from {{ ref('fact_claim') }} fc
    left join {{ ref('dim_procedure') }} dp
        on fc.procedure_id = dp.procedure_id
    group by fc.claim_date, fc.provider_id
),

ProviderPatients as (
    select 
        fa.appointment_date,
        fa.provider_id,
        
        -- Patient relationship metrics
        count(distinct fa.patient_id) as daily_unique_patients,
        count(distinct case when fa.is_new_patient then fa.patient_id end) as new_patients,
        
        -- Patient age demographics
        avg(pt.age) as avg_patient_age,
        count(case when pt.age < 18 then 1 end) as pediatric_patients,
        count(case when pt.age >= 65 then 1 end) as senior_patients,
        
        -- Patient complexity indicators
        avg(array_length(pt.disease_ids, 1)) as avg_diseases_per_patient,
        count(case when pt.premedication_required then 1 end) as premedication_patients
        
    from {{ ref('fact_appointment') }} fa
    left join {{ ref('dim_patient') }} pt
        on fa.patient_id = pt.patient_id
    group by fa.appointment_date, fa.provider_id
),

MonthlyTrends as (
    select 
        date_trunc('month', pp.production_date) as month_date,
        pp.provider_id,
        
        -- Monthly aggregations
        sum(pp.total_production) as monthly_production,
        sum(pp.total_collections) as monthly_collections,
        sum(pp.total_completed_appointments) as monthly_appointments,
        sum(pp.total_procedures) as monthly_procedures,
        sum(pp.total_unique_patients) as monthly_patients,
        avg(pp.avg_collection_rate) as monthly_collection_rate,
        count(*) as working_days
        
    from ProviderProduction pp
    group by date_trunc('month', pp.production_date), pp.provider_id
),

Final as (
    select
        -- Keys and Dimensions
        dd.date_id,
        pp.production_date,
        pp.provider_id,
        
        -- Provider Information
        prov.provider_name,
        prov.provider_type,
        prov.specialty,
        prov.is_active,
        prov.hire_date,
        
        -- Date Information
        dd.day_name,
        dd.is_weekend,
        dd.is_holiday,
        dd.month,
        dd.quarter,
        dd.year,
        
        -- Production Metrics
        pp.total_production,
        pp.total_collections,
        pp.total_scheduled_production,
        round(pp.total_production / nullif(pp.total_productive_hours, 0), 2) as production_per_hour,
        round(pp.total_collections::numeric / nullif(pp.total_production, 0) * 100, 2) as collection_efficiency,
        
        -- Appointment Performance
        pp.total_completed_appointments,
        pp.total_scheduled_appointments,
        pp.total_missed_appointments,
        pa.daily_appointments,
        pa.new_patient_count,
        pa.working_hours,
        pa.appointments_per_hour,
        round(pp.total_completed_appointments::numeric / nullif(pp.total_scheduled_appointments, 0) * 100, 2) as appointment_efficiency,
        
        -- Time Management
        pp.total_productive_hours,
        pa.avg_patient_delay,
        pa.avg_patient_wait_time,
        pa.avg_treatment_time,
        pa.daily_no_show_rate,
        pa.daily_cancellation_rate,
        pa.daily_confirmation_rate,
        
        -- Procedure Metrics
        pp.total_procedures,
        round(pp.total_procedures::numeric / nullif(pp.total_completed_appointments, 0), 2) as procedures_per_appointment,
        pc.unique_procedure_codes as procedure_variety,
        pc.hygiene_procedures,
        pc.radiology_procedures,
        pc.prosthetic_procedures,
        
        -- Financial Performance
        pc.total_claims,
        pc.total_billed,
        pc.total_allowed,
        pc.total_insurance_payments,
        pc.total_write_offs,
        pc.avg_claim_amount,
        round(pc.total_allowed::numeric / nullif(pc.total_billed, 0) * 100, 2) as insurance_acceptance_rate,
        
        -- Patient Metrics
        pt.daily_unique_patients,
        pt.new_patients,
        pt.avg_patient_age,
        pt.pediatric_patients,
        pt.senior_patients,
        pt.avg_diseases_per_patient,
        pt.premedication_patients,
        round(pt.new_patients::numeric / nullif(pt.daily_unique_patients, 0) * 100, 2) as new_patient_rate,
        
        -- Monthly Context
        mt.monthly_production,
        mt.monthly_collections,
        mt.monthly_appointments,
        mt.working_days,
        round(pp.total_production::numeric / nullif(mt.monthly_production, 0) * 100, 2) as daily_production_vs_monthly,
        
        -- Performance Rankings (within specialty and date)
        rank() over (partition by dd.date_actual, prov.specialty order by pp.total_production desc) as production_rank_specialty,
        rank() over (partition by dd.date_actual order by pp.total_production desc) as production_rank_overall,
        
        -- Performance Categories
        case 
            when pp.avg_collection_rate >= 98 then 'Excellent'
            when pp.avg_collection_rate >= 95 then 'Good'
            when pp.avg_collection_rate >= 90 then 'Fair'
            else 'Poor'
        end as collection_performance,
        
        case 
            when pp.avg_completion_rate >= 95 then 'Excellent'
            when pp.avg_completion_rate >= 90 then 'Good'
            when pp.avg_completion_rate >= 85 then 'Fair'
            else 'Poor'
        end as scheduling_performance,
        
        case 
            when pa.daily_no_show_rate <= 5 then 'Excellent'
            when pa.daily_no_show_rate <= 10 then 'Good'
            when pa.daily_no_show_rate <= 15 then 'Fair'
            else 'Poor'
        end as reliability_performance,
        
        -- Productivity Indicators
        case 
            when pp.total_productive_hours >= 7 then 'Full Utilization'
            when pp.total_productive_hours >= 5 then 'Good Utilization'
            when pp.total_productive_hours >= 3 then 'Moderate Utilization'
            else 'Low Utilization'
        end as utilization_category,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from ProviderProduction pp
    inner join {{ ref('dim_date') }} dd
        on pp.production_date = dd.date_actual
    inner join {{ ref('dim_provider') }} prov
        on pp.provider_id = prov.provider_id
    left join ProviderAppointments pa
        on pp.production_date = pa.appointment_date
        and pp.provider_id = pa.provider_id
    left join ProviderClaims pc
        on pp.production_date = pc.claim_date
        and pp.provider_id = pc.provider_id
    left join ProviderPatients pt
        on pp.production_date = pt.appointment_date
        and pp.provider_id = pt.provider_id
    left join MonthlyTrends mt
        on date_trunc('month', pp.production_date) = mt.month_date
        and pp.provider_id = mt.provider_id
)

select * from Final
