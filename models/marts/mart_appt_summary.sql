{{
    config(
        materialized='table',
        schema='marts',
        unique_key=['date_id', 'provider_id', 'clinic_id']
    )
}}

/*
Appointment Summary Mart - Comprehensive appointment analytics and scheduling efficiency
This mart provides detailed appointment metrics aggregated by date, provider, and clinic
to support scheduling optimization and patient flow analysis.

Key metrics:
- Appointment volume and status distribution
- Scheduling efficiency and utilization
- Confirmation and communication effectiveness
- No-show and cancellation analysis
- Wait times and patient experience
*/

with AppointmentMetrics as (
    select 
        fa.appointment_date,
        fa.provider_id,
        fa.clinic_id,
        fa.appointment_type,
        fa.day_of_week_name,
        fa.appointment_hour,
        
        -- Appointment Counts
        count(*) as total_appointments,
        sum(case when fa.is_completed then 1 else 0 end) as completed_appointments,
        sum(case when fa.is_broken then 1 else 0 end) as broken_appointments,
        sum(case when fa.is_no_show then 1 else 0 end) as no_show_appointments,
        sum(case when fa.is_asap then 1 else 0 end) as asap_appointments,
        sum(case when fa.is_future_appointment then 1 else 0 end) as future_appointments,
        sum(case when fa.is_new_patient then 1 else 0 end) as new_patient_appointments,
        sum(case when fa.is_hygiene_appointment then 1 else 0 end) as hygiene_appointments,
        
        -- Confirmation Metrics
        sum(case when fa.has_confirmations then 1 else 0 end) as confirmed_appointments,
        avg(fa.confirmation_count) as avg_confirmations_per_appointment,
        
        -- Time Utilization
        sum(fa.appointment_length_minutes) as total_scheduled_minutes,
        avg(fa.appointment_length_minutes) as avg_appointment_length,
        sum(case when fa.is_completed then fa.appointment_length_minutes else 0 end) as productive_minutes,
        
        -- Wait Time Analysis
        avg(fa.arrival_delay_minutes) as avg_arrival_delay,
        avg(fa.wait_time_minutes) as avg_wait_time,
        avg(fa.treatment_time_minutes) as avg_treatment_time,
        
        -- Financial Metrics
        sum(fa.scheduled_production_amount) as total_scheduled_production,
        avg(fa.scheduled_production_amount) as avg_production_per_appointment,
        sum(case when fa.is_completed then fa.scheduled_production_amount else 0 end) as completed_production,
        
        -- Patient and Procedure Counts
        count(distinct fa.patient_id) as unique_patients,
        sum(fa.procedure_count) as total_procedures,
        avg(fa.procedure_count) as avg_procedures_per_appointment
        
    from {{ ref('fact_appointment') }} fa
    where fa.appointment_date is not null
    group by fa.appointment_date, fa.provider_id, fa.clinic_id, fa.appointment_type, fa.day_of_week_name, fa.appointment_hour
),

SchedulingEfficiency as (
    select 
        appointment_date,
        provider_id,
        clinic_id,
        
        -- Scheduling Patterns
        min(appointment_hour) as first_appointment_hour,
        max(appointment_hour) as last_appointment_hour,
        max(appointment_hour) - min(appointment_hour) as schedule_span_hours,
        
        -- Time Slot Analysis
        count(distinct appointment_hour) as active_hours,
        round(total_scheduled_minutes / 60.0, 2) as total_scheduled_hours,
        round(productive_minutes / 60.0, 2) as productive_hours,
        
        -- Utilization Rates
        round(productive_minutes::numeric / nullif(total_scheduled_minutes, 0) * 100, 2) as time_utilization_rate,
        round(completed_appointments::numeric / nullif(total_appointments, 0) * 100, 2) as appointment_completion_rate,
        round(confirmed_appointments::numeric / nullif(total_appointments, 0) * 100, 2) as confirmation_rate,
        
        -- Problem Rates
        round(no_show_appointments::numeric / nullif(total_appointments, 0) * 100, 2) as no_show_rate,
        round(broken_appointments::numeric / nullif(total_appointments, 0) * 100, 2) as cancellation_rate,
        round((no_show_appointments + broken_appointments)::numeric / nullif(total_appointments, 0) * 100, 2) as lost_appointment_rate,
        
        sum(total_appointments) as total_appointments,
        sum(completed_appointments) as completed_appointments,
        sum(no_show_appointments) as no_show_appointments,
        sum(broken_appointments) as broken_appointments,
        sum(new_patient_appointments) as new_patient_appointments,
        sum(hygiene_appointments) as hygiene_appointments,
        sum(unique_patients) as unique_patients,
        sum(total_procedures) as total_procedures,
        avg(avg_arrival_delay) as avg_arrival_delay,
        avg(avg_wait_time) as avg_wait_time,
        avg(avg_treatment_time) as avg_treatment_time,
        sum(total_scheduled_production) as total_scheduled_production,
        sum(completed_production) as completed_production
        
    from AppointmentMetrics
    group by appointment_date, provider_id, clinic_id
),

Final as (
    select
        -- Keys and Dimensions
        dd.date_id,
        se.appointment_date,
        se.provider_id,
        se.clinic_id,
        
        -- Provider and Date Information
        prov.provider_name,
        prov.provider_type,
        prov.specialty,
        dd.day_name,
        dd.is_weekend,
        dd.is_holiday,
        dd.month,
        dd.quarter,
        dd.year,
        
        -- Schedule Structure
        se.first_appointment_hour,
        se.last_appointment_hour,
        se.schedule_span_hours,
        se.active_hours,
        se.total_scheduled_hours,
        se.productive_hours,
        
        -- Appointment Volume
        se.total_appointments,
        se.completed_appointments,
        se.no_show_appointments,
        se.broken_appointments,
        se.new_patient_appointments,
        se.hygiene_appointments,
        se.unique_patients,
        se.total_procedures,
        round(se.total_procedures::numeric / nullif(se.unique_patients, 0), 2) as procedures_per_patient,
        
        -- Efficiency Metrics
        se.time_utilization_rate,
        se.appointment_completion_rate,
        se.confirmation_rate,
        se.no_show_rate,
        se.cancellation_rate,
        se.lost_appointment_rate,
        
        -- Patient Experience Metrics
        se.avg_arrival_delay,
        se.avg_wait_time,
        se.avg_treatment_time,
        
        -- Financial Performance
        se.total_scheduled_production,
        se.completed_production,
        round(se.completed_production::numeric / nullif(se.total_scheduled_production, 0) * 100, 2) as production_efficiency,
        round(se.completed_production / nullif(se.completed_appointments, 0), 2) as avg_production_per_completed_appointment,
        
        -- Performance Categories
        case 
            when se.appointment_completion_rate >= 95 then 'Excellent'
            when se.appointment_completion_rate >= 90 then 'Good'
            when se.appointment_completion_rate >= 85 then 'Fair'
            else 'Poor'
        end as completion_performance,
        
        case 
            when se.no_show_rate <= 5 then 'Excellent'
            when se.no_show_rate <= 10 then 'Good'
            when se.no_show_rate <= 15 then 'Fair'
            else 'Poor'
        end as reliability_performance,
        
        case 
            when se.time_utilization_rate >= 85 then 'Excellent'
            when se.time_utilization_rate >= 75 then 'Good'
            when se.time_utilization_rate >= 65 then 'Fair'
            else 'Poor'
        end as utilization_performance,
        
        -- Scheduling Insights
        case 
            when se.schedule_span_hours <= 8 then 'Standard Day'
            when se.schedule_span_hours <= 10 then 'Extended Day'
            else 'Long Day'
        end as schedule_intensity,
        
        round(se.total_appointments::numeric / nullif(se.active_hours, 0), 2) as appointments_per_hour,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from SchedulingEfficiency se
    inner join {{ ref('dim_date') }} dd
        on se.appointment_date = dd.date_actual
    inner join {{ ref('dim_provider') }} prov
        on se.provider_id = prov.provider_id
)

select * from Final
