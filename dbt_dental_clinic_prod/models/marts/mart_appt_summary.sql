{{ config(
        materialized='table',
        schema='marts',
        unique_key=['date_id', 'provider_id', 'clinic_id'],
        on_schema_change='fail',
        indexes=[
            {'columns': ['date_id']},
            {'columns': ['provider_id']},
            {'columns': ['clinic_id']}
        ]
    ) }}

/*
Summary Mart model for appointment analytics and scheduling efficiency
Part of System G: Scheduling and Appointment Management

This model:
1. Provides comprehensive appointment metrics aggregated by date, provider, and clinic
2. Supports scheduling optimization and patient flow analysis
3. Enables performance tracking and operational efficiency measurement

Business Logic Features:
- Appointment volume and status distribution analysis
- Scheduling efficiency and utilization calculations
- Confirmation and communication effectiveness metrics
- No-show and cancellation pattern analysis
- Wait times and patient experience tracking
- Financial performance and production efficiency

Key Metrics:
- Appointment completion rates and utilization percentages
- No-show and cancellation rates for operational planning
- Time utilization and scheduling efficiency metrics
- Production efficiency and financial performance indicators
- Patient experience metrics (wait times, delays)

Data Quality Notes:
- Handles null appointment dates by filtering them out
- Uses safe division with nullif() for percentage calculations
- Preserves data integrity with proper join conditions
- Time calculations from fact_appointment include data quality validation
- Negative time values are handled by converting to NULL in source fact table
- Uses COALESCE to handle NULL time values in aggregations

Performance Considerations:
- Aggregated at date/provider/clinic grain for optimal query performance
- Uses efficient joins with dimension tables
- Indexed on primary query dimensions (date_id, provider_id, clinic_id)

Dependencies:
- fact_appointment: Primary source for appointment data and metrics
- dim_date: Date dimension for temporal analysis
- dim_provider: Provider dimension for provider-specific metrics
*/

with appointment_base as (
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

appointment_dimensions as (
    select 
        provider_id,
        provider_first_name,
        provider_last_name,
        provider_preferred_name,
        provider_type_category as provider_type,
        specialty_description as specialty
    from {{ ref('dim_provider') }}
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

appointment_aggregated as (
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
        round(sum(total_scheduled_minutes) / 60.0, 2) as total_scheduled_hours,
        round(sum(productive_minutes) / 60.0, 2) as productive_hours,
        
        -- Appointment Counts
        sum(total_appointments) as total_appointments,
        sum(completed_appointments) as completed_appointments,
        sum(no_show_appointments) as no_show_appointments,
        sum(broken_appointments) as broken_appointments,
        sum(new_patient_appointments) as new_patient_appointments,
        sum(hygiene_appointments) as hygiene_appointments,
        sum(confirmed_appointments) as confirmed_appointments,
        sum(unique_patients) as unique_patients,
        sum(total_procedures) as total_procedures,
        
        -- Time Metrics (using COALESCE to handle NULL values from data quality fixes)
        avg(coalesce(avg_arrival_delay, 0)) as avg_arrival_delay,
        avg(coalesce(avg_wait_time, 0)) as avg_wait_time,
        avg(coalesce(avg_treatment_time, 0)) as avg_treatment_time,
        
        -- Financial Metrics
        sum(total_scheduled_production) as total_scheduled_production,
        sum(completed_production) as completed_production,
        
        -- Utilization Rates (calculated after aggregation)
        round(sum(productive_minutes)::numeric / nullif(sum(total_scheduled_minutes), 0) * 100, 2) as time_utilization_rate,
        round(sum(completed_appointments)::numeric / nullif(sum(total_appointments), 0) * 100, 2) as appointment_completion_rate,
        round(sum(confirmed_appointments)::numeric / nullif(sum(total_appointments), 0) * 100, 2) as confirmation_rate,
        
        -- Problem Rates (calculated after aggregation)
        round(sum(no_show_appointments)::numeric / nullif(sum(total_appointments), 0) * 100, 2) as no_show_rate,
        round(sum(broken_appointments)::numeric / nullif(sum(total_appointments), 0) * 100, 2) as cancellation_rate,
        round((sum(no_show_appointments) + sum(broken_appointments))::numeric / nullif(sum(total_appointments), 0) * 100, 2) as lost_appointment_rate
        
    from appointment_base
    group by appointment_date, provider_id, clinic_id
),

appointment_enhanced as (
    select 
        *,
        -- Performance categorization following standard patterns
        case 
            when appointment_completion_rate >= 95 then 'Excellent'
            when appointment_completion_rate >= 90 then 'Good'
            when appointment_completion_rate >= 85 then 'Fair'
            else 'Poor'
        end as completion_performance,
        
        case 
            when no_show_rate <= 5 then 'Excellent'
            when no_show_rate <= 10 then 'Good'
            when no_show_rate <= 15 then 'Fair'
            else 'Poor'
        end as reliability_performance,
        
        case 
            when time_utilization_rate >= 85 then 'Excellent'
            when time_utilization_rate >= 75 then 'Good'
            when time_utilization_rate >= 65 then 'Fair'
            else 'Poor'
        end as utilization_performance,
        
        -- Scheduling insights
        case 
            when schedule_span_hours <= 8 then 'Standard Day'
            when schedule_span_hours <= 10 then 'Extended Day'
            else 'Long Day'
        end as schedule_intensity,
        
        round(total_appointments::numeric / nullif(active_hours, 0), 2) as appointments_per_hour
    from appointment_aggregated
),

final as (
    select
        -- Date and Dimensions
        dd.date_id,
        ae.appointment_date,
        ae.provider_id,
        ae.clinic_id,
        
        -- Provider and Date Information
        prov.provider_first_name,
        prov.provider_last_name,
        prov.provider_preferred_name,
        prov.provider_type,
        prov.specialty,
        dd.day_name,
        dd.is_weekend,
        dd.is_holiday,
        dd.month,
        dd.quarter,
        dd.year,
        
        -- Schedule Structure
        ae.first_appointment_hour,
        ae.last_appointment_hour,
        ae.schedule_span_hours,
        ae.active_hours,
        ae.total_scheduled_hours,
        ae.productive_hours,
        
        -- Appointment Volume
        ae.total_appointments,
        ae.completed_appointments,
        ae.no_show_appointments,
        ae.broken_appointments,
        ae.new_patient_appointments,
        ae.hygiene_appointments,
        ae.unique_patients,
        ae.total_procedures,
        round(ae.total_procedures::numeric / nullif(ae.unique_patients, 0), 2) as procedures_per_patient,
        
        -- Efficiency Metrics
        ae.time_utilization_rate,
        ae.appointment_completion_rate,
        ae.confirmation_rate,
        ae.no_show_rate,
        ae.cancellation_rate,
        ae.lost_appointment_rate,
        
        -- Patient Experience Metrics
        ae.avg_arrival_delay,
        ae.avg_wait_time,
        ae.avg_treatment_time,
        
        -- Financial Performance
        ae.total_scheduled_production,
        ae.completed_production,
        round(ae.completed_production::numeric / nullif(ae.total_scheduled_production, 0) * 100, 2) as production_efficiency,
        round(ae.completed_production / nullif(ae.completed_appointments, 0), 2) as avg_production_per_completed_appointment,
        
        -- Performance Categories (moved to enhanced CTE)
        ae.completion_performance,
        ae.reliability_performance,
        ae.utilization_performance,
        
        -- Scheduling Insights (moved to enhanced CTE)
        ae.schedule_intensity,
        ae.appointments_per_hour,
        
        -- Standardized Metadata
        {{ standardize_mart_metadata() }}
        
    from appointment_enhanced ae
    left join date_dimension dd
        on ae.appointment_date = dd.date_actual
    left join appointment_dimensions prov
        on ae.provider_id = prov.provider_id
)

select * from Final