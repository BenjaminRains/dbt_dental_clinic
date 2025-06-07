{{
    config(
        materialized='table',
        schema='marts',
        unique_key=['date_id', 'patient_id', 'provider_id']
    )
}}

/*
Hygiene Retention Mart - Preventive care scheduling and recall analysis
This mart tracks hygiene appointment patterns, recall compliance, and preventive
care effectiveness to optimize patient retention and oral health outcomes.

Key metrics:
- Hygiene appointment frequency and timing
- Recall compliance and scheduling effectiveness
- Preventive care completion rates
- Patient retention through hygiene programs
- Hygienist performance and capacity utilization
*/

with HygieneAppointments as (
    select 
        fa.patient_id,
        fa.provider_id,
        fa.hygienist_id,
        fa.appointment_date,
        fa.appointment_datetime,
        fa.is_completed,
        fa.is_no_show,
        fa.is_broken,
        fa.scheduled_production_amount,
        fa.procedure_codes,
        fa.appointment_length_minutes,
        -- Calculate gaps between appointments
        lag(fa.appointment_date) over (partition by fa.patient_id order by fa.appointment_date) as previous_hygiene_date,
        lead(fa.appointment_date) over (partition by fa.patient_id order by fa.appointment_date) as next_hygiene_date,
        row_number() over (partition by fa.patient_id order by fa.appointment_date) as hygiene_visit_number
    from {{ ref('fact_appointment') }} fa
    where fa.is_hygiene_appointment = true
        and fa.appointment_date >= current_date - interval '3 years'
),

HygieneGaps as (
    select 
        ha.*,
        case when ha.previous_hygiene_date is not null 
            then ha.appointment_date - ha.previous_hygiene_date 
        end as days_since_last_hygiene,
        case when ha.next_hygiene_date is not null 
            then ha.next_hygiene_date - ha.appointment_date 
        end as days_to_next_hygiene
    from HygieneAppointments ha
),

RecallScheduling as (
    select 
        rs.patient_id,
        rs.recall_date,
        rs.recall_type,
        rs.interval_months,
        rs.date_scheduled,
        rs.is_disabled,
        -- Find closest hygiene appointment to recall date
        ha.appointment_date as closest_hygiene_date,
        ha.is_completed as recall_completed,
        abs(extract(days from ha.appointment_date - rs.recall_date)) as days_from_recall_target,
        case when ha.appointment_date between rs.recall_date - interval '30 days' 
                and rs.recall_date + interval '60 days' then true else false end as recall_compliance
    from {{ ref('stg_opendental__recall') }} rs
    left join HygieneGaps ha
        on rs.patient_id = ha.patient_id
        and ha.appointment_date = (
            select ha2.appointment_date
            from HygieneGaps ha2
            where ha2.patient_id = rs.patient_id
                and ha2.appointment_date >= rs.recall_date
            order by ha2.appointment_date
            limit 1
        )
    where rs.is_disabled = false
        and rs.recall_date >= current_date - interval '2 years'
),

PatientHygieneHistory as (
    select 
        hg.patient_id,
        hg.provider_id,
        hg.hygienist_id,
        
        -- Appointment counts and patterns
        count(*) as total_hygiene_appointments,
        sum(case when hg.is_completed then 1 else 0 end) as completed_hygiene_appointments,
        sum(case when hg.is_no_show then 1 else 0 end) as hygiene_no_shows,
        sum(case when hg.is_broken then 1 else 0 end) as hygiene_cancellations,
        
        -- Timing and frequency
        min(hg.appointment_date) as first_hygiene_date,
        max(hg.appointment_date) as last_hygiene_date,
        avg(hg.days_since_last_hygiene) as avg_hygiene_interval_days,
        stddev(hg.days_since_last_hygiene) as hygiene_interval_variability,
        
        -- Production and duration
        sum(hg.scheduled_production_amount) as total_hygiene_production,
        avg(hg.scheduled_production_amount) as avg_hygiene_production,
        avg(hg.appointment_length_minutes) as avg_hygiene_duration,
        
        -- Recent activity
        max(case when hg.appointment_date >= current_date - interval '6 months' 
            then hg.appointment_date end) as last_hygiene_6_months,
        count(case when hg.appointment_date >= current_date - interval '12 months' 
            then 1 end) as hygiene_visits_last_year,
        
        -- Consistency metrics
        count(case when hg.days_since_last_hygiene between 120 and 240 then 1 end) as regular_interval_visits,
        count(*) as total_intervals
        
    from HygieneGaps hg
    group by hg.patient_id, hg.provider_id, hg.hygienist_id
),

Final as (
    select
        -- Keys and Dimensions
        dd.date_id,
        phh.patient_id,
        phh.provider_id,
        phh.hygienist_id,
        
        -- Patient Information
        pt.age,
        pt.gender,
        pt.patient_status,
        pt.has_insurance_flag,
        pt.first_visit_date,
        
        -- Provider Information
        prov.provider_name,
        prov.provider_type,
        prov.specialty,
        hyg.provider_name as hygienist_name,
        
        -- Date Information
        dd.year,
        dd.month,
        dd.quarter,
        dd.day_name,
        
        -- Hygiene History Summary
        phh.total_hygiene_appointments,
        phh.completed_hygiene_appointments,
        phh.hygiene_no_shows,
        phh.hygiene_cancellations,
        phh.first_hygiene_date,
        phh.last_hygiene_date,
        
        -- Timing and Frequency Metrics
        phh.avg_hygiene_interval_days,
        round(phh.avg_hygiene_interval_days / 30.0, 1) as avg_hygiene_interval_months,
        phh.hygiene_interval_variability,
        phh.hygiene_visits_last_year,
        
        -- Completion and Reliability
        round(phh.completed_hygiene_appointments::numeric / nullif(phh.total_hygiene_appointments, 0) * 100, 2) as hygiene_completion_rate,
        round(phh.hygiene_no_shows::numeric / nullif(phh.total_hygiene_appointments, 0) * 100, 2) as hygiene_no_show_rate,
        round(phh.hygiene_cancellations::numeric / nullif(phh.total_hygiene_appointments, 0) * 100, 2) as hygiene_cancellation_rate,
        
        -- Consistency and Regularity
        round(phh.regular_interval_visits::numeric / nullif(phh.total_intervals, 0) * 100, 2) as regular_interval_percentage,
        
        -- Financial Metrics
        phh.total_hygiene_production,
        phh.avg_hygiene_production,
        phh.avg_hygiene_duration,
        round(phh.avg_hygiene_production / nullif(phh.avg_hygiene_duration, 0) * 60, 2) as production_per_hour,
        
        -- Recall Compliance (from most recent recall)
        rs.recall_compliance as last_recall_compliance,
        rs.days_from_recall_target as days_from_last_recall_target,
        rs.interval_months as recommended_recall_interval,
        
        -- Current Status Assessment
        case 
            when phh.last_hygiene_date >= current_date - interval '6 months' then 'Current'
            when phh.last_hygiene_date >= current_date - interval '9 months' then 'Due'
            when phh.last_hygiene_date >= current_date - interval '12 months' then 'Overdue'
            else 'Lapsed'
        end as hygiene_status,
        
        -- Patient Retention Categories
        case 
            when phh.hygiene_visits_last_year >= 2 and phh.regular_interval_percentage >= 80 then 'Excellent'
            when phh.hygiene_visits_last_year >= 2 and phh.regular_interval_percentage >= 60 then 'Good'
            when phh.hygiene_visits_last_year >= 1 then 'Fair'
            else 'Poor'
        end as retention_category,
        
        -- Risk Assessment
        case 
            when phh.last_hygiene_date < current_date - interval '12 months' then 'High Risk'
            when phh.hygiene_no_show_rate > 20 then 'Medium Risk'
            when phh.avg_hygiene_interval_days > 240 then 'Medium Risk'
            else 'Low Risk'
        end as patient_risk_category,
        
        -- Frequency Categories
        case 
            when phh.avg_hygiene_interval_days <= 120 then '3-4 Months'
            when phh.avg_hygiene_interval_days <= 180 then '4-6 Months'
            when phh.avg_hygiene_interval_days <= 270 then '6-9 Months'
            else 'Infrequent'
        end as hygiene_frequency_category,
        
        -- Boolean Flags
        case when phh.last_hygiene_6_months is not null then true else false end as recent_hygiene_visit,
        case when phh.hygiene_visits_last_year >= 2 then true else false end as regular_hygiene_patient,
        case when phh.regular_interval_percentage >= 70 then true else false end as consistent_patient,
        case when phh.hygiene_completion_rate >= 90 then true else false end as reliable_patient,
        case when rs.recall_compliance then true else false end as recall_compliant,
        
        -- Days since last hygiene
        case when phh.last_hygiene_date is not null 
            then current_date - phh.last_hygiene_date 
        end as days_since_last_hygiene,
        
        -- Patient lifetime value (hygiene)
        round(phh.total_hygiene_production / nullif(
            extract(days from current_date - pt.first_visit_date) / 365.0, 0), 2) as annual_hygiene_value,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from PatientHygieneHistory phh
    inner join {{ ref('dim_date') }} dd
        on current_date = dd.date_actual
    inner join {{ ref('dim_patient') }} pt
        on phh.patient_id = pt.patient_id
    inner join {{ ref('dim_provider') }} prov
        on phh.provider_id = prov.provider_id
    left join {{ ref('dim_provider') }} hyg
        on phh.hygienist_id = hyg.provider_id
    left join RecallScheduling rs
        on phh.patient_id = rs.patient_id
        and rs.recall_date = (
            select max(rs2.recall_date)
            from RecallScheduling rs2
            where rs2.patient_id = phh.patient_id
        )
    where phh.total_hygiene_appointments > 0
)

select * from Final
