{{
    config(
        materialized='table',
        schema='marts',
        unique_key=['date_id', 'patient_id', 'provider_id', 'hygienist_id'],
        on_schema_change='fail',
        indexes=[
            {'columns': ['date_id']},
            {'columns': ['patient_id']},
            {'columns': ['provider_id']},
            {'columns': ['hygienist_id']},
            {'columns': ['hygiene_status']},
            {'columns': ['retention_category']}
        ]
    )
}}

/*
Hygiene Retention Mart - Preventive care scheduling and recall analysis
Part of System G: Scheduling and Patient Management

This model:
1. Tracks hygiene appointment patterns and patient retention metrics
2. Analyzes recall compliance and scheduling effectiveness
3. Provides hygienist performance and capacity utilization insights
4. Supports preventive care optimization and patient retention strategies

Business Logic Features:
- Hygiene appointment frequency analysis with interval calculations
- Recall compliance tracking with target date comparisons
- Patient retention categorization based on visit patterns
- Risk assessment for patient lapse and no-show patterns
- Hygienist performance metrics and capacity analysis

Key Metrics:
- Hygiene completion rates and appointment reliability
- Average hygiene intervals and consistency patterns
- Recall compliance rates and scheduling effectiveness
- Patient retention categories and risk assessments
- Production metrics and hygienist performance

Data Quality Notes:
- Handles missing recall data with appropriate null handling
- Accounts for appointment status variations (completed, no-show, broken)
- Manages edge cases in interval calculations for new patients
- Decodes OpenDental's encoded interval values to actual months:
  * 393217/393216 (Prophy): 6 months
  * 196609/196608 (Perio): 3 months
  * 16777217 (4BW, 2BW): 6 months
  * 83886081 (Pano, FMX): 12 months
  * Other encoded values mapped to appropriate month intervals
- Falls back to 6-month default for unknown or missing interval values

Performance Considerations:
- Uses window functions for efficient interval calculations
- Optimized joins with dimension tables for performance
- Indexed on key query dimensions (date, patient, provider)

Dependencies:
- fact_appointment: Core appointment data (hygiene appointments identified by hygienist_id)
- dim_date: Date dimension for temporal analysis
- dim_patient: Patient demographics and characteristics
- dim_provider: Provider and hygienist information
- stg_opendental__recall: Recall scheduling and compliance data
- stg_opendental__recalltype: Recall type definitions with proper interval values
*/

-- 1. Base fact data
with hygiene_base as (
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
    where fa.hygienist_id is not null 
        and fa.hygienist_id != 0
        and fa.appointment_date >= current_date - interval '3 years'
),

-- 2. Dimension data
hygiene_dimensions as (
    select 
        hb.*,
        case when hb.previous_hygiene_date is not null 
            then hb.appointment_date - hb.previous_hygiene_date 
        end as days_since_last_hygiene,
        case when hb.next_hygiene_date is not null 
            then hb.next_hygiene_date - hb.appointment_date 
        end as days_to_next_hygiene
    from hygiene_base hb
),

-- 3. Date dimension
date_dimension as (
    select * from {{ ref('dim_date') }}
),

-- 4. Aggregations and calculations
hygiene_aggregated as (
    select 
        hd.patient_id,
        hd.provider_id,
        hd.hygienist_id,
        
        -- Appointment counts and patterns
        count(*) as total_hygiene_appointments,
        sum(case when hd.is_completed then 1 else 0 end) as completed_hygiene_appointments,
        sum(case when hd.is_no_show then 1 else 0 end) as hygiene_no_shows,
        sum(case when hd.is_broken then 1 else 0 end) as hygiene_cancellations,
        
        -- Timing and frequency
        min(hd.appointment_date) as first_hygiene_date,
        max(hd.appointment_date) as last_hygiene_date,
        avg(hd.days_since_last_hygiene) as avg_hygiene_interval_days,
        stddev(hd.days_since_last_hygiene) as hygiene_interval_variability,
        
        -- Production and duration
        sum(hd.scheduled_production_amount) as total_hygiene_production,
        avg(hd.scheduled_production_amount) as avg_hygiene_production,
        avg(hd.appointment_length_minutes) as avg_hygiene_duration,
        
        -- Recent activity
        max(case when hd.appointment_date >= current_date - interval '6 months' 
            then hd.appointment_date end) as last_hygiene_6_months,
        count(case when hd.appointment_date >= current_date - interval '12 months' 
            then 1 end) as hygiene_visits_last_year,
        
        -- Consistency metrics
        count(case when hd.days_since_last_hygiene between 120 and 240 then 1 end) as regular_interval_visits,
        count(*) as total_intervals
        
    from hygiene_dimensions hd
    group by hd.patient_id, hd.provider_id, hd.hygienist_id
),

-- 5. Business logic enhancement
hygiene_enhanced as (
    select 
        ha.*,
        -- Calculate derived metrics first
        round(ha.completed_hygiene_appointments::numeric / nullif(ha.total_hygiene_appointments, 0) * 100, 2) as hygiene_completion_rate,
        round(ha.hygiene_no_shows::numeric / nullif(ha.total_hygiene_appointments, 0) * 100, 2) as hygiene_no_show_rate,
        round(ha.hygiene_cancellations::numeric / nullif(ha.total_hygiene_appointments, 0) * 100, 2) as hygiene_cancellation_rate,
        round(ha.regular_interval_visits::numeric / nullif(ha.total_intervals, 0) * 100, 2) as regular_interval_percentage,
        round(ha.avg_hygiene_interval_days / 30.0, 1) as avg_hygiene_interval_months,
        round(ha.avg_hygiene_production / nullif(ha.avg_hygiene_duration, 0) * 60, 2) as production_per_hour,
        
        -- Days since last hygiene
        case when ha.last_hygiene_date is not null 
            then current_date - ha.last_hygiene_date 
        end as days_since_last_hygiene
        
    from hygiene_aggregated ha
),

-- 6. Business categorization (separate CTE to use calculated fields)
hygiene_categorized as (
    select 
        he.*,
        -- Business categorization using calculated fields
        case 
            when he.last_hygiene_date >= current_date - interval '6 months' then 'Current'
            when he.last_hygiene_date >= current_date - interval '9 months' then 'Due'
            when he.last_hygiene_date >= current_date - interval '12 months' then 'Overdue'
            else 'Lapsed'
        end as hygiene_status,
        
        case 
            when he.hygiene_visits_last_year >= 2 and he.regular_interval_percentage >= 80 then 'Excellent'
            when he.hygiene_visits_last_year >= 2 and he.regular_interval_percentage >= 60 then 'Good'
            when he.hygiene_visits_last_year >= 1 then 'Fair'
            else 'Poor'
        end as retention_category,
        
        case 
            when he.last_hygiene_date < current_date - interval '12 months' then 'High Risk'
            when he.hygiene_no_show_rate > 20 then 'Medium Risk'
            when he.avg_hygiene_interval_days > 240 then 'Medium Risk'
            else 'Low Risk'
        end as patient_risk_category,
        
        case 
            when he.avg_hygiene_interval_days <= 120 then '3-4 Months'
            when he.avg_hygiene_interval_days <= 180 then '4-6 Months'
            when he.avg_hygiene_interval_days <= 270 then '6-9 Months'
            else 'Infrequent'
        end as hygiene_frequency_category,
        
        -- Boolean flags
        case when he.last_hygiene_6_months is not null then true else false end as recent_hygiene_visit,
        case when he.hygiene_visits_last_year >= 2 then true else false end as regular_hygiene_patient,
        case when he.regular_interval_percentage >= 70 then true else false end as consistent_patient,
        case when he.hygiene_completion_rate >= 90 then true else false end as reliable_patient
        
    from hygiene_enhanced he
),

-- 7. Recall compliance data
recall_scheduling as (
    select 
        recall_stg.patient_id,
        recall_stg.date_due as recall_date,
        recall_stg.recall_type_id,
        -- Decode encoded interval values to actual months
        case 
            when coalesce(rt.default_interval, 0) in (393217, 393216) then 6  -- Prophy: 6 months
            when coalesce(rt.default_interval, 0) in (196609, 196608) then 3  -- Perio: 3 months
            when coalesce(rt.default_interval, 0) in (16777217) then 6        -- 4BW, 2BW: 6 months
            when coalesce(rt.default_interval, 0) in (83886081) then 12       -- Pano, FMX: 12 months
            when coalesce(rt.default_interval, 0) in (262144) then 4          -- 4 months
            when coalesce(rt.default_interval, 0) in (327680) then 5          -- 5 months
            when coalesce(rt.default_interval, 0) in (786432) then 8          -- 8 months
            when coalesce(rt.default_interval, 0) in (2359296) then 18        -- 18 months
            when coalesce(rt.default_interval, 0) = 0 then 6                  -- Default for missing values
            else 6                                                             -- Fallback to 6 months
        end as interval_months,
        recall_stg.date_scheduled,
        recall_stg.is_disabled,
        -- Find closest hygiene appointment to recall date
        hd.appointment_date as closest_hygiene_date,
        hd.is_completed as recall_completed,
        abs(hd.appointment_date::date - recall_stg.date_due::date) as days_from_recall_target,
        case when hd.appointment_date::date between recall_stg.date_due::date - interval '30 days' 
                and recall_stg.date_due::date + interval '60 days' then true else false end as recall_compliance
    from {{ ref('stg_opendental__recall') }} recall_stg
    left join {{ ref('stg_opendental__recalltype') }} rt
        on recall_stg.recall_type_id = rt.recall_type_id
    left join hygiene_dimensions hd
        on recall_stg.patient_id = hd.patient_id
        and hd.appointment_date = (
            select hd2.appointment_date
            from hygiene_dimensions hd2
            where hd2.patient_id = recall_stg.patient_id
                and hd2.appointment_date::date >= recall_stg.date_due::date
            order by hd2.appointment_date
            limit 1
        )
    where recall_stg.is_disabled = false
        and recall_stg.date_due::date >= current_date - interval '2 years'
),

-- 8. Final integration
final as (
    select
        -- Date and dimensions
        dd.date_id,
        hc.patient_id,
        hc.provider_id,
        hc.hygienist_id,
        
        -- Patient Information
        pt.age,
        pt.gender,
        pt.patient_status,
        pt.has_insurance_flag,
        pt.first_visit_date,
        
        -- Provider Information
        concat(prov.provider_first_name, ' ', prov.provider_last_name) as provider_name,
        prov.provider_type_category,
        prov.specialty_description,
        concat(hyg.provider_first_name, ' ', hyg.provider_last_name) as hygienist_name,
        
        -- Date Information
        dd.year,
        dd.month,
        dd.quarter,
        dd.day_name,
        
        -- Hygiene History Summary
        hc.total_hygiene_appointments,
        hc.completed_hygiene_appointments,
        hc.hygiene_no_shows,
        hc.hygiene_cancellations,
        hc.first_hygiene_date,
        hc.last_hygiene_date,
        
        -- Timing and Frequency Metrics
        hc.avg_hygiene_interval_days,
        hc.avg_hygiene_interval_months,
        hc.hygiene_interval_variability,
        hc.hygiene_visits_last_year,
        
        -- Completion and Reliability
        hc.hygiene_completion_rate,
        hc.hygiene_no_show_rate,
        hc.hygiene_cancellation_rate,
        
        -- Consistency and Regularity
        hc.regular_interval_percentage,
        
        -- Financial Metrics
        hc.total_hygiene_production,
        hc.avg_hygiene_production,
        hc.avg_hygiene_duration,
        hc.production_per_hour,
        
        -- Recall Compliance (from most recent recall)
        rs.recall_compliance as last_recall_compliance,
        rs.days_from_recall_target as days_from_last_recall_target,
        rs.interval_months as recommended_recall_interval,
        
        -- Business Categorization (from categorized CTE)
        hc.hygiene_status,
        hc.retention_category,
        hc.patient_risk_category,
        hc.hygiene_frequency_category,
        
        -- Boolean Flags
        hc.recent_hygiene_visit,
        hc.regular_hygiene_patient,
        hc.consistent_patient,
        hc.reliable_patient,
        case when rs.recall_compliance then true else false end as recall_compliant,
        
        -- Days since last hygiene
        hc.days_since_last_hygiene,
        
        -- Patient lifetime value (hygiene)
        round(hc.total_hygiene_production / nullif(
            (current_date - pt.first_visit_date) / 365.0, 0), 2) as annual_hygiene_value,
        
        -- Metadata
        {{ standardize_mart_metadata(
            primary_source_alias='pt',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at']
        ) }}
        
    from hygiene_categorized hc
    inner join date_dimension dd
        on current_date = dd.date_day
    inner join {{ ref('dim_patient') }} pt
        on hc.patient_id = pt.patient_id
    inner join {{ ref('dim_provider') }} prov
        on hc.provider_id = prov.provider_id
    left join {{ ref('dim_provider') }} hyg
        on hc.hygienist_id = hyg.provider_id
    left join recall_scheduling rs
        on hc.patient_id = rs.patient_id
        and rs.recall_date = (
            select max(rs2.recall_date)
            from recall_scheduling rs2
            where rs2.patient_id = hc.patient_id
        )
    where hc.total_hygiene_appointments > 0
)

select * from final