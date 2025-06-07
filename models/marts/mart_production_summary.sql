{{
    config(
        materialized='table',
        schema='marts',
        unique_key=['date_id', 'provider_id', 'clinic_id']
    )
}}

/*
Production Summary Mart - Daily production metrics and financial performance
This mart provides comprehensive daily production analytics aggregated by provider
and clinic to support financial performance monitoring and reporting.

Key metrics:
- Daily production totals and targets
- Procedure counts and average fees
- Collection rates and payment timing
- Provider performance comparison
- Clinic efficiency metrics
*/

with ProductionBase as (
    select 
        fa.appointment_date,
        fa.provider_id,
        fa.clinic_id,
        fa.patient_id,
        fa.procedure_count,
        fa.scheduled_production_amount,
        fa.is_completed,
        fa.is_hygiene_appointment,
        fa.appointment_length_minutes,
        
        -- Get actual production from procedures
        coalesce(fc.billed_amount, 0) as actual_production,
        coalesce(fc.paid_amount, 0) as collected_amount,
        
        dp.procedure_category_id,
        dp.is_hygiene_flag,
        dp.base_units
        
    from {{ ref('fact_appointment') }} fa
    left join {{ ref('fact_claim') }} fc
        on fa.appointment_id = fc.claim_id  -- Assuming appointments link to claims
    left join {{ ref('dim_procedure') }} dp
        on fc.procedure_id = dp.procedure_id
    where fa.appointment_date is not null
),

PaymentMetrics as (
    select 
        fp.payment_date,
        fp.provider_id,
        fp.clinic_id,
        sum(case when fp.is_patient_payment then fp.payment_amount else 0 end) as patient_payments,
        sum(case when fp.is_insurance_payment then fp.payment_amount else 0 end) as insurance_payments,
        sum(case when fp.is_adjustment then fp.payment_amount else 0 end) as adjustments,
        count(*) as payment_transaction_count
    from {{ ref('fact_payment') }} fp
    where fp.payment_date is not null
    group by fp.payment_date, fp.provider_id, fp.clinic_id
),

Daily_Production as (
    select
        pb.appointment_date as production_date,
        pb.provider_id,
        pb.clinic_id,
        
        -- Appointment Metrics
        count(*) as total_appointments,
        sum(case when pb.is_completed then 1 else 0 end) as completed_appointments,
        sum(case when not pb.is_completed then 1 else 0 end) as missed_appointments,
        sum(case when pb.is_hygiene_appointment then 1 else 0 end) as hygiene_appointments,
        
        -- Time Utilization
        sum(pb.appointment_length_minutes) as total_scheduled_minutes,
        sum(case when pb.is_completed then pb.appointment_length_minutes else 0 end) as productive_minutes,
        avg(pb.appointment_length_minutes) as avg_appointment_length,
        
        -- Production Metrics
        sum(pb.scheduled_production_amount) as scheduled_production,
        sum(case when pb.is_completed then pb.actual_production else 0 end) as actual_production,
        sum(pb.collected_amount) as collections,
        
        -- Procedure Analysis
        sum(pb.procedure_count) as total_procedures,
        avg(pb.actual_production / nullif(pb.procedure_count, 0)) as avg_fee_per_procedure,
        
        -- Patient Flow
        count(distinct pb.patient_id) as unique_patients,
        sum(pb.procedure_count) / nullif(count(distinct pb.patient_id), 0) as procedures_per_patient
        
    from ProductionBase pb
    group by pb.appointment_date, pb.provider_id, pb.clinic_id
),

Final as (
    select
        -- Keys and Dimensions
        dd.date_id,
        dp.production_date,
        dp.provider_id,
        dp.clinic_id,
        
        -- Provider and Clinic Information
        prov.provider_name,
        prov.provider_type,
        prov.specialty,
        
        -- Date Dimensions
        dd.year,
        dd.month,
        dd.quarter,
        dd.day_of_week,
        dd.day_name,
        dd.is_weekend,
        dd.is_holiday,
        
        -- Appointment Metrics
        dp.total_appointments,
        dp.completed_appointments,
        dp.missed_appointments,
        dp.hygiene_appointments,
        round(dp.completed_appointments::numeric / nullif(dp.total_appointments, 0) * 100, 2) as completion_rate,
        
        -- Time Utilization Metrics
        dp.total_scheduled_minutes,
        dp.productive_minutes,
        dp.avg_appointment_length,
        round(dp.productive_minutes::numeric / nullif(dp.total_scheduled_minutes, 0) * 100, 2) as time_utilization_rate,
        round(dp.productive_minutes / 60.0, 2) as productive_hours,
        
        -- Production and Financial Metrics
        dp.scheduled_production,
        dp.actual_production,
        dp.collections,
        round(dp.actual_production::numeric / nullif(dp.scheduled_production, 0) * 100, 2) as production_efficiency,
        round(dp.collections::numeric / nullif(dp.actual_production, 0) * 100, 2) as collection_rate,
        
        -- Payment Information
        coalesce(pm.patient_payments, 0) as patient_payments,
        coalesce(pm.insurance_payments, 0) as insurance_payments,
        coalesce(pm.adjustments, 0) as adjustments,
        coalesce(pm.payment_transaction_count, 0) as payment_transactions,
        
        -- Procedure Metrics
        dp.total_procedures,
        dp.avg_fee_per_procedure,
        dp.unique_patients,
        dp.procedures_per_patient,
        
        -- Performance Indicators
        case 
            when dp.completion_rate >= 95 then 'Excellent'
            when dp.completion_rate >= 90 then 'Good'
            when dp.completion_rate >= 80 then 'Fair'
            else 'Poor'
        end as appointment_performance,
        
        case 
            when dp.collection_rate >= 98 then 'Excellent'
            when dp.collection_rate >= 95 then 'Good'
            when dp.collection_rate >= 90 then 'Fair'
            else 'Poor'
        end as collection_performance,
        
        -- Goals and Targets (placeholder - would come from practice goals)
        dp.actual_production as production_goal, -- Would be replaced with actual goal
        round((dp.actual_production / nullif(dp.actual_production, 0)) * 100, 2) as goal_achievement, -- Placeholder
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from Daily_Production dp
    inner join {{ ref('dim_date') }} dd
        on dp.production_date = dd.date_actual
    inner join {{ ref('dim_provider') }} prov
        on dp.provider_id = prov.provider_id
    left join PaymentMetrics pm
        on dp.production_date = pm.payment_date
        and dp.provider_id = pm.provider_id
        and dp.clinic_id = pm.clinic_id
)

select * from Final
