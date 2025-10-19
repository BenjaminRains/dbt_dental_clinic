{{ config(
    materialized='table',
    schema='marts',
    unique_key=['date_id', 'provider_id', 'clinic_id'],
    on_schema_change='fail',
    indexes=[
        {'columns': ['date_id']},
        {'columns': ['provider_id']},
        {'columns': ['clinic_id']},
        {'columns': ['production_date']}
    ]
) }}

/*
Summary Mart model for Production Summary
Part of System G: Scheduling and Production Management

This model:
1. Provides comprehensive daily production analytics aggregated by provider and clinic
2. Supports financial performance monitoring and operational reporting
3. Enables provider performance comparison and clinic efficiency analysis

Business Logic Features:
- Daily production totals and efficiency calculations
- Appointment completion and time utilization metrics
- Collection rate analysis and payment timing
- Provider performance categorization and benchmarking
- Procedure analysis and patient flow metrics

Key Metrics:
- Production efficiency: Actual vs scheduled production ratios
- Collection rates: Collected vs billed amounts
- Completion rates: Completed vs total appointments
- Time utilization: Productive vs scheduled time
- Provider performance tiers based on multiple KPIs

Data Quality Notes:
- Appointment-claim relationships may have data quality issues (assumed join logic)
- Production amounts may include estimates vs actuals
- Payment timing may not align perfectly with appointment dates

Performance Considerations:
- Large aggregation across multiple fact tables requires proper indexing
- Date-based partitioning recommended for historical analysis
- Provider and clinic dimensions should be pre-joined for efficiency

Dependencies:
- fact_appointment: Core scheduling and appointment data
- fact_claim: Billing and production amounts
- fact_payment: Payment and collection data
- int_procedure_complete: Procedure data with fees
- int_payment_split: Payment split data with provider associations
- dim_date: Date dimension for temporal analysis
- dim_provider: Provider information and categorization
- dim_procedure: Procedure categorization and analysis
*/

-- 1. Base fact data using intermediate models
with production_base as (
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
        
        -- Get actual production from procedures using intermediate
        coalesce(pc.procedure_fee, 0) as actual_production,
        coalesce(fc.paid_amount, 0) as collected_amount,
        
        -- Procedure information
        pc.procedure_code_id,
        dp.procedure_category_id,
        dp.is_hygiene,
        dp.base_units,
        
        -- Metadata from fact_appointment
        fa._loaded_at,
        fa._created_at,
        fa._updated_at
        
    from {{ ref('fact_appointment') }} fa
    left join {{ ref('int_procedure_complete') }} pc
        on fa.appointment_id = pc.appointment_id
        and pc.procedure_date = fa.appointment_date
    left join {{ ref('fact_claim') }} fc
        on pc.procedure_id = fc.procedure_id
    left join {{ ref('dim_procedure') }} dp
        on pc.procedure_code_id = dp.procedure_code_id
    where fa.appointment_date is not null
),

-- 2. Dimension data
production_dimensions as (
    select 
        provider_id,
        provider_first_name,
        provider_last_name,
        provider_preferred_name,
        specialty_description,
        provider_status_description
    from {{ ref('dim_provider') }}
),

-- 3. Date dimension
date_dimension as (
    select 
        date_id,
        date_day,
        year,
        month,
        quarter,
        day_of_week,
        day_name,
        is_weekend,
        is_holiday
    from {{ ref('dim_date') }}
),

-- 4. Payment metrics aggregation using intermediate payment splits
payment_metrics_raw as (
    select 
        ps.payment_date,
        ps.provider_id,
        coalesce(ps.clinic_id, 0) as clinic_id,  -- Handle NULL clinic_id
        sum(case when ps.payment_type_id = 69 then ps.split_amount else 0 end) as patient_payments,
        sum(case when ps.payment_type_id = 71 then ps.split_amount else 0 end) as insurance_payments,
        0.0 as adjustments,  -- Adjustments handled separately below
        count(distinct ps.payment_id) as payment_transaction_count
    from {{ ref('int_payment_split') }} ps
    where ps.payment_date is not null
        and ps.provider_id is not null  -- Only include records with valid provider
    group by ps.payment_date, ps.provider_id, coalesce(ps.clinic_id, 0)
    
    UNION ALL
    
    -- Add adjustments separately since they're not linked through paysplit
    select 
        adj.adjustment_date as payment_date,
        adj.provider_id,
        coalesce(adj.clinic_id, 0) as clinic_id,
        0.0 as patient_payments,
        0.0 as insurance_payments,
        adj.adjustment_amount as adjustments,
        0 as payment_transaction_count
    from {{ ref('int_adjustments') }} adj
    where adj.adjustment_date is not null
        and adj.provider_id is not null
),

-- Aggregate payment metrics to combine payments and adjustments
payment_metrics as (
    select 
        payment_date,
        provider_id,
        clinic_id,
        sum(patient_payments) as patient_payments,
        sum(insurance_payments) as insurance_payments,
        sum(adjustments) as adjustments,
        sum(payment_transaction_count) as payment_transaction_count
    from payment_metrics_raw
    group by payment_date, provider_id, clinic_id
),

-- 5. Aggregations and calculations
production_aggregated as (
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
        count(distinct pb.procedure_code_id) as total_procedures,
        avg(pb.actual_production) as avg_fee_per_procedure,
        
        -- Patient Flow
        count(distinct pb.patient_id) as unique_patients,
        count(distinct pb.procedure_code_id)::numeric / nullif(count(distinct pb.patient_id)::numeric, 0) as procedures_per_patient,
        
        -- Metadata (use most recent values for aggregated data)
        max(pb._loaded_at) as _loaded_at,
        max(pb._created_at) as _created_at,
        max(pb._updated_at) as _updated_at
        
    from production_base pb
    group by pb.appointment_date, pb.provider_id, pb.clinic_id
),

-- 6. Business logic enhancement
production_enhanced as (
    select
        *,
        -- Performance categorization
        case 
            when round((completed_appointments::numeric / nullif(total_appointments::numeric, 0) * 100)::numeric, 2) >= 95 then 'Excellent'
            when round((completed_appointments::numeric / nullif(total_appointments::numeric, 0) * 100)::numeric, 2) >= 90 then 'Good'
            when round((completed_appointments::numeric / nullif(total_appointments::numeric, 0) * 100)::numeric, 2) >= 80 then 'Fair'
            else 'Poor'
        end as appointment_performance,
        
        case 
            when round((collections::numeric / nullif(actual_production::numeric, 0) * 100)::numeric, 2) >= 98 then 'Excellent'
            when round((collections::numeric / nullif(actual_production::numeric, 0) * 100)::numeric, 2) >= 95 then 'Good'
            when round((collections::numeric / nullif(actual_production::numeric, 0) * 100)::numeric, 2) >= 90 then 'Fair'
            else 'Poor'
        end as collection_performance
    from production_aggregated
),

-- 7. Final integration
final as (
    select
        -- Date and dimensions
        dd.date_id,
        pe.production_date,
        pe.provider_id,
        pe.clinic_id,
        
        -- Provider and Clinic Information
        pd.provider_first_name,
        pd.provider_last_name,
        pd.provider_preferred_name,
        pd.specialty_description,
        pd.provider_status_description,
        
        -- Date Dimensions
        dd.year,
        dd.month,
        dd.quarter,
        dd.day_of_week,
        dd.day_name,
        dd.is_weekend,
        dd.is_holiday,
        
        -- Appointment Metrics
        pe.total_appointments,
        pe.completed_appointments,
        pe.missed_appointments,
        pe.hygiene_appointments,
        round(pe.completed_appointments::numeric / nullif(pe.total_appointments::numeric, 0) * 100, 2) as completion_rate,
        
        -- Time Utilization Metrics
        pe.total_scheduled_minutes,
        pe.productive_minutes,
        pe.avg_appointment_length,
        round(pe.productive_minutes::numeric / nullif(pe.total_scheduled_minutes::numeric, 0) * 100, 2) as time_utilization_rate,
        round(pe.productive_minutes::numeric / 60, 2) as productive_hours,
        
        -- Production and Financial Metrics
        pe.scheduled_production,
        pe.actual_production,
        pe.collections,
        round(pe.actual_production::numeric / nullif(pe.scheduled_production::numeric, 0) * 100, 2) as production_efficiency,
        round(pe.collections::numeric / nullif(pe.actual_production::numeric, 0) * 100, 2) as collection_rate,
        
        -- Payment Information
        coalesce(pm.patient_payments, 0) as patient_payments,
        coalesce(pm.insurance_payments, 0) as insurance_payments,
        coalesce(pm.adjustments, 0) as adjustments,
        coalesce(pm.payment_transaction_count, 0) as payment_transactions,
        
        -- Procedure Metrics
        pe.total_procedures,
        pe.avg_fee_per_procedure,
        pe.unique_patients,
        pe.procedures_per_patient,
        
        -- Performance Indicators (from business logic enhancement)
        pe.appointment_performance,
        pe.collection_performance,
        
        -- Goals and Targets (placeholder - would come from practice goals)
        pe.actual_production as production_goal, -- Would be replaced with actual goal
        round((pe.actual_production::numeric / nullif(pe.actual_production::numeric, 0)) * 100, 2) as goal_achievement, -- Placeholder
        
        -- Metadata
        {{ standardize_mart_metadata(
            primary_source_alias='pe',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at']
        ) }}
        
    from production_enhanced pe
    inner join date_dimension dd
        on pe.production_date = dd.date_day
    inner join production_dimensions pd
        on pe.provider_id = pd.provider_id
    left join payment_metrics pm
        on pe.production_date = pm.payment_date
        and pe.provider_id = pm.provider_id
        and pe.clinic_id = pm.clinic_id
)

select * from final