{{ config(
    materialized='table',
    schema='intermediate',
    unique_key='patient_id',
    on_schema_change='fail',
    indexes=[
        {'columns': ['patient_id'], 'unique': true},
        {'columns': ['patient_status']},
        {'columns': ['total_ar_balance']},
        {'columns': ['balance_over_90_days']},
        {'columns': ['last_payment_date']},
        {'columns': ['_updated_at']}
    ],
    tags=['cross_system', 'financial', 'weekly']
) }}

/*
    Intermediate model for patient financial journey
    Part of Cross-System: Patient Financial Journey Analysis
    
    This model:
    1. Provides comprehensive patient financial journey across all systems
    2. Integrates AR analysis with procedure, payment, and insurance data
    3. Builds on existing AR infrastructure for consistency
    4. Offers patient-centric view of complete financial lifecycle
    
    Business Logic Features:
    - Financial Journey Tracking: Complete view from procedures to payments to collections
    - Cross-System Integration: Connects Fee Processing, Insurance, Payments, and AR Analysis
    - Patient-Centric Aggregation: All financial data aggregated at patient level
    - Aging Analysis: Leverages existing AR aging bucket calculations
    - Payment History: Comprehensive payment tracking across all sources
    
    Data Quality Notes:
    - Builds on validated AR analysis infrastructure
    - Uses existing intermediate models for data consistency
    - Handles null values appropriately with COALESCE
    - Maintains referential integrity with patient profile
    
    Performance Considerations:
    - Table materialization for complex cross-system joins
    - Leverages existing AR analysis for performance
    - Indexed on key lookup fields for efficient queries
    - Weekly refresh appropriate for financial journey data
*/

-- 1. Source data retrieval
with source_ar_analysis as (
    select * from {{ ref('int_ar_analysis') }}
),

source_patient_profile as (
    select * from {{ ref('int_patient_profile') }}
),

-- 2. Lookup/reference data
procedure_summary as (
    select
        patient_id,
        sum(case when procedure_status = 2 then procedure_fee else 0 end) as total_completed_procedures,
        sum(case when procedure_status = 1 then procedure_fee else 0 end) as total_treatment_planned,
        count(case when procedure_status = 2 then procedure_id end) as completed_procedure_count,
        count(case when procedure_status = 1 then procedure_id end) as planned_procedure_count,
        min(case when procedure_status = 2 then procedure_date end) as first_procedure_date,
        max(case when procedure_status = 2 then procedure_date end) as last_procedure_date,
        count(distinct case when procedure_status = 2 then procedure_code end) as unique_procedure_codes_completed,
        count(distinct case when procedure_status = 1 then procedure_code end) as unique_procedure_codes_planned
    from {{ ref('int_procedure_complete') }}
    group by patient_id
),

payment_summary as (
    select
        patient_id,
        sum(case when payment_source_type = 'PATIENT' then split_amount else 0 end) as total_patient_payments,
        sum(case when payment_source_type = 'INSURANCE' then split_amount else 0 end) as total_insurance_payments,
        count(distinct case when payment_source_type = 'PATIENT' then payment_id end) as patient_payment_count,
        count(distinct case when payment_source_type = 'INSURANCE' then payment_id end) as insurance_payment_count,
        min(case when payment_source_type = 'PATIENT' then payment_date end) as first_patient_payment_date,
        max(case when payment_source_type = 'PATIENT' then payment_date end) as last_patient_payment_date,
        min(case when payment_source_type = 'INSURANCE' then payment_date end) as first_insurance_payment_date,
        max(case when payment_source_type = 'INSURANCE' then payment_date end) as last_insurance_payment_date
    from (
        select patient_id, payment_id, split_amount, payment_date, 'PATIENT' as payment_source_type
        from {{ ref('int_patient_payment_allocated') }}
        where include_in_ar = true
        
        union all
        
        select patient_id, payment_id, split_amount, payment_date, 'INSURANCE' as payment_source_type
        from {{ ref('int_insurance_payment_allocated') }}
        where include_in_ar = true
    ) combined_payments
    group by patient_id
),

adjustment_summary as (
    select
        patient_id,
        sum(case when adjustment_amount > 0 then adjustment_amount else 0 end) as total_positive_adjustments,
        sum(case when adjustment_amount < 0 then adjustment_amount else 0 end) as total_negative_adjustments,
        count(*) as adjustment_count,
        count(distinct case when adjustment_category = 'insurance_writeoff' then adjustment_id end) as writeoff_count,
        count(distinct case when adjustment_category like '%discount%' then adjustment_id end) as discount_count,
        max(adjustment_date) as last_adjustment_date
    from {{ ref('int_adjustments') }}
    group by patient_id
),

-- 3. Business logic transformation
financial_journey_enhanced as (
    select
        -- Primary identification
        ara.patient_id,
        
        -- Patient demographics (from patient profile)
        pp.preferred_name as first_name,
        pp.middle_initial,
        pp.gender,
        pp.birth_date,
        pp.age,
        pp.patient_status,
        pp.patient_status_description,
        pp.patient_category,
        pp.family_id,
        pp.primary_provider_id,
        pp.clinic_id,
        
        -- AR Analysis data (core financial metrics)
        ara.total_ar_balance,
        ara.balance_0_30_days,
        ara.balance_31_60_days,
        ara.balance_61_90_days,
        ara.balance_over_90_days,
        ara.estimated_insurance_ar,
        ara.patient_responsibility,
        ara.insurance_responsibility,
        ara.last_payment_date,
        ara.last_visit_date,
        ara.open_procedures_count,
        ara.active_claims_count,
        
        -- Procedure summary
        coalesce(ps.total_completed_procedures, 0) as total_completed_procedures,
        coalesce(ps.total_treatment_planned, 0) as total_treatment_planned,
        coalesce(ps.completed_procedure_count, 0) as completed_procedure_count,
        coalesce(ps.planned_procedure_count, 0) as planned_procedure_count,
        ps.first_procedure_date,
        ps.last_procedure_date,
        coalesce(ps.unique_procedure_codes_completed, 0) as unique_procedure_codes_completed,
        coalesce(ps.unique_procedure_codes_planned, 0) as unique_procedure_codes_planned,
        
        -- Payment summary
        coalesce(pays.total_patient_payments, 0) as total_patient_payments,
        coalesce(pays.total_insurance_payments, 0) as total_insurance_payments,
        coalesce(pays.patient_payment_count, 0) as patient_payment_count,
        coalesce(pays.insurance_payment_count, 0) as insurance_payment_count,
        pays.first_patient_payment_date,
        pays.last_patient_payment_date,
        pays.first_insurance_payment_date,
        pays.last_insurance_payment_date,
        
        -- Adjustment summary
        coalesce(adj.total_positive_adjustments, 0) as total_positive_adjustments,
        coalesce(adj.total_negative_adjustments, 0) as total_negative_adjustments,
        coalesce(adj.adjustment_count, 0) as adjustment_count,
        coalesce(adj.writeoff_count, 0) as writeoff_count,
        coalesce(adj.discount_count, 0) as discount_count,
        adj.last_adjustment_date,
        
        -- Calculated financial journey metrics
        coalesce(ps.total_completed_procedures, 0) 
        - coalesce(pays.total_patient_payments, 0) 
        - coalesce(pays.total_insurance_payments, 0) 
        + coalesce(adj.total_positive_adjustments, 0) 
        + coalesce(adj.total_negative_adjustments, 0) as calculated_balance,
        
        -- Financial journey flags
        case 
            when ara.total_ar_balance > 0 then true 
            else false 
        end as has_outstanding_balance,
        
        case 
            when ara.balance_over_90_days > 0 then true 
            else false 
        end as has_overdue_balance,
        
        case 
            when ara.active_claims_count > 0 then true 
            else false 
        end as has_pending_claims,
        
        case 
            when pays.last_patient_payment_date is not null 
            and pays.last_patient_payment_date >= current_date - interval '90 days' 
            then true 
            else false 
        end as has_recent_payment_activity,
        
        -- Metadata fields (standardized pattern)
        {{ standardize_intermediate_metadata(primary_source_alias='ara') }}
        
    from source_ar_analysis ara
    left join source_patient_profile pp
        on ara.patient_id = pp.patient_id
    left join procedure_summary ps
        on ara.patient_id = ps.patient_id
    left join payment_summary pays
        on ara.patient_id = pays.patient_id
    left join adjustment_summary adj
        on ara.patient_id = adj.patient_id
)

select * from financial_journey_enhanced