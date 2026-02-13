{{ config(
    materialized='table',
    schema='marts',
    unique_key=['date_id', 'patient_id'],
    on_schema_change='fail',
    indexes=[
        {'columns': ['date_id']},
        {'columns': ['patient_id']},
        {'columns': ['primary_provider_id']},
        {'columns': ['clinic_id']},
        {'columns': ['retention_status']},
        {'columns': ['churn_risk_category']}
    ]
) }}

/*
Patient Retention Mart - Comprehensive patient lifecycle and retention analysis
Part of System G: Scheduling and Patient Management

This model:
1. Provides comprehensive patient retention tracking and lifecycle analysis
2. Calculates churn risk assessment and patient lifetime value metrics
3. Enables retention optimization insights and engagement pattern analysis

Business Logic Features:
- Patient lifecycle stage classification (Active, Recent, Moderate, Dormant, Inactive, Lost)
- Churn risk scoring with multi-factor assessment (0-100 scale)
- Patient value categorization based on lifetime production
- Engagement level assessment combining visit frequency and communication responsiveness
- Loyalty segmentation based on tenure, visit patterns, and provider diversity

Key Metrics:
- Retention Status: Current patient engagement level classification
- Churn Risk Score: 0-100 risk assessment for patient loss
- Patient Lifetime Value: Annual production value and visit-based metrics
- Engagement Metrics: Communication response rates and visit frequency patterns
- Loyalty Indicators: Patient tenure and provider relationship diversity

Data Quality Notes:
- Excludes patients without first visit dates to ensure data completeness
- Handles null values in financial calculations with proper coalesce logic
- Uses safe division with nullif to prevent division by zero errors
- Filters to active patient statuses only (Patient, Inactive)
- Handles future appointment dates by excluding them from time-based calculations
- Patients with future appointments are classified as 'Scheduled' retention status
- Negative time calculations are prevented by filtering out future dates

Performance Considerations:
- eligible_patients CTE: single read of dim_patient with final filters; fact tables are joined to it so we only aggregate rows for patients that appear in the mart (avoids full fact scans for excluded patients).
- Indexed on key query dimensions (date_id, patient_id, provider_id, clinic_id)
- Optimized joins using proper foreign key relationships
- Efficient aggregations with conditional counting and summing
- dim_date cross join limited to required columns (date_id, year, month, quarter, day_name) for single-row lookup

Dependencies:
- dim_patient: Patient demographic and status information
- dim_provider: Provider details and specialty information
- dim_date: Date dimension for temporal analysis
- fact_appointment: Appointment history and completion data
- fact_payment: Financial transaction and payment history
- fact_claim: Production and billing information
- fact_communication: Patient communication and engagement data
*/

with
-- 0. Eligible patients: single read of dim_patient with filters; used to restrict fact aggregations and as final driver
eligible_patients as (
    select *
    from {{ ref('dim_patient') }}
    where patient_status in ('Patient', 'Inactive')
      and first_visit_date is not null
      and first_visit_date <= current_date
      and first_visit_date >= '2000-01-01'
      and patient_id != 32974
      and birth_date not in ('0001-01-01', '1900-01-01')  -- OpenDental placeholders for unknown/missing DOB (see clean_opendental_date macro, _dim_patient.yml, _stg_opendental__patient.yml)
      and position_code != 'House'
),

-- 1. Base appointment data aggregation (only for eligible patients)
patient_activity_base as (
    select 
        fa.patient_id,
        count(*) as total_appointments,
        sum(case when fa.is_completed then 1 else 0 end) as completed_appointments,
        sum(case when fa.is_no_show then 1 else 0 end) as no_show_appointments,
        sum(case when fa.is_broken then 1 else 0 end) as cancelled_appointments,
        min(fa.appointment_date) as first_appointment_date,
        max(fa.appointment_date) as last_appointment_date,
        max(case when fa.is_completed then fa.appointment_date end) as last_completed_appointment,
        
        -- Activity by time periods
        count(case when fa.appointment_date >= current_date - interval '30 days' then 1 end) as appointments_last_30_days,
        count(case when fa.appointment_date >= current_date - interval '90 days' then 1 end) as appointments_last_90_days,
        count(case when fa.appointment_date >= current_date - interval '6 months' then 1 end) as appointments_last_6_months,
        count(case when fa.appointment_date >= current_date - interval '1 year' then 1 end) as appointments_last_year,
        count(case when fa.appointment_date >= current_date - interval '2 years' then 1 end) as appointments_last_2_years,
        
        -- Service type diversity
        count(case when fa.is_hygiene_appointment then 1 end) as hygiene_appointments,
        count(case when not fa.is_hygiene_appointment then 1 end) as treatment_appointments,
        count(distinct fa.provider_id) as unique_providers_seen,
        
        -- Recent activity indicators
        max(case when fa.appointment_date >= current_date - interval '6 months' 
            then fa.appointment_date end) as recent_activity_date,
        count(case when fa.appointment_date >= current_date - interval '1 year' 
            and fa.is_completed then 1 end) as completed_visits_last_year
    from {{ ref('fact_appointment') }} fa
    inner join eligible_patients ep on fa.patient_id = ep.patient_id
    group by fa.patient_id
),

-- 2. Financial transaction aggregation (only for eligible patients)
patient_financial_base as (
    select 
        fp.patient_id,
        sum(case when fp.is_patient_payment then fp.payment_amount else 0 end) as lifetime_patient_payments,
        sum(case when fp.is_insurance_payment then fp.payment_amount else 0 end) as lifetime_insurance_payments,
        sum(fp.payment_amount) as lifetime_total_payments,
        count(*) as total_payment_transactions,
        max(fp.payment_date) as last_payment_date,
        
        -- Recent financial activity
        sum(case when fp.payment_date >= current_date - interval '1 year' 
            then fp.payment_amount else 0 end) as payments_last_year,
        count(case when fp.payment_date >= current_date - interval '1 year' 
            then 1 end) as payment_transactions_last_year
    from {{ ref('fact_payment') }} fp
    inner join eligible_patients ep on fp.patient_id = ep.patient_id
    group by fp.patient_id
),

-- 3. Production and billing aggregation (only for eligible patients)
patient_production_base as (
    select 
        fc.patient_id,
        sum(fc.billed_amount) as lifetime_production,
        sum(fc.paid_amount) as lifetime_collections,
        sum(fc.write_off_amount) as lifetime_write_offs,
        count(*) as total_claims,
        max(fc.claim_date) as last_claim_date,
        
        -- Recent production
        sum(case when fc.claim_date >= current_date - interval '1 year' 
            then fc.billed_amount else 0 end) as production_last_year,
        sum(case when fc.claim_date >= current_date - interval '2 years' 
            then fc.billed_amount else 0 end) as production_last_2_years
    from {{ ref('fact_claim') }} fc
    inner join eligible_patients ep on fc.patient_id = ep.patient_id
    group by fc.patient_id
),

-- 4. Communication and engagement aggregation (only for eligible patients)
patient_communication_base as (
    select 
        fcom.patient_id,
        count(*) as total_communications,
        sum(case when fcom.is_patient_initiated then 1 else 0 end) as communications_with_response,
        max(fcom.communication_datetime) as last_communication_date,
        
        -- Communication effectiveness
        count(case when fcom.engagement_level = 'Received' then 1 end) as communications_received,
        count(case when fcom.communication_datetime >= current_date - interval '6 months' 
            then 1 end) as communications_last_6_months,
        round(
            (sum(case when fcom.is_patient_initiated then 1 else 0 end)::numeric / 
            nullif(count(*), 0) * 100)::numeric, 2
        ) as communication_response_rate
    from {{ ref('fact_communication') }} fcom
    inner join eligible_patients ep on fcom.patient_id = ep.patient_id
    group by fcom.patient_id
),

-- 5. Patient lifecycle gap analysis
patient_gaps_calculated as (
    select 
        pa.patient_id,
        case when pa.last_completed_appointment is not null and pa.last_completed_appointment <= current_date
            then current_date - pa.last_completed_appointment 
        end as days_since_last_visit,
        case when pa.recent_activity_date is not null and pa.recent_activity_date <= current_date
            then current_date - pa.recent_activity_date 
        end as days_since_recent_activity,
        
        -- Calculate average gap between appointments (excluding future dates)
        case when pa.total_appointments > 1 and pa.first_appointment_date is not null and pa.last_appointment_date is not null
            and pa.last_appointment_date <= current_date
            then (pa.last_appointment_date - pa.first_appointment_date) / 
                 nullif(pa.total_appointments - 1, 0)
            when pa.total_appointments = 1 and pa.first_appointment_date is not null
            then null  -- Single appointment, no gap to calculate
            else null
        end as avg_days_between_appointments
    from patient_activity_base pa
),

-- 6. Final integration with business logic enhancement
final as (
    select
        -- Keys and Dimensions
        dd.date_id,
        pt.patient_id,
        
        -- Patient Demographics
        pt.age,
        pt.gender,
        pt.patient_status,
        pt.first_visit_date,
        pt.primary_provider_id,
        pt.clinic_id,
        pt.has_insurance_flag,
        
        -- Provider Information
        prov.provider_type_category as primary_provider_type,
        prov.provider_specialty_category as primary_provider_specialty,
        
        -- Date Information
        dd.year,
        dd.month,
        dd.quarter,
        dd.day_name,
        
        -- Patient Tenure and Lifecycle
        case 
            when pt.first_visit_date is null then null
            when pt.first_visit_date > current_date then null  -- Future patients have no tenure yet
            else current_date - pt.first_visit_date 
        end as days_as_patient,
        case 
            when pt.first_visit_date > current_date then null  -- Future patients have no tenure yet
            else round(((current_date - pt.first_visit_date) / 365.0)::numeric, 1)
        end as years_as_patient,
        
        -- Activity Summary
        coalesce(pa.total_appointments, 0) as total_appointments,
        coalesce(pa.completed_appointments, 0) as completed_appointments,
        coalesce(pa.no_show_appointments, 0) as no_show_appointments,
        coalesce(pa.cancelled_appointments, 0) as cancelled_appointments,
        pa.first_appointment_date,
        pa.last_appointment_date,
        pa.last_completed_appointment,
        
        -- Recent Activity Metrics
        coalesce(pa.appointments_last_30_days, 0) as appointments_last_30_days,
        coalesce(pa.appointments_last_90_days, 0) as appointments_last_90_days,
        coalesce(pa.appointments_last_6_months, 0) as appointments_last_6_months,
        coalesce(pa.appointments_last_year, 0) as appointments_last_year,
        coalesce(pa.appointments_last_2_years, 0) as appointments_last_2_years,
        coalesce(pa.completed_visits_last_year, 0) as completed_visits_last_year,
        
        -- Service Diversity
        coalesce(pa.hygiene_appointments, 0) as hygiene_appointments,
        coalesce(pa.treatment_appointments, 0) as treatment_appointments,
        coalesce(pa.unique_providers_seen, 0) as unique_providers_seen,
        round((coalesce(pa.hygiene_appointments, 0)::numeric / nullif(coalesce(pa.total_appointments, 0), 0) * 100)::numeric, 2) as hygiene_visit_percentage,
        
        -- Financial Metrics (with proper null handling)
        coalesce(pf.lifetime_patient_payments, 0) as lifetime_patient_payments,
        coalesce(pf.lifetime_insurance_payments, 0) as lifetime_insurance_payments,
        coalesce(pf.lifetime_total_payments, 0) as lifetime_total_payments,
        coalesce(pf.total_payment_transactions, 0) as total_payment_transactions,
        pf.last_payment_date,
        coalesce(pf.payments_last_year, 0) as payments_last_year,
        
        -- Production and Value (with proper null handling)
        coalesce(pp.lifetime_production, 0) as lifetime_production,
        coalesce(pp.lifetime_collections, 0) as lifetime_collections,
        coalesce(pp.lifetime_write_offs, 0) as lifetime_write_offs,
        coalesce(pp.total_claims, 0) as total_claims,
        coalesce(pp.production_last_year, 0) as production_last_year,
        coalesce(pp.production_last_2_years, 0) as production_last_2_years,
        
        -- Patient Lifetime Value (with safe calculations and future date handling)
        case 
            when pt.first_visit_date > current_date then null  -- Future patients have no annual value yet
            when (current_date - pt.first_visit_date) / 365.0 <= 0 then null  -- Handle edge cases
            else round(
                (coalesce(pp.lifetime_production, 0) / 
                nullif((current_date - pt.first_visit_date) / 365.0, 0))::numeric, 2
            )
        end as annual_patient_value,
        round(
            (coalesce(pf.lifetime_total_payments, 0) / 
            nullif(coalesce(pa.total_appointments, 0), 0))::numeric, 2
        ) as avg_payment_per_visit,
        round(
            (coalesce(pp.lifetime_production, 0) / 
            nullif(coalesce(pa.completed_appointments, 0), 0))::numeric, 2
        ) as avg_production_per_visit,
        
        -- Communication and Engagement (with proper null handling)
        coalesce(pc.total_communications, 0) as total_communications,
        coalesce(pc.communications_with_response, 0) as communications_with_response,
        pc.last_communication_date,
        coalesce(pc.communications_received, 0) as communications_received,
        coalesce(pc.communications_last_6_months, 0) as communications_last_6_months,
        coalesce(pc.communication_response_rate, 0) as communication_response_rate,
        
        -- Timing and Gaps
        pg.days_since_last_visit,
        pg.days_since_recent_activity,
        pg.avg_days_between_appointments,
        round((pg.avg_days_between_appointments / 30.0)::numeric, 1) as avg_months_between_visits,
        
        -- Retention Status Assessment (handling future dates)
        case 
            when pa.appointments_last_30_days > 0 then 'Active'
            when pa.appointments_last_90_days > 0 then 'Recent'
            when pa.appointments_last_6_months > 0 then 'Moderate'
            when pa.appointments_last_year > 0 then 'Dormant'
            when pa.appointments_last_2_years > 0 then 'Inactive'
            when pa.last_appointment_date > current_date then 'Scheduled'
            else 'Lost'
        end as retention_status,
        
        -- Churn Risk Assessment (handling NULL values and future dates)
        case 
            when pg.days_since_last_visit is null and pa.last_appointment_date > current_date then 'Low Risk'
            when pg.days_since_last_visit > 365 then 'High Risk'
            when pg.days_since_last_visit > 180 and pa.appointments_last_year = 0 then 'High Risk'
            when pg.days_since_last_visit > 120 and pa.no_show_appointments::numeric / nullif(pa.total_appointments, 0) > 0.3 then 'Medium Risk'
            when pg.days_since_last_visit > 90 then 'Medium Risk'
            when pa.appointments_last_year < 2 and pg.avg_days_between_appointments > 180 then 'Medium Risk'
            else 'Low Risk'
        end as churn_risk_category,
        
        -- Patient Value Category (with null handling)
        case 
            when coalesce(pp.lifetime_production, 0) = 0 then 'No Production'
            when coalesce(pp.lifetime_production, 0) < 500 then 'Low Value'
            when coalesce(pp.lifetime_production, 0) < 2000 then 'Medium Value'
            when coalesce(pp.lifetime_production, 0) < 5000 then 'High Value'
            else 'VIP'
        end as patient_value_category,
        
        -- Engagement Level (with null handling)
        case 
            when coalesce(pa.completed_visits_last_year, 0) >= 3 and coalesce(pc.communication_response_rate, 0) >= 80 then 'Highly Engaged'
            when coalesce(pa.completed_visits_last_year, 0) >= 2 and coalesce(pc.communication_response_rate, 0) >= 60 then 'Engaged'
            when coalesce(pa.completed_visits_last_year, 0) >= 1 then 'Moderately Engaged'
            else 'Disengaged'
        end as engagement_level,
        
        -- Loyalty Indicators
        case 
            when (current_date - pt.first_visit_date) / 365.0 >= 3 
                and pa.appointments_last_2_years >= 4 
                and pa.unique_providers_seen >= 2 then 'Loyal'
            when (current_date - pt.first_visit_date) / 365.0 >= 2 
                and pa.appointments_last_year >= 2 then 'Returning'
            when pa.appointments_last_year >= 1 then 'Active'
            else 'New/Inactive'
        end as loyalty_segment,
        
        -- Boolean Flags (with null handling)
        case when coalesce(pa.appointments_last_year, 0) > 0 then true else false end as active_last_year,
        case when coalesce(pa.hygiene_appointments, 0) >= 2 then true else false end as regular_hygiene_patient,
        case when coalesce(pp.production_last_year, 0) > 0 then true else false end as recent_production,
        case when coalesce(pg.days_since_last_visit, 999) <= 180 then true else false end as recently_seen,
        case when coalesce(pa.no_show_appointments, 0)::numeric / nullif(coalesce(pa.total_appointments, 0), 0) <= 0.1 then true else false end as reliable_patient,
        case when coalesce(pc.communication_response_rate, 0) >= 70 then true else false end as responsive_patient,
        
        -- Risk Scores (0-100) with null handling and future date logic
        round((
            case when pg.days_since_last_visit is null and pa.last_appointment_date > current_date then 0
                 when coalesce(pg.days_since_last_visit, 999) > 365 then 40 
                 when coalesce(pg.days_since_last_visit, 999) > 180 then 20 
                 else 0 end +
            case when coalesce(pa.appointments_last_year, 0) = 0 then 30 else 0 end +
            case when coalesce(pa.no_show_appointments, 0)::numeric / nullif(coalesce(pa.total_appointments, 0), 0) > 0.2 then 20 else 0 end +
            case when coalesce(pc.communication_response_rate, 0) < 50 then 10 else 0 end
        )::numeric, 0) as churn_risk_score,
        
        -- Metadata
        {{ standardize_mart_metadata(
            primary_source_alias='pt',
            source_metadata_fields=['_loaded_at', '_updated_at']
        ) }}
        
    from eligible_patients pt
    cross join (select date_id, year, month, quarter, day_name from {{ ref('dim_date') }} where date_day = current_date limit 1) dd
    inner join {{ ref('dim_provider') }} prov
        on pt.primary_provider_id = prov.provider_id
    left join patient_activity_base pa
        on pt.patient_id = pa.patient_id
    left join patient_financial_base pf
        on pt.patient_id = pf.patient_id
    left join patient_production_base pp
        on pt.patient_id = pp.patient_id
    left join patient_communication_base pc
        on pt.patient_id = pc.patient_id
    left join patient_gaps_calculated pg
        on pt.patient_id = pg.patient_id
)

select * from Final