{{
    config(
        materialized='table',
        schema='marts',
        unique_key=['date_id', 'patient_id']
    )
}}

/*
Patient Retention Mart - Comprehensive patient lifecycle and retention analysis
This mart provides complete patient retention tracking including lifecycle stages,
churn risk assessment, lifetime value calculation, and retention optimization insights.

Key metrics:
- Patient lifecycle and retention stages
- Churn risk scoring and prediction
- Patient lifetime value and revenue analysis
- Engagement patterns and activity trends
- Retention effectiveness by provider and service type
*/

with PatientActivity as (
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
    group by fa.patient_id
),

PatientFinancial as (
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
    group by fp.patient_id
),

PatientProduction as (
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
    group by fc.patient_id
),

PatientCommunication as (
    select 
        fcom.patient_id,
        count(*) as total_communications,
        sum(case when fcom.has_response then 1 else 0 end) as communications_with_response,
        max(fcom.communication_datetime) as last_communication_date,
        
        -- Communication effectiveness
        avg(case when fcom.response_time_hours is not null 
            then fcom.response_time_hours end) as avg_response_time_hours,
        count(case when fcom.communication_datetime >= current_date - interval '6 months' 
            then 1 end) as communications_last_6_months
    from {{ ref('fact_communication') }} fcom
    group by fcom.patient_id
),

PatientGaps as (
    select 
        pa.patient_id,
        case when pa.last_completed_appointment is not null 
            then current_date - pa.last_completed_appointment 
        end as days_since_last_visit,
        case when pa.recent_activity_date is not null 
            then current_date - pa.recent_activity_date 
        end as days_since_recent_activity,
        
        -- Calculate average gap between appointments
        case when pa.total_appointments > 1 and pa.first_appointment_date is not null and pa.last_appointment_date is not null
            then extract(days from pa.last_appointment_date - pa.first_appointment_date) / 
                 nullif(pa.total_appointments - 1, 0)
        end as avg_days_between_appointments
    from PatientActivity pa
),

Final as (
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
        prov.provider_name as primary_provider_name,
        prov.provider_type as primary_provider_type,
        prov.specialty as primary_provider_specialty,
        
        -- Date Information
        dd.year,
        dd.month,
        dd.quarter,
        dd.day_name,
        
        -- Patient Tenure and Lifecycle
        case when pt.first_visit_date is not null 
            then current_date - pt.first_visit_date 
        end as days_as_patient,
        round(extract(days from current_date - pt.first_visit_date) / 365.0, 1) as years_as_patient,
        
        -- Activity Summary
        pa.total_appointments,
        pa.completed_appointments,
        pa.no_show_appointments,
        pa.cancelled_appointments,
        pa.first_appointment_date,
        pa.last_appointment_date,
        pa.last_completed_appointment,
        
        -- Recent Activity Metrics
        pa.appointments_last_30_days,
        pa.appointments_last_90_days,
        pa.appointments_last_6_months,
        pa.appointments_last_year,
        pa.appointments_last_2_years,
        pa.completed_visits_last_year,
        
        -- Service Diversity
        pa.hygiene_appointments,
        pa.treatment_appointments,
        pa.unique_providers_seen,
        round(pa.hygiene_appointments::numeric / nullif(pa.total_appointments, 0) * 100, 2) as hygiene_visit_percentage,
        
        -- Financial Metrics
        pf.lifetime_patient_payments,
        pf.lifetime_insurance_payments,
        pf.lifetime_total_payments,
        pf.total_payment_transactions,
        pf.last_payment_date,
        pf.payments_last_year,
        
        -- Production and Value
        pp.lifetime_production,
        pp.lifetime_collections,
        pp.lifetime_write_offs,
        pp.total_claims,
        pp.production_last_year,
        pp.production_last_2_years,
        
        -- Patient Lifetime Value
        round(pp.lifetime_production / nullif(extract(days from current_date - pt.first_visit_date) / 365.0, 0), 2) as annual_patient_value,
        round(pf.lifetime_total_payments / nullif(pa.total_appointments, 0), 2) as avg_payment_per_visit,
        round(pp.lifetime_production / nullif(pa.completed_appointments, 0), 2) as avg_production_per_visit,
        
        -- Communication and Engagement
        pc.total_communications,
        pc.communications_with_response,
        pc.last_communication_date,
        pc.avg_response_time_hours,
        pc.communications_last_6_months,
        round(pc.communications_with_response::numeric / nullif(pc.total_communications, 0) * 100, 2) as communication_response_rate,
        
        -- Timing and Gaps
        pg.days_since_last_visit,
        pg.days_since_recent_activity,
        pg.avg_days_between_appointments,
        round(pg.avg_days_between_appointments / 30.0, 1) as avg_months_between_visits,
        
        -- Retention Status Assessment
        case 
            when pa.appointments_last_30_days > 0 then 'Active'
            when pa.appointments_last_90_days > 0 then 'Recent'
            when pa.appointments_last_6_months > 0 then 'Moderate'
            when pa.appointments_last_year > 0 then 'Dormant'
            when pa.appointments_last_2_years > 0 then 'Inactive'
            else 'Lost'
        end as retention_status,
        
        -- Churn Risk Assessment
        case 
            when pg.days_since_last_visit > 365 then 'High Risk'
            when pg.days_since_last_visit > 180 and pa.appointments_last_year = 0 then 'High Risk'
            when pg.days_since_last_visit > 120 and pa.no_show_appointments::numeric / nullif(pa.total_appointments, 0) > 0.3 then 'Medium Risk'
            when pg.days_since_last_visit > 90 then 'Medium Risk'
            when pa.appointments_last_year < 2 and pg.avg_days_between_appointments > 180 then 'Medium Risk'
            else 'Low Risk'
        end as churn_risk_category,
        
        -- Patient Value Category
        case 
            when pp.lifetime_production = 0 then 'No Production'
            when pp.lifetime_production < 500 then 'Low Value'
            when pp.lifetime_production < 2000 then 'Medium Value'
            when pp.lifetime_production < 5000 then 'High Value'
            else 'VIP'
        end as patient_value_category,
        
        -- Engagement Level
        case 
            when pa.completed_visits_last_year >= 3 and pc.communication_response_rate >= 80 then 'Highly Engaged'
            when pa.completed_visits_last_year >= 2 and pc.communication_response_rate >= 60 then 'Engaged'
            when pa.completed_visits_last_year >= 1 then 'Moderately Engaged'
            else 'Disengaged'
        end as engagement_level,
        
        -- Loyalty Indicators
        case 
            when extract(days from current_date - pt.first_visit_date) / 365.0 >= 3 
                and pa.appointments_last_2_years >= 4 
                and pa.unique_providers_seen >= 2 then 'Loyal'
            when extract(days from current_date - pt.first_visit_date) / 365.0 >= 2 
                and pa.appointments_last_year >= 2 then 'Returning'
            when pa.appointments_last_year >= 1 then 'Active'
            else 'New/Inactive'
        end as loyalty_segment,
        
        -- Boolean Flags
        case when pa.appointments_last_year > 0 then true else false end as active_last_year,
        case when pa.hygiene_appointments >= 2 then true else false end as regular_hygiene_patient,
        case when pp.production_last_year > 0 then true else false end as recent_production,
        case when pg.days_since_last_visit <= 180 then true else false end as recently_seen,
        case when pa.no_show_appointments::numeric / nullif(pa.total_appointments, 0) <= 0.1 then true else false end as reliable_patient,
        case when pc.communication_response_rate >= 70 then true else false end as responsive_patient,
        
        -- Risk Scores (0-100)
        round((
            case when pg.days_since_last_visit > 365 then 40 
                 when pg.days_since_last_visit > 180 then 20 
                 else 0 end +
            case when pa.appointments_last_year = 0 then 30 else 0 end +
            case when pa.no_show_appointments::numeric / nullif(pa.total_appointments, 0) > 0.2 then 20 else 0 end +
            case when pc.communication_response_rate < 50 then 10 else 0 end
        ), 0) as churn_risk_score,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from {{ ref('dim_patient') }} pt
    inner join {{ ref('dim_date') }} dd
        on current_date = dd.date_actual
    inner join {{ ref('dim_provider') }} prov
        on pt.primary_provider_id = prov.provider_id
    left join PatientActivity pa
        on pt.patient_id = pa.patient_id
    left join PatientFinancial pf
        on pt.patient_id = pf.patient_id
    left join PatientProduction pp
        on pt.patient_id = pp.patient_id
    left join PatientCommunication pc
        on pt.patient_id = pc.patient_id
    left join PatientGaps pg
        on pt.patient_id = pg.patient_id
    where pt.patient_status in ('Patient', 'Inactive')
        and pt.first_visit_date is not null
)

select * from Final
