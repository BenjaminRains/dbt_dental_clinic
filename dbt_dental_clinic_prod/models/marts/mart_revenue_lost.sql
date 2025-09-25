{{ config(
    materialized='table',
    schema='marts',
    unique_key=['date_id', 'opportunity_id'],
    on_schema_change='fail',
    indexes=[
        {'columns': ['date_id']},
        {'columns': ['provider_id']},
        {'columns': ['patient_id']},
        {'columns': ['opportunity_type']},
        {'columns': ['recovery_potential']}
    ]
) }}

/*
Summary Mart model for Revenue Lost Analysis
Part of System G: Scheduling and Revenue Management

This model:
1. Identifies and quantifies all forms of revenue leakage and missed opportunities
2. Provides comprehensive analysis of recoverable revenue across multiple opportunity types
3. Enables proactive revenue recovery and operational optimization

Business Logic Features:
- Multi-source opportunity identification (appointments, capacity, claims, treatment plans)
- Intelligent recovery potential scoring and prioritization
- Time-based analysis and preventability assessment
- Financial impact categorization and timeline estimation

Key Metrics:
- Lost revenue amounts by opportunity type and recovery potential
- Recovery priority scoring for actionable prioritization
- Time impact analysis and preventability assessment
- Estimated recoverable amounts with realistic recovery rates

Data Quality Notes:
- Some treatment plan data may have incomplete status information
- Claim rejection data depends on insurance processing accuracy
- Capacity analysis assumes accurate schedule data from OpenDental

Performance Considerations:
- Large volume model due to comprehensive opportunity tracking
- Indexed on key analytical dimensions for optimal query performance
- Uses efficient window functions for opportunity scoring

Dependencies:
- fact_appointment: Core appointment and scheduling data
- fact_claim: Insurance and billing transaction data
- stg_opendental__schedule: Provider availability and capacity data
- stg_opendental__treatplan: Treatment plan and acceptance data
- stg_opendental__adjustment: Write-off and adjustment data
- dim_date: Date dimension for temporal analysis
- dim_provider: Provider information and attributes
- dim_patient: Patient information for demographic analysis
*/

-- 1. Base fact data
with appointment_base as (
    select * from {{ ref('fact_appointment') }}
    where (is_no_show = true or is_broken = true)
        and appointment_date >= current_date - interval '1 year'
        and scheduled_production_amount > 0
),

claim_base as (
    select * from {{ ref('fact_claim') }}
    where claim_status in ('Denied', 'Rejected', 'Pending')
        and claim_date >= current_date - interval '1 year'
        and billed_amount > paid_amount
        and billed_amount > 0
),

-- Note: schedule_base CTE removed as it was only used for UnfilledSlots analysis
-- which required columns not available in the actual schedule table

treatment_base as (
    select * from {{ ref('stg_opendental__treatplan') }}
    where treatment_plan_status = 0  -- All treatment plans are currently Active (status = 0)
        and treatment_plan_date >= current_date - interval '2 years'
        and current_date - treatment_plan_date > 90  -- Only include delayed treatment plans
),

adjustment_base as (
    select * from {{ ref('stg_opendental__adjustment') }}
    where adjustment_date >= current_date - interval '1 year'
        and adjustment_amount > 0
        and adjustment_direction in ('positive', 'negative')  -- Exclude zero adjustments
),

-- 2. Dimension data
provider_dimension as (
    select 
        provider_id,
        provider_last_name,
        provider_first_name,
        provider_preferred_name,
        provider_type_category,
        provider_specialty_category
    from {{ ref('dim_provider') }}
),

patient_dimension as (
    select 
        patient_id,
        age,
        gender,
        has_insurance_flag
    from {{ ref('dim_patient') }}
),

date_dimension as (
    select * from {{ ref('dim_date') }}
),

-- 3. Opportunity identification and calculations
MissedAppointments as (
    select 
        fa.appointment_date,
        fa.provider_id,
        fa.clinic_id,
        fa.patient_id,
        fa.appointment_id,
        'Missed Appointment' as opportunity_type,
        case 
            when fa.is_no_show then 'No Show'
            when fa.is_broken then 'Cancellation'
            else 'Other'
        end as opportunity_subtype,
        fa.scheduled_production_amount as lost_revenue,
        fa.appointment_length_minutes as lost_time_minutes,
        fa.procedure_codes as missed_procedures,
        fa.appointment_datetime as opportunity_datetime,
        
        -- Calculate notice period for cancellations
        case when fa.is_broken and fa.appointment_datetime is not null
            then extract(hours from fa.appointment_datetime - current_timestamp)
        end as cancellation_notice_hours,
        
        -- Recovery potential
        case 
            when fa.is_no_show then 'High'
            when fa.is_broken and extract(hours from fa.appointment_datetime - current_timestamp) < 24 then 'Medium'
            when fa.is_broken then 'Low'
            else 'None'
        end as recovery_potential
        
    from appointment_base fa
),

-- Note: UnfilledSlots CTE removed as the schedule table doesn't contain 
-- the slot_time, slot_minutes, or production_goal columns needed for this analysis
-- This functionality would need to be implemented differently based on actual data availability

ClaimRejections as (
    select 
        fc.claim_date as appointment_date,
        null::integer as provider_id,  -- No provider_id available in claim data
        null::integer as clinic_id,
        fc.patient_id,
        fc.claim_id as appointment_id,  -- Already integer from transform_id_columns
        'Claim Rejection' as opportunity_type,
        case 
            when fc.claim_status = 'Denied' then 'Insurance Denial'
            when fc.claim_status = 'Rejected' then 'Claim Rejection'
            else 'Processing Issue'
        end as opportunity_subtype,
        fc.billed_amount - fc.paid_amount as lost_revenue,
        null::integer as lost_time_minutes,
        array[fc.procedure_code]::text[] as missed_procedures,  -- Create array from single procedure code
        fc.claim_date as opportunity_datetime,
        null::numeric as cancellation_notice_hours,  -- Not applicable for claim rejections
        case 
            when fc.claim_status = 'Denied' and fc.patient_responsibility > 0 then 'High'
            when fc.claim_status = 'Rejected' then 'High'
            else 'Medium'
        end as recovery_potential
        
    from claim_base fc
),

TreatmentPlanDelays as (
    select 
        tp.treatment_plan_date as appointment_date,
        null::integer as provider_id,  -- No provider_id available in treatplan table
        null::integer as clinic_id,
        tp.patient_id,
        tp.treatment_plan_id as appointment_id,  -- Already integer from transform_id_columns
        'Treatment Plan Delay' as opportunity_type,
        case 
            when current_date - tp.treatment_plan_date > 180 then 'Very Delayed'
            when current_date - tp.treatment_plan_date > 90 then 'Delayed Start'
            else 'In Progress'
        end as opportunity_subtype,
        null::numeric as lost_revenue,  -- No total amount column available in treatplan
        null::integer as lost_time_minutes,
        null::text[] as missed_procedures,  -- No planned procedures column available
        tp.treatment_plan_date as opportunity_datetime,
        null::numeric as cancellation_notice_hours,  -- Not applicable for treatment plan delays
        case 
            when current_date - tp.treatment_plan_date > 180 then 'Low'
            when current_date - tp.treatment_plan_date > 90 then 'Medium'
            else 'High'
        end as recovery_potential
        
    from treatment_base tp
),

WriteOffs as (
    select 
        adj.adjustment_date as appointment_date,
        adj.provider_id,
        adj.clinic_id,
        adj.patient_id,
        adj.adjustment_id as appointment_id,  -- Already integer from transform_id_columns
        'Write Off' as opportunity_type,
        case 
            when adj.adjustment_direction = 'positive' then 'Credit Adjustment'
            when adj.adjustment_direction = 'negative' then 'Charge Adjustment'
            else 'Zero Adjustment'
        end as opportunity_subtype,
        abs(adj.adjustment_amount) as lost_revenue,  -- Use absolute value for lost revenue calculation
        null::integer as lost_time_minutes,
        null::text[] as missed_procedures,
        adj.adjustment_date as opportunity_datetime,
        null::numeric as cancellation_notice_hours,  -- Not applicable for write-offs
        case 
            when adj.adjustment_direction = 'positive' then 'Medium'  -- Credits may be recoverable
            when adj.adjustment_direction = 'negative' then 'Low'     -- Charges less likely to be recoverable
            else 'None'
        end as recovery_potential
        
    from adjustment_base adj
),

-- 4. Business logic enhancement and aggregation
opportunities_enhanced as (
    select 
        *,
        -- Time Analysis
        extract(hour from opportunity_datetime) as opportunity_hour,
        case 
            when extract(hour from opportunity_datetime) between 8 and 11 then 'Morning'
            when extract(hour from opportunity_datetime) between 12 and 16 then 'Afternoon'
            when extract(hour from opportunity_datetime) between 17 and 19 then 'Evening'
            else 'Other'
        end as time_period,
        
        -- Financial Impact Categories
        case 
            when lost_revenue = 0 then 'No Revenue Impact'
            when lost_revenue < 100 then 'Low Impact'
            when lost_revenue < 500 then 'Medium Impact'
            when lost_revenue < 1000 then 'High Impact'
            else 'Very High Impact'
        end as revenue_impact_category,
        
        -- Time Impact Analysis
        case 
            when lost_time_minutes is null then 'No Time Impact'
            when lost_time_minutes < 30 then 'Low Time Impact'
            when lost_time_minutes < 60 then 'Medium Time Impact'
            when lost_time_minutes < 120 then 'High Time Impact'
            else 'Very High Time Impact'
        end as time_impact_category,
        
        -- Recovery Timeline
        case 
            when recovery_potential = 'High' then 'Immediate'
            when recovery_potential = 'Medium' then '30 Days'
            when recovery_potential = 'Low' then '90 Days'
            else 'Unlikely'
        end as recovery_timeline,
        
        -- Priority Scoring (0-100)
        round((
            case 
                when lost_revenue >= 1000 then 40
                when lost_revenue >= 500 then 30
                when lost_revenue >= 200 then 20
                when lost_revenue >= 100 then 10
                else 0
            end +
            case 
                when recovery_potential = 'High' then 30
                when recovery_potential = 'Medium' then 20
                when recovery_potential = 'Low' then 10
                else 0
            end +
            case 
                when opportunity_type = 'Missed Appointment' then 20
                when opportunity_type = 'Claim Rejection' then 15
                when opportunity_type = 'Treatment Plan Delay' then 10
                when opportunity_type = 'Write Off' then 5
                else 0
            end +
            case 
                when appointment_date >= current_date - interval '30 days' then 10
                when appointment_date >= current_date - interval '90 days' then 5
                else 0
            end
        ), 0) as recovery_priority_score,
        
        -- Preventability Assessment
        case 
            when opportunity_type = 'Missed Appointment' and opportunity_subtype = 'No Show' then 'Preventable'
            when opportunity_type = 'Missed Appointment' and opportunity_subtype = 'Cancellation' then 'Partially Preventable'
            when opportunity_type = 'Treatment Plan Delay' then 'Preventable'
            when opportunity_type = 'Claim Rejection' then 'Partially Preventable'
            else 'Not Preventable'
        end as preventability,
        
        -- Boolean Flags
        case when lost_revenue > 0 then true else false end as has_revenue_impact,
        case when lost_time_minutes > 0 then true else false end as has_time_impact,
        case when recovery_potential in ('High', 'Medium') then true else false end as recoverable,
        case when appointment_date >= current_date - interval '30 days' then true else false end as recent_opportunity,
        case when opportunity_type = 'Missed Appointment' then true else false end as appointment_related,
        
        -- Days since opportunity
        current_date - appointment_date as days_since_opportunity,
        
        -- Estimated recovery amount (based on potential and type)
        round(lost_revenue::numeric * 
            case 
                when recovery_potential = 'High' then 0.8
                when recovery_potential = 'Medium' then 0.5
                when recovery_potential = 'Low' then 0.2
                else 0
            end, 2) as estimated_recoverable_amount
        
    from (
        -- Union all opportunity sources
        select * from MissedAppointments
        union all
        select * from ClaimRejections
        union all
        select * from TreatmentPlanDelays
        union all
        select * from WriteOffs
    ) all_opportunities
    where lost_revenue > 0 or lost_time_minutes > 0
),

-- 5. Final integration with dimensions
final as (
    select
        -- Keys and Dimensions
        dd.date_id,
        row_number() over (order by oe.appointment_date, oe.opportunity_type, oe.appointment_id) as opportunity_id,
        oe.appointment_date,
        oe.provider_id,
        oe.clinic_id,
        oe.patient_id,
        oe.appointment_id,
        
        -- Provider Information
        prov.provider_last_name,
        prov.provider_first_name,
        prov.provider_preferred_name,
        prov.provider_type_category,
        prov.provider_specialty_category,
        
        -- Patient Information (when applicable)
        pt.age as patient_age,
        pt.gender as patient_gender,
        pt.has_insurance_flag,
        case when pt.patient_id is not null then true else false end as patient_specific,
        
        -- Date Information
        dd.year,
        dd.month,
        dd.quarter,
        dd.day_name,
        dd.is_weekend,
        dd.is_holiday,
        
        -- Opportunity Details
        oe.opportunity_type,
        oe.opportunity_subtype,
        oe.lost_revenue,
        oe.lost_time_minutes,
        oe.missed_procedures,
        oe.opportunity_datetime,
        oe.recovery_potential,
        
        -- Enhanced Business Logic (from opportunities_enhanced)
        oe.opportunity_hour,
        oe.time_period,
        oe.revenue_impact_category,
        oe.time_impact_category,
        oe.recovery_timeline,
        oe.recovery_priority_score,
        oe.preventability,
        oe.has_revenue_impact,
        oe.has_time_impact,
        oe.recoverable,
        oe.recent_opportunity,
        oe.appointment_related,
        oe.days_since_opportunity,
        oe.estimated_recoverable_amount,
        
        -- Metadata
        {{ standardize_mart_metadata() }}
        
    from opportunities_enhanced oe
    inner join date_dimension dd
        on oe.appointment_date = dd.date_day
    left join provider_dimension prov
        on oe.provider_id = prov.provider_id  -- Left join since treatment plans don't have provider_id
    left join patient_dimension pt
        on oe.patient_id = pt.patient_id
)

select * from final