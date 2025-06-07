{{
    config(
        materialized='table',
        schema='marts',
        unique_key=['date_id', 'opportunity_id']
    )
}}

/*
Revenue Lost Mart - Missed revenue opportunity identification and analysis
This mart tracks and quantifies all forms of revenue leakage including appointment
no-shows, cancellations, unfilled time slots, and delayed treatment acceptance.

Key metrics:
- No-show and cancellation revenue impact
- Unfilled appointment slots and capacity loss
- Treatment plan acceptance delays
- Insurance claim rejections and adjustments
- Revenue recovery opportunities and timing
*/

with MissedAppointments as (
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
        
    from {{ ref('fact_appointment') }} fa
    where (fa.is_no_show = true or fa.is_broken = true)
        and fa.appointment_date >= current_date - interval '1 year'
        and fa.scheduled_production_amount > 0
),

UnfilledSlots as (
    select 
        cal.calendar_date as appointment_date,
        sch.provider_id,
        sch.clinic_id,
        null as patient_id,
        sch.schedule_id as appointment_id,
        'Unfilled Capacity' as opportunity_type,
        'Open Slot' as opportunity_subtype,
        sch.production_goal * (sch.slot_minutes / 60.0) as lost_revenue,
        sch.slot_minutes as lost_time_minutes,
        null as missed_procedures,
        cal.calendar_date + sch.slot_time as opportunity_datetime,
        'Medium' as recovery_potential
        
    from {{ ref('stg_opendental__schedule') }} sch
    inner join {{ ref('dim_date') }} cal
        on cal.date_actual between current_date - interval '90 days' and current_date
        and cal.is_weekend = false
        and cal.is_holiday = false
    left join {{ ref('fact_appointment') }} fa
        on sch.provider_id = fa.provider_id
        and cal.date_actual = fa.appointment_date
        and sch.slot_time = extract(time from fa.appointment_datetime)
    where fa.appointment_id is null  -- No appointment scheduled
        and sch.is_available = true
        and sch.slot_minutes >= 30  -- Minimum meaningful slot
),

ClaimRejections as (
    select 
        fc.claim_date as appointment_date,
        fc.provider_id,
        null as clinic_id,
        fc.patient_id,
        fc.claim_id as appointment_id,
        'Claim Rejection' as opportunity_type,
        case 
            when fc.claim_status = 'Denied' then 'Insurance Denial'
            when fc.claim_status = 'Rejected' then 'Claim Rejection'
            else 'Processing Issue'
        end as opportunity_subtype,
        fc.billed_amount - fc.paid_amount as lost_revenue,
        null as lost_time_minutes,
        array[fc.procedure_code] as missed_procedures,
        fc.claim_date as opportunity_datetime,
        case 
            when fc.claim_status = 'Denied' and fc.patient_responsibility > 0 then 'High'
            when fc.claim_status = 'Rejected' then 'High'
            else 'Medium'
        end as recovery_potential
        
    from {{ ref('fact_claim') }} fc
    where fc.claim_status in ('Denied', 'Rejected', 'Pending')
        and fc.claim_date >= current_date - interval '1 year'
        and fc.billed_amount > fc.paid_amount
        and fc.billed_amount > 0
),

TreatmentPlanDelays as (
    select 
        tp.treatment_plan_date as appointment_date,
        tp.provider_id,
        tp.clinic_id,
        tp.patient_id,
        tp.treatment_plan_id as appointment_id,
        'Treatment Plan Delay' as opportunity_type,
        case 
            when tp.treatment_plan_status = 'Inactive' then 'Not Accepted'
            when tp.treatment_plan_status = 'Active' and current_date - tp.treatment_plan_date > 90 then 'Delayed Start'
            else 'In Progress'
        end as opportunity_subtype,
        tp.treatment_plan_total_amount as lost_revenue,
        null as lost_time_minutes,
        tp.planned_procedures as missed_procedures,
        tp.treatment_plan_date as opportunity_datetime,
        case 
            when tp.treatment_plan_status = 'Inactive' then 'Medium'
            when current_date - tp.treatment_plan_date > 180 then 'Low'
            else 'High'
        end as recovery_potential
        
    from {{ ref('stg_opendental__treatmentplan') }} tp
    where tp.treatment_plan_status in ('Inactive', 'Active')
        and tp.treatment_plan_date >= current_date - interval '2 years'
        and tp.treatment_plan_total_amount > 0
        and (tp.treatment_plan_status = 'Inactive' 
             or (tp.treatment_plan_status = 'Active' and current_date - tp.treatment_plan_date > 90))
),

WriteOffs as (
    select 
        adj.adjustment_date as appointment_date,
        adj.provider_id,
        null as clinic_id,
        adj.patient_id,
        adj.adjustment_id as appointment_id,
        'Write Off' as opportunity_type,
        case 
            when adj.adjustment_type = 'Contractual' then 'Insurance Adjustment'
            when adj.adjustment_type = 'Bad Debt' then 'Uncollectable'
            when adj.adjustment_type = 'Courtesy' then 'Courtesy Adjustment'
            else 'Other Adjustment'
        end as opportunity_subtype,
        adj.adjustment_amount as lost_revenue,
        null as lost_time_minutes,
        null as missed_procedures,
        adj.adjustment_date as opportunity_datetime,
        case 
            when adj.adjustment_type = 'Bad Debt' then 'Low'
            when adj.adjustment_type = 'Courtesy' then 'None'
            else 'Medium'
        end as recovery_potential
        
    from {{ ref('stg_opendental__adjustment') }} adj
    where adj.adjustment_date >= current_date - interval '1 year'
        and adj.adjustment_amount > 0
        and adj.adjustment_type != 'Payment'
),

Final as (
    select
        -- Keys and Dimensions
        dd.date_id,
        row_number() over (order by rl.appointment_date, rl.opportunity_type, rl.appointment_id) as opportunity_id,
        rl.appointment_date,
        rl.provider_id,
        rl.clinic_id,
        rl.patient_id,
        rl.appointment_id,
        
        -- Provider Information
        prov.provider_name,
        prov.provider_type,
        prov.specialty,
        
        -- Patient Information (when applicable)
        pt.age as patient_age,
        pt.gender as patient_gender,
        pt.has_insurance_flag,
        
        -- Date Information
        dd.year,
        dd.month,
        dd.quarter,
        dd.day_name,
        dd.is_weekend,
        dd.is_holiday,
        
        -- Opportunity Details
        rl.opportunity_type,
        rl.opportunity_subtype,
        rl.lost_revenue,
        rl.lost_time_minutes,
        rl.missed_procedures,
        rl.opportunity_datetime,
        rl.recovery_potential,
        
        -- Time Analysis
        extract(hour from rl.opportunity_datetime) as opportunity_hour,
        case 
            when extract(hour from rl.opportunity_datetime) between 8 and 11 then 'Morning'
            when extract(hour from rl.opportunity_datetime) between 12 and 16 then 'Afternoon'
            when extract(hour from rl.opportunity_datetime) between 17 and 19 then 'Evening'
            else 'Other'
        end as time_period,
        
        -- Financial Impact Categories
        case 
            when rl.lost_revenue = 0 then 'No Revenue Impact'
            when rl.lost_revenue < 100 then 'Low Impact'
            when rl.lost_revenue < 500 then 'Medium Impact'
            when rl.lost_revenue < 1000 then 'High Impact'
            else 'Very High Impact'
        end as revenue_impact_category,
        
        -- Time Impact Analysis
        case 
            when rl.lost_time_minutes is null then 'No Time Impact'
            when rl.lost_time_minutes < 30 then 'Low Time Impact'
            when rl.lost_time_minutes < 60 then 'Medium Time Impact'
            when rl.lost_time_minutes < 120 then 'High Time Impact'
            else 'Very High Time Impact'
        end as time_impact_category,
        
        -- Recovery Timeline
        case 
            when rl.recovery_potential = 'High' then 'Immediate'
            when rl.recovery_potential = 'Medium' then '30 Days'
            when rl.recovery_potential = 'Low' then '90 Days'
            else 'Unlikely'
        end as recovery_timeline,
        
        -- Priority Scoring (0-100)
        round((
            case 
                when rl.lost_revenue >= 1000 then 40
                when rl.lost_revenue >= 500 then 30
                when rl.lost_revenue >= 200 then 20
                when rl.lost_revenue >= 100 then 10
                else 0
            end +
            case 
                when rl.recovery_potential = 'High' then 30
                when rl.recovery_potential = 'Medium' then 20
                when rl.recovery_potential = 'Low' then 10
                else 0
            end +
            case 
                when rl.opportunity_type = 'Missed Appointment' then 20
                when rl.opportunity_type = 'Claim Rejection' then 15
                when rl.opportunity_type = 'Treatment Plan Delay' then 10
                else 5
            end +
            case 
                when rl.appointment_date >= current_date - interval '30 days' then 10
                when rl.appointment_date >= current_date - interval '90 days' then 5
                else 0
            end
        ), 0) as recovery_priority_score,
        
        -- Preventability Assessment
        case 
            when rl.opportunity_type = 'Missed Appointment' and rl.opportunity_subtype = 'No Show' then 'Preventable'
            when rl.opportunity_type = 'Missed Appointment' and rl.opportunity_subtype = 'Cancellation' then 'Partially Preventable'
            when rl.opportunity_type = 'Unfilled Capacity' then 'Preventable'
            when rl.opportunity_type = 'Treatment Plan Delay' then 'Preventable'
            when rl.opportunity_type = 'Claim Rejection' then 'Partially Preventable'
            else 'Not Preventable'
        end as preventability,
        
        -- Boolean Flags
        case when rl.lost_revenue > 0 then true else false end as has_revenue_impact,
        case when rl.lost_time_minutes > 0 then true else false end as has_time_impact,
        case when rl.recovery_potential in ('High', 'Medium') then true else false end as recoverable,
        case when rl.appointment_date >= current_date - interval '30 days' then true else false end as recent_opportunity,
        case when rl.opportunity_type = 'Missed Appointment' then true else false end as appointment_related,
        case when pt.patient_id is not null then true else false end as patient_specific,
        
        -- Days since opportunity
        current_date - rl.appointment_date as days_since_opportunity,
        
        -- Estimated recovery amount (based on potential and type)
        round(rl.lost_revenue * 
            case 
                when rl.recovery_potential = 'High' then 0.8
                when rl.recovery_potential = 'Medium' then 0.5
                when rl.recovery_potential = 'Low' then 0.2
                else 0
            end, 2) as estimated_recoverable_amount,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from (
        -- Union all opportunity sources
        select * from MissedAppointments
        union all
        select * from UnfilledSlots
        union all
        select * from ClaimRejections
        union all
        select * from TreatmentPlanDelays
        union all
        select * from WriteOffs
    ) rl
    inner join {{ ref('dim_date') }} dd
        on rl.appointment_date = dd.date_actual
    inner join {{ ref('dim_provider') }} prov
        on rl.provider_id = prov.provider_id
    left join {{ ref('dim_patient') }} pt
        on rl.patient_id = pt.patient_id
    where rl.lost_revenue > 0 or rl.lost_time_minutes > 0
)

select * from Final
