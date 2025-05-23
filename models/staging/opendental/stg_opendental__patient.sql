{{ config(
    materialized='table'
) }}

with source as (
    select * from {{ source('opendental', 'patient') }}
),

staged as (
    select
        -- Primary Key
        "PatNum" as patient_id,
        
        -- Relationships
        "Guarantor" as guarantor_id,
        "PriProv" as primary_provider_id,
        "SecProv" as secondary_provider_id,
        "ClinicNum" as clinic_id,
        "FeeSched" as fee_schedule_id,
        
        -- Demographics
        "MiddleI" as middle_initial,
        "Preferred" as preferred_name,
        "Gender" as gender,
        "Language" as language,
        
        -- Status and Classification
        "PatStatus" as patient_status,
        "Position" as position_code,
        "StudentStatus" as student_status,
        "Urgency" as urgency,
        "Premed" as premedication_required,
        
        -- Contact Preferences
        "PreferConfirmMethod" as preferred_confirmation_method,
        "PreferContactMethod" as preferred_contact_method,
        "PreferRecallMethod" as preferred_recall_method,
        "TxtMsgOk" as text_messaging_consent,
        "PreferContactConfidential" as prefer_confidential_contact,
        
        -- Financial Information
        "EstBalance" as estimated_balance,
        "BalTotal" as total_balance,
        "Bal_0_30" as balance_0_30_days,
        "Bal_31_60" as balance_31_60_days,
        "Bal_61_90" as balance_61_90_days,
        "BalOver90" as balance_over_90_days,
        "InsEst" as insurance_estimate,
        "PayPlanDue" as payment_plan_due,
        "HasIns" as has_insurance_flag,
        "BillingCycleDay" as billing_cycle_day,
        
        -- Dates
        "Birthdate" as birth_date,
        DATE_PART('year', AGE("Birthdate"))::integer as age,
        "DateFirstVisit" as first_visit_date,
        "DateTimeDeceased" as deceased_datetime,
        "AdmitDate" as admit_date,
        
        -- Scheduling Preferences
        "SchedBeforeTime" as schedule_not_before_time,
        "SchedAfterTime" as schedule_not_after_time,
        "SchedDayOfWeek" as preferred_day_of_week,
        "AskToArriveEarly" as ask_to_arrive_early_minutes,
        
        -- Business Logic
        "PlannedIsDone" as planned_treatment_complete,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        case 
            when "SecDateEntry" = '0001-01-01' then null
            else "SecDateEntry"
        end as _created_at,   -- When the record was created in source
        "DateTStamp" as _updated_at,     -- Last update timestamp
        "SecUserNumEntry" as _created_by_user_id  -- User who created the record

    from source
)

select * from staged