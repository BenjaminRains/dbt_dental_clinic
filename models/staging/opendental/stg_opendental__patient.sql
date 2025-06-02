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
        case 
            when "Gender" = 0 then 'M'
            when "Gender" = 1 then 'F'
            else 'U'
        end as gender,
        "Language" as language,
        
        -- Status and Classification
        case 
            when "PatStatus" = 0 then 'Patient'
            when "PatStatus" = 1 then 'NonPatient'
            when "PatStatus" = 2 then 'Inactive'
            when "PatStatus" = 3 then 'Archived'
            when "PatStatus" = 4 then 'Deceased'
            when "PatStatus" = 5 then 'Deleted'
            else 'Unknown'
        end as patient_status,
        case 
            when "Position" = 0 then 'Default'
            when "Position" = 1 then 'House'
            when "Position" = 2 then 'Staff'
            when "Position" = 3 then 'VIP'
            when "Position" = 4 then 'Other'
            else 'Unknown'
        end as position_code,
        "StudentStatus" as student_status,
        case 
            when "Urgency" = 0 then 'Normal'
            when "Urgency" = 1 then 'High'
            else 'Unknown'
        end as urgency,
        case 
            when COALESCE("Premed", '0') = '0' then false
            when COALESCE("Premed", '0') = '1' then true
            else false
        end as premedication_required,
        
        -- Contact Preferences
        case 
            when "PreferConfirmMethod" = 0 then 'None'
            when "PreferConfirmMethod" = 2 then 'Email'
            when "PreferConfirmMethod" = 4 then 'Text'
            when "PreferConfirmMethod" = 8 then 'Phone'
            else 'Unknown'
        end as preferred_confirmation_method,
        case 
            when "PreferContactMethod" = 0 then 'None'
            when "PreferContactMethod" = 2 then 'Email'
            when "PreferContactMethod" = 3 then 'Mail'
            when "PreferContactMethod" = 4 then 'Phone'
            when "PreferContactMethod" = 5 then 'Text'
            when "PreferContactMethod" = 6 then 'Other'
            when "PreferContactMethod" = 8 then 'Portal'
            else 'Unknown'
        end as preferred_contact_method,
        case 
            when "PreferRecallMethod" = 0 then 'None'
            when "PreferRecallMethod" = 2 then 'Email'
            when "PreferRecallMethod" = 4 then 'Text'
            when "PreferRecallMethod" = 8 then 'Phone'
            else 'Unknown'
        end as preferred_recall_method,
        case 
            when COALESCE("TxtMsgOk", '0') = '0' then false
            when COALESCE("TxtMsgOk", '0') = '1' then true
            else false
        end as text_messaging_consent,
        case 
            when COALESCE("PreferContactConfidential", '0') = '0' then false
            when COALESCE("PreferContactConfidential", '0') = '1' then true
            else false
        end as prefer_confidential_contact,
        
        -- Financial Information
        "EstBalance" as estimated_balance,
        "BalTotal" as total_balance,
        "Bal_0_30" as balance_0_30_days,
        "Bal_31_60" as balance_31_60_days,
        "Bal_61_90" as balance_61_90_days,
        "BalOver90" as balance_over_90_days,
        "InsEst" as insurance_estimate,
        "PayPlanDue" as payment_plan_due,
        case 
            when COALESCE("HasIns", '0') = '0' then false
            when COALESCE("HasIns", '0') = '1' then true
            else false
        end as has_insurance_flag,
        "BillingCycleDay" as billing_cycle_day,
        
        -- Dates
        "Birthdate" as birth_date,
        DATE_PART('year', AGE("Birthdate"))::integer as age,
        "DateFirstVisit" as first_visit_date,
        case 
            when "DateTimeDeceased" = '0001-01-01 00:00:00.000' then null
            else "DateTimeDeceased"
        end as deceased_datetime,
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