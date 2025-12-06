{{ config(
    materialized='table'
) }}

with source_data as (
    select * from {{ source('opendental', 'patient') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"Guarantor"', 'target': 'guarantor_id'},
            {'source': '"PriProv"', 'target': 'primary_provider_id'},
            {'source': '"SecProv"', 'target': 'secondary_provider_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'},
            {'source': '"FeeSched"', 'target': 'fee_schedule_id'}
        ]) }},
        
        -- Demographics
        "Gender" as gender,
        "Language" as language,
        
        -- Status and Classification
        "PatStatus" as patient_status,
        "Position" as position_code,
        "StudentStatus" as student_status,
        "Urgency" as urgency,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"Premed"') }} as premedication_required,
        {{ convert_opendental_boolean('"TxtMsgOk"') }} as text_messaging_consent,
        {{ convert_opendental_boolean('"PreferContactConfidential"') }} as prefer_confidential_contact,
        {{ convert_opendental_boolean('"HasIns"') }} as has_insurance_flag,
        
        -- Contact Preferences
        "PreferConfirmMethod" as preferred_confirmation_method,
        "PreferContactMethod" as preferred_contact_method,
        "PreferRecallMethod" as preferred_recall_method,
        
        -- Financial Information
        "EstBalance" as estimated_balance,
        "BalTotal" as total_balance,
        "Bal_0_30" as balance_0_30_days,
        "Bal_31_60" as balance_31_60_days,
        "Bal_61_90" as balance_61_90_days,
        "BalOver90" as balance_over_90_days,
        "InsEst" as insurance_estimate,
        "PayPlanDue" as payment_plan_due,
        "BillingCycleDay" as billing_cycle_day,
        
        -- Date fields using macro (includes age calculation)
        "Birthdate" as birth_date,
        DATE_PART('year', AGE("Birthdate"))::integer as age,
        "DateFirstVisit" as first_visit_date,
        {{ clean_opendental_date('"DateTimeDeceased"') }} as deceased_datetime,
        "AdmitDate" as admit_date,
        
        -- Scheduling Preferences
        "SchedBeforeTime" as schedule_not_before_time,
        "SchedAfterTime" as schedule_not_after_time,
        "SchedDayOfWeek"::integer as preferred_day_of_week,
        "AskToArriveEarly" as ask_to_arrive_early_minutes,
        
        -- Business Logic
        "PlannedIsDone" as planned_treatment_complete,

        -- date fields
        {{ clean_opendental_date('"SecDateEntry"') }} as date_created,
        {{ clean_opendental_date('"DateTStamp"') }} as date_updated,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"DateTStamp"',
            created_by_column='"SecUserNumEntry"'
        ) }}

    from source_data
)

select * from renamed_columns
