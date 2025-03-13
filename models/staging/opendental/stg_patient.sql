with source as (
    select * from {{ source('opendental', 'patient') }}
),

renamed as (
    select
        -- Keys
        PatNum as patient_id,
        Guarantor as guarantor_id,
        PriProv as primary_provider_id,
        SecProv as secondary_provider_id,
        ClinicNum as clinic_id,
        FeeSched as fee_schedule_id,
        BillingType as billing_type_id,
        DiscountPlanNum as discount_plan_id,
        
        -- Patient demographics (minimizing PHI)
        CONCAT(LName, ', ', FName) as patient_name, -- Consider removing in production
        MiddleI as middle_initial,
        Preferred as preferred_name,
        City as city,
        State as state,
        Zip as zip,
        Gender as gender,
        County as county,
        Country as country,
        Language as language,
        
        -- Status and classification fields
        PatStatus as patient_status,
        Position as position_code,
        StudentStatus as student_status,
        SchoolName as school_name,
        GradeLevel as grade_level,
        Urgency as urgency,
        Premed as premedication_required,
        Ward as ward,
        Title as title,
        
        -- Contact preferences (important for operations)
        PreferConfirmMethod as preferred_confirmation_method,
        PreferContactMethod as preferred_contact_method,
        PreferRecallMethod as preferred_recall_method,
        TxtMsgOk as text_messaging_consent,
        PreferContactConfidential as prefer_confidential_contact,
        
        -- Financial fields
        EstBalance as estimated_balance,
        BalTotal as total_balance,
        Bal_0_30 as balance_0_30_days,
        Bal_31_60 as balance_31_60_days,
        Bal_61_90 as balance_61_90_days,
        BalOver90 as balance_over_90_days,
        InsEst as insurance_estimate,
        PayPlanDue as payment_plan_due,
        ChartNumber as chart_number,
        HasIns as has_insurance_flag,
        BillingCycleDay as billing_cycle_day,
        
        -- Dates
        BirthDate as birth_date,
        TIMESTAMPDIFF(YEAR, BirthDate, CURRENT_DATE()) as age,
        DateFirstVisit as first_visit_date,
        DateTimeDeceased as deceased_datetime,
        AdmitDate as admit_date,
        
        -- Scheduling preferences
        SchedBeforeTime as schedule_not_before_time,
        SchedAfterTime as schedule_not_after_time,
        SchedDayOfWeek as preferred_day_of_week,
        AskToArriveEarly as ask_to_arrive_early_minutes,
        
        -- Metadata and system fields
        SecDateEntry as created_at,
        DateTStamp as updated_at,
        SecUserNumEntry as created_by_user_id,
        PlannedIsDone as planned_treatment_complete,
        HasSuperBilling as has_super_billing,
        ResponsParty as responsible_party_id,
        SuperFamily as super_family_id
        
        -- NOTE: Explicitly excluded sensitive fields:
        -- SSN, Address, Address2, HmPhone, WkPhone, WirelessPhone, 
        -- Email, MedicaidID, SecurityHash
    from source
)

select * from renamed