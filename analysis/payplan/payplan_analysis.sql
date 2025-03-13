with source as (
    select * from {{ source('opendental', 'payplan') }}
),

renamed as (
    select
        -- Primary key
        PayPlanNum as payment_plan_id,

        -- Relationships
        PatNum as patient_id,
        Guarantor as guarantor_id,
        PlanNum as plan_id,
        InsSubNum as insurance_subscriber_id,
        PlanCategory as plan_category_id,
        MobileAppDeviceNum as mobile_device_id,

        -- Plan dates
        PayPlanDate as plan_created_date,
        DatePayPlanStart as plan_start_date,
        DateInterestStart as interest_start_date,

        -- Financial details
        APR as annual_percentage_rate,
        CompletedAmt as completed_amount,
        PayAmt as payment_amount,
        DownPayment as down_payment_amount,

        -- Payment configuration
        PaySchedule as payment_schedule,
        NumberOfPayments as number_of_payments,
        ChargeFrequency as charge_frequency,

        -- Status flags
        IsClosed as is_closed_flag,
        IsDynamic as is_dynamic_flag,
        IsLocked as is_locked_flag,
        DynamicPayPlanTPOption as dynamic_treatment_plan_option,

        -- Security and signatures
        SigIsTopaz as is_topaz_signature_flag,
        SecurityHash as security_hash,
        Signature as signature_data,

        -- Notes (may contain PHI)
        Note as plan_notes
    from source
)

select * from renamed