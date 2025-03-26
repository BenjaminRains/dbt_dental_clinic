with source as (
    select * 
    from {{ source('opendental', 'refattach') }}
    where "RefDate" >= '2023-01-01'
),

renamed as (
    select
        -- Primary Key
        "RefAttachNum" as ref_attach_num,
        
        -- Foreign Keys
        "ReferralNum" as referral_num,
        "PatNum" as patient_num,
        "ProcNum" as procedure_num,
        "ProvNum" as provider_num,
        
        -- Regular columns
        "ItemOrder" as item_order,
        "RefDate" as referral_date,
        "RefType" as referral_type,
        "RefToStatus" as referral_to_status,
        "Note" as note,
        "IsTransitionOfCare" as is_transition_of_care,
        "DateProcComplete" as procedure_completion_date,
        
        -- Meta columns
        "DateTStamp" as created_at

    from source
)

select * from renamed
