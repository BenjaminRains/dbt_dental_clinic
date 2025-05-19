with source as (
    select * 
    from {{ source('opendental', 'userod') }}
),

renamed as (
    select
        -- Primary Key
        "UserNum" as user_id,
        
        -- Foreign Keys
        "UserGroupNum" as user_group_id,
        "EmployeeNum" as employee_id,
        "ClinicNum" as clinic_id,
        "ProvNum" as provider_id,
        "UserNumCEMT" as cemt_user_id,
        
        -- User Information
        "UserName" as username,
        "Password" as password_hash,
        "DomainUser" as domain_username,
        "BadgeId" as badge_id,
        
        -- Boolean Flags
        "IsHidden" as is_hidden,
        "PasswordIsStrong" as has_strong_password,
        "ClinicIsRestricted" as is_clinic_restricted,
        "IsPasswordResetRequired" as is_password_reset_required,
        
        -- Security and Access
        "TaskListInBox" as task_list_inbox,
        "AnesthProvType" as anesthesia_provider_type,
        "DefaultHidePopups" as is_default_hide_popups,
        "InboxHidePopups" as is_inbox_hide_popups,
        
        -- Mobile and Web Access
        "MobileWebPin" as mobile_web_pin,
        "MobileWebPinFailedAttempts" as mobile_web_pin_failed_attempts,
        "EClipboardClinicalPin" as eclipboard_clinical_pin,
        
        -- Security Tracking
        "DateTFail" as last_failed_login_at,
        "FailedAttempts" as failed_login_attempts,
        "DateTLastLogin" as last_login_at

    from source
)

select * from renamed
