with source as (
    select * from {{ source('opendental', 'userod') }}
),

renamed as (
    select
        -- Primary key
        UserNum as user_id,
        
        -- Attributes
        UserName as username,
        IsHidden as is_hidden_flag,
        
        -- Relationships
        UserGroupNum as user_group_id,
        EmployeeNum as employee_id,
        ClinicNum as clinic_id,
        ProvNum as provider_id,
        UserNumCEMT as parent_user_id,
        
        -- Configuration
        TaskListInBox as task_list_inbox_id,
        AnesthProvType as anesthesia_provider_type_id,
        DefaultHidePopups as default_hide_popups_flag,
        ClinicIsRestricted as clinic_restricted_flag,
        InboxHidePopups as inbox_hide_popups_flag,
        DomainUser as domain_username,
        
        -- Security status (non-sensitive)
        PasswordIsStrong as password_is_strong_flag,
        IsPasswordResetRequired as password_reset_required_flag,
        FailedAttempts as login_failed_attempts,
        MobileWebPinFailedAttempts as mobile_pin_failed_attempts,
        
        -- Tracking
        DateTFail as last_failed_login_at,
        DateTLastLogin as last_successful_login_at,
        
        -- Identification
        BadgeId as badge_id
        
    from source
)

select * from renamed