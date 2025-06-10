{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'userod') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"UserNum"', 'target': 'user_id'},
            {'source': 'NULLIF("UserGroupNum", 0)', 'target': 'user_group_id'},
            {'source': 'NULLIF("EmployeeNum", 0)', 'target': 'employee_id'},
            {'source': 'NULLIF("ClinicNum", 0)', 'target': 'clinic_id'},
            {'source': 'NULLIF("ProvNum", 0)', 'target': 'provider_id'},
            {'source': 'NULLIF("UserNumCEMT", 0)', 'target': 'cemt_user_id'}
        ]) }},
        
        -- User Information
        {{ clean_opendental_string('"UserName"') }} as username,
        {{ clean_opendental_string('"Password"') }} as password_hash,
        {{ clean_opendental_string('"DomainUser"') }} as domain_username,
        {{ clean_opendental_string('"BadgeId"') }} as badge_id,
        
        -- Boolean Flags (mixed types: IsHidden is already boolean, others are smallint)
        "IsHidden"::boolean as is_hidden,  -- Already boolean in PostgreSQL
        {{ convert_opendental_boolean('"PasswordIsStrong"') }} as has_strong_password,
        {{ convert_opendental_boolean('"ClinicIsRestricted"') }} as is_clinic_restricted,
        {{ convert_opendental_boolean('"IsPasswordResetRequired"') }} as is_password_reset_required,
        {{ convert_opendental_boolean('"DefaultHidePopups"') }} as is_default_hide_popups,
        {{ convert_opendental_boolean('"InboxHidePopups"') }} as is_inbox_hide_popups,
        
        -- Task and Security Settings
        "TaskListInBox"::bigint as task_list_inbox,
        "AnesthProvType"::smallint as anesthesia_provider_type,
        
        -- Mobile and Web Access
        {{ clean_opendental_string('"MobileWebPin"') }} as mobile_web_pin,
        "MobileWebPinFailedAttempts"::integer as mobile_web_pin_failed_attempts,
        {{ clean_opendental_string('"EClipboardClinicalPin"') }} as eclipboard_clinical_pin,
        
        -- Security Tracking
        {{ clean_opendental_date('"DateTFail"') }} as last_failed_login_at,
        "FailedAttempts"::integer as failed_login_attempts,
        {{ clean_opendental_date('"DateTLastLogin"') }} as last_login_at,

        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
