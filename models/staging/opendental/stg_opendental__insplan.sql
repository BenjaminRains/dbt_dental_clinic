{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'insplan') }}
    -- Removed date filter to include all insurance plans
    -- This ensures we have access to all plans that might be referenced by other models
    -- Plan status is tracked via is_hidden and hide_from_verify_list flags
),

renamed as (
    select
        -- Primary Key
        "PlanNum" as insurance_plan_id,
        
        -- Foreign Keys
        "EmployerNum" as employer_id,
        "CarrierNum" as carrier_id,
        "SecUserNumEntry" as user_entry_id,
        
        -- Fee Schedule Related
        "FeeSched" as fee_schedule_id,
        "CopayFeeSched" as copay_fee_schedule_id,
        "AllowedFeeSched" as allowed_fee_schedule_id,
        "ManualFeeSchedNum" as manual_fee_schedule_id,
        
        -- Plan Details
        "GroupName" as group_name,
        "GroupNum" as group_number,
        "PlanNote" as plan_note,
        "PlanType" as plan_type,
        "DivisionNo" as division_number,
        "TrojanID" as trojan_id,
        
        -- Flags and Settings
        CASE 
            WHEN "IsMedical" = 1 THEN TRUE
            ELSE FALSE
        END as is_medical,
        CASE 
            WHEN "IsHidden" = 1 THEN TRUE
            ELSE FALSE
        END as is_hidden,
        "ShowBaseUnits"::boolean as show_base_units,
        "CodeSubstNone"::boolean as code_subst_none,
        CASE 
            WHEN "HideFromVerifyList" = 1 THEN TRUE
            ELSE FALSE
        END as hide_from_verify_list,
        CASE 
            WHEN "HasPpoSubstWriteoffs" = 1 THEN TRUE
            ELSE FALSE
        END as has_ppo_subst_writeoffs,
        CASE 
            WHEN "IsBlueBookEnabled" = 1 THEN TRUE
            ELSE FALSE
        END as is_blue_book_enabled,
        
        -- Ortho Related
        "OrthoType" as ortho_type,
        "OrthoAutoProcFreq" as ortho_auto_proc_freq,
        "OrthoAutoProcCodeNumOverride" as ortho_auto_proc_code_num_override,
        "OrthoAutoFeeBilled" as ortho_auto_fee_billed,
        "OrthoAutoClaimDaysWait" as ortho_auto_claim_days_wait,
        
        -- Financial
        "PerVisitPatAmount" as per_visit_patient_amount,
        "PerVisitInsAmount" as per_visit_insurance_amount,
        
        -- Other Fields
        "ClaimFormNum" as claim_form_id,
        CASE 
            WHEN "UseAltCode" = 1 THEN TRUE
            ELSE FALSE
        END as use_alt_code,
        CASE 
            WHEN "ClaimsUseUCR" = 1 THEN TRUE
            ELSE FALSE
        END as claims_use_ucr,
        "FilingCode" as filing_code,
        "FilingCodeSubtype" as filing_code_subtype,
        "MonthRenew" as month_renew,
        "RxBIN" as rx_bin,
        "CobRule" as cob_rule,
        "SopCode" as sop_code,
        "BillingType" as billing_type,
        "ExclusionFeeRule" as exclusion_fee_rule,
        
        -- Required Metadata Columns
        current_timestamp as _loaded_at,
        "SecDateEntry" as _created_at,
        "SecDateTEdit" as _updated_at

    from source
)

select * from renamed
