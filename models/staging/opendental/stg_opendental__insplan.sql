{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'insplan') }}
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
        "IsMedical"::boolean as is_medical,
        "IsHidden"::boolean as is_hidden,
        "ShowBaseUnits"::boolean as show_base_units,
        "CodeSubstNone"::boolean as code_subst_none,
        "HideFromVerifyList"::boolean as hide_from_verify_list,
        "HasPpoSubstWriteoffs"::boolean as has_ppo_subst_writeoffs,
        "IsBlueBookEnabled"::boolean as is_blue_book_enabled,
        
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
        "UseAltCode"::boolean as use_alt_code,
        "ClaimsUseUCR"::boolean as claims_use_ucr,
        "FilingCode" as filing_code,
        "FilingCodeSubtype" as filing_code_subtype,
        "MonthRenew" as month_renew,
        "RxBIN" as rx_bin,
        "CobRule" as cob_rule,
        "SopCode" as sop_code,
        "BillingType" as billing_type,
        "ExclusionFeeRule" as exclusion_fee_rule,
        
        -- Audit Fields
        "SecDateEntry" as created_at,
        "SecDateTEdit" as updated_at

    from source
)

select * from renamed
