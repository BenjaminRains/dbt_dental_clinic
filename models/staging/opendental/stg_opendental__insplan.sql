{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'insplan') }}
    -- Removed date filter to include all insurance plans
    -- This ensures we have access to all plans that might be referenced by other models
    -- Plan status is tracked via is_hidden and hide_from_verify_list flags
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"PlanNum"', 'target': 'insurance_plan_id'},
            {'source': '"EmployerNum"', 'target': 'employer_id'},
            {'source': '"CarrierNum"', 'target': 'carrier_id'},
            {'source': '"FeeSched"', 'target': 'fee_schedule_id'},
            {'source': '"CopayFeeSched"', 'target': 'copay_fee_schedule_id'},
            {'source': '"AllowedFeeSched"', 'target': 'allowed_fee_schedule_id'},
            {'source': '"ManualFeeSchedNum"', 'target': 'manual_fee_schedule_id'}
        ]) }},
        
        -- Plan Details
        "GroupName" as group_name,
        "GroupNum" as group_number,
        "PlanNote" as plan_note,
        "PlanType" as plan_type,
        "DivisionNo" as division_number,
        "TrojanID" as trojan_id,
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"IsMedical"') }} as is_medical,
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"ShowBaseUnits"') }} as show_base_units,
        {{ convert_opendental_boolean('"CodeSubstNone"') }} as code_subst_none,
        {{ convert_opendental_boolean('"HideFromVerifyList"') }} as hide_from_verify_list,
        {{ convert_opendental_boolean('"HasPpoSubstWriteoffs"') }} as has_ppo_subst_writeoffs,
        {{ convert_opendental_boolean('"IsBlueBookEnabled"') }} as is_blue_book_enabled,
        {{ convert_opendental_boolean('"UseAltCode"') }} as use_alt_code,
        {{ convert_opendental_boolean('"ClaimsUseUCR"') }} as claims_use_ucr,
        
        -- Ortho Related
        "OrthoType" as ortho_type,
        "OrthoAutoProcFreq" as ortho_auto_proc_freq,
        "OrthoAutoProcCodeNumOverride" as ortho_auto_proc_code_num_override,
        "OrthoAutoFeeBilled" as ortho_auto_fee_billed,
        "OrthoAutoClaimDaysWait" as ortho_auto_claim_days_wait,
        
        -- Financial
        "PerVisitPatAmount" as per_visit_patient_amount,
        "PerVisitInsAmount" as per_visit_insurance_amount,
        
        -- Other ID Fields
        {{ transform_id_columns([
            {'source': '"ClaimFormNum"', 'target': 'claim_form_id'}
        ]) }},
        "FilingCode" as filing_code,
        "FilingCodeSubtype" as filing_code_subtype,
        "MonthRenew" as month_renew,
        "RxBIN" as rx_bin,
        "CobRule" as cob_rule,
        "SopCode" as sop_code,
        "BillingType" as billing_type,
        "ExclusionFeeRule" as exclusion_fee_rule,
        
        -- Date Fields
        {{ clean_opendental_date('"SecDateEntry"') }} as date_created,
        {{ clean_opendental_date('"SecDateTEdit"') }} as date_updated,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column='"SecUserNumEntry"'
        ) }}

    from source_data
)

select * from renamed_columns
