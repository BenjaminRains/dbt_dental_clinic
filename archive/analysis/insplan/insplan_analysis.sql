with source as (
    select * from {{ source('opendental', 'insplan') }}
),

renamed as (
    select
        -- Primary key
        PlanNum as insurance_plan_id,

        -- Relationships
        CarrierNum as carrier_id,
        EmployerNum as employer_id,
        FeeSched as fee_schedule_id,
        CopayFeeSched as copay_fee_schedule_id,
        AllowedFeeSched as allowed_fee_schedule_id,
        ManualFeeSchedNum as manual_fee_schedule_id,
        ClaimFormNum as claim_form_id,
        BillingType as billing_type_id,
        FilingCode as filing_code_id,
        FilingCodeSubtype as filing_code_subtype_id,

        -- Plan identification
        GroupName as group_name,
        GroupNum as group_number,
        DivisionNo as division_number,
        TrojanID as trojan_id,

        -- Plan configuration
        PlanType as plan_type,
        IsMedical as is_medical,
        IsHidden as is_hidden,
        MonthRenew as renewal_month,
        HideFromVerifyList as hide_from_verify_list,

        -- Fee and claim options
        UseAltCode as use_alternate_codes,
        ClaimsUseUCR as claims_use_ucr,
        ShowBaseUnits as show_base_units,
        CodeSubstNone as code_substitution_none,
        CobRule as coordination_of_benefits_rule,
        HasPpoSubstWriteoffs as has_ppo_substitute_writeoffs,
        ExclusionFeeRule as exclusion_fee_rule,
        IsBlueBookEnabled as is_bluebook_enabled,
        InsPlansZeroWriteOffsOnAnnualMaxOverride as zero_writeoffs_on_annual_max_override,
        InsPlansZeroWriteOffsOnFreqOrAgingOverride as zero_writeoffs_on_freq_or_aging_override,

        -- Per visit payments
        PerVisitPatAmount as per_visit_patient_amount,
        PerVisitInsAmount as per_visit_insurance_amount,

        -- Orthodontic options
        OrthoType as orthodontic_type,
        OrthoAutoProcFreq as orthodontic_auto_procedure_frequency,
        OrthoAutoProcCodeNumOverride as orthodontic_auto_procedure_code_override,
        OrthoAutoFeeBilled as orthodontic_auto_fee_billed,
        OrthoAutoClaimDaysWait as orthodontic_auto_claim_days_wait,

        -- Canadian-specific fields
        CanadianPlanFlag as canadian_plan_flag,
        CanadianDiagnosticCode as canadian_diagnostic_code,
        CanadianInstitutionCode as canadian_institution_code,
        DentaideCardSequence as dentaide_card_sequence,

        -- Prescription information
        RxBIN as rx_bin,
        SopCode as sop_code,

        -- Metadata
        SecUserNumEntry as created_by_user_id,
        SecDateEntry as created_at,
        SecDateTEdit as updated_at

        -- Excluded fields with potential PHI:
        -- PlanNote (may contain patient-specific information)
    from source
)

select * from renamed