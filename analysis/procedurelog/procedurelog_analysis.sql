with source as (
    select * from {{ source('opendental', 'procedurelog') }}
),

renamed as (
    select
        -- Primary key
        ProcNum as procedure_id,

        -- Relationships
        PatNum as patient_id,
        AptNum as appointment_id,
        PlannedAptNum as planned_appointment_id,
        ProvNum as provider_id,
        ClinicNum as clinic_id,
        ProcNumLab as lab_procedure_id,
        StatementNum as statement_id,
        RepeatChargeNum as repeat_charge_id,
        ProvOrderOverride as provider_order_override_id,
        OrderingReferralNum as ordering_referral_id,
        SecUserNumEntry as created_by_user_id,

        -- Procedure identification
        CodeNum as procedure_code_id,
        OldCode as old_procedure_code,
        MedicalCode as medical_code,
        RevCode as revenue_code,

        -- Clinical details
        ToothNum as tooth_number,
        ToothRange as tooth_range,
        Surf as surface,
        SnomedBodySite as body_site,
        Urgency as urgency,
        Prognosis as prognosis,

        -- Diagnostic information
        DiagnosticCode as diagnostic_code,
        DiagnosticCode2 as diagnostic_code_2,
        DiagnosticCode3 as diagnostic_code_3,
        DiagnosticCode4 as diagnostic_code_4,
        IsPrincDiag as is_principal_diagnosis_flag,
        IcdVersion as icd_version,

        -- Procedure modifiers
        CodeMod1 as modifier_1,
        CodeMod2 as modifier_2,
        CodeMod3 as modifier_3,
        CodeMod4 as modifier_4,

        -- Financial
        ProcFee as procedure_fee,
        Discount as discount_amount,
        DiscountPlanAmt as discount_plan_amount,
        TaxAmt as tax_amount,
        BillingTypeOne as billing_type_1_id,
        BillingTypeTwo as billing_type_2_id,
        BillingNote as billing_note,

        -- Status and tracking
        ProcStatus as procedure_status,
        Priority as priority,
        IsLocked as is_locked_flag,
        HideGraphics as hide_graphics_flag,
        IsCpoe as is_computerized_order_flag,

        -- Timing
        ProcDate as procedure_date,
        DateComplete as completion_date,
        DateTP as treatment_plan_date,
        ProcTime as procedure_start_time,
        ProcTimeEnd as procedure_end_time,
        StartTime as start_minutes,
        StopTime as stop_minutes,

        -- Units and quantities
        UnitQty as unit_quantity,
        UnitQtyType as unit_quantity_type,
        BaseUnits as base_units,
        DrugUnit as drug_unit,
        DrugQty as drug_quantity,

        -- Prosthetic information
        Prosthesis as prosthesis_code,
        DateOriginalProsth as original_prosthesis_date,
        IsDateProsthEst as is_prosthesis_date_estimated_flag,

        -- Service details
        PlaceService as place_of_service,
        CanadianTypeCodes as canadian_type_codes,

        -- Notes
        ClaimNote as claim_note,

        -- Metadata
        DateEntryC as entry_date,
        SecDateEntry as created_at,
        DateTStamp as updated_at
    from source
)

select * from renamed