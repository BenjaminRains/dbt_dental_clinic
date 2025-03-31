{{ config(
    materialized='incremental',
    unique_key='"ProcNum"'
) }}

with source as (
    select * from {{ source('opendental', 'procedurelog') }}
    where "ProcDate" >= '2023-01-01'
    or "ProcNum" in (
        select i."ProcNum" 
        from {{ source('opendental', 'insbluebook') }} i
        where i."DateTEntry" >= '2023-01-01'
    )
    or "ProcNum" in (
        select "ProcNum"
        from {{ source('opendental', 'procgroupitem') }}
    )
    or "ProcNum" in (
        select "ProcNum"
        from {{ source('opendental', 'procnote') }}
        where "EntryDateTime" >= '2023-01-01'
    )
    {% if is_incremental() %}
        and "DateTStamp" > (select max(date_timestamp) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "ProcNum" as procedure_id,
        
        -- Foreign keys
        "PatNum" as patient_id,
        "ProvNum" as provider_id,
        "AptNum" as appointment_id,
        "ClinicNum" as clinic_id,
        "CodeNum" as procedure_code_id,
        "SiteNum" as site_id,
        "PlannedAptNum" as planned_appointment_id,
        "ProcNumLab" as procedure_lab_id,
        "BillingTypeOne" as billing_type_one_id,
        "BillingTypeTwo" as billing_type_two_id,
        "StatementNum" as statement_id,
        "RepeatChargeNum" as repeat_charge_id,
        "ProvOrderOverride" as provider_order_override_id,
        "SecUserNumEntry" as sec_user_entry_id,
        "OrderingReferralNum" as ordering_referral_id,
        
        -- Date and time fields
        "ProcDate" as procedure_date,
        "DateOriginalProsth" as date_original_prosthesis,
        "DateEntryC" as date_entry,
        "DateTP" as date_treatment_plan,
        "DateTStamp" as date_timestamp,
        "SecDateEntry" as sec_date_entry,
        "DateComplete" as date_complete,
        "ProcTime" as procedure_time,
        "ProcTimeEnd" as procedure_time_end,
        
        -- Procedure details
        "ProcFee" as procedure_fee,
        "OldCode" as old_code,
        "Surf" as surface,
        "ToothNum" as tooth_number,
        "ToothRange" as tooth_range,
        "Priority" as priority,
        "ProcStatus" as procedure_status,
        "Dx" as diagnosis,
        "PlaceService" as place_service,
        "Prosthesis" as prosthesis,
        "ClaimNote" as claim_note,
        "MedicalCode" as medical_code,
        "DiagnosticCode" as diagnostic_code,
        "IsPrincDiag" as is_principal_diagnosis,
        "CodeMod1" as code_modifier_1,
        "CodeMod2" as code_modifier_2,
        "CodeMod3" as code_modifier_3,
        "CodeMod4" as code_modifier_4,
        "RevCode" as revenue_code,
        "UnitQty" as unit_quantity,
        "BaseUnits" as base_units,
        "StartTime" as start_time,
        "StopTime" as stop_time,
        "HideGraphics" as hide_graphics,
        "CanadianTypeCodes" as canadian_type_codes,
        "Prognosis" as prognosis,
        "DrugUnit" as drug_unit,
        "DrugQty" as drug_quantity,
        "UnitQtyType" as unit_quantity_type,
        "IsLocked" as is_locked,
        "BillingNote" as billing_note,
        "SnomedBodySite" as snomed_body_site,
        "DiagnosticCode2" as diagnostic_code_2,
        "DiagnosticCode3" as diagnostic_code_3,
        "DiagnosticCode4" as diagnostic_code_4,
        "Discount" as discount,
        "IsDateProsthEst" as is_date_prosthesis_estimated,
        "IcdVersion" as icd_version,
        "IsCpoe" as is_cpoe,
        "Urgency" as urgency,
        "TaxAmt" as tax_amount,
        "DiscountPlanAmt" as discount_plan_amount,
        
        -- Metadata
        '{{ invocation_id }}' as _airbyte_ab_id,
        current_timestamp as _airbyte_loaded_at
    from source
)

select * from renamed
