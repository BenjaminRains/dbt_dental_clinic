{{ config(
    materialized='incremental',
    unique_key='procedure_id',
    schema='staging'
) }}

with source_data as (
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
        and "DateTStamp" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"ProcNum"', 'target': 'procedure_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"ProvNum"', 'target': 'provider_id'},
            {'source': '"AptNum"', 'target': 'appointment_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'},
            {'source': '"CodeNum"', 'target': 'procedure_code_id'},
            {'source': '"SiteNum"', 'target': 'site_id'},
            {'source': '"PlannedAptNum"', 'target': 'planned_appointment_id'},
            {'source': '"ProcNumLab"', 'target': 'procedure_lab_id'},
            {'source': '"BillingTypeOne"', 'target': 'billing_type_one_id'},
            {'source': '"BillingTypeTwo"', 'target': 'billing_type_two_id'},
            {'source': '"StatementNum"', 'target': 'statement_id'},
            {'source': '"RepeatChargeNum"', 'target': 'repeat_charge_id'},
            {'source': '"ProvOrderOverride"', 'target': 'provider_order_override_id'},
            {'source': '"SecUserNumEntry"', 'target': 'sec_user_entry_id'},
            {'source': '"OrderingReferralNum"', 'target': 'ordering_referral_id'}
        ]) }},
        
        -- Date fields using macro
        {{ clean_opendental_date('"ProcDate"') }} as procedure_date,
        {{ clean_opendental_date('"DateOriginalProsth"') }} as date_original_prosthesis,
        {{ clean_opendental_date('"DateEntryC"') }} as date_entry,
        {{ clean_opendental_date('"DateTP"') }} as date_treatment_plan,
        {{ clean_opendental_date('"SecDateEntry"') }} as sec_date_entry,
        {{ clean_opendental_date('"DateComplete"') }} as date_complete,
        
        -- Time fields
        "ProcTime" as procedure_time,
        "ProcTimeEnd" as procedure_time_end,
        "StartTime" as start_time,
        "StopTime" as stop_time,
        
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
        "DiagnosticCode2" as diagnostic_code_2,
        "DiagnosticCode3" as diagnostic_code_3,
        "DiagnosticCode4" as diagnostic_code_4,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsPrincDiag"') }} as is_principal_diagnosis,
        {{ convert_opendental_boolean('"HideGraphics"') }} as hide_graphics,
        {{ convert_opendental_boolean('"IsLocked"') }} as is_locked,
        {{ convert_opendental_boolean('"IsDateProsthEst"') }} as is_date_prosthesis_estimated,
        {{ convert_opendental_boolean('"IsCpoe"') }} as is_cpoe,
        
        -- Code modifiers and additional fields
        "CodeMod1" as code_modifier_1,
        "CodeMod2" as code_modifier_2,
        "CodeMod3" as code_modifier_3,
        "CodeMod4" as code_modifier_4,
        "RevCode" as revenue_code,
        "UnitQty" as unit_quantity,
        "BaseUnits" as base_units,
        "CanadianTypeCodes" as canadian_type_codes,
        "Prognosis" as prognosis,
        "DrugUnit" as drug_unit,
        "DrugQty" as drug_quantity,
        "UnitQtyType" as unit_quantity_type,
        "BillingNote" as billing_note,
        "SnomedBodySite" as snomed_body_site,
        "Discount" as discount,
        "IcdVersion" as icd_version,
        "Urgency" as urgency,
        "TaxAmt" as tax_amount,
        "DiscountPlanAmt" as discount_plan_amount,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateEntryC"',
            updated_at_column='"DateTStamp"',
            created_by_column='"SecUserNumEntry"'
        ) }}

    from source_data
)

select * from renamed_columns
