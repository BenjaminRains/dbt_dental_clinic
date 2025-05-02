with source as (
    select * from {{ source('opendental', 'procedurecode') }}
),

renamed as (
    select
        -- Primary key
        CodeNum as procedure_code_id,

        -- Code identifiers
        ProcCode as procedure_code,
        AlternateCode1 as alternate_code,
        MedicalCode as medical_code,
        SubstitutionCode as substitution_code,
        DrugNDC as drug_ndc_code,
        RevenueCodeDefault as default_revenue_code,
        TaxCode as tax_code,
        DiagnosticCodes as diagnostic_codes,

        -- Descriptions
        Descript as description,
        AbbrDesc as abbreviated_description,
        LaymanTerm as layman_terms,
        PaintText as paint_text,

        -- Procedure attributes
        ProcTime as procedure_time,
        ProcCat as procedure_category_id,
        TreatArea as treatment_area,
        GTypeNum as graphic_type_id,
        BaseUnits as base_units,
        CanadaTimeUnits as canada_time_units,

        -- Configuration flags
        NoBillIns as no_bill_insurance_flag,
        IsProsth as is_prosthetic_flag,
        IsHygiene as is_hygiene_flag,
        IsTaxed as is_taxed_flag,
        IsCanadianLab as is_canadian_lab_flag,
        PreExisting as is_pre_existing_flag,
        IsMultiVisit as is_multi_visit_flag,
        IsRadiology as is_radiology_flag,
        BypassGlobalLock as bypass_global_lock_flag,
        AreaAlsoToothRange as area_also_tooth_range_flag,

        -- Visual settings
        PaintType as paint_type,
        GraphicColor as graphic_color,

        -- Default values
        ProvNumDefault as default_provider_id,
        SubstOnlyIf as substitution_condition,
        DefaultNote as default_note,
        DefaultClaimNote as default_claim_note,
        DefaultTPNote as default_treatment_plan_note,

        -- Metadata
        DateTStamp as updated_at
    from source
)

select * from renamed