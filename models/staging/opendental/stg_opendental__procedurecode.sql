{{ config(
    materialized='incremental',
    unique_key='procedure_code_id',
    schema='staging'
) }}

with source as (
    select * from {{ source('opendental', 'procedurecode') }}
    where "DateTStamp" >= '2023-01-01'::timestamp
    
    {% if is_incremental() %}
    AND "DateTStamp"::timestamp > (select max(date_timestamp) from {{ this }})
    {% endif %}
),

with_prefixes as (
    select 
        *,
        -- Extract just the prefix (D0, D1, etc.)
        REGEXP_SUBSTR(procedure_code, '^D[0-9]') as code_prefix
    from source
),

renamed as (
    select
        -- Primary Key
        "CodeNum"::integer as procedure_code_id,
        
        -- Attributes
        "ProcCode"::varchar as procedure_code,
        "Descript"::varchar as description,
        "AbbrDesc"::varchar as abbreviated_description,
        "ProcTime"::varchar as procedure_time,
        "ProcCat"::bigint as procedure_category_id,
        "TreatArea"::smallint as treatment_area,
        "NoBillIns"::smallint as no_bill_insurance_flag,
        "IsProsth"::smallint as is_prosthetic_flag,
        "DefaultNote"::text as default_note,
        "IsHygiene"::smallint as is_hygiene_flag,
        "GTypeNum"::smallint as graphic_type_num,
        "AlternateCode1"::varchar as alternate_code1,
        "MedicalCode"::varchar as medical_code,
        "IsTaxed"::smallint as is_taxed_flag,
        "PaintType"::smallint as paint_type,
        "GraphicColor"::integer as graphic_color,
        "LaymanTerm"::varchar as layman_term,
        "IsCanadianLab"::smallint as is_canadian_lab_flag,
        "PreExisting"::boolean as pre_existing_flag,
        "BaseUnits"::integer as base_units,
        "SubstitutionCode"::varchar as substitution_code,
        "SubstOnlyIf"::integer as substitution_only_if,
        "DateTStamp"::timestamp as date_timestamp,
        "IsMultiVisit"::smallint as is_multi_visit_flag,
        "DrugNDC"::varchar as drug_ndc,
        "RevenueCodeDefault"::varchar as default_revenue_code,
        "ProvNumDefault"::bigint as default_provider_id,
        "CanadaTimeUnits"::float as canada_time_units,
        "IsRadiology"::smallint as is_radiology_flag,
        "DefaultClaimNote"::text as default_claim_note,
        "DefaultTPNote"::text as default_treatment_plan_note,
        "BypassGlobalLock"::smallint as bypass_global_lock_flag,
        "TaxCode"::varchar as tax_code,
        "PaintText"::varchar as paint_text,
        "AreaAlsoToothRange"::smallint as area_also_tooth_range_flag,
        "DiagnosticCodes"::varchar as diagnostic_codes,
        
        -- Metadata
        current_timestamp as _loaded_at,
        code_prefix
    from with_prefixes
)

select * from renamed
