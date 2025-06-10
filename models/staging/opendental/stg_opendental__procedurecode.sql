{{ config(
    materialized='incremental',
    unique_key='procedure_code_id',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'procedurecode') }}
    
    {% if is_incremental() %}
    AND "DateTStamp"::timestamp > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"CodeNum"', 'target': 'procedure_code_id'},
            {'source': 'NULLIF("ProcCat", 0)', 'target': 'procedure_category_id'},
            {'source': 'NULLIF("GTypeNum", 0)', 'target': 'graphic_type_id'},
            {'source': 'NULLIF("ProvNumDefault", 0)', 'target': 'default_provider_id'}
        ]) }},
        
        -- Core procedure attributes
        "ProcCode"::varchar as procedure_code,
        -- Extracts D-prefix only for CDT codes, will be NULL for numeric procedure codes
        REGEXP_SUBSTR("ProcCode", '^D[0-9]') as code_prefix,
        nullif(trim("Descript"), '') as description,
        nullif(trim("AbbrDesc"), '') as abbreviated_description,
        nullif(trim("ProcTime"), '') as procedure_time,
        nullif(trim("DefaultNote"), '') as default_note,
        nullif(trim("LaymanTerm"), '') as layman_term,
        nullif(trim("DefaultClaimNote"), '') as default_claim_note,
        nullif(trim("DefaultTPNote"), '') as default_treatment_plan_note,
        
        -- Classification and treatment fields
        "TreatArea"::smallint as treatment_area,
        "PaintType"::smallint as paint_type,
        "GraphicColor"::integer as graphic_color,
        nullif(trim("PaintText"), '') as paint_text,
        
        -- Medical and insurance codes
        nullif(trim("AlternateCode1"), '') as alternate_code1,
        nullif(trim("MedicalCode"), '') as medical_code,
        nullif(trim("SubstitutionCode"), '') as substitution_code,
        "SubstOnlyIf"::integer as substitution_only_if,
        nullif(trim("DrugNDC"), '') as drug_ndc,
        nullif(trim("RevenueCodeDefault"), '') as default_revenue_code,
        nullif(trim("TaxCode"), '') as tax_code,
        nullif(trim("DiagnosticCodes"), '') as diagnostic_codes,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"NoBillIns"') }} as no_bill_insurance,
        {{ convert_opendental_boolean('"IsProsth"') }} as is_prosthetic,
        {{ convert_opendental_boolean('"IsHygiene"') }} as is_hygiene,
        {{ convert_opendental_boolean('"IsTaxed"') }} as is_taxed,
        {{ convert_opendental_boolean('"IsCanadianLab"') }} as is_canadian_lab,
        {{ convert_opendental_boolean('"IsMultiVisit"') }} as is_multi_visit,
        {{ convert_opendental_boolean('"IsRadiology"') }} as is_radiology,
        {{ convert_opendental_boolean('"BypassGlobalLock"') }} as bypass_global_lock,
        {{ convert_opendental_boolean('"AreaAlsoToothRange"') }} as area_also_tooth_range,
        
        -- Numeric and specialized fields
        "BaseUnits"::integer as base_units,
        "CanadaTimeUnits"::double precision as canada_time_units,
        "PreExisting"::boolean as pre_existing_flag,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
