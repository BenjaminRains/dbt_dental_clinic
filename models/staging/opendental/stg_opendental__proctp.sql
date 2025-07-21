{{ config(
    materialized='incremental',
    unique_key='proctp_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'proctp') }}
    where "DateTP" >= '2023-01-01'
    {% if is_incremental() %}
        and "SecDateTEdit" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"ProcTPNum"', 'target': 'proctp_id'},
            {'source': '"TreatPlanNum"', 'target': 'treatment_plan_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"ProcNumOrig"', 'target': 'procedure_id_orig'},
            {'source': '"ProvNum"', 'target': 'provider_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'},
            {'source': '"SecUserNumEntry"', 'target': 'user_num_entry'}
        ]) }},
        
        -- Treatment Plan Attributes
        "ItemOrder" as item_order,
        "Priority" as priority,
        "ToothNumTP" as tooth_num_tp,
        "Surf" as surface,
        "ProcCode" as procedure_code,
        "Descript" as description,
        "ProcAbbr" as procedure_abbreviation,
        "Prognosis" as prognosis,
        "Dx" as diagnosis,
        
        -- Financial Fields
        "FeeAmt" as fee_amount,
        "PriInsAmt" as primary_insurance_amount,
        "SecInsAmt" as secondary_insurance_amount,
        "PatAmt" as patient_amount,
        "Discount" as discount,
        "FeeAllowed" as fee_allowed,
        "TaxAmt" as tax_amount,
        "CatPercUCR" as category_percent_ucr,
        
        -- Date Fields
        {{ clean_opendental_date('"DateTP"') }} as treatment_plan_date,
        {{ clean_opendental_date('"SecDateEntry"') }} as entry_date,
        {{ clean_opendental_date('"SecDateTEdit"') }} as last_edit_timestamp,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column='"SecUserNumEntry"'
        ) }}
    
    from source_data
)

select * from renamed_columns
