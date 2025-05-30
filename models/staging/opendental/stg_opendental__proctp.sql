{{ config(
    materialized='incremental',
    unique_key='proctp_id'
) }}

with source as (
    select * from {{ source('opendental', 'proctp') }}
    where "DateTP" >= '2023-01-01'
    {% if is_incremental() %}
        and "SecDateTEdit" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "ProcTPNum" as proctp_id,
        
        -- Foreign keys
        "TreatPlanNum" as treatment_plan_id,
        "PatNum" as patient_id,
        "ProcNumOrig" as procedure_id_orig,
        "ProvNum" as provider_id,
        "ClinicNum" as clinic_id,
        "SecUserNumEntry" as user_num_entry,
        
        -- Regular fields
        "ItemOrder" as item_order,
        "Priority" as priority,
        "ToothNumTP" as tooth_num_tp,
        "Surf" as surface,
        "ProcCode" as procedure_code,
        "Descript" as description,
        "FeeAmt" as fee_amount,
        "PriInsAmt" as primary_insurance_amount,
        "SecInsAmt" as secondary_insurance_amount,
        "PatAmt" as patient_amount,
        "Discount" as discount,
        "Prognosis" as prognosis,
        "Dx" as diagnosis,
        "ProcAbbr" as procedure_abbreviation,
        "FeeAllowed" as fee_allowed,
        "TaxAmt" as tax_amount,
        "CatPercUCR" as category_percent_ucr,
        
        -- Dates
        "DateTP" as treatment_plan_date,
        "SecDateEntry" as entry_date,
        "SecDateTEdit" as last_edit_timestamp,
        
        -- Metadata
        current_timestamp as _loaded_at,
        "SecDateEntry" as _created_at,
        "SecDateTEdit" as _updated_at
    
    from source
)

select * from renamed
