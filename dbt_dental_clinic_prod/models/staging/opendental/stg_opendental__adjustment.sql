{{ config(
    materialized='incremental',
    unique_key='adjustment_id'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'adjustment') }}
    where {{ clean_opendental_date('"AdjDate"') }} >= '2023-01-01'::date
        and {{ clean_opendental_date('"AdjDate"') }} <= {{ var("max_valid_date") }}::date
    {% if is_incremental() %}
        and {{ clean_opendental_date('"AdjDate"') }} > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"AdjNum"', 'target': 'adjustment_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': 'NULLIF("ProcNum", 0)', 'target': 'procedure_id'},
            {'source': 'NULLIF("ProvNum", 0)', 'target': 'provider_id'},
            {'source': 'NULLIF("ClinicNum", 0)', 'target': 'clinic_id'},
            {'source': 'NULLIF("StatementNum", 0)', 'target': 'statement_id'},
            {'source': '"AdjType"', 'target': 'adjustment_type_id'},
            {'source': 'NULLIF("TaxTransID", 0)', 'target': 'tax_transaction_id'}
        ]) }},
        
        -- Adjustment details
        "AdjAmt"::double precision as adjustment_amount,  
        nullif("AdjNote", '')::text as adjustment_note,  
        
        -- Date fields using macro
        {{ clean_opendental_date('"AdjDate"') }} as adjustment_date,
        {{ clean_opendental_date('"ProcDate"') }} as procedure_date,
        {{ clean_opendental_date('"DateEntry"') }} as date_entry,
        
        -- Basic calculated fields only (minimal staging logic)
        case 
            when "AdjAmt" > 0 then 'positive'
            when "AdjAmt" < 0 then 'negative'
            else 'zero'
        end as adjustment_direction,
        
        case 
            when "ProcNum" > 0 then true
            else false
        end as is_procedure_adjustment,
        
        case
            when {{ clean_opendental_date('"ProcDate"') }} != {{ clean_opendental_date('"AdjDate"') }} then true
            else false
        end as is_retroactive_adjustment,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column='"SecUserNumEntry"'
        ) }}
    from source_data
)

select * from renamed_columns
