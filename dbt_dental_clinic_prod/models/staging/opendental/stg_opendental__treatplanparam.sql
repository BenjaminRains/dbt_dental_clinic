{{ config(
    materialized='view'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'treatplanparam') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"TreatPlanParamNum"', 'target': 'treatplan_param_id'},
            {'source': 'NULLIF("PatNum", 0)', 'target': 'patient_id'},
            {'source': 'NULLIF("TreatPlanNum", 0)', 'target': 'treatplan_id'}
        ]) }},
        
        -- Display parameters using boolean conversion macro
        {{ convert_opendental_boolean('"ShowDiscount"') }} as show_discount,
        {{ convert_opendental_boolean('"ShowMaxDed"') }} as show_max_deductible,
        {{ convert_opendental_boolean('"ShowSubTotals"') }} as show_subtotals,
        {{ convert_opendental_boolean('"ShowTotals"') }} as show_totals,
        {{ convert_opendental_boolean('"ShowCompleted"') }} as show_completed,
        {{ convert_opendental_boolean('"ShowFees"') }} as show_fees,
        {{ convert_opendental_boolean('"ShowIns"') }} as show_insurance,
        
        -- Standardized metadata using macro (no timestamps in source)
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
