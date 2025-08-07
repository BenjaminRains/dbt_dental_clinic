{{ config(
    materialized='view'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'treatplanattach') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"TreatPlanAttachNum"', 'target': 'treatplan_attach_id'},
            {'source': 'NULLIF("TreatPlanNum", 0)', 'target': 'treatplan_id'},
            {'source': 'NULLIF("ProcNum", 0)', 'target': 'procedure_id'}
        ]) }},
        
        -- Other fields
        "Priority" as priority,
        
        -- Standardized metadata using macro (no timestamps in source)
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
