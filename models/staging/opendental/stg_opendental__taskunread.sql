{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'taskunread') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"TaskUnreadNum"', 'target': 'task_unread_id'},
            {'source': 'NULLIF("TaskNum", 0)', 'target': 'task_id'},
            {'source': 'NULLIF("UserNum", 0)', 'target': 'user_id'}
        ]) }},
        
        -- Standardized metadata using macro (no creation/update timestamps in source)
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}
        
    from source_data
),

filtered_data as (
    select r.*
    from renamed_columns r
    inner join {{ ref('stg_opendental__task') }} t
        on r.task_id = t.task_id
    where t.original_timestamp >= '2023-01-01'
)

select * from filtered_data
