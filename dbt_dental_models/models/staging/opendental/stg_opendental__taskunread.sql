{{ config(
    materialized='view'
) }}

with source_data as (
    select tu.*
    from {{ source('opendental', 'taskunread') }} tu
    inner join {{ ref('stg_opendental__task') }} t
        on NULLIF(tu."TaskNum", 0) = t.task_id
    where t.original_timestamp >= '2023-01-01'
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
)

select * from renamed_columns
