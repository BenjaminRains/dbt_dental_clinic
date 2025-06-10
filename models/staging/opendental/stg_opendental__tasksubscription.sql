{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'tasksubscription') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"TaskSubscriptionNum"', 'target': 'task_subscription_id'},
            {'source': 'NULLIF("UserNum", 0)', 'target': 'user_id'},
            {'source': 'NULLIF("TaskListNum", 0)', 'target': 'task_list_id'},
            {'source': 'NULLIF("TaskNum", 0)', 'target': 'task_id'}
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
