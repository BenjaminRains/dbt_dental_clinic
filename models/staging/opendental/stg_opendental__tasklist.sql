{{ config(
    materialized='incremental',
    unique_key='task_list_id',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'tasklist') }}
    where "DateTimeEntry" >= '2023-01-01'
    {% if is_incremental() %}
        and "DateTimeEntry" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"TaskListNum"', 'target': 'task_list_id'},
            {'source': 'NULLIF("Parent", 0)', 'target': 'parent_id'},
            {'source': 'NULLIF("FromNum", 0)', 'target': 'from_id'}
        ]) }},
        
        -- Task list attributes
        "Descript" as description,
        "GlobalTaskFilterType" as global_task_filter_type,
        "TaskListStatus" as task_status,
        "ObjectType" as object_type,
        "DateType" as date_type,
        
        -- Date fields using macro
        {{ clean_opendental_date('"DateTL"') }} as task_date,
        
        -- Timestamps
        "DateTimeEntry" as entry_datetime,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsRepeating"') }} as is_repeating,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTimeEntry"',
            updated_at_column='"DateTimeEntry"',
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
