with source as (
    select * from {{ source('opendental', 'tasklist') }}
    where "DateTimeEntry" >= '2023-01-01'
),

renamed as (
    select
        -- Primary Key
        "TaskListNum" as task_list_id,
        
        -- Attributes
        "Descript" as description,
        "Parent" as parent_id,
        "DateTL" as task_date,
        "IsRepeating" as is_repeating,
        "DateType" as date_type,
        "FromNum" as from_id,
        "ObjectType" as object_type,
        "DateTimeEntry" as entry_datetime,
        "GlobalTaskFilterType" as global_task_filter_type,
        "TaskListStatus" as task_status,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        "DateTimeEntry" as _created_at,
        "DateTimeEntry" as _updated_at
    from source
)

select * from renamed
