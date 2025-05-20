with source as (
    select * from {{ source('opendental', 'tasklist') }}
),

renamed as (
    select
        "TaskListNum" as task_id,
        "Descript" as description,
        "Parent" as parent_id,
        "DateTL" as task_date,
        "IsRepeating" as is_repeating,
        "DateType" as date_type,
        "FromNum" as from_id,
        "ObjectType" as object_type,
        "DateTimeEntry" as entry_datetime,
        "GlobalTaskFilterType" as global_task_filter_type,
        "TaskListStatus" as task_status
    from source
)

select * from renamed
