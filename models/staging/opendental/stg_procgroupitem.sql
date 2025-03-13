with source as (
    select * from {{ source('opendental', 'procgroupitem') }}
),

renamed as (
    select
        -- Primary key
        ProcGroupItemNum as procedure_group_item_id,
        
        -- Relationships
        ProcNum as procedure_id,
        GroupNum as group_id
    from source
)

select * from renamed