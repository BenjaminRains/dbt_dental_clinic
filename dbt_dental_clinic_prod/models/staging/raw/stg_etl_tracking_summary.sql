{{
  config(
    materialized='view',
    tags=['etl', 'tracking', 'summary']
  )
}}

with load_status as (
    select 
        table_name,
        last_extracted,
        rows_extracted,
        extraction_status,
        _loaded_at as load_timestamp,
        case 
            when extraction_status = 'completed' then true
            else false
        end as is_loaded
    from {{ source('opendental', 'etl_load_status') }}
),

transform_status as (
    select 
        table_name,
        last_transformed,
        rows_transformed,
        transformation_status,
        _transformed_at as transform_timestamp,
        case 
            when transformation_status = 'completed' then true
            else false
        end as is_transformed
    from {{ source('opendental', 'etl_transform_status') }}
),

final as (
    select 
        coalesce(ls.table_name, ts.table_name) as table_name,
        ls.last_extracted,
        ls.rows_extracted,
        ls.extraction_status,
        ls.load_timestamp,
        ls.is_loaded,
        ts.last_transformed,
        ts.rows_transformed,
        ts.transformation_status,
        ts.transform_timestamp,
        ts.is_transformed,
        case 
            when ls.is_loaded and ts.is_transformed then 'fully_processed'
            when ls.is_loaded and not ts.is_transformed then 'loaded_not_transformed'
            when not ls.is_loaded then 'not_loaded'
            else 'unknown'
        end as processing_status
    from load_status ls
    full outer join transform_status ts 
        on ls.table_name = ts.table_name
)

select * from final