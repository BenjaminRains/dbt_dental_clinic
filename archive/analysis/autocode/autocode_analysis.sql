with source as (
    select * from {{ source('opendental', 'autocode') }}
),

renamed as (
    select
        AutoCodeNum as autocode_id,
        Description as description,
        IsHidden as is_hidden,
        LessIntrusive as is_less_intrusive
    from source
)

select * from renamed
