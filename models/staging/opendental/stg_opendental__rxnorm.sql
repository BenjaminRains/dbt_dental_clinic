with source as (
    select * from {{ source('opendental', 'rxnorm') }}
),

renamed as (
    select
        "RxNormNum" as rxnorm_num,
        "RxCui" as rx_cui,
        "MmslCode" as mmsl_code,
        "Description" as description
    from source
)

select * from renamed
