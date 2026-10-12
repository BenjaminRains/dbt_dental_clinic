with source as (
    select * from {{ source('opendental', 'rxnorm') }}
),

renamed as (
    select
        -- Primary key
        RxNormNum as rxnorm_id,

        -- Drug identifiers
        RxCui as rx_cui_code,
        MmslCode as mmsl_code,

        -- Drug information
        Description as drug_description
    from source
)

select * from renamed