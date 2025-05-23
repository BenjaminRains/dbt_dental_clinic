with source as (
    select * from {{ source('opendental', 'rxnorm') }}
),

renamed as (
    select
        -- Primary Key
        "RxNormNum" as rxnorm_id,
        
        -- Attributes
        "RxCui" as rx_cui,
        "MmslCode" as mmsl_code,
        "Description" as description,
        
        -- Metadata
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        null::timestamp as _created_at,   -- No creation date available in source
        current_timestamp as _updated_at  -- Last update is when we loaded it
    
    from source
)

select * from renamed
