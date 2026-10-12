with source as (
    select * from {{ source('opendental', 'statementprod') }}
),

renamed as (
    select
        -- Primary key
        StatementProdNum as statement_production_id,

        -- Relationships
        StatementNum as statement_id,
        FKey as foreign_key_id, -- References different tables based on ProdType
        DocNum as document_id,
        LateChargeAdjNum as late_charge_adjustment_id,

        -- Type
        ProdType as production_type
    from source
)

select * from renamed