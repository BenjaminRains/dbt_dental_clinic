-- depends_on: {{ ref('stg_opendental__statement') }}
{{ config(
    materialized='incremental',
    unique_key='statement_prod_id',
    schema='staging'
) }}

with source as (
    select sp.* 
    from {{ source('opendental', 'statementprod') }} sp
    inner join {{ source('opendental', 'statement') }} s
        on s."StatementNum" = sp."StatementNum"
    where s."DateSent" >= '2023-01-01'::date
        and s."DateSent" <= current_date
        and s."DateSent" > '2000-01-01'::date
    {% if is_incremental() %}
        and s."DateSent" > (select max(date_sent) from {{ ref('stg_opendental__statement') }})
    {% endif %}
),

renamed as (
    select
        -- Primary Key
        "StatementProdNum" as statement_prod_id,
        
        -- Foreign Keys
        "StatementNum" as statement_id,
        "FKey" as fkey,
        "DocNum" as doc_id,
        "LateChargeAdjNum" as late_charge_adj_id,
        
        -- Additional Fields
        "ProdType" as prod_type,

        -- Meta Fields
        current_timestamp as _loaded_at

    from source
)

select * from renamed