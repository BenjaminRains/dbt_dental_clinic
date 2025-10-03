-- depends_on: {{ ref('stg_opendental__statement') }}
{{ config(
    materialized='incremental',
    unique_key='statement_prod_id'
) }}

with statement_data as (
    select 
        "StatementNum",
        "DateSent",
        "DateTStamp"
    from {{ source('opendental', 'statement') }} 
    where {{ clean_opendental_date('"DateSent"') }} >= '2023-01-01'::date
        and {{ clean_opendental_date('"DateSent"') }} <= current_date
        and {{ clean_opendental_date('"DateSent"') }} > '2000-01-01'::date
),

source_data as (
    select 
        sp.*,
        s."DateTStamp" as statement_timestamp
    from {{ source('opendental', 'statementprod') }} sp
    inner join statement_data s
        on s."StatementNum" = sp."StatementNum"
    {% if is_incremental() %}
    where {{ clean_opendental_date('"DateTStamp"') }} > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"StatementProdNum"', 'target': 'statement_prod_id'}
        ]) }},
        
        -- Foreign Keys
        {{ transform_id_columns([
            {'source': '"StatementNum"', 'target': 'statement_id'},
            {'source': '"DocNum"', 'target': 'doc_id'},
            {'source': '"LateChargeAdjNum"', 'target': 'late_charge_adj_id'}
        ]) }},
        
        -- Additional Fields
        "FKey" as fkey,
        "ProdType" as prod_type,

        -- Source Metadata
        {{ clean_opendental_date('"statement_timestamp"') }} as statement_timestamp,

        -- Metadata columns (using statement timestamp since statementprod has no timestamps)
        {{ standardize_metadata_columns(
            created_at_column='statement_timestamp',
            updated_at_column='statement_timestamp',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
