{{ config(
    materialized='incremental',
    unique_key='fee_id'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'fee') }}
    where "SecDateEntry" >= '2023-01-01'::date
        and "SecDateEntry" <= current_date
        and "SecDateEntry" > '2000-01-01'::date
    {% if is_incremental() %}
        and "SecDateEntry" > (select max(_created_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key and Foreign Keys
        {{ transform_id_columns([
            {'source': '"FeeNum"', 'target': 'fee_id'},
            {'source': '"FeeSched"', 'target': 'fee_schedule_id'},
            {'source': '"CodeNum"', 'target': 'procedure_code_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'},
            {'source': '"ProvNum"', 'target': 'provider_id'}
        ]) }},
        
        -- Regular Fields
        "Amount"::double precision as fee_amount,
        "OldCode" as ada_code,
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"UseDefaultFee"') }} as is_default_fee,
        {{ convert_opendental_boolean('"UseDefaultCov"') }} as is_default_coverage,
        
        -- Date Fields
        {{ clean_opendental_date('"SecDateEntry"') }} as date_created,
        {{ clean_opendental_date('"SecDateTEdit"') }} as date_updated,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column='"SecUserNumEntry"'
        ) }}

    from source_data
)

select * from renamed_columns
