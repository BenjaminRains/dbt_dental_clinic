{{ config(
    materialized='incremental',
    unique_key='recall_id',
    schema='staging'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'recall') }}
    where "DateDue" >= '2023-01-01'::date  
        and "DateDue" <= current_date
    {% if is_incremental() %}
        and "DateTStamp" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"RecallNum"', 'target': 'recall_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"RecallTypeNum"', 'target': 'recall_type_id'}
        ]) }},
        
        -- Date fields using macro
        {{ clean_opendental_date('"DateDueCalc"') }} as date_due_calc,
        {{ clean_opendental_date('"DateDue"') }} as date_due,
        {{ clean_opendental_date('"DatePrevious"') }} as date_previous,
        {{ clean_opendental_date('"DateScheduled"') }} as date_scheduled,
        {{ clean_opendental_date('"DisableUntilDate"') }} as disable_until_date,
        
        -- Status and configuration
        "RecallInterval"::integer as recall_interval,
        "RecallStatus"::smallint as recall_status,
        coalesce("Priority", 0)::smallint as priority,
        "DisableUntilBalance"::double precision as disable_until_balance,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsDisabled"') }} as is_disabled,
        
        -- Text fields
        nullif(trim("Note"), '') as note,
        nullif(trim("TimePatternOverride"), '') as time_pattern_override,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
