{% macro intermediate_model_template(
    model_name,
    source_models,
    system_name,
    business_purpose,
    model_type='standard',
    materialized='table',
    unique_key_field='id'
) %}

{{
    config(
        materialized=materialized,
        schema='intermediate',
        unique_key=unique_key_field,
        on_schema_change='fail',
        {% if materialized == 'incremental' %}
        incremental_strategy='merge',
        {% endif %}
        indexes=[
            {'columns': [unique_key_field], 'unique': true},
            {'columns': ['patient_id']},
            {'columns': ['_updated_at']},
            {'columns': ['clinic_id']}
        ]
    )
}}

/*
    Intermediate model for {{ model_name }}
    Part of System {{ system_name }}
    
    This model:
    1. {{ business_purpose }}
    2. Standardizes data from OpenDental source systems
    3. Implements business rules and validation logic
    4. Provides foundation for downstream analytics
    
    Business Logic Features:
    - ID standardization: All "XxxNum" fields → "xxx_id" with snake_case
    - Boolean conversion: OpenDental 0/1 → PostgreSQL true/false
    - Metadata tracking: ETL pipeline timestamps and data lineage
    - Clinic standardization: Multi-location practice support
    - Audit trail: Comprehensive change tracking
    
    Performance Considerations:
    - Materialized as {{ materialized }} for optimal query performance
    - Indexed on primary key and common filter columns
    {% if materialized == 'incremental' %}
    - Incremental updates based on _updated_at timestamp
    {% endif %}
*/

-- Source data retrieval with proper column quoting
with source_data as (
    {% for source in source_models %}
    select 
        -- Transform OpenDental ID columns (CamelCase "XxxNum" → snake_case "xxx_id")
        {% for col in get_opendental_id_columns(source) %}
        "{{ col }}" as {{ col.lower().replace('num', '_id') }},
        {% endfor %}
        
        -- Transform boolean columns using macro
        {% for col in get_opendental_boolean_columns(source) %}
        {{ convert_opendental_boolean('"' + col + '"') }} as {{ col.lower() }},
        {% endfor %}
        
        -- Standard metadata fields
        _extracted_at,
        "DateEntry" as _created_at,
        coalesce("DateTStamp", "DateEntry") as _updated_at,
        current_timestamp as _transformed_at,
        
        -- Clinic standardization
        coalesce("ClinicNum", 0) as clinic_id,
        
        -- Audit trail fields
        "SecUserNumEntry" as created_by_user_id,
        "SecUserNumEdit" as updated_by_user_id,
        "SecDateEntry" as audit_created_at,
        "SecDateTEdit" as audit_updated_at,
        
        -- All other columns with proper casing
        {% for col in get_remaining_columns(source) %}
        "{{ col }}" as {{ col | snake_case }},
        {% endfor %}
        
    from {{ ref(source) }}
    {% if not loop.last %}
    
    union all
    
    {% endif %}
    {% endfor %}
),

-- Business logic and validation
{% if model_type == 'financial' %}
business_logic as (
    select
        *,
        -- Financial categorization logic
        case 
            when amount > 1000 then 'large'
            when amount > 100 then 'medium'
            when amount > 0 then 'small'
            else 'other'
        end as amount_category,
        
        -- Financial validation flags
        case when amount >= 0 then true else false end as is_valid_amount,
        
        -- Clinic-specific validation
        case when clinic_id > 0 then true else false end as is_clinic_specific
    from source_data
),
{% elif model_type == 'clinical' %}
business_logic as (
    select
        *,
        -- Clinical status logic
        case
            when status = 'Complete' then 'completed'
            when status = 'In Progress' then 'active'
            when status = 'Cancelled' then 'cancelled'
            else 'other'
        end as standardized_status,
        
        -- Clinical validation flags
        case when provider_id is not null then true else false end as has_provider,
        
        -- Clinic-specific validation
        case when clinic_id > 0 then true else false end as is_clinic_specific
    from source_data
),
{% else %}
business_logic as (
    select
        *,
        -- Standard validation
        case when _created_at is not null then true else false end as is_valid_record,
        
        -- Clinic-specific validation
        case when clinic_id > 0 then true else false end as is_clinic_specific
    from source_data
),
{% endif %}

-- Final data quality filtering
final as (
    select * 
    from business_logic
    where _created_at is not null  -- Ensure valid records
    {% if materialized == 'incremental' %}
    {% if is_incremental() %}
    and _updated_at > (select max(_updated_at) from {{ this }})
    {% endif %}
    {% endif %}
)

select * from final

{% endmacro %}

{# Helper macros for column transformation #}
{% macro get_opendental_id_columns(source_model) %}
    {# This would need to be implemented to query information schema #}
    {# For now, return common ID patterns #}
    {{ return(['PatNum', 'ProvNum', 'ClaimNum', 'ProcNum', 'ClinicNum']) }}
{% endmacro %}

{% macro get_opendental_boolean_columns(source_model) %}
    {# This would need to be implemented to identify boolean columns #}
    {{ return(['IsHidden', 'IsActive', 'IsCompleted']) }}
{% endmacro %}

{% macro get_remaining_columns(source_model) %}
    {# This would exclude ID and boolean columns already handled #}
    {{ return(['DateService', 'Description', 'Amount']) }}
{% endmacro %}

{% macro convert_opendental_boolean(column_name) %}
    case 
        when {{ column_name }} = 1 then true
        when {{ column_name }} = 0 then false
        else null
    end
{% endmacro %}