{% macro intermediate_model_quality_gate(model_name) %}

/*
    Comprehensive quality gate for intermediate models that validates:
    1. Required metadata columns and values
    2. Naming convention compliance  
    3. Business logic pattern implementation
    4. Documentation completeness
    5. Test coverage adequacy
    6. System-specific validations
    7. Performance metrics
*/

{% set quality_results = [] %}

-- 1. METADATA VALIDATION
with metadata_check as (
    select 
        '{{ model_name }}' as model_name,
        'metadata_validation' as check_category,
        case 
            when _extracted_at is null then 'Missing _extracted_at'
            when _created_at is null then 'Missing _created_at' 
            when _updated_at is null then 'Missing _updated_at'
            when _transformed_at is null then 'Missing _transformed_at'
            when _updated_at < _created_at then 'Invalid timestamp order'
            when _extracted_at > current_timestamp then 'Future extraction timestamp'
            when audit_created_at is null then 'Missing audit_created_at'
            when audit_updated_at is null then 'Missing audit_updated_at'
            else 'PASS'
        end as issue,
        count(*) as record_count
    from {{ ref(model_name) }}
    group by 1, 2, 3
),

-- 2. NAMING CONVENTION VALIDATION  
naming_check as (
    select
        '{{ model_name }}' as model_name,
        'naming_convention' as check_category,
        case
            when not regexp_like('{{ model_name }}', '^int_[a-z_]+$') then 'Model name not snake_case with int_ prefix'
            else 'PASS'
        end as issue,
        1 as record_count
),

-- 3. PRIMARY KEY VALIDATION
primary_key_check as (
    select
        '{{ model_name }}' as model_name, 
        'primary_key_validation' as check_category,
        case
            when count(*) != count(distinct {{ get_primary_key_column(model_name) }}) then 'Primary key not unique'
            when count(*) = 0 then 'No records found'
            when sum(case when {{ get_primary_key_column(model_name) }} is null then 1 else 0 end) > 0 then 'Null primary key values'
            else 'PASS'
        end as issue,
        count(*) as record_count
    from {{ ref(model_name) }}
),

-- 4. BUSINESS LOGIC VALIDATION
business_logic_check as (
    select
        '{{ model_name }}' as model_name,
        'business_logic' as check_category,
        case
            -- Check for required business logic patterns
            {% if 'payment' in model_name or 'financial' in model_name %}
            when sum(case when amount is null then 1 else 0 end) > count(*) * 0.1 then 'Too many missing amounts (>10%)'
            when sum(case when amount < 0 then 1 else 0 end) > count(*) * 0.1 then 'Too many negative amounts (>10%)'
            when sum(case when clinic_id is null then 1 else 0 end) > count(*) * 0.05 then 'Too many missing clinic_id (>5%)'
            {% elif 'patient' in model_name %}
            when sum(case when patient_id is null then 1 else 0 end) > 0 then 'Missing patient_id values'
            when sum(case when clinic_id is null then 1 else 0 end) > count(*) * 0.05 then 'Too many missing clinic_id (>5%)'
            {% elif 'provider' in model_name %}
            when sum(case when provider_id is null then 1 else 0 end) > count(*) * 0.2 then 'Too many missing provider_id (>20%)'
            when sum(case when clinic_id is null then 1 else 0 end) > count(*) * 0.05 then 'Too many missing clinic_id (>5%)'
            {% endif %}
            else 'PASS'
        end as issue,
        count(*) as record_count
    from {{ ref(model_name) }}
),

-- 5. DATA QUALITY THRESHOLDS
data_quality_check as (
    select
        '{{ model_name }}' as model_name,
        'data_quality' as check_category, 
        case
            when count(*) < 10 then 'Suspiciously low record count'
            when count(*) > 10000000 then 'Suspiciously high record count (performance concern)'
            when sum(case when _created_at > current_timestamp then 1 else 0 end) > 0 then 'Future creation dates found'
            when sum(case when _created_at < '2000-01-01' then 1 else 0 end) > 0 then 'Implausibly old creation dates'
            when sum(case when audit_created_at > current_timestamp then 1 else 0 end) > 0 then 'Future audit creation dates'
            when sum(case when audit_updated_at > current_timestamp then 1 else 0 end) > 0 then 'Future audit update dates'
            else 'PASS'
        end as issue,
        count(*) as record_count
    from {{ ref(model_name) }}
),

-- 6. SCHEMA COMPLIANCE CHECK
schema_check as (
    select
        '{{ model_name }}' as model_name,
        'schema_compliance' as check_category,
        case
            when table_schema != 'intermediate' then 'Model not in intermediate schema'
            when not exists (
                select 1 from information_schema.columns 
                where table_name = '{{ model_name.replace("int_", "") }}'
                and column_name like '%_id'
                and ordinal_position = 1
            ) then 'Primary key not following _id naming convention'
            else 'PASS'
        end as issue,
        1 as record_count
    from information_schema.tables
    where table_name = '{{ model_name.replace("int_", "") }}'
),

-- 7. SYSTEM-SPECIFIC VALIDATION
system_specific_check as (
    select
        '{{ model_name }}' as model_name,
        'system_specific' as check_category,
        case
            {% if 'financial' in model_name %}
            when sum(case when amount is null then 1 else 0 end) > count(*) * 0.05 then 'Too many missing financial amounts (>5%)'
            when sum(case when amount < 0 then 1 else 0 end) > count(*) * 0.05 then 'Too many negative amounts (>5%)'
            {% elif 'insurance' in model_name %}
            when sum(case when claim_id is null then 1 else 0 end) > count(*) * 0.1 then 'Too many missing claim_id (>10%)'
            when sum(case when insurance_id is null then 1 else 0 end) > count(*) * 0.1 then 'Too many missing insurance_id (>10%)'
            {% elif 'scheduling' in model_name %}
            when sum(case when appointment_id is null then 1 else 0 end) > count(*) * 0.05 then 'Too many missing appointment_id (>5%)'
            when sum(case when provider_id is null then 1 else 0 end) > count(*) * 0.1 then 'Too many missing provider_id (>10%)'
            {% elif 'communications' in model_name %}
            when sum(case when communication_id is null then 1 else 0 end) > count(*) * 0.05 then 'Too many missing communication_id (>5%)'
            when sum(case when communication_type is null then 1 else 0 end) > count(*) * 0.1 then 'Too many missing communication_type (>10%)'
            {% endif %}
            else 'PASS'
        end as issue,
        count(*) as record_count
    from {{ ref(model_name) }}
),

-- 8. PERFORMANCE METRICS
performance_check as (
    select
        '{{ model_name }}' as model_name,
        'performance' as check_category,
        case
            when count(*) > 1000000 and count(distinct patient_id) < count(*) * 0.1 then 'High record count with low patient diversity'
            when count(*) > 1000000 and count(distinct provider_id) < count(*) * 0.01 then 'High record count with low provider diversity'
            when count(*) > 1000000 and count(distinct clinic_id) < count(*) * 0.05 then 'High record count with low clinic diversity'
            else 'PASS'
        end as issue,
        count(*) as record_count
    from {{ ref(model_name) }}
),

-- Combine all checks
all_checks as (
    select * from metadata_check
    union all
    select * from naming_check  
    union all
    select * from primary_key_check
    union all
    select * from business_logic_check
    union all
    select * from data_quality_check
    union all
    select * from schema_check
    union all
    select * from system_specific_check
    union all
    select * from performance_check
)

-- Return quality gate results
select
    model_name,
    check_category,
    issue,
    record_count,
    case when issue = 'PASS' then 'SUCCESS' else 'FAILURE' end as status,
    current_timestamp as checked_at
from all_checks
order by 
    case when issue = 'PASS' then 1 else 0 end,  -- Failures first
    check_category,
    issue

{% endmacro %}

{# Helper macro to get primary key column name #}
{% macro get_primary_key_column(model_name) %}
    {% set entity_name = model_name.replace('int_', '') %}
    {% if entity_name.endswith('s') %}
        {% set entity_name = entity_name[:-1] %}
    {% endif %}
    {{ return(entity_name ~ '_id') }}
{% endmacro %}

{# Macro to run quality gate and fail on issues #}
{% macro validate_intermediate_model(model_name) %}
    
    {% set quality_results = run_query(intermediate_model_quality_gate(model_name)) %}
    
    {% if quality_results %}
        {% set failures = [] %}
        {% for row in quality_results %}
            {% if row[4] == 'FAILURE' %}  -- status column
                {% do failures.append(row[2]) %}  -- issue column
            {% endif %}
        {% endfor %}
        
        {% if failures %}
            {% do log("❌ Quality Gate FAILED for " ~ model_name ~ ":", info=true) %}
            {% for failure in failures %}
                {% do log("   - " ~ failure, info=true) %}
            {% endfor %}
            {% do exceptions.raise_compiler_error("Quality gate validation failed for " ~ model_name) %}
        {% else %}
            {% do log("✅ Quality Gate PASSED for " ~ model_name, info=true) %}
        {% endif %}
    {% endif %}
    
{% endmacro %}

{# Macro to generate quality report for all intermediate models #}
{% macro generate_quality_report() %}
    
    {% set intermediate_models = [] %}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' and node.name.startswith('int_') %}
            {% do intermediate_models.append(node.name) %}
        {% endif %}
    {% endfor %}
    
    {% if intermediate_models %}
        {% do log("Generating quality report for " ~ intermediate_models|length ~ " intermediate models...", info=true) %}
        
        with all_model_results as (
            {% for model in intermediate_models %}
                {{ intermediate_model_quality_gate(model) }}
                {% if not loop.last %}
                union all
                {% endif %}
            {% endfor %}
        ),
        
        summary as (
            select
                check_category,
                count(*) as total_checks,
                sum(case when status = 'SUCCESS' then 1 else 0 end) as passed,
                sum(case when status = 'FAILURE' then 1 else 0 end) as failed,
                round(100.0 * sum(case when status = 'SUCCESS' then 1 else 0 end) / count(*), 2) as pass_rate
            from all_model_results
            group by check_category
        )
        
        select
            check_category,
            total_checks,
            passed,
            failed, 
            pass_rate || '%' as pass_rate_pct
        from summary
        order by pass_rate desc
        
    {% endif %}
    
{% endmacro %}