{% macro get_latest_etl_load_time(table_name) %}
    {% set query %}
        select max(_loaded_at) as latest_load_time
        from {{ source('opendental', 'etl_load_status') }}
        where table_name = '{{ table_name }}'
        and extraction_status = 'completed'
    {% endset %}
    
    {% set result = run_query(query) %}
    {% if result %}
        {% set latest_time = result.columns[0].values()[0] %}
        {{ return(latest_time) }}
    {% else %}
        {{ return(none) }}
    {% endif %}
{% endmacro %}

{% macro check_etl_freshness(table_name, hours_threshold=24) %}
    {% set query %}
        select 
            case 
                when max(_loaded_at) is null then false
                when max(_loaded_at) < current_timestamp - interval '{{ hours_threshold }} hours' then false
                else true
            end as is_fresh
        from {{ source('opendental', 'etl_load_status') }}
        where table_name = '{{ table_name }}'
        and extraction_status = 'completed'
    {% endset %}
    
    {% set result = run_query(query) %}
    {% if result %}
        {% set is_fresh = result.columns[0].values()[0] %}
        {{ return(is_fresh) }}
    {% else %}
        {{ return(false) }}
    {% endif %}
{% endmacro %}

{% macro get_etl_load_status(table_name) %}
    {% set query %}
        select 
            table_name,
            last_extracted,
            rows_extracted,
            extraction_status,
            _loaded_at
        from {{ source('opendental', 'etl_load_status') }}
        where table_name = '{{ table_name }}'
        order by _loaded_at desc
        limit 1
    {% endset %}
    
    {% set result = run_query(query) %}
    {{ return(result) }}
{% endmacro %} 