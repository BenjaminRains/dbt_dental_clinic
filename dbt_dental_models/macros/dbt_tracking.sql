{% macro update_etl_transform_status(status) %}
    {# 
    Macro to update the overall ETL transform status.
    This is called by on-run-start and on-run-end hooks.
    #}
    
    {% if execute %}
        {% set update_sql %}
            INSERT INTO {{ target.database }}.raw.etl_transform_status (
                table_name, 
                transform_status, 
                last_transformed, 
                rows_transformed,
                _transformed_at
            ) VALUES (
                'dbt_transformation_run',
                '{{ status }}',
                CURRENT_TIMESTAMP,
                0,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (table_name) 
            DO UPDATE SET
                transform_status = EXCLUDED.transform_status,
                last_transformed = EXCLUDED.last_transformed,
                _transformed_at = EXCLUDED._transformed_at;
        {% endset %}
        
        {% do run_query(update_sql) %}
        {{ log("Updated ETL transform status to: " ~ status, info=true) }}
    {% endif %}
{% endmacro %}

{% macro update_model_transform_status(model_name, status) %}
    {# 
    Macro to update the transform status for a specific model.
    This is called by on-model-start and on-model-end hooks.
    #}
    
    {% if execute %}
        {% set update_sql %}
            INSERT INTO {{ target.database }}.raw.etl_transform_status (
                table_name, 
                transform_status, 
                last_transformed, 
                rows_transformed,
                _transformed_at
            ) VALUES (
                '{{ model_name }}',
                '{{ status }}',
                CURRENT_TIMESTAMP,
                {% if status == 'completed' %}
                    (SELECT COUNT(*) FROM {{ this }})
                {% else %}
                    0
                {% endif %},
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (table_name) 
            DO UPDATE SET
                transform_status = EXCLUDED.transform_status,
                last_transformed = EXCLUDED.last_transformed,
                rows_transformed = EXCLUDED.rows_transformed,
                _transformed_at = EXCLUDED._transformed_at;
        {% endset %}
        
        {% do run_query(update_sql) %}
        {{ log("Updated model " ~ model_name ~ " transform status to: " ~ status, info=true) }}
    {% endif %}
{% endmacro %}

{% macro get_last_transform_status(table_name) %}
    {# 
    Macro to get the last transform status for a table.
    Useful for incremental models to check if source data has been transformed.
    #}
    
    {% set query %}
        SELECT 
            transform_status,
            last_transformed,
            rows_transformed
        FROM {{ target.database }}.raw.etl_transform_status 
        WHERE table_name = '{{ table_name }}'
    {% endset %}
    
    {% set result = run_query(query) %}
    {% if result %}
        {% set row = result.rows[0] %}
        {{ return({
            'status': row[0],
            'last_transformed': row[1],
            'rows_transformed': row[2]
        }) }}
    {% else %}
        {{ return({
            'status': 'pending',
            'last_transformed': none,
            'rows_transformed': 0
        }) }}
    {% endif %}
{% endmacro %}
