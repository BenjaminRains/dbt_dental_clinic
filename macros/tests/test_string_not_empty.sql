{% test test_string_not_empty(model, column_name, allow_null=true) %}
    {# 
        Test that validates a string column is not empty when present.
        
        Args:
            model: The model to test (automatically provided by dbt)
            column_name: The name of the column to test (automatically provided by dbt)
            allow_null: Whether to allow null values (default true)
    #}

    select
        {{ column_name }},
        'should be null or non-empty when present' as expected_value
    from {{ model }}
    where 
        {% if allow_null %}
        {{ column_name }} is not null and
        {% endif %}
        length(trim({{ column_name }})) = 0

{% endtest %} 