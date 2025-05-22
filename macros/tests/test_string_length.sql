{% test test_string_length(model, column_name, max_length, allow_null=true) %}
    {# 
        Test that validates a string column's length is within bounds.
        
        Args:
            model: The model to test (automatically provided by dbt)
            column_name: The name of the column to test (automatically provided by dbt)
            max_length: Maximum allowed length for the string
            allow_null: Whether to allow null values (default true)
    #}

    select
        {{ column_name }},
        'should be null or have length <= {{ max_length }}' as expected_value
    from {{ model }}
    where 
        {% if allow_null %}
        {{ column_name }} is not null and
        {% endif %}
        length(trim({{ column_name }})) > {{ max_length }}

{% endtest %} 