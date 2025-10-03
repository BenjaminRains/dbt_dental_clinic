{% test test_timestamp_not_future(model, column_name, allow_null=true) %}
    {# 
        Test that validates a timestamp column is not in the future.
        
        Args:
            model: The model to test (automatically provided by dbt)
            column_name: The name of the timestamp column to test (automatically provided by dbt)
            allow_null: Whether to allow null values in the test column (default true)
    #}

    select
        {{ column_name }},
        'should not be in the future' as expected_value
    from {{ model }}
    where 
        {% if allow_null %}
        {{ column_name }} is not null and
        {% endif %}
        {{ column_name }} > current_timestamp

{% endtest %} 