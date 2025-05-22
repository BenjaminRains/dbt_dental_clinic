{% test test_date_not_future(model, column_name, years_ahead=5, allow_null=true) %}
    {# 
        Test that validates a date column is not too far in the future.
        
        Args:
            model: The model to test (automatically provided by dbt)
            column_name: The name of the date column to test (automatically provided by dbt)
            years_ahead: Maximum number of years in the future allowed (default 5)
            allow_null: Whether to allow null values in the test column (default true)
    #}

    select
        {{ column_name }},
        'should not be more than {{ years_ahead }} years in the future' as expected_value
    from {{ model }}
    where 
        {% if allow_null %}
        {{ column_name }} is not null and
        {% endif %}
        not({{ column_name }} <= current_date + interval '{{ years_ahead }} years')

{% endtest %} 