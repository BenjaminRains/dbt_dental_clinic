{% test test_column_greater_than(model, column_name, value) %}
    {# 
        Test that validates a column value is greater than a specified value.
        
        Args:
            model: The model to test (automatically provided by dbt)
            column_name: The name of the column to test (automatically provided by dbt)
            value: The value to compare against
    #}

    select
        {{ column_name }},
        'should be greater than {{ value }}' as expected_value
    from {{ model }}
    where not({{ column_name }} > {{ value }})

{% endtest %} 