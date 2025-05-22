{% test test_date_after(model, column_name, compare_column, allow_null=true) %}
    {# 
        Test that validates a date column is after another date column.
        
        Args:
            model: The model to test (automatically provided by dbt)
            column_name: The name of the date column to test (automatically provided by dbt)
            compare_column: The name of the date column to compare against
            allow_null: Whether to allow null values in the test column (default true)
    #}

    select
        {{ column_name }},
        {{ compare_column }},
        'should be after {{ compare_column }}' as expected_value
    from {{ model }}
    where 
        {% if allow_null %}
        {{ column_name }} is not null and
        {% endif %}
        not({{ column_name }} >= {{ compare_column }})

{% endtest %} 