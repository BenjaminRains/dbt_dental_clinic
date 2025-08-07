{% test test_status_with_date(model, column_name, date_column, status_value, date_condition) %}
    {# 
        Test that validates a status field against a date condition.
        
        Args:
            model: The model to test (automatically provided by dbt)
            column_name: The name of the status column to test (automatically provided by dbt)
            date_column: The name of the date column to check against
            status_value: The expected status value when date_condition is true
            date_condition: The date condition to check (e.g., "is null", "< current_date")
    #}

    select
        {{ column_name }},
        {{ date_column }},
        case 
            when {{ date_condition }} then 'should be {{ status_value }}'
            else 'should not be {{ status_value }}'
        end as expected_status
    from {{ model }}
    where 
        -- When date condition is true, status should match
        ({{ date_condition }} and {{ column_name }} != '{{ status_value }}')
        or 
        -- When date condition is false, status should not match
        (not ({{ date_condition }}) and {{ column_name }} = '{{ status_value }}')

{% endtest %} 