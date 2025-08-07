{% test valid_collection_efficiency(model, column_name) %}
    select
        *
    from {{ model }}
    where total_ar_balance >= 0.01 
    and ({{ column_name }} < -2 OR {{ column_name }} > 10)
{% endtest %} 