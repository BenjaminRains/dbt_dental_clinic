{% test numeric_range_test(model, column_name, min_value, max_value) %}
  select
    *
  from {{ model }}
  where not({{ column_name }}::numeric BETWEEN {{ min_value }} AND {{ max_value }})
{% endtest %} 