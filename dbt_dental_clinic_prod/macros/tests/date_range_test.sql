{% test date_range_test(model, column_name, min_date, max_date) %}
  select
    *
  from {{ model }}
  where not({{ column_name }} BETWEEN {{ min_date }} AND {{ max_date }})
{% endtest %} 