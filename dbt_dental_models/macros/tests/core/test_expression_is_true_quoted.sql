{% test expression_is_true_quoted(model, column_name, expression) %}

  select *
  from {{ model }}
  where not({{ expression }})

{% endtest %}
