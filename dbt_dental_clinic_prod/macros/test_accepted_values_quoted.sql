{% test accepted_values_quoted(model, column_name, values) %}

  select "{{ column_name }}" as value_field,
         count(*) as n_records

  from {{ model }}
  where "{{ column_name }}" is not null
    and "{{ column_name }}" not in (
      {%- for value in values -%}
        {%- if not loop.first -%},{%- endif -%}
        {{ value }}
      {%- endfor -%}
    )
  group by "{{ column_name }}"

{% endtest %}
