{% macro quote_column(column_name) %}
  {% if target.type == 'postgres' %}
    "{{ column_name }}"
  {% else %}
    {{ column_name }}
  {% endif %}
{% endmacro %}
