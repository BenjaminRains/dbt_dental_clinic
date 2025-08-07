{% macro test_relationships_with_third_table(model, column_name, to, field, from_condition='1=1', to_condition='1=1', third_table=none, third_table_field=none) %}

{# Set default values for parameters #}
{% if third_table is none %}
    {% set third_table_check = '1=1' %}
{% else %}
    {% set third_table_check = column_name ~ ' IN (SELECT ' ~ third_table_field ~ ' FROM ' ~ third_table ~ ')' %}
{% endif %}

{# Build the SQL query that implements the test #}
with left_table as (

  select
    {{ column_name }} as id

  from {{ model }}

  where {{ column_name }} is not null
    and {{ from_condition }}
    and {{ third_table_check }}

),

right_table as (

  select
    {{ field }} as id

  from {{ to }}

  where {{ field }} is not null
    and {{ to_condition }}

),

exceptions as (

  select
    left_table.id,
    right_table.id as right_id

  from left_table

  left join right_table
         on left_table.id = right_table.id

  where right_table.id is null

)

select * from exceptions

{% endmacro %}