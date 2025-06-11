{% test validate_procedure_code_prefixes(model, column_name) %}

with validation as (
    select
        {{ column_name }},
        SUBSTRING({{ column_name }}, 1, 2) as code_prefix
    from {{ model }}
    where {{ column_name }} like 'D%'
      and LENGTH({{ column_name }}) > 1  -- Exclude single 'D' 
      and SUBSTRING({{ column_name }}, 1, 2) not in ('D0', 'D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9')
)

select * from validation

{% endtest %} 