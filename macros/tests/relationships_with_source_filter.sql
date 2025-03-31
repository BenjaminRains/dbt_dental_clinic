{% macro test_relationships_with_source_filter(model, column_name, field, to, source_relation, source_column, source_filter) %}

with parent as (
    select 
        {{ field }} as id 
    from {{ to }}
),

child as (
    select 
        {{ column_name }} as id
    from {{ model }}
    where {{ column_name }} in (
        select {{ source_column }}
        from {{ source_relation }}
        where {{ source_filter }}
    )
),

invalid_refs as (
    select 
        child.id
    from child
    left join parent
        on child.id = parent.id
    where parent.id is null
)

select *
from invalid_refs

{% endmacro %} 