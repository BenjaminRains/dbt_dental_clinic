{% test compare_source_staging_rowcount(model, source_relation, source_column, staged_column, date_value) %}

{# Set up the failure calculation for the test #}
{{ config(fail_calc = 'sum(coalesce(diff_count, 0))') }}

with source_counts as (
    select count(*) as source_count
    from {{ source_relation }}
    where {{ source_column }} >= '{{ date_value }}'
),

staged_counts as (
    select count(*) as staged_count
    from {{ model }}
    where {{ staged_column }} >= '{{ date_value }}'
),

final as (
    select
        source_count,
        staged_count,
        abs(source_count - staged_count) as diff_count
    from source_counts
    cross join staged_counts
)

select * from final

{% endtest %} 