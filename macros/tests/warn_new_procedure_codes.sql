{% test warn_new_procedure_codes(model, column_name, reference_table=none, reference_column=none, reference_date_column=none) %}
/*
    Test to detect when new procedure codes have been added to the system.
    This is useful for monitoring changes to the procedure code catalog
    and alerting stakeholders when new codes appear.

    Args:
        model: The model being tested
        column_name: The column containing procedure codes
        reference_table: Optional table with known procedure codes
        reference_column: Optional column in reference table with procedure codes
        reference_date_column: Optional date column to filter reference table (if needed)
*/

-- If reference table is provided, use it, otherwise default to the model itself
{% if reference_table and reference_column %}
    {% set ref_source = ref(reference_table) %}
    {% set ref_col = reference_column %}
    
    {% if reference_date_column %}
        {% set date_filter = "WHERE " ~ reference_date_column ~ " < CURRENT_DATE - INTERVAL '30 days'" %}
    {% else %}
        {% set date_filter = "" %}
    {% endif %}
    
    with reference_codes as (
        select distinct {{ ref_col }} as procedure_code
        from {{ ref_source }}
        {{ date_filter }}
    ),
{% else %}
    -- If no reference is provided, use a temporary table to avoid import during initial load
    {% if is_incremental() %}
        with reference_codes as (
            select distinct {{ column_name }} as procedure_code
            from {{ this }}
        ),
    {% else %}
        -- For initial load, don't warn about new codes
        with reference_codes as (
            select distinct {{ column_name }} as procedure_code
            from {{ model }}
        ),
    {% endif %}
{% endif %}

current_codes as (
    select distinct {{ column_name }} as procedure_code
    from {{ model }}
),

new_codes as (
    select 
        c.procedure_code,
        'New procedure code detected: ' || c.procedure_code as description
    from current_codes c
    left join reference_codes r
        on c.procedure_code = r.procedure_code
    where r.procedure_code is null
)

select * from new_codes

{% endtest %}