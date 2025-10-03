{% macro validate_id_transformation(old_column, new_column, table_ref) %}
{#- 
    Validates that ID column transformations are working correctly
    
    Args:
        old_column (str): Original column name (e.g., '"PatNum"')
        new_column (str): New column name (e.g., 'patient_id')
        table_ref (str): Table reference to validate against
        
    Returns:
        SQL query to check transformation accuracy
        
    Example usage:
        {{ validate_id_transformation('"PatNum"', 'patient_id', 'stg_opendental__patient') }}
-#}
select 
    '{{ old_column }} -> {{ new_column }}' as transformation,
    count(*) as total_rows,
    count({{ old_column }}) as old_column_non_null,
    count({{ new_column }}) as new_column_non_null,
    sum(case when {{ old_column }} = {{ new_column }} then 1 else 0 end) as matching_values,
    sum(case when {{ old_column }} != {{ new_column }} then 1 else 0 end) as non_matching_values
from {{ table_ref }}
{% endmacro %}

{% macro validate_boolean_transformation(old_column, new_column, table_ref) %}
{#- 
    Validates that boolean column transformations are working correctly
    
    Args:
        old_column (str): Original column name (e.g., '"IsHidden"')
        new_column (str): New column name (e.g., 'is_hidden')
        table_ref (str): Table reference to validate against
        
    Returns:
        SQL query to check boolean transformation accuracy
        
    Example usage:
        {{ validate_boolean_transformation('"IsHidden"', 'is_hidden', 'stg_opendental__patient') }}
-#}
select 
    '{{ old_column }} -> {{ new_column }}' as transformation,
    count(*) as total_rows,
    sum(case when {{ old_column }} = 1 and {{ new_column }} = true then 1 else 0 end) as correct_true_conversions,
    sum(case when {{ old_column }} = 0 and {{ new_column }} = false then 1 else 0 end) as correct_false_conversions,
    sum(case when {{ old_column }} is null and {{ new_column }} is null then 1 else 0 end) as correct_null_conversions,
    sum(case 
        when ({{ old_column }} = 1 and {{ new_column }} != true) 
        or ({{ old_column }} = 0 and {{ new_column }} != false)
        or ({{ old_column }} is null and {{ new_column }} is not null)
        then 1 else 0 
    end) as incorrect_conversions
from {{ table_ref }}
{% endmacro %}

{% macro compare_model_outputs(old_model, new_model, key_column) %}
{#- 
    Compares outputs between old and new model versions during migration
    
    Args:
        old_model (str): Reference to old model
        new_model (str): Reference to new model  
        key_column (str): Primary key column for joining
        
    Returns:
        SQL query to identify differences between model versions
        
    Example usage:
        {{ compare_model_outputs('stg_opendental__patient_old', 'stg_opendental__patient', 'patient_id') }}
-#}
with old_data as (
    select * from {{ old_model }}
),
new_data as (
    select * from {{ new_model }}
),
comparison as (
    select 
        coalesce(o.{{ key_column }}, n.{{ key_column }}) as {{ key_column }},
        case 
            when o.{{ key_column }} is null then 'new_only'
            when n.{{ key_column }} is null then 'old_only'
            else 'both'
        end as presence,
        o.* as old_values,
        n.* as new_values
    from old_data o
    full outer join new_data n on o.{{ key_column }} = n.{{ key_column }}
)
select 
    presence,
    count(*) as row_count
from comparison
group by presence
{% endmacro %} 