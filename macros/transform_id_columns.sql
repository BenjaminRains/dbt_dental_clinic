{% macro transform_id_columns(transformations) %}
{#- 
    Transforms OpenDental ID columns to standardized snake_case naming
    
    Args:
        transformations (list): List of dicts with 'source' and 'target' keys
        
    Returns:
        SQL columns for standardized ID transformations
        
    Example usage:
        {{ transform_id_columns([
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'},
            {'source': '"PriProv"', 'target': 'primary_provider_id'}
        ]) }}
-#}
    {%- for transformation in transformations -%}
        {{ transformation.source }} as {{ transformation.target }}
        {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
{% endmacro %}

{% macro transform_common_id_columns() %}
{#- 
    Transforms the most common OpenDental ID columns using standardized patterns
    
    Returns:
        SQL columns for the most frequently used ID transformations
        
    Example usage:
        -- Primary key and common relationships
        {{ transform_common_id_columns() }},
        
        -- Additional specific IDs
        "FeeSched" as fee_schedule_id
-#}
        -- Primary key (most common pattern)
        COALESCE("PatNum", "ClaimNum", "ProcNum", "PayNum", "ApptNum") as primary_id,
        
        -- Common relationship IDs
        "PatNum" as patient_id,
        "ClinicNum" as clinic_id,
        "PriProv" as primary_provider_id,
        "SecProv" as secondary_provider_id,
        "Guarantor" as guarantor_id
{% endmacro %} 