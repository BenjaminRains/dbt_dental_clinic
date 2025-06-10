{% macro transform_id_columns(transformations) %}
{#- 
    Transforms OpenDental ID columns using PostgreSQL's safe conversion approach
    
    Source columns are INTEGER type but may contain invalid data like "N"
    so we need to cast to text first before pattern matching
-#}
    {%- for transformation in transformations -%}
        CASE 
            WHEN {{ transformation.source }}::text ~ '^[1-9][0-9]*$' THEN {{ transformation.source }}::text::integer
            ELSE NULL
        END as {{ transformation.target }}
        {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
{% endmacro %}

{% macro transform_common_id_columns() %}
{#- 
    Transforms common ID columns with safe conversion
-#}
        CASE 
            WHEN COALESCE("PatNum", "ClaimNum", "ProcNum", "PayNum", "ApptNum")::text ~ '^[1-9][0-9]*$' 
            THEN COALESCE("PatNum", "ClaimNum", "ProcNum", "PayNum", "ApptNum")::text::integer
            ELSE NULL
        END as primary_id,
        
        CASE 
            WHEN "PatNum"::text ~ '^[1-9][0-9]*$' THEN "PatNum"::text::integer
            ELSE NULL
        END as patient_id,
        
        CASE 
            WHEN "ClinicNum"::text ~ '^[1-9][0-9]*$' THEN "ClinicNum"::text::integer
            ELSE NULL
        END as clinic_id,
        
        CASE 
            WHEN "PriProv"::text ~ '^[1-9][0-9]*$' THEN "PriProv"::text::integer
            ELSE NULL
        END as primary_provider_id,
        
        CASE 
            WHEN "SecProv"::text ~ '^[1-9][0-9]*$' THEN "SecProv"::text::integer
            ELSE NULL
        END as secondary_provider_id,
        
        CASE 
            WHEN "Guarantor"::text ~ '^[1-9][0-9]*$' THEN "Guarantor"::text::integer
            ELSE NULL
        END as guarantor_id
{% endmacro %} 