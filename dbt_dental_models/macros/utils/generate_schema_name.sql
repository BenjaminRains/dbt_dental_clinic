{#
    Override dbt's default schema naming behavior.
    
    By default, dbt prefixes custom schema names with the target schema from profiles.yml.
    For example, if target.schema = 'raw' and a model specifies schema: 'staging',
    dbt would create a schema named 'raw_staging'.
    
    This macro overrides that behavior to use custom schema names directly without prefixing.
    This ensures models are created in clean schema names: staging, int, marts
    
    IMPORTANT: This macro is automatically called by dbt for ALL models. You don't need to
    explicitly call it - dbt uses it internally when determining schema names.
    
    See: https://docs.getdbt.com/reference/dbt-jinja-functions/generate_schema_name
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {#- Use custom schema name directly without prefixing with target schema -#}
        {#- This prevents schemas like raw_staging, raw_int, raw_marts -#}
        {#- Instead creates: staging, int, marts -#}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
