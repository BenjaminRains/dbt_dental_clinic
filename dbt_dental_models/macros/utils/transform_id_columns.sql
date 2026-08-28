{% macro transform_id_columns(transformations) %}
{#- Dispatch so Postgres (~ regex) and Snowflake (REGEXP_LIKE) both work. -#}
    {{ return(adapter.dispatch('transform_id_columns', 'dbt_dental_models')(transformations)) }}
{% endmacro %}

{% macro default__transform_id_columns(transformations) %}
{#-
    Transforms OpenDental ID columns using PostgreSQL's safe conversion approach

    Source columns are INTEGER type but may contain invalid data like "N"
    so we need to cast to text first before pattern matching

    Updated to preserve 0 values which are meaningful in OpenDental
    (e.g., coverage_category_id = 0 means "no specific category/general benefit")
-#}
    {%- for transformation in transformations -%}
        CASE
            WHEN {{ transformation.source }}::text ~ '^[0-9]+$' THEN {{ transformation.source }}::text::integer
            ELSE NULL
        END as {{ transformation.target }}
        {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
{% endmacro %}

{% macro snowflake__transform_id_columns(transformations) %}
{#- Snowflake: REGEXP_LIKE + TRY_TO_NUMBER (no Postgres ~ operator). -#}
    {%- for transformation in transformations -%}
        CASE
            WHEN REGEXP_LIKE(TO_VARCHAR({{ transformation.source }}), '^[0-9]+$')
                THEN TRY_TO_NUMBER(TO_VARCHAR({{ transformation.source }}))::INTEGER
            ELSE NULL
        END as {{ transformation.target }}
        {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
{% endmacro %}

{% macro transform_common_id_columns() %}
    {{ return(adapter.dispatch('transform_common_id_columns', 'dbt_dental_models')()) }}
{% endmacro %}

{% macro default__transform_common_id_columns() %}
{#-
    Transforms common ID columns with safe conversion (Postgres)
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

{% macro snowflake__transform_common_id_columns() %}
        CASE
            WHEN REGEXP_LIKE(TO_VARCHAR(COALESCE("PatNum", "ClaimNum", "ProcNum", "PayNum", "ApptNum")), '^[1-9][0-9]*$')
            THEN TRY_TO_NUMBER(TO_VARCHAR(COALESCE("PatNum", "ClaimNum", "ProcNum", "PayNum", "ApptNum")))::INTEGER
            ELSE NULL
        END as primary_id,

        CASE
            WHEN REGEXP_LIKE(TO_VARCHAR("PatNum"), '^[1-9][0-9]*$') THEN TRY_TO_NUMBER(TO_VARCHAR("PatNum"))::INTEGER
            ELSE NULL
        END as patient_id,

        CASE
            WHEN REGEXP_LIKE(TO_VARCHAR("ClinicNum"), '^[1-9][0-9]*$') THEN TRY_TO_NUMBER(TO_VARCHAR("ClinicNum"))::INTEGER
            ELSE NULL
        END as clinic_id,

        CASE
            WHEN REGEXP_LIKE(TO_VARCHAR("PriProv"), '^[1-9][0-9]*$') THEN TRY_TO_NUMBER(TO_VARCHAR("PriProv"))::INTEGER
            ELSE NULL
        END as primary_provider_id,

        CASE
            WHEN REGEXP_LIKE(TO_VARCHAR("SecProv"), '^[1-9][0-9]*$') THEN TRY_TO_NUMBER(TO_VARCHAR("SecProv"))::INTEGER
            ELSE NULL
        END as secondary_provider_id,

        CASE
            WHEN REGEXP_LIKE(TO_VARCHAR("Guarantor"), '^[1-9][0-9]*$') THEN TRY_TO_NUMBER(TO_VARCHAR("Guarantor"))::INTEGER
            ELSE NULL
        END as guarantor_id
{% endmacro %}
