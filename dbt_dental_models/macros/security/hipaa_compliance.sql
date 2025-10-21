-- =====================================================
-- HIPAA Compliance & Security Macros
-- =====================================================
--
-- ⚠️  WARNING: DEVELOPMENT / DEMONSTRATION ONLY ⚠️
--
-- This macro file is currently in development and has NOT been reviewed
-- for HIPAA compliance by legal or security professionals.
--
-- DO NOT USE IN PRODUCTION without proper compliance review:
-- - Legal counsel review required
-- - Security audit required
-- - HIPAA compliance officer approval required
-- - Privacy impact assessment required
--
-- This code is provided as a DEMONSTRATION ONLY to illustrate concepts
-- for implementing security and access controls in a dbt project.
--
-- Actual HIPAA compliance requires:
-- - Comprehensive security policies and procedures
-- - Technical safeguards (encryption, access controls, audit logs)
-- - Administrative safeguards (training, policies, risk assessments)
-- - Physical safeguards (facility access, workstation security)
-- - Business associate agreements
-- - Regular compliance audits and monitoring
--
-- For production use, consult qualified HIPAA compliance professionals.
--
-- =====================================================

{% macro apply_hipaa_grants(model_name, schema_name='marts') %}
  {% set hipaa_sql %}
    -- Apply HIPAA-compliant grants based on model type
    
    {% if 'patient' in model_name.lower() %}
      -- Patient data - clinical and administrative access only
      GRANT SELECT ON {{ schema_name }}.{{ model_name }} TO clinical_staff_role, administrative_staff_role, management_role;
      
    {% elif 'payment' in model_name.lower() or 'billing' in model_name.lower() %}
      -- Financial data - financial staff and management only
      GRANT SELECT ON {{ schema_name }}.{{ model_name }} TO financial_staff_role, billing_role, finance_role, management_role;
      
    {% elif 'appointment' in model_name.lower() or 'schedule' in model_name.lower() %}
      -- Scheduling data - administrative and clinical access
      GRANT SELECT ON {{ schema_name }}.{{ model_name }} TO administrative_staff_role, clinical_staff_role, scheduler_role;
      
    {% elif 'procedure' in model_name.lower() or 'treatment' in model_name.lower() %}
      -- Clinical data - clinical staff and management
      GRANT SELECT ON {{ schema_name }}.{{ model_name }} TO clinical_staff_role, clinical_manager_role, management_role;
      
    {% else %}
      -- Default - analyst and management access
      GRANT SELECT ON {{ schema_name }}.{{ model_name }} TO analyst_role, management_role;
    {% endif %}
  {% endset %}
  
  {% do run_query(hipaa_sql) %}
  {{ log("Applied HIPAA-compliant grants to " ~ schema_name ~ "." ~ model_name, info=true) }}
{% endmacro %}

{% macro audit_hipaa_access() %}
  {% set audit_sql %}
    -- Audit HIPAA-sensitive table access
    SELECT 
        t.table_schema,
        t.table_name,
        p.privilege_type,
        p.grantee,
        CASE 
            WHEN t.table_name LIKE '%patient%' THEN 'Patient Data'
            WHEN t.table_name LIKE '%payment%' THEN 'Financial Data'
            WHEN t.table_name LIKE '%procedure%' THEN 'Clinical Data'
            ELSE 'General Data'
        END as data_classification
    FROM information_schema.tables t
    JOIN information_schema.table_privileges p 
        ON t.table_name = p.table_name 
        AND t.table_schema = p.table_schema
    WHERE t.table_schema IN ('staging', 'intermediate', 'marts')
    AND p.privilege_type = 'SELECT'
    ORDER BY data_classification, t.table_name, p.grantee;
  {% endset %}
  
  {% set results = run_query(audit_sql) %}
  {{ log("=== HIPAA Access Audit ===", info=true) }}
  {% for row in results %}
    {{ log(row.data_classification ~ ": " ~ row.table_schema ~ "." ~ row.table_name ~ " -> " ~ row.grantee, info=true) }}
  {% endfor %}
  
  {{ return(results) }}
{% endmacro %}

{% macro check_hipaa_compliance() %}
  {% set compliance_sql %}
    -- Check for potential HIPAA violations
    WITH hipaa_tables AS (
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ('staging', 'intermediate', 'marts')
        AND (table_name LIKE '%patient%' OR table_name LIKE '%payment%' OR table_name LIKE '%procedure%')
    ),
    table_grants AS (
        SELECT 
            ht.table_schema,
            ht.table_name,
            tp.grantee,
            tp.privilege_type
        FROM hipaa_tables ht
        JOIN information_schema.table_privileges tp
            ON ht.table_name = tp.table_name 
            AND ht.table_schema = tp.table_schema
        WHERE tp.privilege_type = 'SELECT'
    )
    SELECT 
        table_schema,
        table_name,
        grantee,
        CASE 
            WHEN table_name LIKE '%patient%' AND grantee NOT IN ('clinical_staff_role', 'administrative_staff_role', 'management_role') 
                THEN 'POTENTIAL VIOLATION: Patient data accessible to non-authorized role'
            WHEN table_name LIKE '%payment%' AND grantee NOT IN ('financial_staff_role', 'billing_role', 'finance_role', 'management_role')
                THEN 'POTENTIAL VIOLATION: Financial data accessible to non-authorized role'
            ELSE 'COMPLIANT'
        END as compliance_status
    FROM table_grants
    WHERE compliance_status != 'COMPLIANT';
  {% endset %}
  
  {% set results = run_query(compliance_sql) %}
  {% if results %}
    {{ log("=== HIPAA COMPLIANCE VIOLATIONS DETECTED ===", info=true) }}
    {% for row in results %}
      {{ log("VIOLATION: " ~ row.compliance_status ~ " (" ~ row.table_schema ~ "." ~ row.table_name ~ " -> " ~ row.grantee ~ ")", info=true) }}
    {% endfor %}
  {% else %}
    {{ log("=== HIPAA COMPLIANCE CHECK PASSED ===", info=true) }}
  {% endif %}
  
  {{ return(results) }}
{% endmacro %}

{% macro get_hipaa_access_level(column_meta) %}
  {% if column_meta is mapping %}
    {% set access_level = column_meta.get('access_level', 'unrestricted') %}
    
    {% if access_level == 'clinical_only' %}
      {% set allowed_roles = ['clinical_staff_role', 'clinical_manager_role'] %}
    {% elif access_level == 'administrative_only' %}
      {% set allowed_roles = ['administrative_staff_role', 'office_manager_role'] %}
    {% elif access_level == 'financial_only' %}
      {% set allowed_roles = ['financial_staff_role', 'billing_role', 'finance_role'] %}
    {% elif access_level == 'restricted' %}
      {% set allowed_roles = ['clinical_staff_role', 'administrative_staff_role', 'management_role'] %}
    {% elif access_level == 'unrestricted' %}
      {% set allowed_roles = ['analyst_role', 'management_role', 'technical_staff_role'] %}
    {% else %}
      {% set allowed_roles = [] %}
    {% endif %}
    
    {{ return(allowed_roles) }}
  {% else %}
    {{ return(['analyst_role']) }}
  {% endif %}
{% endmacro %}

{% macro classify_hipaa_data(table_name) %}
  {% if 'patient' in table_name.lower() %}
    {{ return('patient_data') }}
  {% elif 'payment' in table_name.lower() or 'billing' in table_name.lower() %}
    {{ return('financial_data') }}
  {% elif 'procedure' in table_name.lower() or 'treatment' in table_name.lower() %}
    {{ return('clinical_data') }}
  {% elif 'appointment' in table_name.lower() or 'schedule' in table_name.lower() %}
    {{ return('operational_data') }}
  {% else %}
    {{ return('general_data') }}
  {% endif %}
{% endmacro %}

{% macro get_required_roles_for_data_type(data_type) %}
  {% if data_type == 'patient_data' %}
    {{ return(['clinical_staff_role', 'administrative_staff_role', 'management_role']) }}
  {% elif data_type == 'financial_data' %}
    {{ return(['financial_staff_role', 'billing_role', 'finance_role', 'management_role']) }}
  {% elif data_type == 'clinical_data' %}
    {{ return(['clinical_staff_role', 'clinical_manager_role', 'management_role']) }}
  {% elif data_type == 'operational_data' %}
    {{ return(['administrative_staff_role', 'clinical_staff_role', 'scheduler_role']) }}
  {% else %}
    {{ return(['analyst_role', 'management_role']) }}
  {% endif %}
{% endmacro %}
