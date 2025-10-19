# dbt Built-in Grants Functionality - Implementation Guide

**Date:** October 19, 2025  
**Purpose:** Guide for implementing database access controls using dbt's native grants functionality  
**Status:** Production-Ready (uses dbt's official features)

---

## Overview

dbt provides built-in, production-tested functionality for managing database grants declaratively. This guide shows how to implement grants for the dbt_dental_clinic project using dbt's native features instead of custom macros.

**Why Use dbt's Built-in Grants?**
- ✅ **Battle-tested** across thousands of production environments
- ✅ **Database agnostic** (Postgres, Snowflake, BigQuery, Redshift, etc.)
- ✅ **Officially supported** by dbt Labs with ongoing maintenance
- ✅ **Safe & reliable** with proper error handling
- ✅ **Well-documented** with community support

---

## Basic Concepts

### What Are Grants?

Grants control who can access database objects (tables, views, schemas). In SQL:

```sql
-- Grant SELECT permission to a role
GRANT SELECT ON schema.table TO role_name;

-- Revoke permission
REVOKE SELECT ON schema.table FROM role_name;
```

### How dbt Manages Grants

dbt applies grants automatically after building models:

1. **Declarative Configuration**: Define grants in model config
2. **Automatic Application**: dbt applies grants after `dbt run`
3. **Idempotent**: Running multiple times produces same result
4. **Audit Trail**: Logs all grant operations

---

## Configuration Methods

### Method 1: In-Model Configuration (Recommended for Specific Models)

Configure grants directly in SQL model files:

```sql
-- models/marts/dim_patient.sql
{{
    config(
        materialized='table',
        grants={
            'select': ['clinical_staff_role', 'administrative_staff_role']
        }
    )
}}

SELECT
    patient_id,
    preferred_name,
    -- ... other fields
FROM {{ ref('int_patient_profile') }}
```

**Use When:**
- Model has unique access requirements
- Need model-specific security controls
- Override project-level defaults

---

### Method 2: Project-Level Configuration (Recommended for Consistency)

Configure grants in `dbt_project.yml` for consistent access patterns:

```yaml
# dbt_project.yml
models:
  dbt_dental_clinic:
    marts:
      # Default grants for all marts
      +grants:
        select: ['analyst_role', 'reporting_role']
      
      # Patient data - restricted PHI access
      dim_patient:
        +grants:
          select: ['clinical_staff_role', 'administrative_staff_role', 'management_role']
      
      # Financial data - finance team only
      fact_payment:
        +grants:
          select: ['finance_role', 'billing_role', 'management_role']
      
      fact_claim:
        +grants:
          select: ['finance_role', 'billing_role', 'insurance_coordinator_role']
      
      # Clinical data - clinical staff access
      fact_procedure:
        +grants:
          select: ['clinical_staff_role', 'clinical_manager_role', 'management_role']
      
      fact_appointment:
        +grants:
          select: ['scheduler_role', 'clinical_staff_role', 'administrative_staff_role']
      
    intermediate:
      # Intermediate models - limited access
      +grants:
        select: ['dbt_user', 'analyst_role']
```

**Use When:**
- Standardizing access patterns across model groups
- Implementing organization-wide security policies
- Simplifying grant management at scale

---

## Dental Clinic Use Cases

### Use Case 1: PHI Protection (Patient Data)

**Requirements:**
- HIPAA compliance
- Restrict access to clinical and administrative staff only
- No access for general analysts

**Implementation:**

```yaml
# dbt_project.yml
models:
  dbt_dental_clinic:
    marts:
      dim_patient:
        +grants:
          select: 
            - 'clinical_staff_role'
            - 'administrative_staff_role'
            - 'management_role'
            - 'hipaa_auditor_role'
```

**Models Affected:**
- `dim_patient`
- `dim_provider` (limited PHI)
- `fact_communication` (patient communications)
- Any model with patient identifiers or PHI

---

### Use Case 2: Financial Data (Payment/Billing)

**Requirements:**
- Restrict to finance and billing teams
- Management oversight access
- No clinical staff access to financial details

**Implementation:**

```yaml
# dbt_project.yml
models:
  dbt_dental_clinic:
    marts:
      fact_payment:
        +grants:
          select: ['finance_role', 'billing_role', 'management_role']
      
      fact_claim:
        +grants:
          select: ['finance_role', 'billing_role', 'insurance_coordinator_role']
      
      mart_ar_summary:
        +grants:
          select: ['finance_role', 'management_role', 'collections_role']
```

**Models Affected:**
- `fact_payment`
- `fact_claim`
- `mart_ar_summary`
- `mart_production_summary`

---

### Use Case 3: General Analytics (Non-PHI)

**Requirements:**
- Broad access for reporting and analysis
- De-identified or aggregated data only
- Standard business intelligence use

**Implementation:**

```yaml
# dbt_project.yml
models:
  dbt_dental_clinic:
    marts:
      mart_production_summary:
        +grants:
          select: ['analyst_role', 'reporting_role', 'management_role', 'bi_tool_role']
      
      mart_provider_performance:
        +grants:
          select: ['analyst_role', 'management_role', 'provider_role']
      
      dim_procedure:
        +grants:
          select: ['analyst_role', 'clinical_staff_role', 'reporting_role']
```

**Models Affected:**
- `mart_production_summary`
- `mart_provider_performance`
- `dim_procedure`
- `dim_date`
- `dim_clinic`

---

## Advanced Configuration

### Multiple Privileges

Grant different permission levels:

```sql
{{
    config(
        grants={
            'select': ['analyst_role', 'reporting_role'],
            'insert': ['etl_role'],
            'update': ['etl_role'],
            'delete': ['admin_role']
        }
    )
}}
```

### Copy Grants from Another Model

Inherit grants from existing objects:

```yaml
models:
  my_model:
    +grants:
      copy_grants: true  # Copy from source or existing table
```

---

## Role-Based Access Control (RBAC) Strategy

### Recommended Roles for Dental Clinic

```sql
-- Clinical roles
CREATE ROLE clinical_staff_role;
CREATE ROLE clinical_manager_role;
CREATE ROLE provider_role;

-- Administrative roles
CREATE ROLE administrative_staff_role;
CREATE ROLE scheduler_role;
CREATE ROLE front_desk_role;

-- Financial roles
CREATE ROLE finance_role;
CREATE ROLE billing_role;
CREATE ROLE collections_role;
CREATE ROLE insurance_coordinator_role;

-- Analytics roles
CREATE ROLE analyst_role;
CREATE ROLE reporting_role;
CREATE ROLE bi_tool_role;

-- Management roles
CREATE ROLE management_role;
CREATE ROLE executive_role;

-- Compliance roles
CREATE ROLE hipaa_auditor_role;
CREATE ROLE security_admin_role;

-- Technical roles
CREATE ROLE dbt_user;
CREATE ROLE etl_role;
```

### Role Assignment Matrix

| Model Type | Roles with Access |
|-----------|------------------|
| **PHI Models** (dim_patient) | clinical_staff_role, administrative_staff_role, management_role, hipaa_auditor_role |
| **Financial Models** (fact_payment, fact_claim) | finance_role, billing_role, management_role |
| **Clinical Models** (fact_procedure, fact_appointment) | clinical_staff_role, clinical_manager_role, scheduler_role |
| **Analytics Models** (marts) | analyst_role, reporting_role, management_role |
| **Intermediate Models** | dbt_user, analyst_role (limited) |
| **Staging Models** | dbt_user only (no direct access) |

---

## Implementation Steps

### Step 1: Create Database Roles

```sql
-- Run this in your PostgreSQL database as superuser
-- File: setup_roles.sql

-- Create all necessary roles
CREATE ROLE clinical_staff_role;
CREATE ROLE administrative_staff_role;
CREATE ROLE finance_role;
CREATE ROLE billing_role;
CREATE ROLE analyst_role;
CREATE ROLE management_role;
CREATE ROLE dbt_user;

-- Grant schema usage
GRANT USAGE ON SCHEMA raw_marts TO 
    clinical_staff_role,
    administrative_staff_role,
    finance_role,
    analyst_role,
    management_role;

-- Grant schema usage to intermediate (limited)
GRANT USAGE ON SCHEMA raw_intermediate TO 
    dbt_user,
    analyst_role;

-- Verify roles were created
SELECT rolname FROM pg_roles WHERE rolname LIKE '%_role';
```

### Step 2: Configure dbt User Permissions

```sql
-- dbt_user needs GRANT privileges to manage access
GRANT ALL PRIVILEGES ON SCHEMA raw_marts TO dbt_user WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON SCHEMA raw_intermediate TO dbt_user WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON SCHEMA raw_staging TO dbt_user WITH GRANT OPTION;
```

### Step 3: Add Grants to dbt_project.yml

```yaml
# dbt_project.yml
models:
  dbt_dental_clinic:
    marts:
      +grants:
        select: ['analyst_role', 'management_role']  # Default
      
      # Override for specific models
      dim_patient:
        +grants:
          select: ['clinical_staff_role', 'administrative_staff_role']
```

### Step 4: Apply Grants

```bash
# Run dbt to build models and apply grants
dbt run --models marts

# Verify grants were applied
dbt run-operation check_grants --args '{model_name: dim_patient}'
```

### Step 5: Verify Grants

```sql
-- Check grants on a specific table
SELECT 
    grantee,
    privilege_type,
    is_grantable
FROM information_schema.table_privileges
WHERE table_schema = 'raw_marts'
  AND table_name = 'dim_patient'
ORDER BY grantee, privilege_type;
```

---

## Testing & Validation

### Test 1: Verify Correct Access

```sql
-- Test as clinical staff role
SET ROLE clinical_staff_role;
SELECT COUNT(*) FROM raw_marts.dim_patient;  -- Should work
SELECT COUNT(*) FROM raw_marts.fact_payment; -- Should fail

-- Reset
RESET ROLE;
```

### Test 2: Verify Denied Access

```sql
-- Test as analyst role (should NOT have PHI access)
SET ROLE analyst_role;
SELECT COUNT(*) FROM raw_marts.dim_patient;  -- Should fail
SELECT COUNT(*) FROM raw_marts.mart_production_summary; -- Should work

-- Reset
RESET ROLE;
```

### Test 3: Audit All Grants

```sql
-- Get all grants for dental clinic schemas
SELECT 
    table_schema,
    table_name,
    grantee,
    privilege_type
FROM information_schema.table_privileges
WHERE table_schema IN ('raw_marts', 'raw_intermediate', 'raw_staging')
  AND grantee NOT IN ('postgres', 'PUBLIC')
ORDER BY table_schema, table_name, grantee;
```

---

## Best Practices

### 1. Principle of Least Privilege

Grant only the minimum permissions needed:

```yaml
# ❌ BAD: Over-permissive
dim_patient:
  +grants:
    select: ['PUBLIC']  # Everyone can see PHI!

# ✅ GOOD: Restricted access
dim_patient:
  +grants:
    select: ['clinical_staff_role', 'administrative_staff_role']
```

### 2. Use Role Hierarchies

Create role hierarchies for easier management:

```sql
-- Management has all lower-level permissions
GRANT clinical_staff_role TO management_role;
GRANT finance_role TO management_role;
GRANT analyst_role TO management_role;
```

### 3. Document Role Assignments

Maintain documentation of who should have which roles:

```markdown
## Role Assignments

**clinical_staff_role:**
- Dr. Smith (provider)
- Nurse Jones (hygienist)
- Tech Williams (dental assistant)

**finance_role:**
- Mary Johnson (billing manager)
- Bob Chen (collections specialist)
```

### 4. Regular Audits

Schedule regular grant audits:

```sql
-- Run monthly to verify grants are correct
SELECT 
    table_name,
    grantee,
    privilege_type,
    NOW() as audit_date
FROM information_schema.table_privileges
WHERE table_schema = 'raw_marts'
  AND grantee NOT IN ('postgres', 'PUBLIC')
ORDER BY table_name, grantee;
```

### 5. Test in Development First

Always test grant changes in dev before production:

```bash
# Test in dev
dbt run --target dev --models marts.dim_patient

# Verify grants work
# ... run tests ...

# Deploy to prod
dbt run --target prod --models marts.dim_patient
```

---

## Troubleshooting

### Issue: "permission denied for table"

**Cause:** Role doesn't have necessary grants

**Solution:**
```sql
-- Check current grants
SELECT * FROM information_schema.table_privileges
WHERE table_name = 'dim_patient';

-- Manually grant if needed
GRANT SELECT ON raw_marts.dim_patient TO clinical_staff_role;
```

### Issue: "must be owner of relation"

**Cause:** dbt user doesn't have GRANT privileges

**Solution:**
```sql
-- Grant dbt_user ability to manage grants
GRANT ALL PRIVILEGES ON SCHEMA raw_marts TO dbt_user WITH GRANT OPTION;
```

### Issue: Grants not applying

**Cause:** dbt version doesn't support grants or incorrect syntax

**Solution:**
```bash
# Check dbt version (grants added in 0.21.0)
dbt --version

# Upgrade if needed
pip install --upgrade dbt-postgres
```

---

## Security Considerations

### HIPAA Compliance

For PHI (Protected Health Information):
- ✅ Restrict access to authorized personnel only
- ✅ Implement role-based access control
- ✅ Maintain audit logs of data access
- ✅ Regular access reviews and updates
- ✅ Document all role assignments

### Access Logging

Enable query logging to track access:

```sql
-- PostgreSQL: Enable logging
ALTER SYSTEM SET log_statement = 'all';
ALTER SYSTEM SET log_connections = 'on';
ALTER SYSTEM SET log_disconnections = 'on';
SELECT pg_reload_conf();
```

### Encryption

Ensure data encryption:
- At-rest: Database storage encryption
- In-transit: SSL/TLS for connections
- Credentials: Use environment variables, never hardcode

---

## Migration from Custom Macros

If you previously used custom grant macros:

### Step 1: Identify Current Grant Logic

Review `hipaa_compliance.sql` or similar custom macros to understand current grant patterns.

### Step 2: Map to dbt Config

Convert custom macro logic to dbt config:

```sql
-- OLD: Custom macro
{% do apply_hipaa_grants('dim_patient', 'marts') %}

-- NEW: dbt config
{{ config(grants={'select': ['clinical_staff_role', 'admin_role']}) }}
```

### Step 3: Remove Custom Macros

Delete custom grant macros once migrated:

```bash
# Remove untested custom macros
rm dbt_dental_models/macros/utils/manage_grants.sql
```

### Step 4: Test Thoroughly

Verify grants work correctly:

```bash
# Rebuild with new grants
dbt run --models marts --full-refresh

# Verify access
# ... run SQL tests as different roles ...
```

---

## Further Reading

- **dbt Docs:** https://docs.getdbt.com/reference/resource-configs/grants
- **dbt Security Best Practices:** https://docs.getdbt.com/docs/cloud/dbt-cloud-ide/develop-in-the-cloud#security
- **Postgres Grants:** https://www.postgresql.org/docs/current/sql-grant.html
- **HIPAA Security Rule:** https://www.hhs.gov/hipaa/for-professionals/security/

---

## Summary

**Key Takeaways:**
1. ✅ Use dbt's built-in grants instead of custom macros
2. ✅ Configure grants in `dbt_project.yml` for consistency
3. ✅ Follow principle of least privilege
4. ✅ Test grants in development before production
5. ✅ Document role assignments and audit regularly
6. ✅ Ensure HIPAA compliance for PHI data

**Next Steps:**
1. Create database roles (Step 1)
2. Configure grants in dbt_project.yml (Step 3)
3. Test in development environment
4. Audit and verify access controls
5. Deploy to production with monitoring

---

**Document Version:** 1.0  
**Last Updated:** October 19, 2025  
**Maintained By:** Data Engineering Team

