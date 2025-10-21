# dbt Analytics Engineering Certification - Study Guide

## Table of Contents
1. [Core Concepts](#core-concepts)
2. [dbt Commands](#dbt-commands)
3. [Materializations](#materializations)
4. [Testing](#testing)
5. [Jinja & Macros](#jinja--macros)
6. [Sources & Seeds](#sources--seeds)
7. [Documentation](#documentation)
8. [Project Structure](#project-structure)
9. [Development Workflow](#development-workflow)
10. [Advanced Features](#advanced-features)
11. [Best Practices](#best-practices)
12. [Common Exam Questions](#common-exam-questions)

---

## Core Concepts

### The DAG (Directed Acyclic Graph)
- **What it is**: A dependency graph showing how models depend on each other
- **How it's built**: Using `ref()` and `source()` functions
- **Why it matters**: Determines execution order, enables lineage tracking
- **Key rule**: No circular dependencies allowed

### The ref() Function
```sql
select * from {{ ref('model_name') }}
```
- Creates dependencies between models
- Automatically uses correct schema/database names
- Enables dbt to build models in the correct order
- Works across different materializations

### The source() Function
```sql
select * from {{ source('source_name', 'table_name') }}
```
- References raw source tables
- Defined in `sources.yml` files
- Enables source freshness checks
- Documents external dependencies

### The config() Function
```sql
{{ config(
    materialized='table',
    tags=['daily'],
    schema='marts'
) }}
```
- Sets model-specific configurations
- Can be placed in model file or `dbt_project.yml`
- Model-level config overrides project-level

---

## dbt Commands

### Essential Commands
| Command | Purpose | Key Flags |
|---------|---------|-----------|
| `dbt run` | Execute models | `-m`, `-s`, `--full-refresh` |
| `dbt test` | Run tests | `-m`, `-s`, `--store-failures` |
| `dbt build` | Run + test in DAG order | `-m`, `-s`, `--full-refresh` |
| `dbt compile` | Compile without running | `-m`, `-s` |
| `dbt docs generate` | Generate documentation | |
| `dbt docs serve` | Serve documentation site | `--port` |
| `dbt source freshness` | Check source freshness | |
| `dbt snapshot` | Run snapshots | |
| `dbt seed` | Load CSV files | `--full-refresh` |
| `dbt clean` | Delete dbt artifacts | |
| `dbt debug` | Test connection/setup | |
| `dbt deps` | Install packages | |

### Model Selection Syntax
```bash
# Run specific model
dbt run -m my_model

# Run model and all downstream (children)
dbt run -m my_model+

# Run model and all upstream (parents)
dbt run -m +my_model

# Run model and ALL dependencies
dbt run -m +my_model+

# Run by tag
dbt run -m tag:daily

# Run by folder
dbt run -m marts.finance

# Run by source
dbt run -m source:raw_data+

# Run modified models and downstream
dbt run -m state:modified+

# Exclude models
dbt run -m my_model --exclude tag:deprecated
```

---

## Materializations

### 1. View (Default)
```sql
{{ config(materialized='view') }}
```
- **When to use**: Lightweight transformations, frequently changing logic
- **Pros**: No storage cost, always up-to-date
- **Cons**: Slow query performance, computed at query time
- **Best for**: Staging models, simple transformations

### 2. Table
```sql
{{ config(materialized='table') }}
```
- **When to use**: Heavy transformations, stable data
- **Pros**: Fast query performance
- **Cons**: Storage cost, takes time to rebuild
- **Best for**: Mart models, aggregations, final outputs
- **Rebuild**: Full refresh on each run

### 3. Incremental
```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'  -- or 'delete+insert', 'insert_overwrite'
) }}

select * from {{ ref('source_table') }}
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```
- **When to use**: Large tables, append-only or slowly changing data
- **Pros**: Fast, efficient, only processes new/changed records
- **Cons**: Complexity, can drift if not careful
- **Key macro**: `is_incremental()` - Returns true if table exists
- **Key variable**: `{{ this }}` - References current model

#### Incremental Strategies
- **merge** (default): Updates existing records, inserts new ones
- **delete+insert**: Deletes matching records, then inserts
- **insert_overwrite**: Overwrites partitions (BigQuery, Spark)
- **append**: Only inserts new records, no updates

### 4. Ephemeral
```sql
{{ config(materialized='ephemeral') }}
```
- **When to use**: Reusable logic, not needed as separate table
- **Pros**: No storage, DRY code
- **Cons**: Can't be tested or documented separately
- **How it works**: Interpolated as CTE in dependent models

### 5. Materialized View (Warehouse-specific)
```sql
{{ config(materialized='materialized_view') }}
```
- **When to use**: Need table-like performance with automatic refresh
- **Pros**: Fast queries (results stored), warehouse manages refresh automatically
- **Cons**: Storage cost (same as table!), warehouse-specific, limited portability
- **Best for**: Frequently queried aggregations that need to stay fresh
- **How it differs from View**: 
  - Regular View = No storage (just SQL), slow queries
  - Materialized View = HAS storage (results stored), fast queries
- **How it differs from Table**: 
  - Table = You control refresh (dbt run)
  - Materialized View = Warehouse controls refresh (automatic)
- **Availability**: Postgres, Redshift, BigQuery (syntax varies by warehouse)
- **⚠️ Important**: Despite the name similarity to "view", materialized views DO have storage costs!

---

## Testing

### Generic Tests (Schema Tests)
Defined in `.yml` files:

```yaml
models:
  - name: customers
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
      - name: status
        tests:
          - accepted_values:
              values: ['active', 'inactive', 'pending']
      - name: email
        tests:
          - unique
          - not_null
```

#### Built-in Generic Tests
1. **unique**: No duplicate values
2. **not_null**: No NULL values
3. **accepted_values**: Value in specified list
4. **relationships**: Foreign key relationship exists
   ```yaml
   - name: customer_id
     tests:
       - relationships:
           to: ref('customers')
           field: customer_id
   ```

### Singular Tests (Data Tests)
SQL files in `tests/` folder:

```sql
-- tests/assert_positive_revenue.sql
select
    order_id,
    revenue
from {{ ref('orders') }}
where revenue < 0
```
- **Pass condition**: Query returns zero rows
- **Fail condition**: Query returns any rows

### Custom Generic Tests
Create in `macros/` folder:

```sql
-- macros/test_valid_email.sql
{% test valid_email(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} not like '%@%.%'
{% endtest %}
```

Usage:
```yaml
- name: email
  tests:
    - valid_email
```

### Test Configurations
```yaml
tests:
  - unique:
      severity: warn  # or 'error' (default)
  - not_null:
      error_if: ">10"  # Fail if more than 10 failures
      warn_if: ">0"    # Warn if any failures
```

### Store Test Failures
```bash
dbt test --store-failures
```
- Saves failed records to **data warehouse** (not local files)
- **Default location**: `<database>.dbt_test__audit.<test_name>`
- **Table naming**:
  - Generic tests: `<model>_<test_type>_<column>` (e.g., `customers_unique_customer_id`)
  - Singular tests: Uses test file name (e.g., `assert_positive_revenue`)
- **Default format**: VIEW (configurable to TABLE)
- Useful for debugging - query failed records directly

#### Configuration
```yaml
# In dbt_project.yml
tests:
  +store_failures: true
  +schema: test_failures  # Custom schema (default: dbt_test__audit)
  +store_failures_as: table  # or 'view' (default)

# Or per-test in YAML
tests:
  - unique:
      store_failures: true
      store_failures_as: table
```

#### Querying Failures
```sql
-- See which records failed the test
SELECT * FROM dbt_test__audit.customers_unique_customer_id
```

---

## Jinja & Macros

### Jinja Basics

#### Variables
```sql
{% set my_var = 'value' %}
select '{{ my_var }}' as my_column
```

#### If Statements
```sql
{% if target.name == 'prod' %}
    -- production logic
{% else %}
    -- development logic
{% endif %}
```

#### For Loops
```sql
{% set payment_methods = ['credit', 'debit', 'cash'] %}

select
    order_id,
    {% for method in payment_methods %}
    sum(case when payment_method = '{{ method }}' then amount else 0 end) as {{ method }}_amount
    {{ "," if not loop.last }}
    {% endfor %}
from payments
group by 1
```

#### Filters
```sql
{{ var('start_date') | as_text }}
{{ 'hello' | upper }}
{{ column_name | replace(' ', '_') }}
```

### Macros

#### Basic Macro
```sql
{% macro cents_to_dollars(column_name, precision=2) %}
    round({{ column_name }} / 100, {{ precision }})
{% endmacro %}

-- Usage:
select {{ cents_to_dollars('amount_cents', 2) }} as amount_dollars
```

#### Macro with Documentation
```sql
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name is none -%}
        {{ node.name }}
    {%- else -%}
        {{ custom_alias_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

### Built-in Macros & Variables

#### Target Variable
```sql
{{ target.name }}      -- 'dev' or 'prod'
{{ target.schema }}    -- target schema name
{{ target.database }}  -- target database name
{{ target.type }}      -- 'postgres', 'snowflake', etc.
```

#### Adapter Functions
```sql
{{ adapter.get_columns_in_relation(ref('my_model')) }}
{{ adapter.get_relation(database, schema, identifier) }}
```

#### dbt Project Variables
```sql
{{ var('variable_name') }}
{{ var('variable_name', 'default_value') }}
```

Define in `dbt_project.yml`:
```yaml
vars:
  start_date: '2024-01-01'
  excluded_statuses: ['deleted', 'archived']
```

#### Environment Variables
```sql
{{ env_var('DBT_ENV_SECRET_PASSWORD') }}
{{ env_var('MY_VAR', 'default_value') }}
```

---

## Sources & Seeds

### Sources

#### Define in YAML
```yaml
# models/staging/sources.yml
version: 2

sources:
  - name: raw_data
    database: raw
    schema: opendental
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
    tables:
      - name: customers
        description: "Raw customer data from CRM"
        columns:
          - name: customer_id
            tests:
              - unique
              - not_null
      - name: orders
        loaded_at_field: updated_at
        freshness:
          warn_after: {count: 12, period: hour}
```

#### Reference in Models
```sql
select * from {{ source('raw_data', 'customers') }}
```

#### Check Freshness
```bash
dbt source freshness
dbt source freshness -m source:raw_data
```

### Seeds

#### CSV Files in seeds/ folder
```csv
# seeds/country_codes.csv
country_code,country_name
US,United States
CA,Canada
GB,United Kingdom
```

#### Configure Seeds
```yaml
# dbt_project.yml
seeds:
  dbt_dental_models:
    country_codes:
      +column_types:
        country_code: varchar(2)
```

#### Load Seeds
```bash
dbt seed
dbt seed --full-refresh
dbt seed -s country_codes
```

#### Reference Seeds
```sql
select * from {{ ref('country_codes') }}
```

---

## Documentation

### Model Documentation
```yaml
# models/schema.yml
version: 2

models:
  - name: customers
    description: |
      One record per customer. This model includes:
      - Customer demographics
      - Lifetime value calculations
      - Status tracking
    
    columns:
      - name: customer_id
        description: "Primary key for customers"
        tests:
          - unique
          - not_null
      
      - name: lifetime_value
        description: "Total revenue from this customer"
```

### Doc Blocks
```yaml
# models/docs.md
{% docs customer_status %}
Customer status can be one of:
- **active**: Currently purchasing
- **inactive**: No purchases in 90 days
- **churned**: Explicitly cancelled
{% enddocs %}
```

Reference in YAML:
```yaml
- name: status
  description: "{{ doc('customer_status') }}"
```

### Generate & Serve Docs
```bash
dbt docs generate
dbt docs serve --port 8080
```

### Documentation Features
- Lineage graph (DAG visualization)
- Column-level descriptions
- Test results
- Source freshness
- Project README

---

## Project Structure

### Standard dbt Project Layout
```
dbt_project/
├── dbt_project.yml          # Project configuration
├── packages.yml             # dbt packages
├── profiles.yml            # Connection profiles (NOT in repo)
├── models/
│   ├── staging/            # 1:1 with source tables
│   │   ├── _staging.yml
│   │   └── stg_customers.sql
│   ├── intermediate/       # Purpose-built transformations
│   │   └── int_customer_orders.sql
│   └── marts/              # Business-facing models
│       ├── finance/
│       └── marketing/
├── macros/                 # SQL macros
├── tests/                  # Singular tests
├── seeds/                  # CSV files
├── snapshots/              # Type 2 SCD tracking
├── analysis/               # Ad-hoc queries
└── target/                 # Compiled SQL (gitignored)
```

### dbt_project.yml Essentials
```yaml
name: 'my_project'
version: '1.0.0'
config-version: 2

profile: 'default'

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  my_project:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

### profiles.yml
```yaml
default:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: dev_user
      password: "{{ env_var('DBT_PASSWORD') }}"
      port: 5432
      dbname: analytics
      schema: dev_{{ env_var('USER') }}
      threads: 4
    
    prod:
      type: postgres
      host: prod.example.com
      user: prod_user
      password: "{{ env_var('DBT_PROD_PASSWORD') }}"
      port: 5432
      dbname: analytics
      schema: production
      threads: 8
```

---

## Development Workflow

### Environment Strategy
1. **Development**: Individual dev schemas, test freely
2. **Staging**: Test production logic with production data
3. **Production**: Scheduled runs, final outputs

### Git Workflow
1. Create feature branch
2. Develop models in dev environment
3. Test thoroughly
4. Open pull request
5. Review & merge
6. Deploy to production

### Slim CI/CD
```bash
# Only run modified models and downstream
dbt run -m state:modified+ --state ./prod-manifest/
dbt test -m state:modified+ --state ./prod-manifest/
```

### Job Orchestration
Common job types:
- **Daily Full Refresh**: All models
- **Hourly Incremental**: Time-sensitive models
- **Source Freshness**: Check data arrival
- **Test Only**: Data quality validation

---

## Advanced Features

### Snapshots (Type 2 SCD)
```sql
-- snapshots/customers_snapshot.sql
{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select * from {{ source('raw', 'customers') }}

{% endsnapshot %}
```

Strategies:
- **timestamp**: Track by timestamp column
- **check**: Track by comparing column values

Run snapshots:
```bash
dbt snapshot
```

### Packages
```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.0
  - package: calogica/dbt_expectations
    version: 0.9.0
  - git: "https://github.com/user/repo.git"
    revision: main
```

Install:
```bash
dbt deps
```

Use package macros:
```sql
{{ dbt_utils.surrogate_key(['customer_id', 'order_id']) }}
{{ dbt_utils.date_spine(...) }}
```

### Exposures
```yaml
# models/exposures.yml
exposures:
  - name: weekly_revenue_dashboard
    type: dashboard
    maturity: high
    url: https://bi.example.com/dashboards/revenue
    description: Executive revenue dashboard
    
    depends_on:
      - ref('fct_orders')
      - ref('dim_customers')
    
    owner:
      name: Analytics Team
      email: analytics@example.com
```

### Hooks
```sql
-- dbt_project.yml
models:
  pre-hook: "{{ log('Starting model execution') }}"
  post-hook: 
    - "grant select on {{ this }} to role reporter"
    - "{{ audit_log() }}"

on-run-start:
  - "{{ create_udfs() }}"

on-run-end:
  - "{{ cleanup_temp_tables() }}"
```

### Operations
```sql
-- macros/grant_permissions.sql
{% macro grant_select(schema, role) %}
    {% set sql %}
        grant select on all tables in schema {{ schema }} to {{ role }}
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
```

Run:
```bash
dbt run-operation grant_select --args '{schema: analytics, role: reporter}'
```

---

## Best Practices

### Naming Conventions
- **Staging models**: `stg_<source>_<entity>.sql`
- **Intermediate models**: `int_<entity>_<verb>.sql`
- **Fact tables**: `fct_<entity>.sql`
- **Dimension tables**: `dim_<entity>.sql`
- **Metrics**: `mtr_<metric_name>.sql`

### Code Organization
1. **Use CTEs**: One CTE per logical step
2. **Import → Logical → Final**: Structure CTEs logically
3. **Staging layer**: Clean and rename columns
4. **One model per business entity**
5. **Keep it simple**: Prefer readability over cleverness

### SQL Style
```sql
-- Good
with

orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        orders.order_id,
        orders.order_date,
        customers.customer_name,
        orders.total_amount
    from orders
    inner join customers
        on orders.customer_id = customers.customer_id
)

select * from final
```

### Testing Strategy
1. **Test primary keys**: unique + not_null
2. **Test foreign keys**: relationships test
3. **Test business logic**: Custom tests
4. **Test critical columns**: not_null on required fields
5. **Document exceptions**: If a test should fail, document why

### Performance Tips
1. Use incremental models for large tables
2. Materialize as table for complex aggregations
3. Use indexes in post-hooks (warehouse-specific)
4. Partition large tables (BigQuery, Snowflake)
5. Use `{{ limit_data_in_dev(100) }}` for development

---

## Common Exam Questions

### Question Types & Examples

#### 1. Materialization Selection
**Q**: You have a 10GB table that updates nightly with new records. Query performance is critical. What materialization should you use?

**A**: Incremental materialization - processes only new records efficiently while maintaining query performance.

---

#### 2. Testing Knowledge
**Q**: What happens when a generic test is run?

**A**: The test query executes, and the test passes if it returns zero rows. Any rows returned indicate test failures.

---

#### 3. ref() vs source()
**Q**: What's the difference between `ref()` and `source()`?

**A**: 
- `ref()`: References other dbt models, creates DAG dependencies
- `source()`: References raw source tables, enables freshness checks

---

#### 4. Incremental Logic
**Q**: What does `is_incremental()` return on the first run?

**A**: False - because the target table doesn't exist yet, so dbt runs the full query.

---

#### 5. Command Understanding
**Q**: What's the difference between `dbt run` and `dbt build`?

**A**: 
- `dbt run`: Only executes models
- `dbt build`: Executes models AND runs tests in DAG order

---

#### 6. Model Selection
**Q**: What does `dbt run -m +my_model+` do?

**A**: Runs `my_model` plus ALL upstream (parent) and downstream (child) models.

---

#### 7. Test Configuration
**Q**: How do you make a test a warning instead of an error?

**A**: Use `severity: warn` in the test configuration:
```yaml
tests:
  - unique:
      severity: warn
```

---

#### 8. Jinja Logic
**Q**: When would you use an ephemeral materialization?

**A**: When you want reusable logic (DRY) that doesn't need to be materialized as a separate view/table, and won't be queried directly or tested independently.

---

#### 9. Package Management
**Q**: What command installs dbt packages?

**A**: `dbt deps` - reads `packages.yml` and installs to `dbt_packages/`

---

#### 10. Environment Targeting
**Q**: How do you reference the current environment in a model?

**A**: Use `{{ target.name }}` or other target variables:
```sql
{% if target.name == 'prod' %}
    -- production logic
{% endif %}
```

---

#### 11. Source Freshness
**Q**: What field is required to check source freshness?

**A**: `loaded_at_field` - a timestamp column indicating when the record was loaded.

---

#### 12. Macro Basics
**Q**: What's the difference between `{% %}` and `{{ }}`?

**A**: 
- `{% %}`: Jinja logic (if, for, set, macro definitions)
- `{{ }}`: Expression/output (renders values)

---

### Practice Scenarios

#### Scenario 1: Performance Issue
*A staging model runs slowly in production. It's a simple SELECT * with column renaming from a source table. What should you do?*

**Answer**: Check the source table size. If large, consider:
1. Keep as view (staging models typically are views)
2. Investigate source query performance
3. Don't over-materialize - staging should be lightweight

#### Scenario 2: Test Failures
*Your uniqueness test on `customer_id` fails with 5 duplicates. What are your options?*

**Answer**:
1. Fix source data (best)
2. Change severity to `warn` (temporary)
3. Add deduplication logic in model
4. Document the issue and use `error_if: ">10"` threshold

#### Scenario 3: Incremental Strategy
*You need to update historical records when source data changes. Which incremental strategy?*

**Answer**: `merge` strategy with `unique_key` - updates existing records and inserts new ones.

#### Scenario 4: Model Organization
*Where should you place a model that joins orders with customers and adds calculated fields?*

**Answer**: `intermediate/` folder - it's a purpose-built transformation combining multiple staging models.

---

## Quick Reference Cards

### Top 10 Things to Memorize

1. **ref() creates DAG dependencies** ✓
2. **is_incremental() is false on first run** ✓
3. **Tests pass when they return 0 rows** ✓
4. **Ephemeral = CTE in dependent models** ✓
5. **{{ this }} references current model** ✓
6. **source() enables freshness checks** ✓
7. **dbt build = run + test in DAG order** ✓
8. **Incremental needs unique_key for merge** ✓
9. **target.name accesses environment** ✓
10. **Generic tests in YAML, singular in tests/ folder** ✓
11. **Only VIEW and EPHEMERAL have no storage cost** ✓
12. **Materialized Views DO have storage cost (same as tables!)** ✓

### Command Cheat Sheet
```bash
dbt run -m model_name          # Run one model
dbt run -m model_name+         # Model + downstream
dbt run -m +model_name         # Model + upstream
dbt run -m +model_name+        # Model + all dependencies
dbt run -m tag:daily          # By tag
dbt test -m model_name        # Test one model
dbt build                     # Run + test everything
dbt docs generate && dbt docs serve  # Documentation
```

### Storage Cost Comparison

| Materialization | Storage Cost | Query Speed | Data Freshness | Refresh Control |
|----------------|--------------|-------------|----------------|-----------------|
| **View** | ❌ None ($0) | Slow | Always current | N/A (recomputes) |
| **Table** | ✅ Yes ($$) | Fast | Stale until rebuild | You (dbt run) |
| **Incremental** | ✅ Yes ($$) | Fast | Stale until run | You (dbt run) |
| **Materialized View** | ✅ Yes ($$) | Fast | Auto-refresh | Warehouse |
| **Ephemeral** | ❌ None ($0) | N/A (CTE) | Always current | N/A (inlined) |

**Key Takeaway:** Only regular **View** and **Ephemeral** have no storage cost. Everything else stores data = $$!

### Materialization Decision Tree
```
Is the model a staging model?
├─ Yes → VIEW
└─ No → Is the table > 1GB?
    ├─ Yes → Does it get new records daily?
    │   ├─ Yes → INCREMENTAL
    │   └─ No → TABLE
    └─ No → Is query performance critical?
        ├─ Yes → TABLE
        └─ No → VIEW
```

---

## Study Tips

### Week Before Exam
- [ ] Review all materializations and when to use each
- [ ] Practice writing incremental models
- [ ] Memorize built-in generic tests
- [ ] Understand model selection syntax
- [ ] Practice Jinja templating
- [ ] Review ref() vs source() vs var()

### Day Before Exam
- [ ] Review this entire guide
- [ ] Quiz yourself on command flags
- [ ] Practice quick mental model selection
- [ ] Review common error scenarios
- [ ] Get good sleep!

### During Exam
- Read questions carefully (scenario-based questions give context clues)
- Eliminate obviously wrong answers first
- Remember: dbt favors explicit over implicit
- Think about "dbt way" best practices
- Manage your time (don't spend 10 min on one question)

---

## Additional Resources

- **dbt Docs**: docs.getdbt.com
- **dbt Discourse**: discourse.getdbt.com
- **dbt Learn**: Learn.getdbt.com (free courses)
- **dbt Slack**: community.getdbt.com

---

**Good luck on your certification exam! 🎯**

*Last updated: October 2025*

