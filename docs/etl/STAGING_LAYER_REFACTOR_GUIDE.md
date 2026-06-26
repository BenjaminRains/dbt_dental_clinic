# Staging Layer Refactor Guide

## for stg_opendental__* YAML & SQL models

---

## 1. Purpose

This document defines how to gradually refactor staging models in the `stg_opendental__*` family so that:

- Model configuration is centralized in YAML (materializations, unique keys, tags, etc.).
- SQL files focus on transform logic only.
- Column naming, metadata, and macros are consistent across models.
- Documentation and tests stay in sync with the actual columns.

**Scope includes at least:**

- `stg_opendental__claimproc`
- `stg_opendental__insplan`
- `stg_opendental__patient`
- `stg_opendental__procedurecode`
- `stg_opendental__procnote`
- …and their corresponding `_*.yml` files.

---

## 2. Guiding Principles

### Static config belongs in YAML

`materialized`, `unique_key`, `incremental_strategy`, `schema`, `tags`, `grants`, `access/contract` config → YAML.

SQL `{{ config(...) }}` is only used when you truly need dynamic / Jinja-driven behavior.

### SQL should read like a transformation script

Standard pattern: `source_data` CTE → `renamed_columns` CTE → `final select * from renamed_columns`.

Use macros (`transform_id_columns`, `convert_opendental_boolean`, `clean_opendental_date`, `standardize_metadata_columns`, etc.) consistently across all staging models.

### One model = one clearly defined grain and primary key

Grain is enforced with a `unique_key` (for incremental models) and documented in YAML.

PK/FK naming patterns like `*_id` should be consistent across models.

### Metadata should be standardized

Downstream models should be able to rely on common metadata columns like `_loaded_at`, `_created_at`, `_updated_at`, `_created_by`, etc., produced through `standardize_metadata_columns`.

### Refactor gradually, model-by-model

For each model: add YAML config → verify → remove SQL config → normalize SQL & columns → update tests.

---

## 2.5 When to Use Dynamic Config Blocks (SQL `{{ config(...) }}`)

**TL;DR: Based on codebase review, none of the current `stg_opendental__*` models need dynamic config blocks. All configs can be moved to YAML.**

### What is Dynamic Config?

Dynamic config uses Jinja logic to determine configuration values at compile time. Examples:

```sql
-- Environment-based materialization
{{ config(
    materialized='incremental' if target.name == 'prod' else 'view'
) }}

-- Variable-driven config
{{ config(
    materialized=var('appointment_materialization', 'incremental')
) }}

-- Conditional schema based on target
{{ config(
    schema=target.schema + '_staging' if target.name == 'dev' else 'staging'
) }}

-- Complex conditional logic
{{ config(
    materialized='incremental' if var('full_refresh', false) == false else 'table',
    unique_key='appointment_id' if var('full_refresh', false) == false else none
) }}
```

### When You'd Actually Need Dynamic Config

**Rare cases where dynamic config is justified:**

1. **Environment-specific materialization** - Different materializations for dev vs prod
   - Example: Views in dev (fast iteration) but incremental tables in prod (performance)
   - **Alternative**: Use YAML with `dbt_project.yml` folder-level configs or separate profiles

2. **Variable-driven behavior** - Config changes based on dbt variables
   - Example: Toggle between incremental and full-refresh via `--vars`
   - **Alternative**: Use `dbt run --full-refresh` flag instead

3. **Complex conditional logic** - Config depends on multiple factors
   - Example: Schema name depends on target + model type + date
   - **Alternative**: Usually better handled in `dbt_project.yml` with folder-level configs

### Current State: All Configs Are Static

**Codebase review findings:**

- ✅ All 91+ staging models use **static config blocks** only
- ✅ Examples found:
  - `{{ config(materialized='incremental', unique_key='appointment_id') }}`
  - `{{ config(materialized='view') }}`
  - `{{ config(materialized='table') }}`
- ❌ **Zero models** use dynamic Jinja in config blocks
- ❌ **Zero models** use `var()`, `target.*`, or conditionals in config

**Conclusion:** All current `{{ config(...) }}` blocks can be safely moved to YAML without any dynamic logic.

### Recommendation

**For staging models:** Always use YAML config. If you later need environment-specific behavior:

1. **First try:** Folder-level configs in `dbt_project.yml`
2. **Second try:** Profile-specific configs in `profiles.yml`
3. **Last resort:** Dynamic config block (document why it's necessary)

**If you add dynamic config later:** Document the business reason in the model's YAML `meta.usage_notes` or `meta.business_rules`.

---

## 3. Target Patterns

### 3.1 YAML model pattern (staging)

For each `_stg_opendental__*.yml`:

```yaml
version: 2

models:
  - name: stg_opendental__<entity>
    description: >
      Staging model for <entity> from the OpenDental <TABLE> table.
      Provides a cleaned, analytics-ready view at the <row/grain> level.

    config:
      materialized: incremental        # or view/table as appropriate
      incremental_strategy: merge      # if model is incremental
      unique_key: <primary_key_column> # e.g. claim_procedure_id, procedure_code_id
      tags:
        - staging
        - opendental
        - <domain_tag>   # e.g. claims, scheduling, patients, insurance

    meta:
      record_count: "~N rows (as of YYYY-MM-DD)"
      data_scope: "From 2023-01-01 onward, incrementally loaded"  # adjust per model

      business_rules:
        - rule: "<short rule>"
          impact: "<why it matters>"
        - rule: "<short rule>"
          impact: "<why it matters>"

      known_issues:
        - description: "<issue>"
          severity: "warn|error|info"
          identified_date: "YYYY-MM-DD"
          test: "<dbt_test_name_if_any>"

      usage_notes: >
        Optional notes on most important downstream marts and KPIs.

    columns:
      - name: <primary_key_column>
        description: "Primary key for this model. One row per <grain>."
        tests:
          - unique
          - not_null

      - name: <fk_column>
        description: "FK to stg_opendental__patient.patient_id."
        tests:
          - not_null
          - relationships:
              to: ref('stg_opendental__patient')
              field: patient_id

      # remaining columns, each with description and tests where useful
```

**Key decisions:**

- All static config (materialization, strategies, keys, tags) goes here.
- `meta` keys are the same across all staging YAMLs for consistency.
- Columns from macros (e.g. `_loaded_at`, `_created_at`, `_updated_at`, `_created_by`) are explicitly documented.

### 3.2 SQL model pattern (staging)

For each `stg_opendental__*.sql`, target:

```sql
with source_data as (
    select *
    from {{ source('opendental', '<TABLE>') }}
    -- Optional static business filter (e.g. limit to recent dates) if justified
    {% if is_incremental() %}
        -- Filter only new/changed rows based on a reliable timestamp
        and {{ clean_opendental_date('<timestamp_column>') }} >
            (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- ID / key transformations
        {{ transform_id_columns([
            {'source': '"SomePK"', 'target': 'some_id'},
            {'source': '"SomeFK"', 'target': 'some_foreign_id'}
        ]) }},

        -- Domain-specific attributes
        -- ...

        -- Dates
        {{ clean_opendental_date('"SomeDate"') }} as some_date,

        -- Booleans
        {{ convert_opendental_boolean('"SomeFlag"') }} as some_flag,

        -- Standardized metadata
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column='"SecUserNumEntry"'
        ) }}

    from source_data
)

select *
from renamed_columns
```

**Notes:**

- `{{ config(...) }}` is removed once YAML config is in place.
- `source_data` contains only the source + where logic (date or incremental filters).
- `renamed_columns` handles renaming, casting, cleaning, macro usage.
- Final `select * from renamed_columns` keeps the pattern uniform.

---

## 5. Step-By-Step Refactor Process

Repeat this process per model (one at a time, PR by PR):

### Step 1 – Add YAML config

For the target model, open its `_*.yml` file.

Add a `config:` block under the `models: - name: ...` entry with:
- `materialized`
- `incremental_strategy` (if incremental)
- `unique_key` (if incremental or if you want documented grain)
- `tags`

Optionally add/standardize `meta` and ensure `columns` includes all SQL columns.

**Run:**

```bash
dbt ls -s stg_opendental__<model>
dbt run -s stg_opendental__<model>
dbt test -s stg_opendental__<model>
```

Verify nothing changes unexpectedly in the data.

### Step 2 – Remove SQL config() block

After the YAML config is in place and verified, remove the `{{ config(...) }}` block from the `.sql` file.

Confirm that `dbt run -s stg_opendental__<model>` still behaves as expected.

### Step 3 – Normalize SQL structure

Ensure the SQL follows this structure:

**`with source_data as (...)`**
- Contains `from {{ source('opendental', '...') }}`.
- Contains only incremental and/or date filters.

**`renamed_columns as (...)`**
- Contains all select logic:
  - ID macros: `transform_id_columns`.
  - Date macros: `clean_opendental_date`.
  - Boolean macros: `convert_opendental_boolean`.
  - Metadata macro: `standardize_metadata_columns(...)`.
  - Derived columns like `age`, `code_prefix`, etc.

**Final `select * from renamed_columns`.**

### Step 4 – Align YAML columns & tests

For each column in the select (including metadata from macros), ensure:

- There is a matching `columns:` entry in YAML.
- It has a clear description.
- It has sensible tests:
  - PK → `unique`, `not_null`.
  - FKs → `not_null`, `relationships` where appropriate.
  - Flags → maybe `accepted_values`.
  - Derived metrics → optional sanity tests (e.g., non-negative, ranges).

### Step 5 – Add/standardize meta information

For each model:

Fill in or normalize:
- `record_count` (approximate is fine).
- `data_scope` (e.g. "All records", ">= 2023-01-01", etc.).
- `business_rules` (important filters, joins, or domain assumptions).
- `known_issues` (any edge cases or data quality problems you know about).

This makes your staging layer self-documenting for future you (and for interviews / portfolio review).

---

## 6. Rollout Strategy

1. **Start with low-risk models** (`insplan`, `procedurecode`) to validate the pattern.

2. **Move to core, high-impact models** (`patient`, `claimproc`, `procnote`) once the pattern feels solid.

3. **Keep a running checklist** in your repo (e.g. `docs/refactor_progress.md`) that lists each model and which steps (1-5) are complete.

4. **When done, do a final pass** to:
   - Ensure no staging SQL files still have `{{ config(...) }}` for static config.
   - Confirm `dbt ls --resource-type model | grep stg_opendental__` shows everything tagged consistently.

---

## 7. Checklist Template

For tracking progress on each model:

```markdown
### stg_opendental__<model>

- [ ] Step 1: YAML config added
- [ ] Step 2: SQL config() removed
- [ ] Step 3: SQL structure normalized
- [ ] Step 4: YAML columns & tests aligned
- [ ] Step 5: Meta information standardized
- [ ] Verified: `dbt run` works
- [ ] Verified: `dbt test` passes
- [ ] Verified: No breaking changes to downstream models
```

