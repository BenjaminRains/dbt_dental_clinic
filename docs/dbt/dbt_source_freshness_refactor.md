### Refactor Plan: dbt Source Freshness Configuration

This plan standardizes how we configure `dbt source freshness` for OpenDental sources so freshness checks no longer fail on missing `_loaded_at` and provide useful SLAs.

### Goals
- Replace implicit `_loaded_at` expectation with real, per-table `loaded_at_field`.
- Disable freshness on static or timestamp-poor tables.
- Keep configuration DRY, consistent, and documented.

### Approach Overview
- Add a shared YAML anchor for default thresholds (warn/error windows).
- For each source table, assign a concrete `loaded_at_field` that exists.
- Where no reliable timestamp exists, set `freshness: null`.
- Optionally create raw-layer views to derive a usable timestamp when needed.

### Conventions
- Prefer “last updated” style columns for `loaded_at_field` over “created” dates.
- Suggested precedence when multiple candidates exist:
  1. `SecDateTEdit` (security edit timestamp)
  2. `DateTStamp` or similar ETL/system timestamp
  3. `DateTimeUpdated` / `LastUpdated` style columns
  4. `DateEntry` (creation) only if no update timestamp exists
- Static lookups or rarely-changing reference tables: use `freshness: null`.

### DRY Defaults via YAML Anchors
Place an anchor at the top of each `_sources/*.yml` (or in a common included YAML if you use one):

```yaml
x_freshness_defaults: &freshness_defaults
  warn_after: {count: 1, period: day}
  error_after: {count: 2, period: day}
```

### Example Source File Refactor
Below illustrates how to set `loaded_at_field` and merge defaults per table.

```yaml
version: 2

x_freshness_defaults: &freshness_defaults
  warn_after: {count: 1, period: day}
  error_after: {count: 2, period: day}

sources:
  - name: opendental
    database: opendental_analytics
    schema: raw
    tables:
      - name: patient
        freshness:
          <<: *freshness_defaults
          loaded_at_field: SecDateTEdit

      - name: payment
        freshness:
          <<: *freshness_defaults
          loaded_at_field: DateTStamp

      - name: procedurelog
        freshness:
          <<: *freshness_defaults
          loaded_at_field: SecDateTEdit

      - name: definition  # static-ish reference; no reliable timestamp
        freshness: null
```

### Selection and Testing
- Run freshness for all configured sources:
  - `dbt source freshness --select source:opendental.* --fail-fast`
- Narrow to high-value tables while iterating:
  - `dbt source freshness --select source:opendental.patient source:opendental.payment`

### Handling Missing or Fragmented Timestamps
If no single column is suitable:
- Create a raw-layer view that derives a usable field (example shown below) and point `loaded_at_field` to that derived column.

```sql
-- raw.vw_patient_loaded_at (example)
select
  p.*,
  coalesce(p."SecDateTEdit", p."DateTimeUpdated", p."DateEntry") as loaded_at_candidate
from raw.patient p;
```

Then in the source YAML, reference the view and set:

```yaml
      - name: vw_patient_loaded_at
        identifier: vw_patient_loaded_at
        freshness:
          <<: *freshness_defaults
          loaded_at_field: loaded_at_candidate
```

### Rollout Plan
1. Phase 1 (High-value): `patient`, `appointment`, `procedurelog`, `payment`, `claim*`.
2. Phase 2 (Operational): scheduling, tasks, communications, etc.
3. Phase 3 (Reference/Static): set `freshness: null` where appropriate.

### Documentation Updates
- Add a short note in `dbt_docs/` or the sources README covering:
  - The precedence order for choosing `loaded_at_field`.
  - When to disable freshness.
  - How to add new sources with the shared anchor.

### Notes on Macros vs. Sources
- Our staging/intermediate/mart macros inject `_loaded_at` at model time (via `current_timestamp` fallback) for lineage/debugging.
- Source freshness inspects physical source objects; it never sees model-level macros. Hence the need for `loaded_at_field` in source YAML.

### Next Steps
- Implement anchor and per-table `loaded_at_field` for Phase 1 tables.
- Run freshness iteratively and adjust thresholds where needed.
- Decide which tables are better marked `freshness: null`.


