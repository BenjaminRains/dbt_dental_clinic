# ETL Pipeline: Change Data Capture (CDC) — Current Implementation and Options

This document explains how the ETL pipeline currently handles “change data” and schema evolution, with code references, then outlines options for adding **real deletes** and other CDC improvements.

---

## 1. Current Implementation Overview

The pipeline does **not** use log-based or trigger-based CDC. It uses:

| Layer | What exists | What it does |
|-------|-------------|---------------|
| **Schema** | Schema change detection | Compares MySQL schema to previous `tables.yml`; rewrites `tables.yml` and writes a changelog. No row-level change stream. |
| **Row-level** | Incremental watermark + upsert | Copies only rows where an incremental column is **greater than** a stored watermark; writes via **upsert** (insert or update on key). Deletes are **not** propagated. |

So “CDC” in this codebase means: **schema change detection** plus **incremental batch sync with upserts**. The docstring “True incremental updates with change data capture” in the replicator refers to this incremental/upsert behavior, not to event-based CDC.

---

## 2. Schema Change Detection (Current “Schema CDC”)

### Purpose

Detect when the OpenDental MySQL schema changes (new/removed tables or columns) so that:

- `tables.yml` stays in sync with the source.
- You get a human-readable changelog and can react to breaking changes.

### Where It Lives

| Concern | Location |
|---------|----------|
| Main script | `etl_pipeline/scripts/analyze_opendental_schema.py` |
| Compare with previous schema | `compare_with_previous_schema()` (uses backup of `tables.yml`) |
| Schema changelog (markdown) | `_generate_schema_changelog()` |
| Writing new `tables.yml` | Same script; output path is configurable (e.g. `output_dir/tables.yml`) |
| CLI entrypoint | `etl_pipeline/etl_pipeline/cli/commands.py` (e.g. schema analyze / config generation) |

### Flow

1. **Backup** current `tables.yml` (e.g. to `logs/schema_analysis/backups/tables.yml.backup.{timestamp}`).
2. **Compare** new schema (from MySQL) with the previous config (from backup) → added/removed tables and columns, plus “breaking” changes.
3. **Log** schema changes; optionally **write** a markdown changelog (e.g. `schema_changelog_{timestamp}.md`).
4. **Rewrite** `tables.yml` with the new schema and metadata (primary keys, incremental columns, extraction strategy, etc.).

So “schema CDC” = **detect schema drift and refresh `tables.yml`**; it does **not** capture row-level changes.

### References

- **Section 6 (SCD / schema change detection)** in `analyze_opendental_schema.py`: comments around “SECTION 6: SCD DETECTION” and “slowly changing dimension” (used here to mean schema evolution).
- **Stage 2** of the analysis pipeline: “Detecting schema changes…”, then `compare_with_previous_schema()`, then `_generate_schema_changelog()` when `schema_hash_changed`.
- **Backup path**: e.g. `schema_backups / f'tables.yml.backup.{timestamp}'`; **new config**: `tables_yml_path = output_dir / 'tables.yml'`.

---

## 3. Row-Level “Change” Handling: Incremental Watermark + Upsert

Row-level behavior is **incremental loading by a high-water mark**, not event-based CDC.

### 3.1 Configuration (What Drives Incremental Behavior)

- **Source of truth**: `etl_pipeline/etl_pipeline/config/tables.yml`.
- **Relevant fields** (per table):
  - `incremental_columns`: list of columns used for “after this value” filtering (e.g. `DateTStamp`, `SecDateTEdit`, or auto-increment PK).
  - `primary_incremental_column`: the single column used for the watermark and `WHERE` clause.
  - `extraction_strategy`: `full_table`, `incremental`, or `incremental_chunked`.

These values are produced by `analyze_opendental_schema.py` (e.g. `find_incremental_columns()`, `select_primary_incremental_column()`, `determine_extraction_strategy()`).

### 3.2 Replication (MySQL → Replication DB)

| Concern | Location |
|---------|----------|
| Watermark storage | Replication DB table **`etl_copy_status`**: columns `last_primary_value`, `primary_column_name`, `last_copied`, `copy_status`. |
| Read watermark | `SimpleMySQLReplicator._get_last_copy_primary_value()` — reads `last_primary_value` from `etl_copy_status` for last successful copy. |
| Read last copy time | `_get_last_copy_time()` — reads `last_copied` from `etl_copy_status` (used for “time gap” full vs incremental decision). |
| Write watermark | `_update_copy_status()` — writes `last_primary_value`, `primary_column_name`, `last_copied`, `rows_copied`, `copy_status` to `etl_copy_status` (INSERT/ON DUPLICATE KEY UPDATE). |
| Incremental query | `PerformanceOptimizations._process_incremental_batches_bulk()`: `WHERE primary_column > :current_cursor ORDER BY primary_column LIMIT batch_size`. |
| Full vs incremental choice | `PerformanceOptimizations.should_use_full_refresh()` — uses `_get_last_copy_time()` and optional performance history to decide full refresh when gap is large or previous incremental was slow. |
| Write to replication | `_execute_bulk_operation(..., 'upsert')`: MySQL `INSERT ... ON DUPLICATE KEY UPDATE` so existing rows are **updated** if they appear again in the incremental result set. |

So:

- **Inserts** in MySQL: captured when the new row’s incremental column value is greater than the watermark.
- **Updates** in MySQL: captured **only if** the updated row’s incremental column becomes greater than the watermark (e.g. `DateTStamp`/`SecDateTEdit` is updated); otherwise that update is never reflected.
- **Deletes** in MySQL: **not** captured; the row remains in replication (and later in analytics) until a full refresh or manual correction.

### 3.3 Analytics Load (Replication DB → Postgres)

- **Watermark**: Can come from replication DB’s `etl_copy_status` (e.g. `_get_last_copy_time_from_replication()`, last primary value) or from analytics-side tracking.
- **Incremental filter**: Same idea — `WHERE primary_incremental_column > last_primary_value` (or equivalent).
- **Writes**: **Upsert** (e.g. `bulk_insert(..., use_upsert=True)`, `_build_upsert_sql()` in `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`).

So analytics also get **inserts + updates that pass the watermark**, and **no delete propagation**.

### 3.4 Key Code References

- **Replicator module**: `etl_pipeline/etl_pipeline/core/simple_mysql_replicator.py`
  - Docstring: “True incremental updates with change data capture” (lines ~19–21) — interpret as incremental batch + upsert.
  - `_get_last_copy_primary_value()` (around 1525), `_get_last_copy_time()` (around 825), `_update_copy_status()` (around 788).
  - `_process_incremental_batches_bulk()` (around 417): cursor-based `WHERE primary_column > :current_cursor`.
  - `_execute_bulk_operation(..., 'upsert')` (around 497): `ON DUPLICATE KEY UPDATE`.
- **Postgres loader**: `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`
  - Incremental logic and `WHERE primary_column > last_primary_value` (e.g. around 1088, 1464, 1528, 2015).
  - `_build_upsert_sql()`, `bulk_insert(..., use_upsert=True)`.

---

## 4. What Is Not Implemented (Gaps vs “Full CDC”)

- **Log-based CDC**: No reading of MySQL binlog (no Debezium, Canal, Maxwell, etc.).
- **Trigger-based CDC**: No triggers on MySQL writing to change/audit tables.
- **Delete capture**: Source deletes are not propagated to replication or analytics.
- **Guaranteed update capture**: See below.

### 4.1 Why Some Updates Are Missed (Incremental Column Dependency)

The pipeline only selects rows for incremental copy when **the incremental column value is greater than the last watermark**. So we never re-read a row that was already copied unless that row’s incremental column value *changes* to something higher than the watermark.

**Concrete example:**  
Table `patient` uses `DateTStamp` as the primary incremental column. Last run’s watermark is `2025-02-20 10:00:00`. Someone edits an existing patient in OpenDental on 2025-02-22 (e.g. corrects the phone number) but the application **does not** update `DateTStamp` for that row (it stays `2025-01-15 08:30:00`). The next incremental run runs:

```sql
SELECT * FROM patient WHERE DateTStamp > '2025-02-20 10:00:00' ORDER BY DateTStamp LIMIT ...
```

That updated patient row is **not** in the result set, because its `DateTStamp` is still `2025-01-15 08:30:00`. The replica and analytics databases never receive the correction until a **full refresh** of that table (or until some other process updates that row’s `DateTStamp` and a later incremental run picks it up).

**When updates are captured:**  
- The source application updates the incremental column (e.g. `DateTStamp`, `SecDateTEdit`) whenever the row is modified. Then the row satisfies `incremental_column > watermark` on a later run and is re-copied; the upsert then applies the new values.

**When updates are missed:**  
- The source application updates other columns but **not** the incremental column. The row never re-enters the incremental result set, so the pipeline never re-sends it and the change is invisible until a full refresh.

**Practical impact:**  
- Depends entirely on OpenDental’s behavior: if it reliably updates a “last modified” timestamp (or the chosen incremental column) on every change, most updates are captured. If it does not, some edits will be missing in replica/analytics until the next full table refresh. There is no way in the current design to “see” those in-place updates without re-reading the full table or using log-based CDC.

### 4.2 Investigating the Scope: Does the Source Update the Incremental Column?

The assumption behind the current incremental design is that **the source system (OpenDental) updates the timestamp/incremental column on every row modification**. If that holds, most updates are captured; if not, we need to know which tables are affected and how often. Below are practical ways to investigate.

**1. MySQL trigger audit (database-level guarantee)**  
- The **OpenDental source database has no triggers**. So MySQL does not enforce “update timestamp on change”; any update to the incremental column (`DateTStamp`, `SecDateTEdit`, etc.) must come from the application (OpenDental) only. We cannot assume it happens without testing.  
- To confirm (or re-check after a source upgrade), run against the **source OpenDental MySQL** (database `opendental`):

```sql
-- List all triggers in the database (expected: empty result set for OpenDental)
SELECT trigger_schema, trigger_name, event_manipulation, event_object_table, action_statement
FROM information_schema.triggers
WHERE trigger_schema = 'opendental'
ORDER BY event_object_table, event_manipulation;
```

- If any triggers appear in the future, inspect `action_statement` for references to `DateTStamp`, `SecDateTEdit`, or `NOW()`. Until then, rely on spot-checks and profiling (below) to validate application behavior.

**2. Spot-check: edit and re-query**  
- Pick a few high-value tables (e.g. `patient`, `appointment`, `claim`) and a known row (note its primary key and current `DateTStamp` / `SecDateTEdit`).  
- Have someone make a **non-timestamp** edit in OpenDental (e.g. change a name, date, or status).  
- Re-query that row from MySQL and check whether the incremental column value **changed**.  
- Repeat for 2–3 tables and a few edits each. If the timestamp never changes on edit, that table is at risk for missed updates.

**3. Data profiling: “created” vs “modified” columns**  
- Many tables have both a “created”/“date entered” style column and a “last modified” style column (`DateTStamp`, `SecDateTEdit`, etc.). If the source truly updates on every change, we should see rows where “modified” &gt; “created”.  
- For tables that have both, run a one-off profile (example for a table with `DateCreated` and `DateTStamp`):

```sql
-- Example: do we see evidence of updates? (adjust column names per table)
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN DateTStamp > DateCreated OR DateTStamp != DateCreated THEN 1 ELSE 0 END) AS rows_with_later_timestamp
FROM patient;
```

- If `rows_with_later_timestamp` is 0 for a table that users definitely edit, that suggests the “modified” column is never updated (or is always set equal to “created”). That table is a candidate for missed updates.

**4. Full vs incremental discrepancy (replica vs source)**  
- After a period of normal use (edits in OpenDental), run an **incremental** load only (no full refresh).  
- For a sample of tables, compare **source MySQL** to **replication** (or analytics): same row count and, for a sample of rows, same non-key column values (or a hash of them).  
- Any rows that differ (same PK, different data) and were **not** in the incremental extract indicate missed updates. This is more work but gives direct evidence of scope.  
- Simplified variant: pick one small, frequently edited table; do a full refresh and record row count and a checksum (e.g. `MD5(GROUP_CONCAT(...))` for a few columns). After a week of only incremental loads, recompute the same on source and replica—if they differ, something was missed (update or delete).

**5. Use `tables.yml` to drive the investigation**  
- `tables.yml` (from `analyze_opendental_schema.py`) lists per-table `primary_incremental_column` and `incremental_columns`. Use it to:  
  - Prioritize tables for spot-checks (high business impact + incremental strategy).  
  - Build the list of tables for trigger audit and for “created vs modified” profiling (only for tables that have both column types in the schema).

**Suggested order**  
1. Trigger audit (1) is already known: **no triggers** in the source, so timestamp updates are application-only—assume nothing without testing.  
2. Run spot-checks (2) on 2–3 critical tables.  
3. If evidence of missed updates, run profiling (3) on key tables and optionally a full-vs-incremental comparison (4) for one or two tables to quantify scope.

Documenting which tables pass/fail and any OpenDental version or module notes will help decide where to add periodic full refreshes or other mitigations (see Section 5).

---

## 5. Options for Adding Real Deletes and Other CDC

### 5.1 Real Deletes

**Option A — Periodic full refresh (per table or full pipeline)**  
- **Idea**: Run a full refresh (truncate + full load) on a schedule (e.g. weekly) for tables where deletes matter.  
- **Pros**: No schema or source changes; reuses existing full_table path.  
- **Cons**: Stale deletes between refreshes; higher load and time.

**Option B — Soft deletes in source**  
- **Idea**: If OpenDental (or wrapper) supports a `IsDeleted` / `DeletedAt` column, treat “deleted” rows as inactive; in analytics, filter them out (e.g. dbt `WHERE IsDeleted = 0`).  
- **Pros**: No need to physically delete in replica; works with current incremental + upsert.  
- **Cons**: Requires source support; replica/analytics still hold deleted rows (filtered only in models).

**Option C — Key-list diff (batch “anti-join”)**  
- **Idea**: Periodically fetch the list of primary keys (or key column values) from MySQL for a table; in replica/analytics, delete rows whose key is not in the new list.  
- **Pros**: Real physical deletes in replica/analytics; no binlog.  
- **Cons**: Extra query (and possibly large key set) per table; need to define schedule and error handling.

**Option D — Log-based CDC (e.g. Debezium)**  
- **Idea**: Use MySQL binlog; stream INSERT/UPDATE/DELETE events and apply them to replication (and optionally to analytics).  
- **Pros**: Real-time, includes deletes and all updates.  
- **Cons**: Infrastructure (Kafka/Connect or equivalent), MySQL binlog config, and operational complexity.

**Recommendation (short term)**: **Option A** for critical tables where deletes must eventually be correct; **Option B** if the source can expose soft deletes. **Option C** is a middle ground if you want physical deletes without full refresh. **Option D** is the long-term “full CDC” solution if you need real-time and complete correctness.

---

### 5.2 Capturing All Updates (Not Only When Incremental Column Changes)

**Option A — Full refresh on schedule**  
- Same as delete Option A; full_table run periodically so every update is reflected.

**Option B — Add/use “modified” timestamp in source**  
- **Idea**: Ensure every table that needs incremental sync has a column that the application updates on every change (e.g. `DateTStamp`/`SecDateTEdit`), and that this column is the `primary_incremental_column` in `tables.yml`.  
- **Pros**: Fits current implementation; no new pipeline logic.  
- **Cons**: Requires application/source to maintain that column.

**Option C — Log-based CDC**  
- Same as delete Option D; binlog has every change.

---

### 5.3 Schema Change Handling (Beyond Rewriting tables.yml)

Current behavior is “detect then rewrite `tables.yml`”. Possible enhancements:

- **Automated migration scripts**: When `compare_with_previous_schema()` detects added/removed columns, generate (or suggest) DDL for the replication and/or analytics DB so schema stays in sync.
- **Breaking-change gates**: In CI or a pre-deploy step, run schema comparison and fail or warn if breaking changes (e.g. removed columns still used in dbt) are detected.
- **Versioned tables.yml**: Keep a history of `tables.yml` (you already have backups); optionally tag versions when running production ETL so you can correlate runs with schema versions.

---

### 5.4 Summary Table

| Goal | Option | Effort | Dependencies |
|------|--------|--------|--------------|
| Real deletes | Periodic full refresh | Low | None |
| Real deletes | Soft deletes in source + filter in dbt | Low–Medium | Source schema/application |
| Real deletes | Key-list diff (anti-join) | Medium | New job + key query per table |
| Real deletes + all updates | Log-based CDC (e.g. Debezium) | High | Binlog, Kafka/Connect or similar |
| All updates | “Modified” column + current incremental | Low | Source column discipline |
| Schema | Migration scripts / breaking-change gates | Medium | CI or deploy process |

---

## 6. References Quick Index

| Topic | File / Area |
|-------|-------------|
| Schema change detection | `etl_pipeline/scripts/analyze_opendental_schema.py` — Section 6, `compare_with_previous_schema()`, `_generate_schema_changelog()`, Stage 2; see [schema_analysis_scd_improvements.md](schema_analysis_scd_improvements.md) |
| tables.yml output | Same script; `output_dir/tables.yml`, backup under `schema_backups` |
| Incremental config | `tables.yml`: `incremental_columns`, `primary_incremental_column`, `extraction_strategy` |
| Watermark (replication) | `simple_mysql_replicator.py`: `etl_copy_status`, `_get_last_copy_primary_value()`, `_get_last_copy_time()`, `_update_copy_status()` |
| Incremental query (replication) | `simple_mysql_replicator.py`: `_process_incremental_batches_bulk()` — `WHERE primary_column > :current_cursor` |
| Upsert (replication) | `simple_mysql_replicator.py`: `_execute_bulk_operation(..., 'upsert')`, `ON DUPLICATE KEY UPDATE` |
| Incremental + upsert (analytics) | `postgres_loader.py`: incremental WHERE, `_build_upsert_sql()`, `bulk_insert(..., use_upsert=True)` |
| Full vs incremental decision | `simple_mysql_replicator.py`: `PerformanceOptimizations.should_use_full_refresh()` |

---

## 7. Status (2026-06)

This section records validation outcomes and active follow-up. The implementation description in §1–3 remains accurate; §4–5 gaps are **not** closed by documentation alone.

### Confirmed in production validation

- **In-place updates can be missed** (§4.1): [ETL-FND-001](findings/ETL-FND-001-replica-row-drift-procedurelog.md) — `procedurelog` rows copied once with stale `ProcStatus` (e.g. TP in analytics, Complete in MySQL). Same PK, different attributes; not “ETL didn’t run” and not a dbt bug.
- **KPI evidence (2026-06-10):** MySQL complete production 140 / $15,239; `raw` and staging both 48 / $3,719 — gap is MySQL → `raw`, not staging → mart.
- **Deletes still not propagated** (§4): unchanged; phantom rows in analytics if source hard-deletes remain possible.
- **Staging `_loaded_at` ≠ ETL load time:** dbt sets `_loaded_at` at build; use `raw.etl_load_status` for pipeline freshness (see archived [INCREMENTAL_MODEL_ANALYSIS_EXPLANATION.md](archive/INCREMENTAL_MODEL_ANALYSIS_EXPLANATION.md)).

### Active mitigation (in progress)

| Item | Tracks | Notes |
| --- | --- | --- |
| Lookback re-sync for `procedurelog` | [ETL-FND-001](findings/ETL-FND-001-replica-row-drift-procedurelog.md) P1 | Union watermark incremental with recent `DateComplete` / `ProcDate` window |
| Post-ETL drift detection | ETL-FND-001 P0 | MySQL vs `raw` reconciliation (e.g. complete-production totals by day) |
| `tables.yml` review | ETL-FND-001 P1 | Confirm `DateTStamp` as primary incremental; re-evaluate `and_logic` |

**Tracking:** [TODO.md — ETL-FND-001](../../TODO.md#etl-fnd-001--replica-row-drift-procedurelog)

**One-time unblock (not durable):** full ETL + `dbt run --full-refresh --select stg_opendental__procedurelog+` — masks drift until the next missed in-place update.

### Still deferred (§5 options)

| Option | Status |
| --- | --- |
| **5.1A** Periodic full refresh as primary strategy | Operational only; ETL-FND-001 explicitly rejects as durable fix |
| **5.1B** Soft deletes + dbt filter | Not adopted |
| **5.1C** Key-list anti-join deletes | Not built |
| **5.1D** Log-based CDC (Debezium / binlog) | Long-term; see [EVENT_DRIVEN_ANALYTICS_PROPOSAL.md](../streaming/EVENT_DRIVEN_ANALYTICS_PROPOSAL.md) |
| **5.3** Auto DDL / breaking-change gates | Partial — `mdc etl invoke … check-schema-drift` exists; no generated migration scripts |

### Doc maintenance

- Re-check code paths in §6 after major ETL refactors (e.g. `SimpleMySQLReplicator` rename).
- When ETL-FND-001 closes, update this section and cross-link acceptance criteria in the finding doc.

---

*Document version: 1.1 (2026-06-26). §7 added after KPI validation and ETL-FND-001. §1–3 describe current implementation; code paths should be re-checked if refactors occur.*
