# ETL Sync Semantics

**Document type:** Current-state architecture — *replication sync semantics* (also called a *change-propagation* or *write-path* model).

**Audience:** Anyone reasoning about whether a source INSERT, UPDATE, or DELETE will appear in replication MySQL or analytics `raw`.

**Not this doc:** Implementation roadmap ([ETL_REPLICA_FIDELITY_ROADMAP.md](ETL_REPLICA_FIDELITY_ROADMAP.md)), CDC options and future designs ([ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md](ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md)), incident write-ups ([../findings/](../findings/)).

**Source-system design (OpenDental write layer):** [../opendental/WRITE_LAYER_AND_ETL.md](../opendental/WRITE_LAYER_AND_ETL.md) — why the source mutates in place, soft vs hard deletes, and what timestamps actually mean.

---

## 1. Pipeline hops

```text
OpenDental MySQL (source, read-only)
        │  SimpleMySQLReplicator.copy_table()
        ▼
opendental_replication MySQL
        │  PostgresLoader.load_table()
        ▼
opendental_analytics.raw
        │  dbt
        ▼
staging / intermediate / marts
```

Orchestration: Airflow → `mdc etl invoke` → `PipelineOrchestrator` → `TableProcessor` (extract then load).

Config: `etl_pipeline/etl_pipeline/config/tables.yml` (analyzer v4.1+).

---

## 2. Sync model (one sentence)

The nightly path is **watermark-based incremental batch sync with upsert**. It is **not** log-based CDC. Inserts and watermark-visible updates are applied; **source deletes are never propagated** on the incremental path.

---

## 3. Write operations by hop

### 3.1 Source → replication (`simple_mysql_replicator.py`)

| Operation | When | Mechanism |
| --- | --- | --- |
| **INSERT** | Full refresh after table recreate (esp. large tables) | `INSERT INTO … VALUES` |
| **UPSERT** | Incremental, lookback, and most full batches | `INSERT … ON DUPLICATE KEY UPDATE` |
| **DELETE (structure)** | Full refresh only | `DROP TABLE` + `SHOW CREATE TABLE` recreate |
| **DELETE (row)** | Never on incremental / lookback | — |

Watermark store: replication `etl_copy_status`.

### 3.2 Replication → analytics (`postgres_loader.py`)

| Operation | When | Mechanism |
| --- | --- | --- |
| **INSERT** | Full load after truncate | Plain `INSERT` / COPY |
| **UPSERT** | Incremental / mutation / lookback | `INSERT … ON CONFLICT (pk) DO UPDATE … WHERE IS DISTINCT FROM` |
| **DELETE (bulk)** | Full load only | `TRUNCATE TABLE raw.<table>` |
| **DELETE (row)** | Never in nightly load | Manual scripts only (see §7) |

Watermark store: analytics `raw.etl_load_status`.

---

## 4. Two axes of “copy”

| Axis | Config | Meaning |
| --- | --- | --- |
| **Copy method** | `performance_category` → tiny/small/medium/large | *How* batches are read/written |
| **Extraction strategy** | `full_table` \| `incremental` \| `incremental_chunked` | *What* subset is selected |

`full_table` (and `--full` / `force_full`) is the only built-in way to clear stale rows: DROP/recreate on replication, TRUNCATE on `raw`.

---

## 5. Incremental filters: replicator vs loader

### 5.1 Replicator (source → replication)

- Uses a **single** watermark: `replicator_watermark_column` (fallback `primary_incremental_column`).
- Filter shape: `watermark > last_cursor` (plus optional lookback resync).
- Does **not** apply `or_logic` / `and_logic` from `tables.yml` as the primary filter.

### 5.2 Loader (replication → analytics)

Built by `build_mysql_loader_where_clause()` in `monitoring/replica_sync_config.py`:

| `incremental_strategy` | Meaning | Usage today |
| --- | --- | --- |
| `or_logic` | Any listed column advanced → row selected | Default for multi-column tables |
| `and_logic` | All listed columns must advance | Supported in code; **0 tables** configured |
| `single_column` | One predicate | Single incremental column |

**`in_place_updates`:** loader uses the timestamp watermark only (e.g. `SecDateTEdit > …`), not PK-in-OR. See `uses_in_place_timestamp_watermark()`.

**Lookback:** `(incremental_where) OR (business_date >= CURDATE() - N days)` via `wrap_mysql_incremental_with_lookback_config()`. Re-upserts rows that still exist in the window; does **not** delete missing PKs.

---

## 6. What change types are detected?

| Source change | Detected by nightly incremental? | How |
| --- | --- | --- |
| **Insert** | Yes | New PK / advanced watermark enters SELECT → upsert |
| **Update** | Mostly, if watermark advances | Timestamp watermark + lookback on mutation tables |
| **Delete** | **No** | Deleted row absent from SELECT → never touched |

Missed updates (watermark does not advance) are documented under [ETL-FND-001](../findings/ETL-FND-001-replica-row-drift-procedurelog.md) and §4 of [ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md](ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md).

---

## 7. Orphans (phantoms) — expected failure mode

1. Row exists in source → copied into replication (and later `raw`).
2. Row is hard-deleted in OpenDental.
3. Incremental SELECT only returns rows that still exist and satisfy watermark/lookback.
4. Deleted PK is never seen → **remains forever** in replication (and can remain in `raw` until truncate or purge).

**Detection (observability, not auto-fix):**

| Tool | Compares | Path |
| --- | --- | --- |
| Layer 0 aggregate drift | Source MySQL vs `raw` (rolling windows) | `config/replica_drift_checks.yml`, `check-replica-drift` |
| Phantom investigation | Source PK set vs `raw` PK set | `scripts/investigate_*_phantoms.py`, `investigate_payment_drift.py` |
| Row-count / audit scripts | Source / replication / analytics counts | `scripts/compare_databases.py`, `audit_table_row_counts.py` |

**Remediation today:**

| Target | Method |
| --- | --- |
| `raw` | Manual `scripts/purge_raw_phantoms.py` (PK must be absent in OpenDental) |
| Replication | Full refresh (`--full` / `extraction_strategy: full_table`) — no row-level delete-sync job |

Drift config notes: phantoms may be deleted from `raw` only — never from source. Broader mitigation: Sunday full refresh (roadmap Phase 4) or CDC delete capture (Phase 5).

---

## 8. Consistency guarantees (contract)

| Guarantee | Holds? |
| --- | --- |
| New source rows eventually appear after watermark advances | Yes (incremental tables) |
| Updated source rows appear when mutation timestamp / lookback covers them | Best-effort; not guaranteed for every OD edit |
| Deleted source rows disappear from replication / `raw` on next incremental | **No** |
| Full refresh makes hop equal to its upstream snapshot | Yes (modulo concurrent source writes during copy) |

Treat incremental tables as **append/upsert-only replicas** until a full refresh or an explicit delete-sync mechanism exists.

---

## 9. Key code map

| Concern | Location |
| --- | --- |
| Table config | `etl_pipeline/etl_pipeline/config/tables.yml` |
| Analyzer → config | `etl_pipeline/scripts/analyze_opendental_schema.py` |
| OR/AND, lookback, mutation helpers | `etl_pipeline/etl_pipeline/monitoring/replica_sync_config.py` |
| MySQL replicator | `etl_pipeline/etl_pipeline/core/simple_mysql_replicator.py` |
| Postgres loader | `etl_pipeline/etl_pipeline/loaders/postgres_loader.py` |
| Extract + load orchestration | `etl_pipeline/etl_pipeline/orchestration/table_processor.py` |
| Layer 0 drift | `etl_pipeline/etl_pipeline/monitoring/replica_aggregate_drift.py` |
| Phantom purge (`raw`) | `etl_pipeline/scripts/purge_raw_phantoms.py` |

---

## 10. Related documents

| Doc | Role |
| --- | --- |
| [etl_pipeline/README.md](../../etl_pipeline/README.md) | Pipeline overview and components |
| [../opendental/WRITE_LAYER_AND_ETL.md](../opendental/WRITE_LAYER_AND_ETL.md) | OpenDental write-layer design → ingestion implications |
| [ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md](ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md) | Why this is not binlog CDC; delete/update options |
| [ETL_REPLICA_FIDELITY_ROADMAP.md](ETL_REPLICA_FIDELITY_ROADMAP.md) | Phased fixes (lookback, Layer 0, Sunday full, CDC) |
| [../findings/ETL-FND-001-…](../findings/ETL-FND-001-replica-row-drift-procedurelog.md) | Missed in-place updates |
| [../findings/ETL-FND-002-…](../findings/ETL-FND-002-sync-profile-pk-only-misclassification.md) | `sync_profile` misclassification |

---

*Document version: 1.1 (2026-07-21). Cross-link to OpenDental write-layer notes.*
