# ETL-FND-002 — `sync_profile` PK-only misclassification (sheetfield pattern)

**Type:** Pipeline defect (config generation + loader interaction)  
**Status:** Analyzer fix **local, uncommitted**; `tables.yml` regen and loader guard **pending**  
**Discovered:** 2026-06-29 (parsed `etl_pipeline_run_20260629_205745.log`)  
**Tracking:** [TODO.md — ETL-FND-002](../../../TODO.md#etl-fnd-002--sync-profile-pk-only-misclassification-sheetfield-pattern)

---

## Problem

Analyzer v4.1 set `sync_profile: in_place_updates` for every `is_modeled: true` table, even when
`incremental_columns` contained **only the auto-increment PK** (no `DateTStamp` / `SecDateTEdit`).

Loader `build_mysql_loader_where_clause()` applies a **datetime** predicate for `in_place_updates`:

```sql
`SheetFieldNum` > '2026-06-24 22:10:09'
```

MySQL compares integer PK to a datetime string → ~1.65M rows match → **effective full reload** on
every analytics load while replication correctly copies **0** new rows.

---

## Evidence (local run 2026-06-29)

| Table | Extract | Load | Duration |
| --- | ---: | ---: | --- |
| `sheetfield` | 0 rows | 1,654,910 rows | 73.6 min |
| `procnote` | 264 rows | 579,141 rows | 23.2 min |
| `rxnorm` | 0 rows | (full bulk) | 9.9 min |
| `statementprod` | — | — | 8.3 min |

Parsed log: `etl_pipeline/logs/etl_pipeline/etl_pipeline_run_20260629_205745_parsed.txt`

---

## Scope in current `tables.yml`

**43 modeled tables** would flip `in_place_updates` → `append_only` after regen (PK-only watermark).

**4 large/medium incremental** tables drive nightly runtime: `sheetfield`, `procnote`, `rxnorm`,
`statementprod` (~501 MB est.).

Tables with real timestamp watermarks (~50 modeled + mutation seed list) are **unchanged**.

---

## Fix

### Done (code, not deployed)

- `has_mutation_timestamp_watermark()` in `analyze_opendental_schema.py`
- `determine_sync_profile()` requires non-PK timestamp before `in_place_updates`
- Unit tests in `test_replica_fidelity_unit.py` (52 tests pass)

### Pending

1. Commit analyzer + tests
2. `mdc etl schema --env local --profile full` — regen `tables.yml` (safe during business hours)
3. Loader guard: skip datetime `in_place_updates` branch when watermark is integer PK
4. Reset `raw.etl_load_status` for large tables with stale `primary_column_name = 'timestamp'`
5. After-hours spot ETL: `--tables sheetfield procnote` — expect small `rows_loaded`
6. Deploy to clinic / let nightly DAG pick up regen’d config

---

## Mutation tradeoff

`append_only` + PK watermark captures **new rows** only. In-place edits on existing PKs (e.g.
`sheetfield.FieldValue`) are **not** replicated incrementally until Sunday full refresh or dbt-side
freshness (e.g. `stg_opendental__sheetfield` joins `sheet.DateTSheetEdited`).

---

## Related

- [ETL_REPLICA_FIDELITY_ROADMAP.md](../ETL_REPLICA_FIDELITY_ROADMAP.md) — Phase 1.6
- [schema_drift_automatic_handling.md](../schema_drift_automatic_handling.md) — `sheetfield` schema example
- [ETL-FND-001](./ETL-FND-001-replica-row-drift-procedurelog.md) — true mutation tables with timestamps
