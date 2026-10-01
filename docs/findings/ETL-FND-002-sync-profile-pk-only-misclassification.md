# ETL-FND-002 — `sync_profile` PK-only misclassification (sheetfield pattern)

**Type:** Pipeline defect (config generation + loader interaction)  
**Status:** ✅ **Closed** (2026-07-16) — on `main` via [PR #38](https://github.com/BenjaminRains/dbt_dental_clinic/pull/38) (`df45f9a1`). Analyzer, loader guard, tests, and regenerated `tables.yml` landed as `7d84dbf0` (pre-rebase hash `f3e11ef2` is not on a branch). Clinic schema regen 2026-09-29: 396 `append_only` / 50 `in_place_updates`. `sheetfield`, `procnote`, `rxnorm`, and `statementprod` are `append_only` on their integer PKs. `procedurelog` stays `in_place_updates` on `DateTStamp`.  
**Discovered:** 2026-06-29 (parsed `etl_pipeline_run_20260629_205745.log`)  
**Tracking:** [TODO.md — ETL-FND-002](../../TODO.md#etl-fnd-002--sync-profile-pk-only-misclassification-sheetfield-pattern)

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

## Scope

**43 modeled tables** flipped `in_place_updates` → `append_only` (PK-only watermark). Current
`tables.yml` (clinic regen 2026-09-29) is 396 `append_only` / 50 `in_place_updates`.

**4 large/medium incremental** tables drove nightly runtime: `sheetfield`, `procnote`, `rxnorm`,
`statementprod`. All four are now `append_only` on their integer PKs.

Tables with real timestamp watermarks (~50, including the mutation seed list) are **unchanged**.

---

## Fix

### Done

- [x] `has_mutation_timestamp_watermark()` in `analyze_opendental_schema.py`
- [x] `determine_sync_profile()` requires a non-PK timestamp before `in_place_updates`
- [x] Unit tests in `test_replica_fidelity_unit.py` (`sheetfield` → `append_only`; `procedurelog` unchanged)
- [x] Loader guard in `replica_sync_config.py` — datetime `in_place_updates` path only for timestamp watermarks (`test_replica_sync_config_unit.py`)
- [x] `tables.yml` regen — local 2026-07-09, then clinic 2026-09-29
- [x] Reset `raw.etl_load_status` for `sheetfield`, `procnote`, `rxnorm`, `statementprod` (2026-07-09)
- [x] After-hours spot ETL (2026-07-09): `sheetfield` **36** rows (~0.7 min); `procnote` **649** rows (~0.1 min) — was 1.65M / 579k
- [x] Merged to `main` — PR #38, commit `7d84dbf0`

---

## Mutation tradeoff

`append_only` + PK watermark captures **new rows** only. In-place edits on existing PKs (e.g.
`sheetfield.FieldValue`) are **not** replicated incrementally until Sunday full refresh or dbt-side
freshness (e.g. `stg_opendental__sheetfield` joins `sheet.DateTSheetEdited`).

---

## Related

- [ETL_REPLICA_FIDELITY_ROADMAP.md](../etl/ETL_REPLICA_FIDELITY_ROADMAP.md) — Phase 1.6
- [schema_drift_automatic_handling.md](../etl/schema_drift_automatic_handling.md) — `sheetfield` schema example
- [ETL-FND-001](./ETL-FND-001-replica-row-drift-procedurelog.md) — true mutation tables with timestamps
