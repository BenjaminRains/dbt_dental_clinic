# ETL Pipeline Code Review

**Date:** 2026-06-16 (updated 2026-06-17)  
**Scope:** `etl_pipeline/` — config authority migration, `Settings` class, legacy env loading  
**Current env reference:** [docs/ENVIRONMENT_FILES.md](../../docs/ENVIRONMENT_FILES.md)  
**Refactor history (archived):** [docs/_archive/ENVIRONMENT_HANDLING_REVIEW.md](../../docs/_archive/ENVIRONMENT_HANDLING_REVIEW.md)

---

## Executive Summary

The **Python config authority** for ETL connections is `settings_v2.py` → `FileConfigProvider` → `Settings`. Production library code (`get_settings()`, orchestration, loaders) is on that path when run via `mdc` or with `ETL_ENVIRONMENT` set.

**Remaining gaps:** `Settings` is still a **hybrid facade** (env router + YAML reader) with a **dual validation path** (pydantic for production, legacy `ENV_MAPPINGS` for unit tests). Stage detection is duplicated between `Settings` and `settings_v2`. Loader TODOs and missing architecture docs are tracked below.

**Completed since initial review (2026-06-17):**
- `config/script_env.py` — shared `load_script_settings()` / `--stage` for standalone scripts
- Phase A script migrations (no `load_dotenv` in `scripts/`)
- `create_settings()` `config_dir` fix (`Settings.etl_pipeline_root()`)
- Stale `entry.py` TODO removed from `cli/commands.py`; CLI tests aligned
- `get_cli()` export removed; `postgres_loader_deprecated.py` deleted
- `ConfigReader.list_tables()`; `PriorityProcessor` / `PipelineOrchestrator` use `ConfigReader` for table enumeration
- `Settings.profile` property passthrough from typed settings

---

## 1. Config Authority — Target Architecture

```
ETL_ENVIRONMENT (+ optional ETL_PROFILE)
        ↓
settings_v2.load_etl_connection_settings()   ← authority for connection creds
        ↓
FileConfigProvider._load_environment_file()
        ↓
Settings (pipeline.yml, tables.yml, connection getters)
        ↓
ConnectionFactory / components
```

**Rules (from ENVIRONMENT_FILES.md):**
- OS env wins over on-disk `.env_<stage>` (Phase 0).
- One loader per component; no scattered `load_dotenv` in production paths.
- `ETL_ENVIRONMENT` must be explicit (`local` | `clinic` | `test`); no silent defaulting in library code (CLI defaults to `test` when unset — documented exception).
- `ETLProfile`: `local` defaults to `load` (replication + analytics only); clinic/test default to `full`.

**Correct script pattern:**

```python
from etl_pipeline.config.script_env import add_stage_argument, load_script_settings

# argparse main():
load_script_settings(args.stage)  # sets ETL_ENVIRONMENT, create_settings, set_settings
```

For scripts that need only connection testing: `load_etl_env_dict(environment=stage, config_dir=etl_root, profile="full")`.

---

## 2. Legacy `load_dotenv` Audit

### 2.1 Scripts — migrated ✅

All standalone scripts under `scripts/` now use `config/script_env.py` (`load_script_settings` / `resolve_script_stage`). No `load_dotenv` in production script paths.

| Script | Status |
|--------|--------|
| `analyze_opendental_schema.py` | ✅ `--stage` + `load_script_settings` |
| `setup_test_databases.py` | ✅ `load_script_settings("test")` in `main()` only |
| `test_env_test_connections.py` | ✅ `load_script_settings("test")` |
| `test_root_connections.py` | ✅ `load_script_settings`; root creds via supplemental env dict |
| `grant_etl_privileges.py` | ✅ same pattern |
| `audit_table_row_counts.py` | ✅ `--stage` + `load_script_settings` |
| `compare_databases.py` | ✅ same |
| `cleanup_old_tracking_tables.py` | ✅ same |
| `analyze_connection_usage.py` | ✅ same |
| `update_pipeline_config.py` | ✅ same |
| `initialize_etl_tracking_tables.py` | ✅ `--stage` + `load_script_settings` |
| `run_loader_smoke.py` | ✅ `resolve_script_stage` (no silent default; requires stage or env) |

### 2.2 Test fixtures — acceptable but improvable

| Location | Pattern | Notes |
|----------|---------|-------|
| `tests/fixtures/env_fixtures.py` | `load_dotenv(..., override=True)` for integration | Intentional override for tests; could migrate to `load_etl_env_dict` + `os.environ.update` |
| `tests/fixtures/env_fixtures.py` | `DictConfigProvider` / `create_test_settings` for unit tests | ✅ Correct pattern |

### 2.3 Library code — correct

| Location | Role |
|----------|------|
| `etl_pipeline/config/settings_v2.py` | Authority; `dotenv_values` only for supplemental GLIC_* merge |
| `etl_pipeline/config/script_env.py` | Script bootstrap → `create_settings` |
| `etl_pipeline/config/providers.py` | Delegates to `settings_v2` |
| `etl_pipeline/orchestration/*`, `loaders/*`, `core/connections.py` | Use injected / `get_settings()` — no direct dotenv |

---

## 3. `Settings` Class Review

### 3.1 Layer A — Connection / environment (tied to `settings_v2`)

| Path | Provider | Connection loading | Validation |
|------|----------|-------------------|------------|
| Production / integration | `FileConfigProvider` | `settings_v2` typed models → `connection_config_dict()` | Pydantic at provider init |
| Unit tests | `DictConfigProvider` | Legacy `ENV_MAPPINGS` scan of injected `env` dict | `validate_configs()` + `_validate_environment()` |

**Open issues:**

1. **Duplicate mapping tables** — `Settings.ENV_MAPPINGS` mirrors `settings_v2.STAGE_PREFIXES`. Long-term: tests inject v2-shaped env dicts, then drop `ENV_MAPPINGS`.

2. **`validate_configs()` short-circuit** — Redundant calls in orchestration components.

3. **`ETLProfile` at Settings API** — ✅ `Settings.profile` property added; document `get_source_connection_config()` behavior when profile is `load`.

4. **Stage detection duplicated** — `Settings._detect_environment()` and `settings_v2._detect_stage()` still separate. Delegate when exception mapping is unified.

### 3.2 Layer B — Pipeline / table config

- `Settings`: connection + pipeline YAML facade (`get_pipeline_setting`, `get_table_config`, `list_tables` for CLI dry-run).
- `ConfigReader`: table enumeration for orchestration (`list_tables`, `get_table_config`, `get_tables_by_importance`).

**Status:** ✅ Orchestration uses `ConfigReader` for table lists; `Settings.list_tables()` retained for CLI/settings facade.

### 3.3 `create_settings()` config directory — fixed ✅

`create_settings()` and `Settings.__init__()` both default to `Settings.etl_pipeline_root()` (`etl_pipeline/` package root where `.env_<stage>` files live).

### 3.4 Factory / singleton API

| Function | Purpose | Status |
|----------|---------|--------|
| `get_settings()` | Global singleton for production | ✅ |
| `create_settings()` | Explicit instance + FileConfigProvider | ✅ |
| `create_test_settings()` | DictConfigProvider for unit tests | ✅ |
| `reset_settings()` / `set_settings()` | Test isolation | ✅ |

---

## 4. Other Open Items

| Item | Status |
|------|--------|
| Remove `loaders/postgres_loader_deprecated.py` (~3,800 lines) | ✅ Done (2026-06-17) |
| Remove unused `get_cli()` export | ✅ Done (2026-06-17) |
| Stale TODO in `cli/commands.py` re `entry.py` | ✅ Done (2026-06-17) |
| Align stale CLI unit/comprehensive tests | ✅ Done (2026-06-17) |
| `ConfigReader.list_tables()` + align `PriorityProcessor` | ✅ Done (2026-06-17) |
| `Settings.profile` property | ✅ Done (2026-06-17) |
| `postgres_loader.py` TODOs (parallel load, post-load validation) | ❌ Open — track as issues |
| Deduplicate stage validation (`Settings` → `settings_v2._detect_stage`) | ❌ Open |
| Deprecate `Settings.ENV_MAPPINGS` after test migration | ❌ Open |
| Architecture docs referenced by tests (`docs/PIPELINE_ARCHITECTURE.md`, etc.) | ❌ Missing from repo |

---

## 5. Recommended Next Steps

### Phase B — Settings consolidation (remaining)

1. Delegate `Settings._detect_environment()` to `settings_v2._detect_stage()` with consistent exception types.
2. Plan deprecation of `ENV_MAPPINGS` once DictConfigProvider tests use v2-shaped env dicts.
3. Trim redundant `validate_configs()` calls in orchestration.

### Phase C — Cleanup (remaining)

1. Resolve or ticket `postgres_loader.py` TODOs.
2. Add or restore architecture docs referenced by tests.
3. Optional: migrate integration fixtures from `load_dotenv` to `load_etl_env_dict`.

---

## 6. Positive Observations

- Core library path (`get_settings` → `FileConfigProvider` → `settings_v2`) is sound.
- `config/script_env.py` gives scripts a single bootstrap pattern.
- `tests/unit/config/test_settings_v2_unit.py` covers profiles, precedence, and provider delegation well.
- `create_test_settings()` widely adopted — good unit-test isolation.
- Orchestration and loaders do not import `dotenv` directly.

---

## 7. Quick Reference — Where to Load Env

| Context | Do this | Don't do this |
|---------|---------|---------------|
| Daily dev | `mdc etl run …` / `mdc etl schema …` | `python scripts/…` without env |
| Standalone script | `load_script_settings(stage)` or `--stage` | `load_dotenv` + first-file-wins loop |
| Unit test | `create_test_settings(env_vars=…)` | `load_dotenv` in test code |
| Integration test (real DB) | `load_etl_env_dict` or fixture that sets env once | Scattered `load_dotenv` per test file |
| Shell export | `scripts/export_env_for_shell.py --component etl --stage …` | PowerShell parsing `.env` files |
