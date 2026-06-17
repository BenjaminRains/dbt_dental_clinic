# ETL Pipeline Code Review

**Date:** 2026-06-16 (updated 2026-06-17; dotenv audit + action plan)  
**Scope:** `etl_pipeline/` — config authority migration, `Settings` class, legacy env loading; **§2** includes project-wide `python-dotenv` audit (`api/`, `dbt/`, `mdc`, consult audio)  
**Current env reference:** [docs/ENVIRONMENT_FILES.md](../../docs/ENVIRONMENT_FILES.md)  
**Refactor history (archived):** [docs/_archive/ENVIRONMENT_HANDLING_REVIEW.md](../../docs/_archive/ENVIRONMENT_HANDLING_REVIEW.md)

---

## Executive Summary

The **Python config authority** for ETL connections is `settings_v2.py` → `FileConfigProvider` → `Settings`. Production library code (`get_settings()`, orchestration, loaders) is on that path when run via `mdc` or with `ETL_ENVIRONMENT` set.

**Remaining gaps:** Stage detection is duplicated in three places (see §5.3). **ETL integration test fixtures** still use `load_dotenv(override=True)` — see §2.4. Loader TODOs and missing architecture docs are tracked in §5.6.

**Next up:** Step 1 in §5 — unify stage detection via shared `settings_v2` helper.

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

## 2. `python-dotenv` Audit (project-wide)

**Audited:** 2026-06-17  
**Scope:** `api/`, `etl_pipeline/`, `dbt_dental_models/`, `tools/mdc_cli/`, `consult_audio_pipe/`  
**Reference:** [docs/ENVIRONMENT_FILES.md](../../docs/ENVIRONMENT_FILES.md) §3.4 — one loader per component, no scattered `load_dotenv`, OS env wins (Phase 0).

### 2.1 What the new model expects

| Principle | Intent |
|-----------|--------|
| **One loader per component** | `api/settings.py`, `etl_pipeline/.../settings_v2.py`, `tools/mdc_cli/mdc_cli/dbt_env.py` |
| **No scattered `load_dotenv`** | Avoid mutating `os.environ` from many import sites |
| **OS env wins** | Process / systemd / `mdc` child env beats on-disk `.env_*` files |
| **`mdc` isolated child env** | `run_isolated` + flat env dicts, not shell dot-sourcing |
| **Fail fast + log source** | Typed pydantic settings, one startup log line |

The anti-pattern is **`load_dotenv()` at module import**, especially with `override=True`, which can shadow production/systemd values or fight `mdc`'s scrub-and-inject flow.

**Acceptable dotenv usage:** `dotenv_values()` for read-only file parsing (no `os.environ` mutation); pydantic-settings `_env_file` / `dotenv_settings` source with custom precedence ordering.

### 2.2 Summary by component

| Location | API used | Mutates `os.environ`? | Aligned? |
|----------|----------|------------------------|----------|
| `api/settings.py` | `dotenv_values` + pydantic `dotenv_settings` | No direct `load_dotenv` | ✅ Canonical API loader |
| `api/test_config.py` | `dotenv_values` | No (test harness only) | ✅ |
| `etl_pipeline/.../settings_v2.py` | `dotenv_values` + pydantic `dotenv_settings` | No direct `load_dotenv` | ✅ Canonical ETL loader |
| `etl_pipeline/.../script_env.py` | None | Seeds OS only where empty | ✅ |
| `tools/mdc_cli/mdc_cli/dbt_env.py` | `dotenv_values` | No | ✅ Canonical dbt loader |
| `tools/mdc_cli/mdc_cli/credentials.py` | Hand-rolled parser | No | ✅ (not python-dotenv) |
| `tools/mdc_cli/mdc_cli/consult_audio_env.py` | Hand-rolled `_parse_dotenv_file` | No | ✅ |
| `etl_pipeline/tests/fixtures/env_fixtures.py` | **`load_dotenv(..., override=True)`** | **Yes** | ⚠️ Conflict / legacy test pattern |
| `consult_audio_pipe/` (several files) | `load_dotenv(..., override=False)` at import | Yes | ⚠️ Redundant with `mdc` |
| `dbt_dental_models/` | None | — | N/A (dbt uses `env_var()` + mdc) |

### 2.3 API (`api/`)

**Production — aligned.** `api/settings.py` does not call `load_dotenv()`.

- **`dotenv_values`** in `_resolve_sslmode()` — reads `POSTGRES_ANALYTICS_SSLMODE` from the stage file without touching `os.environ` (comment: "without load_dotenv").
- **`dotenv_values`** in `export_api_env_dict()` — flat dict for `mdc api run` / `mdc_cli/env.py` child-process injection.
- **Pydantic** `dotenv_settings` source when `_env_file` is passed to `AnalyticsDBSettings` / `APIRuntimeSettings` — internal read with `IgnoreBlankEnvSettingsSource` so OS env beats file and blanks are dropped.
- **Phase 0:** `_env_file_for_stage()` returns `None` when analytics creds are complete in `os.environ`, so stale on-disk `api/.env_api_*` cannot shadow systemd/EC2 `api/.env`.

`api/config.py` delegates to settings — no dotenv usage. Dependency: `api/requirements.txt` lists `python-dotenv>=1.0.0` (pydantic-settings + `dotenv_values` helpers).

### 2.4 ETL (`etl_pipeline/`)

#### Scripts — migrated ✅

All standalone scripts under `scripts/` use `config/script_env.py` (`load_script_settings` / `resolve_script_stage`). No `load_dotenv` in production script paths.

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

#### Library code — correct ✅

| Location | Role |
|----------|------|
| `etl_pipeline/config/settings_v2.py` | Authority; `dotenv_values` for supplemental `GLIC_*` merge and error hints only |
| `etl_pipeline/config/script_env.py` | Script bootstrap → `create_settings`; `apply_supplemental_env` fills OS gaps only |
| `etl_pipeline/config/providers.py` | Delegates to `settings_v2` |
| `etl_pipeline/orchestration/*`, `loaders/*`, `core/connections.py` | Use injected / `get_settings()` — no direct dotenv |

`settings_v2.py` mirrors API: pydantic `_env_file` for connection models; `dotenv_values` in `_missing_env_hint()` and `_supplement_env_dict()`; Phase 0 via `_env_file_for_pydantic()` skips file when analytics creds are in OS env.

#### Test fixtures — main tension ⚠️

`tests/fixtures/env_fixtures.py` calls `load_dotenv(..., override=True)` in:

- `load_test_environment_file` — used by integration/comprehensive tests (e.g. `test_postgres_loader.py`, `test_exceptions_integration.py`)
- `load_clinic_environment_file` → `clinic_settings_with_file_provider`

| New practice | Fixture behavior |
|--------------|------------------|
| OS env wins | `override=True` → **file wins** over existing shell vars |
| Single loader (`settings_v2`) | Fixture pre-seeds `os.environ`, then `FileConfigProvider` loads again via `settings_v2` |
| `mdc` child isolation | Tests bypass `mdc`'s scrub + `load_etl_env_dict()` |

The comment in the fixture says override is intentional so tests load `.env_test` even if the shell is polluted — but that **inverts production precedence** and can hide stale-shell bugs Phase 0 was meant to catch. Most unit tests use `DictConfigProvider` + `test_env_vars` and never touch dotenv.

**Recommended migration:** replace fixture `load_dotenv` with `load_etl_env_dict()` + `_overlay_os_environ`, or inject via `DictConfigProvider` / `load_etl_connection_settings_from_env`.

### 2.5 dbt (`dbt_dental_models/` + `tools/mdc_cli/`)

No Python dotenv in `dbt_dental_models/`. dbt reads vars via `profiles.yml` `env_var()`.

**`tools/mdc_cli/mdc_cli/dbt_env.py`** — `_read_env_file()` uses `dotenv_values` (read-only), then `_overlay_os_env()` applies Phase 0 (OS wins). No `load_dotenv`.

`mdc_cli/env.py` wires API / ETL / dbt loaders into `build_child_env()` / `run_isolated()` — the intended runtime path.

### 2.6 Consult audio pipe (related)

| File | Pattern |
|------|---------|
| `consult_audio_pipe/analysis.py` | `load_dotenv(..., override=False)` at **module import** |
| `consult_audio_pipe/scripts/llm_analysis_integration.py` | Same |
| `consult_audio_pipe/tests/test_*_api.py` | Same |

When run via **`mdc consult-audio`**, `consult_audio_env.py` already builds a child env dict and `run_isolated` injects it — import-time `load_dotenv` is **redundant**. `override=False` means mdc's injected env still wins (no precedence bug), but it violates "load once, no scattered calls." Direct `python -m consult_audio_pipe...` without mdc still depends on that import-time load.

### 2.7 Indirect dotenv via pydantic-settings

Both `api/settings.py` and `settings_v2.py` pass `_env_file=...` into pydantic `BaseSettings`. **pydantic-settings uses python-dotenv under the hood** for that file source. This is expected and controlled:

- Custom source order: init → env (OS) → dotenv (file) → secrets
- `IgnoreBlankEnvSettingsSource` strips empty strings
- File path can be `None` when OS is authoritative

`python-dotenv` remains a dependency even where `load_dotenv` is never called directly.

### 2.8 Dependency footprint

| Package | Declares `python-dotenv` |
|---------|--------------------------|
| `api/requirements.txt` | Yes |
| `etl_pipeline/Pipfile` | Yes |
| Root `pyproject.toml` | Yes |
| `tools/mdc_cli/pyproject.toml` | Yes |
| `consult_audio_pipe/requirements.txt` / `pyproject.toml` | Yes |

### 2.9 Conflicts vs acceptable use

**Not conflicts (by design):**

1. `dotenv_values` for read-only parsing — API export, dbt env dict, ETL supplemental vars, SSL mode.
2. Pydantic `dotenv_settings` source — controlled, single entrypoint, OS-wins ordering.
3. `script_env.apply_supplemental_env` — only fills gaps in `os.environ`.

**Actual or potential conflicts:**

1. **ETL test fixtures** (`load_dotenv(..., override=True)`) — biggest mismatch; integration tests don't exercise the same path as `mdc etl run --env test`.
2. **Consult audio import-time `load_dotenv`** — scattered, redundant when using mdc.
3. **Double-loading in clinic integration tests** — `load_clinic_environment_file` → `load_dotenv` → `FileConfigProvider` → `settings_v2` obscures value provenance.

**Already clean:** no `load_dotenv` in API or ETL production code; ETL scripts use `script_env` → `settings_v2`; dbt path is entirely mdc + `dotenv_values`.

---

## 3. `Settings` Class Review

### 3.1 Layer A — Connection / environment (tied to `settings_v2`)

| Path | Provider | Connection loading | Validation |
|------|----------|-------------------|------------|
| Production / integration | `FileConfigProvider` | `settings_v2` typed models → `connection_config_dict()` | Pydantic at provider init |
| Unit tests | `DictConfigProvider` | Legacy `ENV_MAPPINGS` scan of injected `env` dict | `validate_configs()` + `_validate_environment()` |

**Open issues** (action plan in §5):

1. **Duplicate mapping tables** — `Settings.ENV_MAPPINGS` mirrors `settings_v2.STAGE_PREFIXES`. Defer removal until test migration (§5.5).

2. **`validate_configs()` short-circuit** — Redundant calls in orchestration components (§5.4).

3. **`ETLProfile` at Settings API** — ✅ `Settings.profile` property added; still need doc for `get_source_connection_config()` when profile is `load` (§5.2 step 3).

4. **Stage detection duplicated** — `Settings._detect_environment()`, `FileConfigProvider._detect_environment()`, and `settings_v2._detect_stage()` (§5.3).

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
| Deprecate `Settings.ENV_MAPPINGS` after test migration | ✅ Done (PR #27) |
| Migrate integration fixtures from `load_dotenv` to `load_etl_env_dict` (§2.4) | ❌ Open |
| Architecture docs referenced by tests (see §5.6) | ❌ Missing from repo |
| Document `Settings.profile` + `get_source_connection_config()` when profile is `load` | ❌ Open |

---

## 5. Action Plan (prioritized)

Phase A (scripts, deprecated loader, `ConfigReader` alignment) is complete. Remaining work falls into three tracks with different effort and payoff.

### 5.1 Open work — effort vs impact

| Item | Effort | Impact | Track |
|------|--------|--------|-------|
| Unify stage detection | Small–medium | High — removes drift risk | Phase B |
| Trim `validate_configs()` redundancy | Small | Medium — clarity | Phase B |
| Document `load` profile behavior at Settings API | Small | Medium — operator clarity | Phase B |
| Deprecate `ENV_MAPPINGS` | Large | High — blocked on test migration | Phase B |
| Restore missing architecture docs | Medium | Medium — docs hygiene | Phase C |
| `postgres_loader.py` TODOs (parallel load, post-load validation) | Large | Feature work, not refactor | Phase C |
| Integration fixtures → `load_etl_env_dict` | Small | Low — optional | Phase C |

### 5.2 Recommended order

| Step | Task | Rationale |
|------|------|-----------|
| **1** | **Unify stage detection** | Best next bite: contained scope, highest code-health payoff |
| **2** | Trim duplicate `validate_configs()` in orchestration | Quick cleanup once step 1 lands |
| **3** | Document `Settings.profile` / `load` profile in `ENVIRONMENT_FILES.md` | Small doc addition; closes §3.1 item 3 |
| **4** | Plan `ENV_MAPPINGS` test migration | Inventory fixtures that need v2-shaped env dicts before removal |
| **5** | Ticket loader TODOs + restore architecture docs | Separate tasks; not mixed into config refactor |

### 5.3 Step 1 — Unify stage detection (next up)

Stage validation is duplicated in **three** places today:

| Location | Method | Exception types | Notes |
|----------|--------|-----------------|-------|
| `settings.py` | `_detect_environment()` | `EnvironmentError`, `ConfigurationError` | Handles `production` + `demo` rejection |
| `providers.py` | `FileConfigProvider._detect_environment()` | `ValueError` | Missing `demo` rejection — already drifted |
| `settings_v2.py` | `_detect_stage()` | `ValueError` | Returns `ETLStage`; authority for stage rules |

**Approach:** Export a shared helper from `settings_v2` (e.g. `resolve_etl_stage()` wrapping `_detect_stage`). Thin wrappers in `Settings` and `FileConfigProvider` map `ValueError` → typed exceptions where callers expect them. Tests in `test_pipeline_orchestrator.py` assert `EnvironmentError` / `ConfigurationError` from bare `Settings()` — preserve those contracts.

**Scope:** ~2–3 files; run config + orchestration unit/comprehensive tests.

### 5.4 Step 2 — Trim redundant `validate_configs()`

`validate_configs()` is called from:

- `pipeline_orchestrator.initialize()`
- `PriorityProcessor.__init__` → `_validate_environment()`
- `TableProcessor.__init__` → `_validate_environment()`

On the production path (`FileConfigProvider` + pydantic), it **always short-circuits to `True`** because `_typed_connection_settings()` is set at provider init. Nested orchestration components therefore re-validate on every startup — harmless but noisy.

**Approach:** Validate once at the orchestrator boundary. Keep validation in `Settings.__init__` for the legacy `DictConfigProvider` test path (which skips pydantic and relies on `ENV_MAPPINGS` scan).

### 5.5 Step 4 — `ENV_MAPPINGS` deprecation (defer until planned)

`ENV_MAPPINGS` lives only in `settings.py`, but dozens of tests still inject `TEST_*` vars via `create_test_settings()`. Removal requires migrating `DictConfigProvider` fixtures to v2-shaped env dicts so `validate_configs()` can drop the legacy scan entirely.

**Pre-work:** Inventory `tests/fixtures/*` and comprehensive config tests; align env key names with `settings_v2.STAGE_PREFIXES`.

### 5.6 Defer — loader TODOs and architecture docs

**`postgres_loader.py` TODOs** — three stubs, not cleanup:

- ~L600: parallel loading logic (unimplemented `ParallelStrategy`)
- ~L982: config gate for parallel strategy selection
- ~L1078: post-load validation (row counts, PK integrity)

Track as separate GitHub issues; do not mix into config refactor PRs.

**Missing architecture docs** — referenced but absent from repo:

| Referenced path | Referenced by |
|-----------------|---------------|
| `etl_pipeline/docs/PIPELINE_ARCHITECTURE.md` | `airflow/DAGS_STATUS.md` |
| `docs/connection_architecture.md` | CLI unit/integration test module docstrings |
| `docs/TESTING_PLAN.md` | Orchestration test module docstrings |

Restore stubs or write from `ENVIRONMENT_FILES.md` + current code. Lower urgency than code consolidation unless onboarding.

### 5.7 Phase C — optional cleanup

- Migrate integration fixtures in `env_fixtures.py` from `load_dotenv(override=True)` to `load_etl_env_dict` + `os.environ.update` (aligns with production loader; low priority).
- Optional (consult audio): remove import-time `load_dotenv` when entry is always via `mdc consult-audio` (§2.6).

---

## 6. Positive Observations

- Core library path (`get_settings` → `FileConfigProvider` → `settings_v2`) is sound.
- `config/script_env.py` gives scripts a single bootstrap pattern.
- `tests/unit/config/test_settings_v2_unit.py` covers profiles, precedence, and provider delegation well.
- `create_test_settings()` widely adopted — good unit-test isolation.
- Orchestration and loaders do not import `dotenv` directly.
- Production paths for API, ETL, and dbt follow Phase 0: no `load_dotenv`, `dotenv_values` for dict export, pydantic for typed load, `mdc` for child env (§2).

---

## 7. Quick Reference — Where to Load Env

| Context | Do this | Don't do this |
|---------|---------|---------------|
| Daily dev | `mdc etl run …` / `mdc etl schema …` | `python scripts/…` without env |
| Standalone script | `load_script_settings(stage)` or `--stage` | `load_dotenv` + first-file-wins loop |
| Unit test | `create_test_settings(env_vars=…)` | `load_dotenv` in test code |
| Integration test (real DB) | `load_etl_env_dict` or fixture via `settings_v2` / `load_etl_connection_settings_from_env` | `load_dotenv(override=True)` in fixtures (§2.4) |
| Shell export | `scripts/export_env_for_shell.py --component etl --stage …` | PowerShell parsing `.env` files |
