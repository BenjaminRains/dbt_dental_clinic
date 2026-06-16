# Environment & Virtual-Environment Handling — Review and Modernization Plan

> Status: **implemented** (Phases 0–4.6 merged to `main`; mdc **0.6.0**).
> Goal: a **clean, reproducible, easy-for-new-developers** approach to environment
> configuration across the `dbt`, `etl_pipeline`, `api`, and `consult_audio_pipe` subprojects.
> Phase 4 detail and optional follow-ups: `ENVIRONMENT_HANDLING_REVIEW_PHASE4_PROPOSAL.md`.

Environment handling has historically been a pain point in this repo. Phases 0–4 migrated
configuration to typed `pydantic-settings` loaders and daily orchestration to the **`mdc` CLI**
(stateless child-process env injection). Default `load_project.ps1` includes SSM connect/tunnels
(`scripts/ssm_tunnels.ps1`). Use `load_project.ps1 -Legacy` for frontend deploy and other legacy menus.

---

## 1. Executive summary

| Dimension | Before refactor | Now (Phases 0–4.6) | Optional next |
|---|---|---|---|
| **Secret hygiene** | Strong — templates only in git | Same | — |
| **Validation** | Fail-fast; hand-rolled loaders | Typed `pydantic-settings` (API + ETL + dbt via mdc) | — |
| **Stage naming** | Ambiguous per subproject | Documented matrix; `mdc … --env <stage>` per component | Unify edge cases in docs |
| **Env loading** | Python dotenv **and** ~3,200-line PS shim | Python loaders only; `mdc` injects child env; SSM in default aliases; Legacy for frontend | Slim Legacy manager |
| **Virtualenvs** | Pipenv (dbt/ETL) + venv (api/audio) | Same (unchanged) | Single tool (uv/poetry) |
| **Discoverability** | Fragmented docs | `docs/ENVIRONMENT_FILES.md` tracked; `mdc status` | Refresh onboarding READMEs |

**Daily workflow:**

```powershell
.\load_project.ps1              # mdc aliases — no *-init / shell activation
mdc status
mdc api run --env local
mdc etl validate --env local --profile load
mdc deploy api --env clinic     # copies api/.env_api_clinic → EC2 api/.env; restarts dental-clinic-api
.\load_project.ps1 -Legacy      # frontend deploy and other legacy menus
ssm-connect-clinic-api          # SSM shell (default aliases)
```

**Bottom line:** configuration authority is in Python (`api/settings.py`, `settings_v2.py`,
`mdc_cli/dbt_env.py`); orchestration is `mdc`; Legacy PowerShell is optional for frontend and unmigrated menus.

---

## Phase 0 — One source of truth per context (implemented)

> Status: **implemented** (low-risk first step toward the `pydantic-settings` target in §5).
> This establishes the single precedence rule the framework migration would later enforce
> automatically, with three small edits instead of a rewrite.

### The problem it fixes

The clinic API stored the **same credentials in two files**, loaded by two mechanisms:

- `api/.env` — read by **systemd** (`EnvironmentFile=`) into the process environment.
- `api/.env_api_clinic` — read by **`config.py`'s `load_dotenv()`**.

When those drifted (deploy `.env` but leave a stale `.env_api_clinic`), `config.py` could
re-inject the old `POSTGRES_ANALYTICS_PASSWORD` and the API connected to RDS with wrong
creds. The same risk existed wherever a process both inherited OS env **and** read an on-disk
`.env_*` file. The two Python loaders also disagreed on precedence (`api` used
`override=False`, `etl` used `override=True`).

### The rule

> **In any runtime context, exactly one mechanism is authoritative for config, and the OS
> process environment always wins over any on-disk `.env` file.** The `.env` file is a
> fallback for when the environment has not already been populated — never a second authority.

This matches the `pydantic-settings` precedence model (OS env → `.env` → default), so it is
forward-compatible with §5.

| Context | Authoritative source | On-disk `.env` role |
|---|---|---|
| EC2 (clinic/demo API under systemd) | systemd `EnvironmentFile=api/.env` → OS env | none — `config.py` does **not** read a second file here |
| Local dev (API / ETL / dbt) | stage file `.env_api_<stage>` / `.env_<stage>` | loaded by `mdc` child processes or direct Python |
| Dev shell | `load_project.ps1` → `mdc` aliases + SSM (`ssm-connect-*`) | no shell activation; `-Legacy` for frontend / legacy menus |
| CI / tests | `.env_test` / fixtures | the source |
| Docker / compose / Airflow | container env / root `.env` | the source |

### Changes made

1. **`api/config.py`** — `_load_environment_file()` now **skips the `.env_api_*` load entirely
   when the analytics DB host var is already set in the OS environment** (systemd/Docker/PS).
   A stale `.env_api_clinic` on EC2 is therefore inert. Local dev is unchanged (the file is
   still the source when nothing pre-set the env).
2. **`etl_pipeline/etl_pipeline/config/providers.py`** — `load_dotenv(..., override=True)` →
   `override=False`, so ETL follows the same "OS env wins" rule as the API. Behavior is
   identical in the common case where the file is the only source.
3. **`scripts/deployment/deploy_api_file.ps1`** — `-ClinicEnv` now deploys **only** `api/.env`
   (the single source of truth) and **retires any stale `api/.env_api_clinic`** on the
   instance (renames it to `*.retired.<timestamp>`) instead of writing both files.

### Verification

- **Local:** `cd api && python test_config.py` — config load + Phase 0 precedence (OS host wins; file loads when host unset).
- **EC2:** `deploy_api_file.ps1 -ClinicEnv` curls `/health/db` on the instance after restart (see Phase 0 §Verification).

### Remaining (folds into later phases)

- ~~Migrate ad-hoc `load_dotenv()` calls to `override=False`~~ → Phase 1 (below).
- Replace hand-rolled precedence with `pydantic-settings`, which enforces it natively (§5).

---

## Phase 1 — Close Phase 0 gaps (implemented)

> Status: **implemented** (merged via `refactor/environment-handling`).
> Low-risk cleanup before the `pydantic-settings` migration in §5.

### Verification

- **Local:** `cd api && python test_config.py` — config load + Phase 0 precedence (OS host wins; file loads when host unset).
- **EC2:** `scripts/deployment/deploy_api_file.ps1 -ClinicEnv` restarts the API and curls `http://127.0.0.1:8000/health/db` on the instance (use `-SkipHealthCheck` to skip).

### Goals

1. Confirm deploy tooling matches the single-source-of-truth rule (Phase 0 item 3).
2. Align ad-hoc ETL scripts with `override=False` (same precedence as `api/config.py` and `FileConfigProvider`).
3. Track onboarding docs so README links resolve.
4. Sync this review doc with the current repo state.

### Changes

| Item | Action |
|---|---|
| **`scripts/deployment/deploy_api_file.ps1`** | Already implements `-ClinicEnv`: writes `api/.env` only and retires stale `api/.env_api_clinic` on EC2. Header comments corrected to the `scripts/deployment/` path. |
| **Ad-hoc ETL scripts** | `override=True` → `override=False` in `grant_etl_privileges.py`, `test_root_connections.py`, `test_env_test_connections.py`, `setup_test_databases.py`, `analyze_opendental_schema.py`. |
| **Test fixtures** | `env_fixtures.py` keeps `override=True` intentionally (test isolation when `os.environ` is polluted). |
| **`consult_audio_pipe/`** | Explicit `load_dotenv(..., override=False)` loading `consult_audio_pipe/.env` (not cwd `.env`). |
| **`api/README.md`** | EC2 clinic env documented as `api/.env` + `-ClinicEnv` deploy (not `.env_api_clinic` on instance). |
| **Stale backup** | Removed tracked `analyze_opendental_schema.py.backup` (had old `override=True` / env names). |
| **`docs/ENVIRONMENT_FILES.md`** | Whitelisted in `.gitignore` and committed — project-wide env inventory and conventions. |
| **README / `list_env_files.ps1`** | Fixed script path to `scripts/utils/list_env_files.ps1`. |
| **`api/test_config.py`** | Fixed paths to `api/.env_api_*`; added automated Phase 0 precedence test. |
| **§2.5 below** | Updated: `profiles.yml.template` now exists. |
| **Deploy health check** | `-ClinicEnv` runs `GET /health/db` on EC2 after restart; `-SkipHealthCheck` to opt out. |

## Phase 2 — pydantic-settings for API (implemented)

> Status: **implemented** (merged via PR #5).
> Typed loader in `api/settings.py`; `api/config.py` remains the public facade.

### Changes

| Item | Action |
|---|---|
| **`api/settings.py`** | New — `AnalyticsDBSettings`, `APIRuntimeSettings`, `load_api_settings()`, Phase 0 env-file skip |
| **`api/config.py`** | Delegates to settings; `APIConfig` / `get_config()` signatures unchanged |
| **`api/test_config.py`** | Precedence test seeds non-host vars when `api-init` was not run |

## Phase 2b — pydantic-settings for ETL (implemented)

> Status: **implemented** (merged via PR #6).
> Typed loader in `etl_pipeline/etl_pipeline/config/settings_v2.py`; `FileConfigProvider` and `Settings` delegate for env loading.

### Changes

| Item | Action |
|---|---|
| **`settings_v2.py`** | Typed source/replication/analytics loaders, Phase 0 env-file skip, `connection_config_dict()` |
| **`providers.py`** | `FileConfigProvider` stores `_connection_settings`; `get_config('env')` unchanged |
| **`settings.py`** | `_get_base_config()` / `validate_configs()` delegate when typed settings present |
| **`tests/unit/config/test_settings_v2_unit.py`** | Precedence, delegation, Settings integration tests |
| **`Pipfile`** | `pydantic`, `pydantic-settings` |
| **`api/deps.py`** | `get_api_settings()` / `get_api_settings_optional()` for FastAPI Depends |
| **`api/main.py`** | `/health` uses `Depends(get_api_settings_optional)` when env is configured |

## Phase 3 — PowerShell delegates to Python (implemented; bridge removed in 4.6)

> Status: **implemented** (merged). Phase 3 used `export_env_for_shell.py` as a transitional
> bridge; **removed in Phase 4.6** — `mdc` injects env via pydantic loaders directly.

### Changes

| Item | Action |
|---|---|
| **`scripts/export_env_for_shell.py`** | Removed in 4.6 (was JSON export for `--component api\|etl`) |
| **`api/settings.py`** | `export_api_env_dict()` retained for programmatic use |
| **`environment_manager.ps1`** | `Import-StageEnvFromPython` removed in 4.6; `*-init` validate-only on Legacy path |
| **`tests/unit/config/test_export_env_for_shell_unit.py`** | Tests pydantic loaders directly (historical filename) |
| **Phase 3.5** | ETL `load`/`full` profiles; blank-env handling ( `Clear-StaleStageEnvVars` removed in 4.6) |

### Absorbed in Phase 4

| Was listed as remaining | Resolved in |
|---|---|
| `dbt-init` via Python / `mdc dbt` | Phase 4.2b — `mdc dbt validate` / `mdc dbt run` |
| Consult-audio / ad-hoc PS env parsers | Still on Legacy path only (optional: `mdc consult-audio`) |
| Single venv tool | **Deferred** — separate proposal |
| Slim `Settings.ENV_MAPPINGS` | **Optional** — once all callers use typed path |

## Phase 4.1 — `mdc` CLI skeleton (implemented)

> Status: **implemented** (merged).
> See `ENVIRONMENT_HANDLING_REVIEW_PHASE4_PROPOSAL.md` for full Phase 4 plan.

| Item | Action |
|---|---|
| **`tools/mdc_cli/`** | Typer CLI package; `pip install -e tools/mdc_cli` |
| **`mdc status`** | Config paths, pydantic validation, venv discovery (stateless) |
| **`mdc_cli/env.py`** | `run_with_env()`, `load_*_env_dict()` delegating to existing loaders |
| **Stub commands** | `api`, `etl`, `dbt`, `tunnel` subcommands (Phase 4.2+) |
| **`tools/mdc_cli/tests/`** | CLI and path unit tests |

## Phase 4.2 — Read-only validation commands (implemented)

> Status: **implemented** (merged).

| Item | Action |
|---|---|
| **`mdc api test-config`** | Validate API pydantic settings; exit 0/1 |
| **`mdc api health`** | Config-only health (settings load); HTTP `--live` deferred to 4.3 |
| **`mdc etl validate`** | Validate ETL settings with `--profile load\|full` |
| **`mdc_cli` deps** | `pydantic-settings`, `python-dotenv` for direct loader imports |

## Phase 4.2b — dbt env via Python (implemented)

> Status: **implemented** (merged).

| Item | Action |
|---|---|
| **`mdc_cli/dbt_env.py`** | `load_dbt_env_dict()`, `validate_dbt_stage()` mirroring dbt-init rules |
| **`mdc dbt validate`** | Validate dbt env for local/clinic/demo; exit 0/1 |
| **`mdc status`** | dbt rows use real validation; `discover_dbt_python()` for dbt Pipenv |

## Phase 4.3 — Runtime commands (implemented)

> Status: **implemented** (merged).

| Item | Action |
|---|---|
| **`mdc_cli/run_helper.py`** | Isolated child env (`scrub_parent_stage_env`, minimal OS base) |
| **`mdc api run`** | uvicorn in api venv; `--reload` default on `local` only |
| **`mdc etl run` / `test-connections`** | ETL CLI via Pipenv python; `--profile` default `full`; passthrough after `--` |
| **`mdc dbt run` / `test` / `docs` / `invoke`** | dbt executable with injected env; `--target` from `--env` |

## Phase 4.4 — PowerShell delegates to mdc (implemented)

> Status: **implemented** (merged).

| Item | Action |
|---|---|
| **`Invoke-MDC`** | Runs `mdc` or `python -m mdc_cli` from repo root |
| **`api-run`, `etl-run`, `etl-test`, `dbt`** | Thin wrappers — no `$script:Is*Active` guard on migrated run paths |
| **`api-init` / `etl-init` / `dbt-init`** | Deprecated in 4.6 — run `mdc validate` only (see Phase 4.6) |

## Phase 4.5 — Thin PowerShell layer (implemented)

> Status: **implemented** (merged).

| Item | Action |
|---|---|
| **`scripts/mdc_invoke.ps1`** | Shared `Invoke-MDC` helper |
| **`scripts/mdc_aliases.ps1`** | Thin daily-dev aliases (default via `load_project.ps1`) |
| **`load_project.ps1 -Legacy`** | Full `environment_manager.ps1` for deploy/SSM/frontend |
| **4.5a** | `etl-validate`, `api-test`, `etl-status`, `env-status` → `mdc`; `mdc etl status` |
| **4.5b** | `dbt-init` deprecated — runs `mdc dbt validate` only, no shell env |
| **4.5c** | `mdc tunnel clinic-db\|demo-db\|rds`; `mdc deploy frontend\|api` |
| **4.5d** | Default load path is mdc aliases, not full env manager |

## Phase 4.6 — Cleanup (implemented)

> Status: **implemented** (merged).

| Item | Action |
|---|---|
| **`export_env_for_shell.py`** | Removed; mdc injects env via pydantic loaders only |
| **`*-init` / `*-deactivate`** | `api-init`, `etl-init`, `dbt-init` run `mdc validate` only; deactivates are no-ops |
| **`Import-StageEnvFromPython`** | Removed from `environment_manager.ps1` |
| **SSM port-forward** | Dot-sources `scripts/ssm_tunnels.ps1` (no duplicate functions) |
| **`project_profile.ps1`** | Loads default `load_project.ps1` (mdc aliases), not full env manager |
| **Env manager size** | ~2,170 lines (deploy/SSM/frontend/consult-audio retained for `-Legacy`) |

## Phase 5 — Python-first orchestration (proposed)

> Status: **proposed** — see `ENVIRONMENT_HANDLING_REVIEW_PHASE5_PROPOSAL.md`.
> Objective: move deploy, SSM tunnels, frontend, and consult-audio into `mdc`; archive
> `environment_manager.ps1`; keep PowerShell as an optional thin alias loader only.

| Sub-phase | Focus |
|---|---|
| **5.1** | SSM port-forward in Python (`mdc tunnel *` without PS bridge) |
| **5.2** | Shared `credentials.py` for `deployment_credentials.json` + env-file keys |
| **5.3** | `mdc frontend dev`, `mdc deploy frontend`, `mdc deploy dbt-docs` |
| **5.4** | `mdc consult-audio` (stateless venv + child env) |
| **5.5** | Archive or shrink `environment_manager.ps1`; optional minimal `load_project.ps1` |
| **5.6** | Docs, CI smoke without PS, CLI polish |

Phase 4 optional follow-ups (Legacy extraction, consult-audio, CI without `load_project.ps1`)
are tracked in Phase 5 rather than as ad-hoc tasks.

**Still out of Phase 5 scope:** single venv tool (uv/poetry); porting every `scripts/deployment/*.ps1`;
slim `Settings.ENV_MAPPINGS` (ETL internal cleanup).

---

## 2. How it works today

### 2.1 `.env` files: templates in git, secrets gitignored

No runtime `.env` files are committed — only `*.template` files. `.gitignore` enforces this:

```
.env
.env_*
!.env_*.template
!.env.template
!**/.env.template
```

Template families:

| Template (tracked) | Runtime copy (gitignored) | Consumer |
|---|---|---|
| `.env.template` (root) | `.env` | docker-compose / Airflow / Postgres / MySQL |
| `etl_pipeline/.env_local.template` | `.env_local` | ETL + dbt local |
| `etl_pipeline/.env_clinic.template` | `.env_clinic` | ETL + dbt clinic (real PHI) |
| `etl_pipeline/.env_test.template` | `.env_test` | ETL test (`TEST_*` prefixed) |
| `api/.env_api_test.template` | `.env_api_{test,demo,clinic,local}` | API |
| `etl_pipeline/synthetic_data_generator/.env_demo.template` | `.env_demo` | Synthetic data gen |
| `consult_audio_pipe/.env.template` | `.env` | LLM API keys |

### 2.2 Stage selectors differ per subproject

| Subproject | Selector var | Valid stages | Default |
|---|---|---|---|
| ETL | `ETL_ENVIRONMENT` | `local`, `clinic`, `test` | none (CLI falls back to `test`) |
| API | `API_ENVIRONMENT` | `local`, `demo`, `clinic`, `test` | none (fail-fast) |
| dbt | `DBT_TARGET` | `local`, `demo`, `clinic` | `local` |

`demo` exists for API/dbt but **not** ETL; `test` exists for ETL/API but **not** dbt. This is
the core naming ambiguity. (A genuinely good touch: `production` is explicitly rejected with a
helpful message steering you to `clinic`/`demo`.)

### 2.3 Two parallel loading mechanisms

- **Python `python-dotenv`** (runtime): `api/config.py` (`APIConfig`) and
  `etl_pipeline/etl_pipeline/config/providers.py` (`FileConfigProvider`) each load
  `.env_{stage}` and do fail-fast validation. Both use `load_dotenv(..., override=False)` so
  the OS process environment wins over on-disk files (Phase 0).
- **PowerShell `scripts/environment_manager.ps1`** (developer workflow): a ~3,200-line script
  that does **not** use `python-dotenv`. It parses `KEY=VALUE` itself, injects vars via
  `[Environment]::SetEnvironmentVariable(...)`, activates the right venv, and sets
  `DBT_TARGET`/`DBT_PROFILES_DIR`. Exposes aliases like `dbt-init`, `etl-init`, `api-init`.
  Environments are **mutually exclusive** (must `etl-deactivate` before `dbt-init`) and the
  workflow is **PowerShell-only**.

Additionally, ~12 scripts/tests call `load_dotenv()` ad hoc, bypassing the "central" loaders.

### 2.4 Virtual environments: two tools

| Subproject | Tool | Python | Lockfile |
|---|---|---|---|
| `dbt_dental_models/` | Pipenv | 3.11 | `Pipfile.lock` ✅ |
| `etl_pipeline/` | Pipenv (+ editable install) | 3.11 | `Pipfile.lock` ✅ |
| `api/` | `python -m venv` + `requirements.txt` | implicit | none ❌ |
| `consult_audio_pipe/` | `venv` + `requirements.txt` | ≥3.8 | none ❌ |

VS Code is configured to disable its own venv activation so the PowerShell script can own PATH
(`python.terminal.activateEnvironment: false`).

### 2.5 dbt specifics

- `profiles.yml` is gitignored and generated locally from the committed template.
- `dbt_dental_models/profiles.yml.template` **is committed** and uses `env_var()` for all
  credentials — the connection contract is visible in the repo. Copy to `profiles.yml` locally:
  `cp dbt_dental_models/profiles.yml.template dbt_dental_models/profiles.yml`.
- dbt SQL/YAML models do not call `env_var()` directly; connection vars flow through `profiles.yml`.

---

## 3. Pain points (why this is hard for new devs)

1. **"Which environment am I in?" has three different answers** depending on subproject.
2. **Two implementations of "read a `.env` file"** (Python + PowerShell) that can drift.
   The `override=True`/`override=False` mismatch is **fixed** (Phase 0–1); PowerShell still
   parses `.env` files separately (Phase 3).
3. **No single setup command.** Pipenv here, `venv` there; PowerShell-only ergonomics.
4. **Stale/misaligned artifacts** add confusion: root `Dockerfile` does `COPY Pipfile` but there
   is no root `Pipfile`; a duplicate root `pyproject.toml` declares `etl-pipeline` that doesn't
   match the nested layout; `frontend/README.md` references a non-existent `.env.example`.
5. **Onboarding docs were gitignored.** `README.md` pointed to `docs/ENVIRONMENT_FILES.md`, but
   most of `docs/` was gitignored. Phase 1 whitelists and commits `docs/ENVIRONMENT_FILES.md`.
6. ~~**No committed `profiles.yml.template`**~~ — **done** (`dbt_dental_models/profiles.yml.template`).

---

## 4. Recommendations (prioritized, lowest-risk first)

1. **Unify stage names** to one vocabulary — `local` / `test` / `demo` / `clinic` — across ETL,
   API, and dbt. Keep a single env var name where possible (e.g. standardize on `APP_ENV` or
   keep the per-subproject vars but make the *value set* identical and documented).
2. **Adopt `pydantic-settings`** as the single typed source of truth for API and ETL config
   (see the deep dive in §5). This replaces both custom config classes and most ad-hoc
   `load_dotenv()` calls, and gives typing + validation + `.env` support for free.
3. **Commit a `profiles.yml.template`** — **done** (`dbt_dental_models/profiles.yml.template`).
4. **Standardize on one venv tool.** Recommended: [`uv`](https://docs.astral.sh/uv/) for speed
   and a single `uv.lock`, or Poetry. At minimum, add lockfiles for `api/` and
   `consult_audio_pipe/`.
5. **Make the PowerShell script delegate** env loading to the Python settings layer instead of
   re-parsing `.env` files. Keep the ergonomic aliases; drop the second `.env` parser.
6. **Delete/fix stale artifacts**: root `Dockerfile` + compose `dbt` service, duplicate root
   `pyproject.toml`, missing `frontend/.env.example`.
7. **Un-gitignore the onboarding docs** — **partial (Phase 1):** `docs/ENVIRONMENT_FILES.md`
   is tracked; broader `docs/` remains gitignored for sensitive material.

---

## 5. Deep dive: adopting `pydantic-settings`

### 5.1 What it is and why it fits here

`pydantic-settings` (the `BaseSettings` class, split out of Pydantic v2 into the
`pydantic-settings` package) is a configuration library that:

- **Reads values from multiple sources with a defined precedence**: explicit init args →
  environment variables → `.env` file → secrets dir → field defaults.
- **Validates and type-coerces** every field (e.g. a port becomes a real `int`, a bad value
  raises a clear `ValidationError` at startup — exactly the "fail-fast" behavior this repo
  already wants, but typed and centralized).
- **Loads `.env` files natively** via the `env_file` config — no manual `load_dotenv()` calls.
- **Supports nested / prefixed config** so the existing `POSTGRES_ANALYTICS_*`,
  `TEST_POSTGRES_ANALYTICS_*`, and `DEMO_POSTGRES_*` naming maps cleanly onto typed models.
- **Is already in the stack** — `api/requirements.txt` pins `pydantic`, and the API already uses
  Pydantic for request/response models. `pydantic-settings` is a tiny additional dependency.

Why it's a better fit than the current hand-rolled `APIConfig` / `FileConfigProvider`:

| Concern | Today (custom classes) | With `pydantic-settings` |
|---|---|---|
| Reading `.env` | Manual `load_dotenv()` + `override` flag bugs | Declarative `env_file=...`, defined precedence |
| Validation | Hand-written `missing_vars`/`int()` checks | Type system enforces it; clear errors |
| Types | Everything is `str` from `os.getenv` | `int`, `bool`, `SecretStr`, URLs, etc. |
| Discoverability | Must read code to know what vars exist | The settings class **is** the documented contract |
| Testing | Patch `os.environ` / juggle files | Instantiate `Settings(...)` with overrides |
| Duplication | API + ETL each reimplement loading | One shared base pattern |

### 5.2 Precedence model (the mental model new devs need)

For any field, `pydantic-settings` resolves the value in this order (highest wins):

1. Arguments passed to `Settings(...)` in code (great for tests).
2. **OS environment variables** — e.g. systemd `EnvironmentFile=` on EC2, Docker env, or vars
   exported by `environment_manager.ps1`.
3. Variables loaded from the **`.env` file** named for the active stage.
4. The field's **default** in the class.

This single, documented order replaces the confusing `override=True` vs `override=False`
divergence: OS env always wins over the `.env` file, which is what EC2 needs, and is consistent
everywhere.

### 5.3 Proposed API settings (`api/config.py`)

A typed replacement for `APIConfig`. The stage selects which `.env` file and which env-var
prefixes are used, but the *fields* are defined once.

```python
# api/settings.py
from enum import Enum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Stage(str, Enum):
    LOCAL = "local"
    TEST = "test"
    DEMO = "demo"
    CLINIC = "clinic"


def _env_file_for(stage: str) -> Path:
    # api/.env_api_{stage}, resolved relative to this file (matches current layout)
    return Path(__file__).parent / f".env_api_{stage}"


class AnalyticsDB(BaseSettings):
    """Analytics Postgres connection. Prefix is chosen per-stage at load time."""
    host: str
    port: int = 5432
    database: str
    user: str
    password: SecretStr

    @computed_field
    @property
    def url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class APISettings(BaseSettings):
    """Top-level API configuration. Reads API_ENVIRONMENT, then the matching .env file."""
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    environment: Stage = Field(validation_alias="API_ENVIRONMENT")

    @classmethod
    def load(cls) -> "APISettings":
        # 1) figure out the stage (OS env wins, fail-fast if missing/invalid)
        base = cls()  # raises a clear error if API_ENVIRONMENT is unset/invalid
        stage = base.environment.value

        # 2) pick the env-var prefix used for this stage (preserves current naming)
        prefix = {
            "test": "TEST_POSTGRES_ANALYTICS_",
            "demo": "DEMO_POSTGRES_",
            "clinic": "POSTGRES_ANALYTICS_",
            "local": "POSTGRES_ANALYTICS_",
        }[stage]

        # 3) load the typed DB settings from the stage's .env file + OS env
        analytics = AnalyticsDB(
            _env_file=_env_file_for(stage),
            _env_prefix=prefix,
        )
        object.__setattr__(base, "_analytics", analytics)
        return base

    @property
    def analytics(self) -> AnalyticsDB:
        return self._analytics


@lru_cache
def get_settings() -> APISettings:
    return APISettings.load()
```

Usage in FastAPI is then trivial and testable:

```python
from fastapi import Depends
from api.settings import APISettings, get_settings

@app.get("/health")
def health(settings: APISettings = Depends(get_settings)):
    return {"environment": settings.environment, "db_host": settings.analytics.host}
```

Notes:

- `SecretStr` keeps passwords out of logs/`repr` — a small HIPAA-adjacent win.
- Missing `API_ENVIRONMENT`, an invalid value, a non-numeric port, or a missing host all raise a
  single clear `ValidationError` at startup — same fail-fast intent, less code.
- OS env wins over the `.env` file automatically, so the EC2 systemd `EnvironmentFile=api/.env`
  case "just works" without the old `override=False` workaround.

### 5.4 Proposed ETL settings (`etl_pipeline/.../config`)

Same pattern, keyed on `ETL_ENVIRONMENT`, preserving the existing `TEST_*` test prefixes and the
multiple connection roles (source, replication, analytics):

```python
# etl_pipeline/etl_pipeline/config/settings_v2.py
from enum import Enum
from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ETLStage(str, Enum):
    LOCAL = "local"
    TEST = "test"
    CLINIC = "clinic"


class DBConn(BaseSettings):
    host: str
    port: int
    database: str
    user: str
    password: SecretStr


def load_etl_settings(stage: str) -> dict[str, DBConn]:
    env_file = Path(__file__).resolve().parents[2] / f".env_{stage}"
    if not env_file.exists():
        raise FileNotFoundError(f"Missing ETL env file: {env_file}")

    # Per-stage prefixes preserve the current variable names exactly.
    test = stage == "test"
    return {
        "source":      DBConn(_env_file=env_file, _env_prefix="TEST_OPENDENTAL_SOURCE_"   if test else "OPENDENTAL_SOURCE_"),
        "replication": DBConn(_env_file=env_file, _env_prefix="TEST_MYSQL_REPLICATION_"   if test else "MYSQL_REPLICATION_"),
        "analytics":   DBConn(_env_file=env_file, _env_prefix="TEST_POSTGRES_ANALYTICS_"  if test else "POSTGRES_ANALYTICS_"),
    }
```

This can live **alongside** the existing `FileConfigProvider` during migration: the provider can
delegate to it internally so callers don't change while the loader is swapped underneath.

### 5.5 Migration path (incremental, low-risk)

1. Add `pydantic-settings` to `api/requirements.txt` and `etl_pipeline/Pipfile`.
2. Introduce the new `Settings` classes **next to** the existing config (don't delete yet).
3. Re-point the existing public entry points (`APIConfig.get_database_config`,
   `FileConfigProvider.get_config('env')`) to read from the new settings internally. Keep their
   signatures so nothing downstream breaks.
4. Migrate the ~12 ad-hoc `load_dotenv()` scripts/tests to import the settings object instead.
5. Once green, remove the old hand-rolled loading code.
6. Update `environment_manager.ps1` to *set* `API_ENVIRONMENT`/`ETL_ENVIRONMENT` and let Python
   load the `.env` files — stop parsing `KEY=VALUE` in PowerShell.

### 5.6 New-developer experience (target state)

After this, onboarding for any Python subproject collapses to:

```bash
# 1. choose your stage (one vocabulary everywhere)
export API_ENVIRONMENT=local        # or test / demo / clinic

# 2. copy the template, fill in secrets
cp api/.env_api_test.template api/.env_api_local

# 3. install (one tool, locked)
uv sync                              # or: pipenv install

# 4. run — settings validate at startup with clear errors if anything is missing
uv run uvicorn api.main:app --reload
```

The settings class is the **single, typed, self-documenting contract** for what configuration
exists — no need to grep the codebase for `os.getenv` calls.

---

## 6. Target architecture (as implemented)

```
                 stage vocabulary: local | test | demo | clinic
                                   │
        ┌──────────────────────────┼──────────────────────────┐
        │                          │                           │
  pydantic-settings          mdc CLI (Typer)              Legacy PS (-Legacy)
  api/settings.py            child-process env            deploy / SSM / frontend
  settings_v2.py             mdc status | api | etl | dbt   scripts/deployment/*.ps1
  mdc_cli/dbt_env.py         mdc tunnel | deploy
        │                          │
        └──────── load_project.ps1 (default → mdc_aliases.ps1)
```

- One place defines config (typed settings).
- One place orchestrates daily dev (`mdc`).
- PowerShell no longer parses `.env` for api/etl/dbt runs; Legacy PS wraps deploy/tunnels only.
- Virtualenv unification (uv/poetry) remains an optional follow-up.

---

## 7. References (code as of Phase 4.6)

- `api/settings.py` — typed API settings; `load_api_settings()`, Phase 0 env-file skip.
- `api/config.py` — public facade; delegates to settings.
- `etl_pipeline/etl_pipeline/config/settings_v2.py` — typed ETL loaders; `load`/`full` profiles.
- `etl_pipeline/etl_pipeline/config/providers.py` — `FileConfigProvider` delegates to settings_v2.
- `tools/mdc_cli/` — `mdc` CLI (0.6.0); `run_helper.py` child env injection.
- `load_project.ps1` — default → `scripts/mdc_aliases.ps1`; `-Legacy` → `environment_manager.ps1`.
- `scripts/deployment/deploy_api_file.ps1` — EC2 deploy; `-ClinicEnv`; `/health/db` post-deploy check.
- `scripts/environment_manager.ps1` — Legacy deploy/SSM/frontend/consult-audio (~2,170 lines).
- `docs/ENVIRONMENT_FILES.md` — env file inventory and conventions (tracked).
- `ENVIRONMENT_HANDLING_REVIEW_PHASE4_PROPOSAL.md` — Phase 4 plan and success criteria (complete).
- `ENVIRONMENT_HANDLING_REVIEW_PHASE5_PROPOSAL.md` — Phase 5 plan: Python-first deploy/tunnels/frontend.
