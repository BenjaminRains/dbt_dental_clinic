## Phase 4 — Project CLI (`mdc`) replaces environment activation

> Status: **proposed** (gaps closed in this revision).
> Objective: replace `environment_manager.ps1` as the **primary developer workflow** while keeping `pydantic-settings` as the **sole configuration authority** established in Phases 0–3.
> Prerequisite: Phase 3 merged and stable (`api-init` / `etl-init` export via Python).

---

### Background

Phases 0–3 fixed **configuration** — not **orchestration**.

| Layer | Question | Owner today | Target |
|-------|----------|-------------|--------|
| **Configuration** | What host, user, password, stage? | `api/settings.py`, `etl_pipeline/.../settings_v2.py` | Same (Python only) |
| **Orchestration** | Which venv, which command, shell state, tunnels? | `environment_manager.ps1` (~3,400 lines) | `mdc` CLI |

Phase 0–3 accomplishments:

- OS environment wins over on-disk `.env` (Phase 0 precedence).
- Typed loaders via `pydantic-settings` (API + ETL).
- PowerShell `api-init` / `etl-init` delegate validation to Python (`export_env_for_shell.py`).
- Blank-env and cross-component stale var handling in settings + `Clear-StaleStageEnvVars`.

Configuration is no longer the primary architectural risk. Remaining pain:

- PowerShell-only daily workflows.
- Stateful `*-init` / `*-deactivate` and `$script:Is*Active` flags.
- Cross-shell pollution (blank `POSTGRES_*` after switching API ↔ ETL).
- Stage semantics unclear (e.g. ETL `local` = full pipeline vs load-only).
- Fragmented stage vocabulary across API / ETL / dbt / frontend.
- Hard to test or automate init sequences.

---

### Problem with the current model

```powershell
api-init          # sets venv, exports env into process
etl-run           # assumes ETL active, inherits polluted env
api-deactivate    # clears some vars; easy to miss edge cases
```

Phase 3 improved **how** env gets into the shell but still treats the **parent shell as the config store**. Phase 4’s end state must **not** depend on that.

`export_env_for_shell.py` is a **bridge**, not the target architecture.

---

### Architectural shift

**From:** environment activation (hidden shell state)

```powershell
etl-init
etl-run
etl-deactivate
```

**To:** explicit command context (stateless subprocesses)

```bash
mdc etl run --env clinic
mdc etl validate --env local --profile load
mdc api run --env local
mdc dbt run --env clinic
mdc status
```

Each command:

1. Loads settings for `--env` via existing pydantic loaders.
2. Builds a **child-process env dict** (no mutation of the parent shell required).
3. Runs the target in the correct venv/subprocess.
4. Exits; no `$script:IsETLActive`.

```text
                    ┌─────────────────────────────────────┐
                    │  api/settings.py                    │
                    │  etl_pipeline/.../settings_v2.py    │  ← sole config authority
                    └─────────────────┬───────────────────┘
                                      │
                                      ▼
                              ┌───────────────┐
                              │   mdc CLI     │  ← orchestration (Typer)
                              │  load_settings│
                              │  to_env_dict()│
                              │  subprocess   │
                              └───────┬───────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
        api/venv python         pipenv run (etl)         dbt / deploy
        (child env injected)    (child env injected)     (child env injected)

Optional:  scripts/environment_manager.ps1  →  thin wrappers calling `mdc`
            scripts/deployment/*.ps1         →  stay PowerShell (Phase 4.x+)
```

---

### Design principles

#### 1. Configuration lives in Python only

The CLI must **not** introduce a second config system, re-parse `.env` files, or duplicate precedence rules.

Configuration entry points (unchanged):

```text
api/settings.py                          → load_api_settings(), export_api_env_dict()
etl_pipeline/etl_pipeline/config/settings_v2.py  → load_etl_env_dict(), load_etl_connection_settings()
scripts/export_env_for_shell.py          → transitional; retired when mdc injects env directly
```

The CLI imports these modules and calls them. New shared helper (Phase 4.1):

```text
tools/mdc_cli/env.py  →  run_with_env(component, stage, cmd, profile=...)
```

#### 2. Explicit context on every mutating command

Preferred:

```bash
mdc dbt run --env clinic
```

Avoid (legacy; wrappers only during migration):

```powershell
dbt-init -Target clinic
dbt run
```

Invalid `--env` values fail at CLI parse time with the supported set for that subcommand.

#### 3. Stateless execution via subprocess env injection

**Target pattern:**

```python
settings = load_etl_env_dict(environment=env, config_dir=ETL_ROOT)
subprocess.run(
    [venv_python, "-m", "etl_pipeline.cli", "run"],
    env={**os.environ, **settings},  # child only; parent unchanged
    cwd=ETL_ROOT,
)
```

**Not the end state:**

```powershell
Import-StageEnvFromPython   # parent shell pollution
etl-run                     # depends on parent state
```

`export_env_for_shell.py` remains until Phase 4.3 migrates runtime commands; then it becomes optional for interactive PS users only, and is removed in Phase 4.5.

#### 4. Capabilities, not just stage names

A stage name alone does not describe what connections a command needs.

**ETL profiles** (validated subset of connections):

| Profile | Validates | Commands |
|---------|-----------|----------|
| `load` | replication + analytics | loader-only work, local analytics dev |
| `full` | source + replication + analytics | `etl run`, `replicate`, `test_connections` |
| `source-only` | source (+ optional root) | schema analysis against live OpenDental |

Examples:

```bash
mdc etl validate --env local --profile load    # no OPENDENTAL_SOURCE_* required
mdc etl run --env clinic --profile full        # all three connections
mdc etl replicate --env clinic --profile full
```

**Phase 3.5 (before 4.3):** implement profile-aware validation in `settings_v2.py` so ETL `local` matches real `.env_local` (replication + Postgres without source). Do **not** auto-backfill or silently copy creds from template.

#### 5. PowerShell scope: thin wrappers, not zero PowerShell

Phase 4 replaces **daily dev orchestration** (api/etl/dbt init/run/status). It does **not** immediately replace:

| Keep in PowerShell (initially) | Reason |
|--------------------------------|--------|
| `scripts/deployment/*.ps1` | EC2 deploy, SSM, systemd |
| SSM port-forward helpers | Windows/session ergonomics |
| `aws-ssm-init`, tunnel aliases | Interactive multi-terminal workflows |

Phase 4+:

```bash
mdc tunnel clinic-db    # thin wrapper → existing PS script
mdc deploy api --env clinic   # wrapper → deploy_api_file.ps1
```

“PowerShell becomes optional” means **api/etl/dbt daily work**, not “delete all PS on day one.”

#### 6. Virtualenv strategy (pragmatic)

Do **not** block Phase 4 on unifying Pipenv + venv.

| Component | Runtime | `mdc` invokes |
|-----------|---------|---------------|
| API | `api/venv/Scripts/python` | direct path or `venv` discovery |
| ETL / dbt | Pipenv in `etl_pipeline/` | `pipenv run python …` |
| Consult audio | `consult_audio_pipe/venv` | later subcommand |

Unifying on uv/poetry is a **separate** follow-up (see main review §4.6), not a Phase 4 gate.

---

### Stage vocabulary and config files

`--env` is shared syntax; **meaning is component-specific**. Document in `mdc status` and `--help`.

| Stage | API | ETL | dbt (`mdc dbt`) | Notes |
|-------|-----|-----|-----------------|-------|
| `local` | ✓ | ✓ | ✓ | localhost / dev |
| `clinic` | ✓ | ✓ | ✓ | PHI / RDS / real clinic |
| `test` | ✓ | ✓ | — | test DBs; ETL uses `TEST_*` prefixes |
| `demo` | ✓ | — | ✓ | synthetic / portfolio; ETL demo = generator only |

**Canonical on-disk locations** (no repo-root fallback):

| Component | Local | Clinic | Test | Demo |
|-----------|-------|--------|------|------|
| API | `api/.env_api_local` | `api/.env_api_clinic` | `api/.env_api_test` | `api/.env_api_demo` |
| ETL | `etl_pipeline/.env_local` | `etl_pipeline/.env_clinic` | `etl_pipeline/.env_test` | N/A (use `deployment_credentials.json` for generator) |
| dbt shell vars | `dbt_dental_models/.env_local` | `dbt_dental_models/.env_clinic` or `deployment_credentials.json` | — | `deployment_credentials.json` |
| EC2 API | — | `api/.env` (systemd only) | — | `api/.env` on demo instance |

Same variable **names** across `.env_local` and `.env_clinic` (by design); only one file loaded per run. CLI must log:

```text
ETL  clinic  etl_pipeline/.env_clinic  profile=full
API  local   api/.env_api_local
```

---

### ETL local semantics (closes Phase 3 gap)

**Problem:** `settings_v2` requires `OPENDENTAL_SOURCE_*` for all stages; many `.env_local` files only define replication + analytics (load/dbt dev).

**Resolution (Phase 3.5):**

- Add `--profile` / capability to typed validation.
- Default `mdc etl validate --env local` → `--profile load`.
- `mdc etl run --env local` → `--profile full` (or explicit flag) so full pipeline requires source creds in `.env_local`.
- Update `.env_local.template` with documented `OPENDENTAL_SOURCE_*` block for developers who **do** run extract locally; no runtime auto-backfill from template.

---

### Recommended technology stack

```text
Typer          — CLI framework
Rich           — status tables, errors
pydantic-settings — already in api/etl; CLI consumes only
PyYAML         — optional (mdc config introspection)
pytest         — CLI unit tests (mock subprocess)
```

Install: root or `tools/mdc_cli/` package; invokable as `python -m mdc_cli` or console script `mdc` after `pip install -e tools/mdc_cli`.

---

### Command surface

#### Status (Phase 4.1)

```bash
mdc status                    # all components: env files present, venv paths, optional tunnel hints
mdc status --env clinic       # filter to one stage
```

Shows: stage, config file path, file exists?, last validation ok?, active venv path, **not** shell `$script:Is*Active`.

#### API

```bash
mdc api health --env local
mdc api run --env local       # uvicorn in api venv; env injected
mdc api run --env clinic
mdc api test-config --env test
```

#### ETL

```bash
mdc etl validate --env local --profile load
mdc etl validate --env clinic --profile full
mdc etl run --env clinic --profile full
mdc etl replicate --env clinic --profile full
mdc etl test-connections --env test --profile full
```

#### dbt

```bash
mdc dbt run --env clinic
mdc dbt test --env clinic
mdc dbt docs --env demo
```

Loads dbt shell env via Python (Phase 4.2b — replaces `dbt-init` file parsing) from `dbt_dental_models/.env_*` or `deployment_credentials.json` per target.

#### Infrastructure (wrappers)

```bash
mdc tunnel clinic-db          # → existing PS tunnel script
mdc tunnel close
mdc deploy api --env clinic   # → scripts/deployment/deploy_api_file.ps1 -ClinicEnv
```

---

### Migration strategy

#### Phase 3.5 — Config contract fixes (prerequisite for ETL local)

Before `mdc etl run`:

| Item | Action |
|------|--------|
| `settings_v2.py` | Profile-aware validation (`load` vs `full`) |
| `.env_local.template` | Document optional source block; align with `load` profile |
| Tests | Local validate with replication+PG only passes under `--profile load` |
| Docs | Stage × component × capability matrix in `docs/ENVIRONMENT_FILES.md` |

#### Phase 4.1 — CLI skeleton

Create:

```text
tools/mdc_cli/
  pyproject.toml
  mdc_cli/
    __main__.py
    main.py
    env.py              # run_with_env(), resolve venv paths
    commands/
      status.py
      api.py            # stubs
      etl.py
      dbt.py
      tunnel.py         # wrapper only
```

Ship:

```bash
mdc status
mdc --help
```

No change to `environment_manager.ps1` behavior yet. Add CI job: `pytest tools/mdc_cli/tests`.

#### Phase 4.2 — Read-only commands

```bash
mdc status
mdc api health --env <stage>
mdc api test-config --env <stage>
mdc etl validate --env <stage> --profile load|full
```

PowerShell aliases optional; old init paths still work.

#### Phase 4.2b — dbt env via Python

- Add `load_dbt_env_dict(target)` (new small module or under `tools/mdc_cli/`).
- Replace `dbt-init` `.env` parsing with Python export or direct `mdc dbt` subprocess injection.

#### Phase 4.3 — Runtime commands (stateless)

```bash
mdc api run --env <stage>
mdc etl run --env <stage> --profile full
mdc dbt run --env <stage>
```

PowerShell migration:

```powershell
function api-run { param([string]$Env = 'local') mdc api run --env $Env }
# Deprecate api-init / api-deactivate for run workflow (keep for backward compat one release)
```

Child-process env injection replaces `Import-StageEnvFromPython` for these commands.

#### Phase 4.4 — Retire shell export for primary workflow

Remove from primary path:

- `Import-StageEnvFromPython` for api/etl run flows
- Remaining `Get-Content` env parsers in `environment_manager.ps1` for api/etl/dbt
- `$script:IsAPIActive` / `$script:IsETLActive` guards for migrated commands

Keep export script temporarily for users who want `eval $(mdc env export …)` style (optional).

#### Phase 4.5 — Thin PowerShell layer

`load_project.ps1` loads:

```powershell
function status { mdc status @args }
function api-run { mdc api run @args }
# tunnel/deploy → mdc tunnel / mdc deploy → existing PS
```

Target: **orchestration logic in Python**; PS for Windows deployment/tunnels and backward-compatible aliases.

#### Phase 4.6 — Cleanup (optional)

- Remove `export_env_for_shell.py` if unused
- Slim `environment_manager.ps1` to &lt;500 lines or archive
- Unify venv tool (uv/poetry) — separate proposal

---

### Testing strategy

| Test type | What |
|-----------|------|
| Unit | CLI argument parsing, profile validation routing, env dict merge |
| Integration | `mdc etl validate --env test` against `.env_test` (if present) |
| No shell | CI runs `mdc` commands without `load_project.ps1` |

Regression: existing `test_settings_v2_unit.py`, `test_export_env_for_shell_unit.py`, `api/test_config.py` remain; add `tools/mdc_cli/tests/`.

---

### Success criteria

Phase 4 is complete when:

- [ ] Configuration defined only in `api/settings.py` + `settings_v2.py` (no duplicate parsers).
- [ ] Primary dev workflow uses `mdc … --env <stage>` without `*-init` / `*-deactivate`.
- [ ] Runtime commands inject env into **child processes**, not parent shell state.
- [ ] ETL `local` works with `--profile load` without source creds; full pipeline requires `--profile full` + source in file.
- [ ] `mdc status` shows stage, config path, and profile expectations.
- [ ] `environment_manager.ps1` is wrappers + tunnels/deploy only (or deprecated with migration note).
- [ ] New contributor can run API/ETL/dbt from docs without understanding shell activation flags.

---

### Long-term developer experience

```bash
mdc status

mdc api run --env local

mdc etl validate --env local --profile load

mdc etl run --env clinic --profile full

mdc dbt run --env clinic

mdc deploy api --env clinic    # wrapper to existing deploy script
```

Separate **what** (pydantic settings), **how** (`mdc` orchestration), and **where** (EC2/tunnel PowerShell) — instead of one script doing all three.

---

### Relationship to `ENVIRONMENT_HANDLING_REVIEW.md`

| Main review phase | Phase 4 action |
|-------------------|----------------|
| Phase 3 (PS delegates to Python) | Merge; bridge until 4.3 |
| Phase 3+ remaining items | Absorbed into 4.2b, 4.4, 4.6 |
| §4.4 single venv tool | Deferred to 4.6 |
| §2.2 stage naming | Documented in stage matrix above; CLI enforces per-component |

After Phase 4 ships, update main review: Phase 4 **implemented**, mark `environment_manager.ps1` deprecated.
