## Phase 5 — Python-first orchestration; retire the Legacy environment manager

> Status: **in progress** (Phases **5.1–5.5 done**; mdc **0.8.0**). Remaining: **5.6** polish.
> Objective: complete the migration started in Phase 4 by moving **deploy, SSM, frontend, consult-audio, and dbt-docs** workflows into the **`mdc` Python CLI**, leaving PowerShell as an **optional thin wrapper** (or none at all after `pip install -e tools/mdc_cli`).
> Prerequisite: Phase 4 complete (Phases 4.1–4.6 merged; mdc **0.6.0**) — satisfied.
> Merged to `main`: PR #14–#16 (5.1–5.3), #18 (5.4 **0.7.4**), #19 (`--tunnel-db`). **5.5** on `refactor/phase5-5-archive-legacy-manager`.

### Implementation status

| Sub-phase | Status | Notes |
|-----------|--------|-------|
| 5.1 — SSM tunnels in Python | **done** (0.7.1) | `mdc_cli/ssm.py`; `mdc tunnel` + `mdc ssm connect\|status`; aliases call `mdc` (no `ssm_tunnels` dot-source) |
| 5.2 — Shared credentials module | **done** | `credentials.py`; `_norm()` coerces numeric JSON fields |
| 5.3 — Frontend + dbt docs via mdc | **done** (0.7.3) | `mdc frontend dev\|status`; `mdc deploy frontend`; `mdc deploy dbt-docs`; `demo_frontend` + `.frontend-deploy.json` resolution |
| 5.4 — Consult audio via mdc | **done** (0.7.4) | `mdc consult-audio install\|validate\|pipeline\|analyze\|run` |
| 5.5 — Archive Legacy manager | **done** (0.8.0) | Monolith → `scripts/archive/`; thin `load_project.ps1`; deprecation stub |
| 5.6 — Docs, CI, polish | **partial** | **Done:** CI workflow. **Open:** Unicode CLI, test file rename, `etl-status` docs, `ENVIRONMENT_HANDLING_REVIEW.md` |

**Current daily workflow (no `-Legacy` for these):**

```bash
pip install -e tools/mdc_cli    # once per machine / venv

mdc status
mdc api run --env local
mdc etl run --env clinic --profile full
mdc tunnel clinic-db
mdc ssm status
mdc ssm connect clinic-api
mdc frontend dev
mdc deploy frontend --target demo
mdc deploy dbt-docs
mdc consult-audio validate
mdc consult-audio pipeline run --llm claude
mdc deploy api --env clinic
```

**Still requires `-Legacy`:** none for documented daily workflows (`-Legacy` loads archived monolith with deprecation warning only).

**Optional Windows shorthand:**

```powershell
.\load_project.ps1   # optional aliases: api-run → mdc api run ...
pip install -e tools/mdc_cli   # once per machine / venv
```

See `ENVIRONMENT_HANDLING_REVIEW.md` for Phases 0–4 history; Phase 4 detail in `ENVIRONMENT_HANDLING_REVIEW_PHASE4_PROPOSAL.md`.

---

### Background

Phase 4 answered: **how do I run api/etl/dbt without shell activation?**

| Layer | Phase 4 outcome | Phase 5 completes |
|-------|-----------------|-------------------|
| **Configuration** | Python only (`pydantic-settings`) | Same — no second config system |
| **Daily orchestration** | `mdc api\|etl\|dbt` + child env | Same pattern for frontend, tunnels, consult-audio |
| **Infrastructure glue** | Thin PS wrappers (`ps_invoke.py`) | Python subprocess to `aws` / `npm`; boto3 where worthwhile |
| **Legacy monolith** | `environment_manager.ps1` ~2,160 lines under `-Legacy` | Archived or &lt;200-line loader |

Phase 4 optional follow-ups (extract Legacy, consult-audio, CI without PS, contributor docs) are **absorbed into Phase 5** rather than ad-hoc tasks.

---

### Problem with the current model (original Phase 5 pain points)

Phase 4 made api/etl/dbt stateless. **Phase 5.1–5.4 addressed remaining glue:**

| Was broken | Now |
|------------|-----|
| `mdc tunnel` → PowerShell → `ssm_tunnels.ps1` | `mdc_cli/ssm.py` → `aws ssm start-session` |
| `mdc deploy frontend` → `Deploy-Frontend` in monolith | `deploy_frontend.py` + `credentials.py` |
| `mdc deploy dbt-docs` → `Deploy-DBTDocs` | `deploy_dbt_docs.py` |
| `frontend-dev` only in Legacy manager | `mdc frontend dev` + alias |
| `consult-audio-init` shell venv activation | `mdc consult-audio install` + stateless `pipeline` / `run` |
| Duplicate JSON parsing in PS | `credentials.py` (+ `demo_frontend`, `.frontend-deploy.json`) |
| `ssm-connect-*` dot-sourced `ssm_tunnels.ps1` | `mdc ssm connect` + thin aliases |

**Still open (5.6):** Unicode CLI polish, test file rename, `etl-status` docs.

1. **`ps_invoke.py`** — wraps `deploy_api_file.ps1` only (acceptable deferral).

Phase 5 end state remains: **one CLI entry point** (`mdc`) for validation, runtime, tunnels, and deploy wrappers ops already run.

---

### Architectural shift

**Before Phase 5 (partially still true for Legacy-only commands):**

```text
mdc (Python) ──api/etl/dbt──► child subprocess + pydantic env
mdc tunnel ──► powershell ──► ssm_tunnels.ps1 ──► aws          [fixed in 5.1]
mdc deploy frontend ──► powershell ──► environment_manager.ps1 [fixed in 5.3]
load_project.ps1 -Legacy ──► full environment_manager.ps1      [5.5 remaining]
```

**Implemented (5.1–5.4):**

```text
                    ┌─────────────────────────────────────┐
                    │  api/settings.py                    │
                    │  etl_pipeline/.../settings_v2.py    │
                    │  mdc_cli/dbt_env.py                 │  ← config authority
                    └─────────────────┬───────────────────┘
                                      │
                    ┌─────────────────┴───────────────────┐
                    │  mdc_cli/credentials.py             │  ← deployment_credentials.json,
                    │  mdc_cli/consult_audio_env.py       │     .frontend-deploy.json
                    │  mdc_cli/ssm.py                     │  ← SSM / port-forward
                    │  deploy_frontend.py / deploy_dbt_docs.py
                    └─────────────────┬───────────────────┘
                                      │
                              ┌───────────────┐
                              │   mdc CLI     │
                              └───────┬───────┘
                                      │
        ┌──────────────┬──────────────┼──────────────┬──────────────┐
        ▼              ▼              ▼              ▼              ▼
   api/etl/dbt    frontend dev   S3/CloudFront   aws ssm        deploy_api_file.ps1
   consult-audio npm subprocess  (aws cli)       start-session  (wrap until ported)

Optional:  load_project.ps1  →  function aliases only (no dot-source monolith)
            scripts/deployment/*.ps1  →  rare EC2 batch deploys (unchanged until touched)
```

Each new command:

1. Loads repo paths and credentials via **Python** (not `Get-Content` + `ConvertFrom-Json` in PS).
2. Validates prerequisites (`aws`, `node`, `npm`, session-manager-plugin) with clear errors.
3. Runs **subprocesses**; does not mutate parent shell env for stage secrets.
4. Is **unit-testable** (mock subprocess, fixture `deployment_credentials.json`).

---

### Design principles

#### 1. Configuration stays in pydantic-settings

Phase 5 adds **deployment metadata** loaders (`deployment_credentials.json`, PEM API key path, `DEMO_API_KEY` from `api/.env_api_demo`) — not a second runtime config system for api/etl/dbt.

New module (implemented):

```text
tools/mdc_cli/mdc_cli/credentials.py   # done
tools/mdc_cli/mdc_cli/ssm.py           # done
tools/mdc_cli/mdc_cli/consult_audio_env.py  # done (5.4)
tools/mdc_cli/mdc_cli/process_util.py  # Windows npm/aws executable resolution
```

#### 2. Prefer subprocess to existing CLIs over boto3 duplication (initially)

| Tool | Phase 5 approach |
|------|----------------|
| SSM port-forward | `subprocess` → `aws ssm start-session` (same as `ssm_tunnels.ps1`) |
| S3 sync / CloudFront invalidation | `subprocess` → `aws s3 sync`, `aws cloudfront create-invalidation` (parity with PS) |
| npm / Vite | `subprocess` → `npm install`, `npm run build`, `npm run dev` |

**Later optional:** boto3 for deploy when scripts need progress callbacks or fewer shell quoting issues on Windows.

#### 3. No new shell activation model

`mdc frontend dev` and `mdc consult-audio run` set **child** env (Vite vars, OpenAI keys) — not `$script:IsConsultAudioActive` / parent `PATH` mutation.

#### 4. PowerShell scope after Phase 5

| Keep (initially) | Migrated in Phase 5 | Out of scope |
|------------------|---------------------|--------------|
| `scripts/deployment/deploy_codebase_to_clinic_ec2.ps1` etc. | `mdc tunnel *`, `mdc ssm connect` (**done**) | Every `scripts/verification/*` script |
| `mdc deploy api` → `deploy_api_file.ps1` | `mdc frontend dev`, `mdc deploy frontend` (**done**) | EC2 dbt runtime (`scripts/ec2/*`) |
| Optional `load_project.ps1` aliases | — (5.4 done) | Single venv tool (uv) |

“Minimal PS wrapper” means: **no `environment_manager.ps1` dot-sourced into the shell**; only standalone `.ps1` files ops invoke directly, plus optional 20-line alias loader.

#### 5. Backward-compatible aliases during transition

Keep `demo-frontend-deploy` etc. as **wrappers** that call `mdc deploy frontend --target demo` for one release cycle, then remove from monolith.

---

### Inventory: what lives in `environment_manager.ps1` today

| Function / alias | Lines (approx.) | Phase 5 action | Status |
|------------------|-----------------|----------------|--------|
| `Sync-EnvironmentManagerScript`, `prompt`, `$script:Is*Active` | ~150 | **Delete** in 5.5 | Open |
| `Initialize-*` / `Stop-*` (dbt/etl/api) | ~100 | **Delete** (validate-only / no-op) | Open |
| `Invoke-DBT`, `Invoke-ETL`, `Invoke-API`, help/status shims | ~350 | **Delete** — use `mdc` / `mdc_aliases.ps1` | Open |
| `Start-FrontendDev` | ~90 | `mdc frontend dev` | **Done** |
| `Deploy-Frontend`, `Deploy-ClinicFrontend`, `Merge-DemoFrontendFromCredentialsFile` | ~720 | `mdc deploy frontend` | **Done** |
| `Get-FrontendStatus` | ~150 | `mdc frontend status` | **Done** |
| `Deploy-DBTDocs` | ~200 | `mdc deploy dbt-docs` | **Done** |
| `Initialize-ConsultAudioEnvironment` / `Stop-*` | ~120 | `mdc consult-audio` | **Done** |
| `Get-SSMStatus` | ~80 | `mdc ssm status` | **Done** |
| SSM connect aliases | external | `mdc ssm connect` | **Done** |

`scripts/ssm_tunnels.ps1` — port-forward logic **ported** to `mdc_cli/ssm.py`; file kept for Legacy env manager dot-source with deprecation header.
`scripts/mdc_run_ssm_tunnel.ps1` — **no longer used** by `mdc tunnel` (safe to remove in 5.5 cleanup).

---

### Recommended technology stack (additions)

| Package | Use |
|---------|-----|
| Existing: Typer, Rich, pydantic-settings | CLI + tables |
| `subprocess` (stdlib) | aws, npm, powershell (transitional deploy scripts) |
| Optional: `boto3` | Phase 5 stretch — only if `aws` CLI wrapping proves painful |

Bump `mdc-cli` to **0.7.4** when 5.4 ships (**done**); **0.8.0** when Legacy manager is archived (5.5).

---

### Command surface (new and changed)

#### SSM / tunnels (5.1)

```bash
mdc tunnel clinic-db          # Python implementation (no PS bridge)
mdc tunnel demo-db
mdc tunnel rds                # via demo API instance
mdc tunnel close              # unchanged messaging

mdc ssm status                # plugin, AWS identity, cached instance IDs from credentials file
mdc ssm connect clinic-api    # optional: aws ssm start-session --target ...
```

#### Frontend (5.3)

```bash
mdc frontend dev              # local Vite; writes frontend/.env.local; npm run dev
mdc frontend status           # buckets, dist IDs, keys resolved (replaces Get-FrontendStatus)

mdc deploy frontend --target demo     # portfolio → dbtdentalclinic.com
mdc deploy frontend --target clinic   # clinic.dbtdentalclinic.com
mdc deploy dbt-docs                   # S3 prefix dbt-docs/ + CloudFront invalidation
```

`mdc deploy frontend` **replaces** current behavior that only supports demo via `Deploy-Frontend`.

#### Consult audio (5.4)

```bash
mdc consult-audio install           # venv + pip install -r requirements.txt (explicit; slow)
mdc consult-audio validate          # venv, OPENAI + ANTHROPIC keys, ffmpeg warning
mdc consult-audio pipeline run --llm claude   # default: needs ANTHROPIC_API_KEY
mdc consult-audio pipeline status|validate|cleanup
mdc consult-audio analyze|tokens    # scripts/llm_analysis_integration.py helpers
mdc consult-audio run -- scripts/llm_analysis_integration.py analyze
```

Use `python -m consult_audio_pipe.pipeline` (not legacy `dental_consultation_pipeline` README name).

#### Deploy API (unchanged path in 5.1–5.3)

```bash
mdc deploy api --env clinic   # still → scripts/deployment/deploy_api_file.ps1
```

Port to Python in a **future Phase 5.x or Phase 6** if desired; not a gate for archiving the monolith.

---

### Migration strategy

#### Phase 5.1 — SSM tunnels in Python — **done**

| Item | Result |
|------|--------|
| `mdc_cli/ssm.py` | `load_ssm_context()`, `port_forward_parameters_json()`, tunnel + connect helpers |
| `credentials.py` | Instance IDs, RDS endpoint, demo DB host from `deployment_credentials.json` |
| `commands/tunnel.py`, `commands/ssm.py` | No `invoke_tunnel_function` / `mdc_run_ssm_tunnel.ps1` |
| `mdc_aliases.ps1` | `ssm-connect-*` → `mdc ssm connect`; no dot-source of `ssm_tunnels.ps1` |
| Tests | `tests/test_ssm.py`; JSON escaping parity with PowerShell |

#### Phase 5.2 — Shared credentials module — **done**

| Item | Result |
|------|--------|
| `mdc_cli/credentials.py` | `load_deployment_credentials()`, demo/clinic frontend resolution, `read_env_file_value()` |
| `_norm()` | Coerces numeric JSON (e.g. demo DB `port: 5432`) |
| Tests | `tests/test_credentials.py` |

#### Phase 5.3 — Frontend + dbt docs via mdc — **done**

| Item | Result |
|------|--------|
| `commands/frontend.py` | `dev`, `status` |
| `deploy_frontend.py` | Build + S3 sync + CloudFront invalidation (demo/clinic) |
| `deploy_dbt_docs.py` | Generate if needed + S3 sync under `dbt-docs/` + CloudFront invalidation |
| `credentials.py` | `demo_frontend` + `.frontend-deploy.json` hosting resolution (0.7.3) |
| `process_util.py` | Windows `npm.cmd` / `aws` resolution |
| `commands/deploy.py` | `deploy frontend`, `deploy dbt-docs` |
| Tests | `test_frontend_commands.py`, `test_deploy_dbt_docs.py`, `test_deploy_dbt_docs_module.py` |
| Alias | `dbt-docs-deploy` → `mdc deploy dbt-docs` |

#### Phase 5.4 — Consult audio via mdc — **done**

| Item | Result |
|------|--------|
| `consult_audio_env.py` | venv discovery, `.env` child env (OS wins), validate, install |
| `commands/consult_audio.py` | `install`, `validate`, `analyze`, `tokens`, `run --`, `pipeline *` |
| Aliases | `consult-audio-validate`, `consult-audio-run`; deprecated `consult-audio-init` / `consult-audio-deactivate` |
| Tests | `test_consult_audio_env.py`, `test_consult_audio_commands.py` |
| Verified | Operator: `validate`, `pipeline run`, `pipeline status` on Windows |

#### Phase 5.5 — Archive Legacy environment manager — **done**

| Item | Result |
|------|--------|
| `environment_manager.ps1` | Moved to `scripts/archive/`; deprecation stub at `scripts/environment_manager.ps1` |
| `load_project.ps1` | Default → `mdc_aliases.ps1`; `-Legacy` deprecated (loads archive with warning) |
| `mdc_run_ps_function.ps1`, `mdc_run_ssm_tunnel.ps1` | Archived; removed from active `scripts/` |
| `ps_invoke.py` | `invoke_ps_script_file` only (deploy API) |
| `project_profile.ps1`, `load_env.ps1`, `.ps1_profile` | Thin alias loaders only |
| mdc version | **0.8.0** |

#### Phase 5.6 — Docs, CI, polish — **partial**

| Item | Status |
|------|--------|
| `.github/workflows/mdc_cli.yml` | **Done** — pytest (86 tests) + smoke on Ubuntu 3.11; no `node_modules` required |
| `test_frontend_commands.py` | **Done** — isolated tmp `frontend/` + mocked npm (CI-safe) |
| `tools/mdc_cli/README.md`, `scripts/README.md` | **Done** — command reference incl. consult-audio |
| `ENVIRONMENT_HANDLING_REVIEW.md` | **Partial** — Phase 5 section; update on 5.5 completion |
| Unicode `->` in CLI output (cp1252) | **Open** |
| Rename `test_export_env_for_shell_unit.py` | **Open** |
| `etl-status` default stage docs | **Open** |

---

### Testing strategy

| Test type | What |
|-----------|------|
| Unit | `credentials.py` parsing; SSM JSON parameter encoding; frontend config resolution |
| Unit | CLI routing for `--target demo\|clinic`; missing key error messages |
| Mocked integration | `subprocess.run` called with expected `aws`/`npm` argv |
| Regression | `tools/mdc_cli/tests/*` (86 tests); frontend dev tests use tmp dirs, not repo `node_modules` |
| Manual | `mdc tunnel clinic-db` + `mdc etl validate --env clinic`; demo + clinic frontend deploy to real AWS (operator) |

**CI must not require:** AWS credentials, `deployment_credentials.json`, or npm install for default PR checks.

---

### Success criteria

Phase 5 is **complete** when:

- [x] `mdc tunnel *` runs without spawning PowerShell.
- [x] `mdc frontend dev`, `mdc deploy frontend --target demo|clinic`, `mdc deploy dbt-docs` without `environment_manager.ps1`.
- [x] `deployment_credentials.json` and env-file key parsing in **one** Python module (`credentials.py`).
- [x] `.\load_project.ps1 -Legacy` not required for **documented daily workflows** (api/etl/dbt, frontend, tunnels, SSM, dbt-docs, consult-audio).
- [x] `environment_manager.ps1` archived (`scripts/archive/`); active path is deprecation stub.
- [x] `mdc consult-audio validate` without shell venv activation.
- [x] CI runs `mdc status` and pytest without PowerShell / `load_project.ps1` (`.github/workflows/mdc_cli.yml`).
- [x] `scripts/README.md` and main review describe Python-first onboarding.

Acceptable deferrals (unchanged):

- [x] `mdc deploy api` wraps `deploy_api_file.ps1`.
- [x] Rare `scripts/deployment/*.ps1` and `scripts/ec2/*` remain standalone for ops.

---

### What is explicitly out of Phase 5

| Item | Track |
|------|-------|
| **Single venv tool (uv/poetry)** | Separate proposal; `mdc` venv discovery unchanged |
| **Port every `scripts/deployment/*.ps1`** | Only frontend + tunnels + consult-audio in scope |
| **Port `deploy_api_file.ps1` to Python** | Optional 5.x stretch or Phase 6 |
| **Slim `Settings.ENV_MAPPINGS`** | ETL internal cleanup; not blocking |
| **Unify stage env var names** (`APP_ENV`) | Docs-only unless trivial |

---

### Relationship to prior phases

| Prior work | Phase 5 outcome |
|------------|-----------------|
| Phase 4 optional follow-ups | Absorbed here (extract Legacy, consult-audio, CI, docs) |
| `ps_invoke.py` | Shrinks to deploy-api wrapper only |
| `ssm_tunnels.ps1` | Superseded by `mdc_cli/ssm.py` |
| `mdc_aliases.ps1` | Remains optional; may shrink further |
| §6 target architecture (main review) | Fully realized for contributor workflows |

---

### Suggested implementation order (remaining)

1. **5.6 remainder** — Unicode polish, test file rename, `etl-status` default docs, `ENVIRONMENT_HANDLING_REVIEW.md`

**Completed order:** 5.2 → 5.1 → 5.3 → 5.6 (CI) → 5.4 → CI frontend fix → 5.5 (0.8.0).

---

### Long-term developer experience (post–Phase 5)

```bash
# One-time
pip install -e tools/mdc_cli

# Every day
mdc status
mdc api run --env local
mdc etl validate --env local --profile load
mdc tunnel clinic-db
mdc frontend dev
mdc consult-audio pipeline run --llm claude

# Release
mdc deploy frontend --target clinic
mdc deploy dbt-docs
mdc deploy api --env clinic
```

PowerShell users who want shorthand:

```powershell
.\load_project.ps1   # optional aliases: api-run → mdc api run ...
```

**Separate concerns:** typed settings (what), `mdc` (how), standalone deploy scripts (rare EC2 batch ops) — not one 2,000-line shell script.
