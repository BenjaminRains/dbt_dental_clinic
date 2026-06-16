## Phase 5 — Python-first orchestration; retire the Legacy environment manager

> Status: **proposed** (not started).
> Objective: complete the migration started in Phase 4 by moving **deploy, SSM, frontend, and consult-audio** workflows into the **`mdc` Python CLI**, leaving PowerShell as an **optional thin wrapper** (or none at all after `pip install -e tools/mdc_cli`).
> Prerequisite: Phase 4 complete (Phases 4.1–4.6 merged; mdc **0.6.0**) — satisfied.

### Implementation status

| Sub-phase | Status | Notes |
|-----------|--------|-------|
| 5.1 — SSM tunnels in Python | **done** (0.7.1) | `mdc_cli/ssm.py`; `mdc tunnel` + `mdc ssm connect|status` |
| 5.2 — Shared credentials module | **done** | `credentials.py` |
| 5.3 — Frontend via mdc | **done** (0.7.0) | `mdc frontend dev`; `mdc deploy frontend --target` |
| 5.4 — Consult audio via mdc | **planned** | Stateless venv + child env (no shell activation) |
| 5.5 — Archive Legacy manager | **planned** | Remove or shrink `environment_manager.ps1`; optional `load_project.ps1` |
| 5.6 — Docs, CI, polish | **planned** | Onboarding, smoke CI, CLI encoding, alias defaults |

**Target daily workflow (no `-Legacy`):**

```bash
pip install -e tools/mdc_cli    # once per machine / venv

mdc status
mdc api run --env local
mdc etl run --env clinic --profile full
mdc tunnel clinic-db
mdc frontend dev
mdc deploy frontend --target demo
mdc deploy api --env clinic
```

**Optional Windows shorthand** (after 5.5):

```powershell
.\load_project.ps1   # ~20 lines: ensure mdc on PATH + optional aliases only
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

### Problem with the current model

Daily api/etl/dbt work is stateless and Python-driven. **Deploy and frontend still require:**

```powershell
.\load_project.ps1 -Legacy
demo-frontend-deploy          # ~400 lines of PS in environment_manager.ps1
clinic-frontend-deploy        # duplicates credential loading logic
frontend-dev                  # writes .env.local, runs npm
mdc tunnel clinic-db          # spawns PowerShell → ssm_tunnels.ps1
mdc deploy frontend           # spawns PowerShell → Deploy-Frontend in monolith
```

Pain points:

1. **Two orchestration implementations** — Python `mdc` for runs; PowerShell for deploy/frontend (credential JSON parsing duplicated in PS).
2. **`ps_invoke.py` bridge** — every tunnel/deploy-frontend call spawns `powershell -File` (slow, hard to test, quoting edge cases).
3. **`-Legacy` is a misnomer** — required for common workflows, not edge cases.
4. **Monolith maintenance** — self-reload, custom `prompt`, `$script:Is*Active`, deprecated `*-init` shims, and deploy logic in one file.
5. **Cross-platform** — Linux/macOS contributors cannot use `-Legacy` ergonomics without PowerShell 7.

Phase 5’s end state: **one CLI entry point** (`mdc`) for configuration validation, runtime, tunnels, and deploy wrappers that ops already run.

---

### Architectural shift

**From:** Python daily dev + PowerShell “second app” for everything else

```text
mdc (Python) ──api/etl/dbt──► child subprocess + pydantic env
mdc tunnel ──► powershell ──► ssm_tunnels.ps1 ──► aws
mdc deploy frontend ──► powershell ──► environment_manager.ps1 (Deploy-Frontend)
load_project.ps1 -Legacy ──► full environment_manager.ps1
```

**To:** Python orchestration everywhere; PowerShell optional

```text
                    ┌─────────────────────────────────────┐
                    │  api/settings.py                    │
                    │  etl_pipeline/.../settings_v2.py    │
                    │  mdc_cli/dbt_env.py                 │  ← config authority
                    └─────────────────┬───────────────────┘
                                      │
                    ┌─────────────────┴───────────────────┐
                    │  mdc_cli/credentials.py (new)       │  ← deployment_credentials.json
                    │  mdc_cli/ssm.py (new)                 │  ← instance IDs, port-forward
                    └─────────────────┬───────────────────┘
                                      │
                              ┌───────────────┐
                              │   mdc CLI     │
                              └───────┬───────┘
                                      │
        ┌──────────────┬──────────────┼──────────────┬──────────────┐
        ▼              ▼              ▼              ▼              ▼
   api/etl/dbt    frontend dev   S3/CloudFront   aws ssm        deploy_api_file.ps1
   (existing)    npm subprocess  (aws cli or     start-session  (wrap until ported)
                                  boto3)         (subprocess)

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

New module (proposed):

```text
tools/mdc_cli/mdc_cli/credentials.py
  load_deployment_credentials() -> DeploymentCredentials  # typed dataclass or pydantic model
  resolve_demo_frontend_config()
  resolve_clinic_frontend_config()
  read_env_file_key(path, key_name)   # CLINIC_API_KEY, DEMO_API_KEY
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

| Keep (initially) | Migrate in Phase 5 | Out of scope |
|------------------|-------------------|--------------|
| `scripts/deployment/deploy_codebase_to_clinic_ec2.ps1` etc. | `mdc tunnel *` | Every verification script under `scripts/verification/` |
| `mdc deploy api` → `deploy_api_file.ps1` until ported | `mdc deploy frontend`, `mdc frontend dev`, `mdc deploy dbt-docs` | EC2 dbt runtime (`scripts/ec2/*`) |
| Optional `load_project.ps1` aliases | `mdc consult-audio` | Single venv tool (uv) — separate track |

“Minimal PS wrapper” means: **no `environment_manager.ps1` dot-sourced into the shell**; only standalone `.ps1` files ops invoke directly, plus optional 20-line alias loader.

#### 5. Backward-compatible aliases during transition

Keep `demo-frontend-deploy` etc. as **wrappers** that call `mdc deploy frontend --target demo` for one release cycle, then remove from monolith.

---

### Inventory: what lives in `environment_manager.ps1` today

| Function / alias | Lines (approx.) | Phase 5 action |
|------------------|-----------------|----------------|
| `Sync-EnvironmentManagerScript`, `prompt`, `$script:Is*Active` | ~150 | **Delete** in 5.5 |
| `Initialize-*` / `Stop-*` (dbt/etl/api) | ~100 | **Delete** (already validate-only / no-op) |
| `Invoke-DBT`, `Invoke-ETL`, `Invoke-API`, help/status shims | ~350 | **Delete** — use `mdc` / `mdc_aliases.ps1` only |
| `Start-FrontendDev` | ~90 | **5.3** → `mdc frontend dev` |
| `Deploy-Frontend`, `Deploy-ClinicFrontend`, `Merge-DemoFrontendFromCredentialsFile` | ~720 | **5.2 + 5.3** → `mdc deploy frontend` |
| `Get-FrontendStatus` | ~150 | **5.3** → `mdc frontend status` |
| `Deploy-DBTDocs` | ~200 | **5.3** → `mdc deploy dbt-docs` |
| `Initialize-ConsultAudioEnvironment` / `Stop-*` | ~120 | **5.4** → `mdc consult-audio` |
| `Get-SSMStatus` | ~80 | **5.1** → `mdc ssm status` (or extend `mdc status`) |
| SSM connect aliases (via `ssm_tunnels.ps1` dot-source) | external | **5.1** optional `mdc ssm connect` |

Already externalized: port-forward functions in `scripts/ssm_tunnels.ps1` (~200 lines) — ported to Python in 5.1.

---

### Recommended technology stack (additions)

| Package | Use |
|---------|-----|
| Existing: Typer, Rich, pydantic-settings | CLI + tables |
| `subprocess` (stdlib) | aws, npm, powershell (transitional deploy scripts) |
| Optional: `boto3` | Phase 5 stretch — only if `aws` CLI wrapping proves painful |

Bump `mdc-cli` to **0.7.0** when 5.1–5.3 ship; **0.8.0** when Legacy manager is archived.

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
mdc consult-audio validate    # venv exists, requirements, .env keys present
mdc consult-audio run -- <cmd> # e.g. python -m consult_audio_pipe.scripts...
```

#### Deploy API (unchanged path in 5.1–5.3)

```bash
mdc deploy api --env clinic   # still → scripts/deployment/deploy_api_file.ps1
```

Port to Python in a **future Phase 5.x or Phase 6** if desired; not a gate for archiving the monolith.

---

### Migration strategy

#### Phase 5.1 — SSM tunnels in Python

**Goal:** `mdc tunnel *` never calls `mdc_run_ssm_tunnel.ps1`.

| Item | Action |
|------|--------|
| `mdc_cli/ssm.py` | `load_instance_ids()`, `port_forward_parameters_json()`, `start_port_forward_session()` |
| `credentials.py` (minimal) | Read `deployment_credentials.json` for instance IDs, RDS endpoint, demo DB host |
| `commands/tunnel.py` | Call `ssm.py` instead of `invoke_tunnel_function` |
| Tests | JSON escaping matches PS; mock `subprocess.run`; fixture credentials file |
| Deprecate | `scripts/mdc_run_ssm_tunnel.ps1`; keep `ssm_tunnels.ps1` one release with warning banner |

**Parity requirement:** reproduce `New-SsmPortForwardParametersJson` escaped JSON for Windows AWS CLI (see comment in `ssm_tunnels.ps1`).

#### Phase 5.2 — Shared credentials module

**Goal:** one Python module used by SSM, frontend deploy, and `mdc status`.

| Item | Action |
|------|--------|
| `mdc_cli/credentials.py` | Typed load of `deployment_credentials.json`; demo/clinic frontend resolution |
| `read_env_file_value(path, key)` | Replace PS regex loops for `CLINIC_API_KEY`, `DEMO_API_KEY` |
| `mdc status` | Optional rows: frontend deploy targets, SSM instance cache |
| Tests | Golden-file tests against template `deployment_credentials.json` structure |

#### Phase 5.3 — Frontend via mdc

**Goal:** remove largest chunk from `environment_manager.ps1`.

| Item | Action |
|------|--------|
| `mdc_cli/commands/frontend.py` | `dev`, `status` subcommands |
| `mdc_cli/deploy_frontend.py` | Build + S3 sync + invalidation (demo and clinic targets) |
| `commands/deploy.py` | `deploy frontend --target demo\|clinic`; `deploy dbt-docs` |
| Prerequisites | Check `aws`, `node`, `npm`, STS identity (same checks as PS today) |
| Vite env | Child env: `VITE_API_URL`, `VITE_API_KEY`, `VITE_IS_DEMO` |
| Local dev | Read API key from `.ssh/dbt-dental-clinic-api-key.pem` (optional, same as PS) |
| Tests | Mock npm/aws; integration test optional (skipped without credentials) |

**Do not** auto-backfill secrets from templates; fail with actionable paths (mirror PS error messages).

#### Phase 5.4 — Consult audio via mdc

| Item | Action |
|------|--------|
| `mdc_cli/commands/consult_audio.py` | Discover `consult_audio_pipe/venv`, `requirements.txt` |
| `run_helper` pattern | Child env from `consult_audio_pipe/.env` (`override=False` precedence) |
| `mdc consult-audio validate` | venv + key presence |

#### Phase 5.5 — Archive Legacy environment manager

| Item | Action |
|------|--------|
| `environment_manager.ps1` | Move to `scripts/archive/environment_manager.ps1` or delete after parity checklist |
| `load_project.ps1` | Default: message pointing to `pip install -e tools/mdc_cli`; optional dot-source **only** `mdc_aliases.ps1` |
| Remove `-Legacy` flag | Or keep as alias that prints "deprecated; use mdc" |
| `mdc_invoke.ps1` | Keep for aliases; remove `Invoke-MDC` dependency on monolith |
| `ps_invoke.py` | Remove `invoke_ps_function` for frontend; keep `invoke_ps_script_file` for deploy API |
| `project_profile.ps1` | Load thin aliases only |

**Stretch:** `environment_manager.ps1` absent from repo; `scripts/README.md` lists `mdc` only.

#### Phase 5.6 — Docs, CI, polish

| Item | Action |
|------|--------|
| `ENVIRONMENT_HANDLING_REVIEW.md` | Phase 5 section when sub-phases land |
| `scripts/README.md` | Remove `-Legacy` as primary path for frontend/deploy |
| `tools/mdc_cli/README.md` | Full command reference |
| CI | Job: `pytest tools/mdc_cli/tests` + `mdc status` without `load_project.ps1` |
| Unicode | ASCII `->` in new commands (cp1252-safe) |
| `test_export_env_for_shell_unit.py` | Rename to `test_pydantic_loaders_unit.py` |
| `etl-status` default | Document `clinic` default in aliases or switch to `local` when tunnel active |

---

### Testing strategy

| Test type | What |
|-----------|------|
| Unit | `credentials.py` parsing; SSM JSON parameter encoding; frontend config resolution |
| Unit | CLI routing for `--target demo\|clinic`; missing key error messages |
| Mocked integration | `subprocess.run` called with expected `aws`/`npm` argv |
| Regression | Existing `tools/mdc_cli/tests/*`; no change to api/etl/dbt run tests |
| Manual | `mdc tunnel clinic-db` + `mdc etl validate --env clinic`; demo + clinic frontend deploy to real AWS (operator) |

**CI must not require:** AWS credentials, `deployment_credentials.json`, or npm install for default PR checks.

---

### Success criteria

Phase 5 is **complete** when:

- [ ] `mdc tunnel *` runs without spawning PowerShell.
- [ ] `mdc frontend dev`, `mdc deploy frontend --target demo|clinic`, and `mdc deploy dbt-docs` run without `environment_manager.ps1`.
- [ ] `deployment_credentials.json` and env-file key parsing live in **one** Python module (no duplicate PS `Merge-DemoFrontendFromCredentialsFile`).
- [ ] `.\load_project.ps1 -Legacy` is **not required** for documented contributor workflows.
- [ ] `environment_manager.ps1` is archived or under **200 lines** (loader/aliases only, no deploy logic).
- [ ] `mdc consult-audio validate` covers consult_audio_pipe without shell venv activation.
- [ ] CI runs `mdc status` and pytest without PowerShell profile / `load_project.ps1`.
- [ ] `scripts/README.md` and main review doc describe Python-first onboarding.

Partially acceptable for v0.7.0:

- [ ] `mdc deploy api` still wraps `deploy_api_file.ps1` (Python port deferred).
- [ ] Rare `scripts/deployment/*.ps1` and `scripts/ec2/*` remain standalone for ops.

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

### Suggested implementation order

1. **5.2** (credentials module) — small, unblocks 5.1 and 5.3.
2. **5.1** (SSM tunnels) — daily value, removes PS bridge immediately.
3. **5.3** (frontend) — largest line-count reduction.
4. **5.6** (docs + CI) — parallel with 5.3.
5. **5.4** (consult-audio) — lower frequency workflow.
6. **5.5** (archive monolith) — after parity checklist green.

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

# Release
mdc deploy frontend --target clinic
mdc deploy api --env clinic
```

PowerShell users who want shorthand:

```powershell
.\load_project.ps1   # optional aliases: api-run → mdc api run ...
```

**Separate concerns:** typed settings (what), `mdc` (how), standalone deploy scripts (rare EC2 batch ops) — not one 2,000-line shell script.
