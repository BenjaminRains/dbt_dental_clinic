# Rename `etl_pipeline/` → `opendental_pipeline/`

**Status:** Planned — not started  
**Priority:** Low  
**Decision date:** 2026-10-12  
**Tracking:** [TODO.md](../../TODO.md) Tier 8 (local; gitignored)

The project directory is the OpenDental extract path: source MySQL → local replica → Postgres `raw`. dbt lives in `dbt_dental_models/`, and consult audio already has its own pipeline in `consult_audio_pipe/`. `etl_pipeline` does not say which source it owns and will collide with the next one.

This is a directory rename. The Python import and the Airflow DAG id stay `etl_pipeline`.

---

## Decisions (locked)

| Item | Choice |
| --- | --- |
| **Project directory** | `etl_pipeline/` → `opendental_pipeline/` |
| **Python package** | Stay `opendental_pipeline/etl_pipeline/` (import `etl_pipeline`) |
| **Distribution name** | Stay `etl_pipeline` in `setup.py` and the Pipfile editable install |
| **Console script** | Stay `etl` |
| **`mdc` command** | Stay `mdc etl` |
| **Airflow DAG id** | Stay `etl_pipeline` |
| **DAG module filename** | Stay `airflow/dags/etl_pipeline_dag.py` in this change |
| **Run-log subdirectory** | Stay `opendental_pipeline/logs/etl_pipeline/` |
| **Docs topic folder** | Stay `docs/etl/` |

After the move the layout is `opendental_pipeline/etl_pipeline/`. The outer name is the product. The inner name is the import.

Do this on its own branch. Do not combine it with the GitHub repo rename to `mdc-analytics` ([REPO_RENAME_MDC_ANALYTICS_PLAN.md](../deployment/REPO_RENAME_MDC_ANALYTICS_PLAN.md)). That plan changes the repo root and remotes. This plan changes one child directory.

---

## What follows the directory automatically

These resolve from `__file__` and keep working as long as the project stays a direct child of the repo root and the inner package name stays `etl_pipeline`:

- `.env_<stage>` lookup in `etl_pipeline/config/settings_v2.py` (three parents up from the config module, i.e. the project directory)
- `dbt_dental_models/.env_local` lookup (`config_dir.parent / "dbt_dental_models"`)
- `etl_project_root()` / `monorepo_root()` in `etl_pipeline/config/paths.py` when `repo_root` is not passed
- `python -m etl_pipeline.cli.main` once the new directory is on `PYTHONPATH` the way the old one was

`git mv` moves tracked files only. `.env_*` (except templates) and `logs/` are untracked. On each machine, move those leftovers into `opendental_pipeline/` before deleting the old directory.

---

## Phase 0 — Inventory

- [ ] Re-read this plan. Confirm the directory name stays `opendental_pipeline` and the import / DAG id stay `etl_pipeline`.
- [ ] Search the repo for the path string `etl_pipeline` and split hits into three buckets: directory path (update), import / module / DAG id (leave), prose in historical docs (update only when the path would be wrong).
- [ ] List machines that have a checkout: laptop, clinic EC2, any Compose host. Note where `.env_local`, `.env_clinic`, `.env_test`, and `logs/` actually sit.
- [ ] Confirm no Airflow run is in progress on the host you will update.

**Exit criteria:** The file list in Phase 2 is still accurate, and you know which untracked files must be moved by hand.

---

## Phase 1 — Move the directory

From the repo root:

```powershell
git mv etl_pipeline opendental_pipeline
```

Then move untracked contents that `git mv` left behind:

- `etl_pipeline/.env_local`, `.env_clinic`, `.env_test` (and any other `.env_*` that is not a template)
- `etl_pipeline/logs/`
- `etl_pipeline/.venv` or the Pipenv virtualenv is not portable — reinstall in Phase 3 instead of moving it

Remove the empty old directory only after those files are in `opendental_pipeline/`.

---

## Phase 2 — Update hardcoded directory paths

Update the directory string. Leave `import etl_pipeline`, `python -m etl_pipeline.cli.main`, `setup.py` name `etl_pipeline`, and DAG id `etl_pipeline` alone.

### Code and config that join the path

- [ ] `tools/mdc_cli/mdc_cli/paths.py` — `ETL_DIR = REPO_ROOT / "opendental_pipeline"`. `etl_pipeline_config()` still appends `/etl_pipeline/config/pipeline.yml`.
- [ ] `tools/mdc_cli` tests that build `tmp_path / "etl_pipeline"` or assert the path `etl_pipeline/config/pipeline.yml` as a repo-relative path (`test_etl_env.py`, `test_paths.py`, `test_phase45.py`).
- [ ] `airflow/dags/etl_pipeline_dag.py` — `PROJECT_ROOT / 'opendental_pipeline'`. The inner `etl_pipeline/config/tables.yml` segment stays.
- [ ] `airflow/dags/schema_analysis_dag.py` — same split.
- [ ] `airflow/dags/lib/schema_refresh.py` — `etl_pipeline_dir()` returns `project_root / "opendental_pipeline"`. Rename the helper only if the call sites stay obvious.
- [ ] `opendental_pipeline/etl_pipeline/config/paths.py` — `logs_root(repo_root=...)` currently does `repo_root / "etl_pipeline" / "logs"`. Point that at `opendental_pipeline`. Keep `etl_run_logs_dir()` as `logs/etl_pipeline`. Update the display string in `schema_analysis_backup_display_path()`.
- [ ] `docker-compose.yml` — bind mounts `./opendental_pipeline:/opt/airflow/dbt_dental_clinic/opendental_pipeline` (three services).
- [ ] `.gitignore` — `etl_pipeline/docs/` and `etl_pipeline/CODE_REVIEW.md`.
- [ ] `.github/dependabot.yml` — `directory: /opendental_pipeline`.
- [ ] `.github/codeql/codeql-config.yml` — exclude paths.
- [ ] `.github/workflows/mdc_cli.yml` — path filters under `opendental_pipeline/etl_pipeline/config/**`.

### Scripts

- [ ] `scripts/maintenance/prune_logs.ps1` — parent folder `opendental_pipeline\logs\...`. The inner run-log folder stays `logs\etl_pipeline`.
- [ ] `scripts/utils/backup_dbt_dental_clinic.ps1`
- [ ] `scripts/utils/list_env_files.ps1`
- [ ] `scripts/publish/publish_analytics_to_rds.ps1`
- [ ] `scripts/testing/test_glic_connection.ps1`
- [ ] `scripts/utils/export_mdc_table_list.ps1`
- [ ] `scripts/utils/export_glic_table_list.ps1`
- [ ] `scripts/database/setup_local_demo_db.ps1`
- [ ] `scripts/archive/environment_manager.ps1` and `scripts/archive/etl_functions.ps1` — update if they are still invoked; otherwise leave a one-line note that they are stale.

### Docs that state a path

Update path strings in active docs so the next person does not recreate `etl_pipeline/.env_clinic`. Start with:

- [ ] `opendental_pipeline/README.md`
- [ ] `docs/deployment/ENVIRONMENT_FILES.md`
- [ ] `docs/deployment/ENVIRONMENT_SETUP.md`
- [ ] `docs/etl/` pages that tell you to `cd etl_pipeline` or read `etl_pipeline/.env_*`

Historical findings and archive docs can keep the old name when they describe a past layout. Do not bulk-rewrite them.

**Exit criteria:** A search for the directory prefix `etl_pipeline/` (with the slash) in tracked files is either updated or an intentional historical mention. Imports of the package `etl_pipeline` remain.

---

## Phase 3 — Recreate local environments

Pipenv hashes the project path. The virtualenv at the old location will not follow the folder.

On the laptop, from `opendental_pipeline/`:

```powershell
pipenv install --dev
pipenv run python -c "import etl_pipeline; print(etl_pipeline.__file__)"
```

The printed path should sit under `opendental_pipeline\etl_pipeline\`.

On each other checkout (clinic EC2, Compose host):

- [ ] Pull the branch.
- [ ] Move that machine's `.env_clinic` (and logs, if you need them) into `opendental_pipeline/`.
- [ ] Reinstall the Pipenv environment from the moved Pipfile.
- [ ] Recreate or restart Compose so the bind mount is the new directory. Do this outside the nightly window.

**Exit criteria:** `mdc etl` can see `.env_<stage>` at `opendental_pipeline/.env_<stage>` and imports `etl_pipeline` from the new tree.

---

## Phase 4 — Verify

- [ ] `pipenv run pytest` from `opendental_pipeline/` for a unit slice that imports the package (connections or CLI). Full suite is optional for this rename.
- [ ] `mdc etl status --env local` (or `test-connections`) resolves config under `opendental_pipeline/etl_pipeline/config/`.
- [ ] Airflow DAG bag still contains DAG id `etl_pipeline` (`airflow/tests/test_dags.py`).
- [ ] Schema refresh helper points `tables.yml` at `opendental_pipeline/etl_pipeline/config/tables.yml`.
- [ ] `tools/mdc_cli` tests that cover `ETL_DIR` and etl env paths pass.
- [ ] One Compose config check (`docker compose config`) shows the new mount and no stale `./etl_pipeline` source.

**Exit criteria:** Import, env file, `mdc etl`, and DAG id `etl_pipeline` all work against the new directory. Airflow run history under `dag_id=etl_pipeline` is untouched.

---

## Out of scope

- Renaming the Python package, `setup.py` name, or `python -m etl_pipeline...`. That is a separate change across the import graph, coverage (`--cov=etl_pipeline`), and every installed editable copy.
- Changing the Airflow DAG id. A new id orphans run history and `airflow/logs` for `dag_id=etl_pipeline`, and it changes the `mdc airflow` default.
- Renaming `mdc etl`, the `etl` console script, or `docs/etl/`.
- Renaming databases (`opendental`, `opendental_repl`, `opendental_analytics`) or dbt model prefixes (`stg_opendental__`).
- The repo-root rename to `mdc-analytics`, EC2 path `/opt/dbt_dental_clinic`, or the `dbtdentalclinic.com` domain.

---

## Rollback

Before the branch is deployed to a host that runs nightly ETL, `git revert` the commit and move `.env_*` back. After a host has been updated, rollback is the same directory move in reverse plus a Pipenv reinstall. DAG history does not need a migration because the DAG id never changes.
