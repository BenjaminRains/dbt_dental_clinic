# Repository maintenance

Housekeeping for this laptop checkout. GitHub security alerts, Dependabot, and Bugbot stay as they are. This note is the local cleanup cadence.

## Weekly

Log prune is `prune_logs.ps1`. Retention is by file age. `tables.yml` backups under `etl_pipeline/logs/schema_analysis/backups/` keep 90 days **and** the newest 20 files. `tables.yml` is gitignored, so those copies are the local history. Do not shorten that rule.

```powershell
.\scripts\maintenance\prune_logs.ps1 -DryRun
.\scripts\maintenance\prune_logs.ps1                 # Recycle Bin
.\scripts\maintenance\prune_logs.ps1 -Permanent      # scheduled run
```

Task Scheduler job **MDC Log Prune** runs the permanent form weekly. The 2026-10-08 run used the Recycle Bin (866 files, about 352 MB). Empty the bin once you are sure nothing from that run is needed.

This script only sees logs in this checkout. API or dbt logs on the clinic EC2 host need the same age rule on that machine.

## Local config backup

GitHub is the offsite copy of tracked code. Untracked config is copied by `scripts/utils/backup_dbt_dental_clinic.ps1` into dated folders under `C:\backup\dbt_dental_clinic_config` (newest 5 kept):

- `etl_pipeline/etl_pipeline/config/tables.yml`
- `deployment_credentials.json`
- root `.env` and `.env_ec2`
- component `.env` files (`api`, `etl_pipeline`, `dbt_dental_models`, consult-audio, frontend)

The script does not copy virtualenvs, logs, or `.git`. It does not delete the old full-tree folder `C:\backup\dbt_dental_clinic`. That copy is about 9.5 GB and includes secrets and virtualenvs. Remove it manually after a dated config snapshot exists and you do not need the full tree.

`.env_ec2` is the EC2 deploy source. It is not a duplicate of root `.env`.

Confirm separately, outside git: RDS automated backups for the analytics database, and the clinic's own OpenDental MySQL backups.

## Monthly

- Push branches you intend to keep.
- Merge or close Dependabot PRs. Clear the Bugbot and GitHub security inbox.
- New files at the repo root should be entry points (`README`, `SECURITY`, compose, `load_project.ps1`). Everything else goes in an existing folder.

## Quarterly

- Delete archived scripts that have sat unused for two quarters (`scripts/archive/README.md`).
- Banner or move superseded plans into the existing `docs/*/archive/` folders.
- Confirm RDS and OpenDental backups still match what you expect.
- Rotate passwords. `mdc secrets pull` lists the rows and waits. `mdc secrets pull clinic` stays a non-interactive sync. Last pass: 2026-10-12.
  - Clinic RDS `analytics_user` is a sync. Secrets Manager rotates it about every 7 days (last secret date 2026-10-06). `mdc secrets pull clinic` copies it into `api/.env_api_clinic` and `deployment_credentials.json`.
  - Demo `opendental_demo_user`: `mdc tunnel demo-db`, then `mdc secrets pull demo`, then `mdc deploy api --env demo`. Leaves `etl_pipeline/synthetic_data_generator/.env_demo` alone.
  - Anthropic: console name `consult_audio_key`. Replace `ANTHROPIC_API_KEY` in `consult_audio_pipe/.env`. It is not in `deployment_credentials.json`.
  - Airflow admin for native runs is `AIRFLOW_ADMIN_PASSWORD` in `airflow/.env.native`. Generate a 16-character password the same way as `scripts/airflow/init-airflow-native.ps1`, write that line, then `airflow users reset-password --username admin`. If reset fails while decoding the `session` table, delete those rows and retry. Root `.env` is the Compose sandbox password, a different secret. `AIRFLOW_FERNET_KEY` and `AIRFLOW_JWT_SECRET` there are still one shared value; split them the next time Compose is used.
  - Laptop warehouse `analytics_user` in `dbt_dental_models/.env_local` and `etl_pipeline/.env_local` stays off this list.

## Left in place on purpose

| Item | Why |
|------|-----|
| `scripts/ec2/fix_ec2_*.ps1` | `fix_ec2_dbt_setup.ps1` still calls the path and permissions scripts |
| `etl_pipeline/scripts/investigate_*.py` | PK-level phantom checks in `docs/etl/ETL_SYNC_SEMANTICS.md` |
| `method_tracker.py`, `analyze_connection_usage.py`, `test_root_connections.py` | Imported or documented as current tools |
| `scripts/utils/monitor_workflow.ps1` | Reusable sampler. Output dir `workload_monitor_logs/` is gitignored |
| `databricks_dental_lakehouse/` | Parked future project. See its README |
| `archive/analysis`, `archive/stakeholders` | Historical. Not inputs to the pipeline |
