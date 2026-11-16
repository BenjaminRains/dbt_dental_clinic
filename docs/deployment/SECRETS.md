# Secrets

How a secret is created, where it lives, and which command moves it. Env-file paths and loaders are in [ENVIRONMENT_FILES.md](ENVIRONMENT_FILES.md).

Commands that write a password do not print it.

## Generate

A password or signing secret this repo creates is one call:

```text
secrets.token_urlsafe(32)
```

That is 32 random bytes, encoded with letters, digits, `-`, and `_`. `generate_demo_password()` in `tools/mdc_cli/mdc_cli/secrets_manager.py` is that call. The Airflow admin password and `AIRFLOW__API_AUTH__JWT_SECRET` use the same call (`scripts/airflow/init-airflow-native.ps1`, `scripts/airflow/ensure_jwt_secret.py`).

Write the value into the file below. Do not paste it into a ticket, a commit, or a Power BI project.

Three values are not this call:

| Value | Why |
| --- | --- |
| Clinic `analytics_user` | AWS rotates the RDS master secret (`rds!db-...`). `mdc secrets pull clinic` copies it. |
| Airflow Fernet key | Airflow requires `Fernet.generate_key()`. |
| QuickBooks OAuth `state` | One-request nonce. It is not stored. |

## Where each secret lives

| Secret | Local file or store | How it is set |
| --- | --- | --- |
| Clinic `analytics_user` | Secrets Manager, then `api/.env_api_clinic` and `deployment_credentials.json` | `mdc secrets pull clinic` |
| Demo `opendental_demo_user` | `api/.env_api_demo` and `deployment_credentials.json` | `mdc tunnel demo-db`, then `mdc secrets pull demo` |
| Local `bi_user` | `powerbi/.env_local` (`POSTGRES_BI_PASSWORD`) | Generate with `token_urlsafe(32)` and write the file. Host, port, and database stay in `dbt_dental_models/.env_local`. |
| Clinic `bi_user` | Secrets Manager | Separate secret from the RDS `analytics_user` secret. |
| Airflow admin and JWT | `airflow/.env.native` | Native init script. Admin rotation is manual. |
| Anthropic API key | `consult_audio_pipe/.env` | Revoke in the Anthropic console, then replace the file. |

`analytics_user` and `bi_user` are different logins. `POSTGRES_BI_*` stays out of `etl_pipeline/.env_*` and `api/.env_api_*`. `POSTGRES_ANALYTICS_*` stays out of `etl_pipeline/.env_clinic`.

## `mdc secrets pull`

```text
mdc secrets pull
```

With no target, this lists the catalog and waits. Nothing changes until you confirm a row.

| Target | What it does |
| --- | --- |
| `mdc secrets pull clinic` | Copies the live RDS master secret into `api/.env_api_clinic` and the clinic fields in `deployment_credentials.json`. No prompt. |
| `mdc secrets pull demo` | Sets a new `opendental_demo_user` password through the demo tunnel, then writes `api/.env_api_demo` and both demo password fields. Asks first. Leaves `etl_pipeline/synthetic_data_generator/.env_demo` alone. |

Anthropic and Airflow admin are listed in the catalog and are manual. The command will not change those files.

After a clinic sync: `mdc status --env clinic --tunnel-db`, then `mdc deploy api --env clinic`.

After a demo rotation: `scripts\deployment\deploy_api_file.ps1 -FilePath api\.env_api_demo -NoRestart`, then restart `dental-clinic-api-demo`. `mdc deploy api` does not accept the demo stage. See [scripts/maintenance/README.md](../../scripts/maintenance/README.md).
