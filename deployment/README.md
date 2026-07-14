# Deployment ops artifacts

Runnable scripts and SSM one-shots for AWS / demo DB / analysis helpers.

Long-form plans, checklists, and architecture notes live in **[`docs/deployment/`](../docs/deployment/)**. Credential inventory: [`deployment_credentials.json.template`](../deployment_credentials.json.template) (local file is gitignored) and [`docs/deployment/README_CREDENTIALS.md`](../docs/deployment/README_CREDENTIALS.md).

## Layout

| Path | Purpose |
|------|---------|
| [`environment/`](environment/) | Load DB connection env vars from `deployment_credentials.json` |
| [`demo-db/`](demo-db/) | Demo Postgres schema apply, dbt schemas, grants (SSM / on-box) |
| [`generator/`](generator/) | Synthetic data generator SSM runners |
| [`dbt/`](dbt/) | Remote shell helper to run dbt on EC2 against demo DB |
| [`analysis/`](analysis/) | Production volume analysis |
| [`logs/`](logs/) | Local run output |

## Credentials policy

- **Never commit** passwords, private IPs, instance IDs with secrets, or expired S3 presigned URLs.
- Scripts that resolve passwords via **AWS Secrets Manager** keep that path — do not rewrite them to read passwords from `deployment_credentials.json`.
- Hardcoded / local loaders here read **`deployment_credentials.json`** (see template) or env vars set by `aws-ssm-init`.

## Preferred day-to-day tooling

| Task | Prefer |
|------|--------|
| dbt on EC2 | [`scripts/ec2/run_dbt_on_ec2.ps1`](../scripts/ec2/run_dbt_on_ec2.ps1) |
| EC2 dbt env (Secrets Manager / credentials) | [`scripts/ec2/setup_ec2_dbt_env.sh`](../scripts/ec2/setup_ec2_dbt_env.sh) |
| Clinic analytics publish | `mdc` + [`docs/deployment/CLINIC_ANALYTICS_WORKFLOW.md`](../docs/deployment/CLINIC_ANALYTICS_WORKFLOW.md) |
| Local SSM helpers | `aws-ssm-init` from environment manager |

`deployment/dbt/run_dbt_on_ec2.sh` is the older remote shell one-shot for SSM, not the primary local entrypoint.

## Related docs (moved from the old `scripts/` pile)

- [ENVIRONMENT_SETUP.md](../docs/deployment/ENVIRONMENT_SETUP.md)
- [DBT_SCHEMA_SETUP.md](../docs/deployment/DBT_SCHEMA_SETUP.md)
- [CREATE_DBT_SCHEMAS_RUN_COMMAND.md](../docs/deployment/CREATE_DBT_SCHEMAS_RUN_COMMAND.md)
- [SYNTHETIC_GENERATOR_RUN_COMMAND.md](../docs/deployment/SYNTHETIC_GENERATOR_RUN_COMMAND.md)
