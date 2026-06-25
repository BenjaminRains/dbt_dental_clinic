"""Sync rotating clinic credentials from AWS Secrets Manager."""

from __future__ import annotations

import typer

from mdc_cli.paths import API_DIR, REPO_ROOT
from mdc_cli.secrets_manager import (
    clinic_analytics_secret_region,
    clinic_rds_master_secret_id,
    sync_clinic_env_from_secrets,
)

secrets_app = typer.Typer(
    help="Pull rotating credentials from AWS Secrets Manager into local env files",
)


@secrets_app.command("pull")
def secrets_pull(
    target: str = typer.Argument(
        "clinic",
        help="Target to sync (clinic = opendental_analytics on RDS)",
    ),
    update_deployment_credentials: bool = typer.Option(
        True,
        "--update-deployment-credentials/--no-update-deployment-credentials",
        help="Also update deployment_credentials.json clinic RDS fields (default: on)",
    ),
) -> None:
    """
    Fetch the current clinic analytics DB password from the RDS master user secret
    (rds!db-...) and write it to api/.env_api_clinic.

    Use when rotation is enabled (default every 7 days) and local publish/status fails
    with password authentication errors.
    """
    if target != "clinic":
        typer.echo("Only 'clinic' is supported today.", err=True)
        raise typer.Exit(code=2)

    api_env = API_DIR / ".env_api_clinic"
    try:
        master_secret_id = clinic_rds_master_secret_id()
    except RuntimeError as exc:
        typer.echo(f"Failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(
        f"SECRETS  pull clinic  secret={master_secret_id}  "
        f"region={clinic_analytics_secret_region()}"
    )
    typer.echo(f"  -> {api_env.relative_to(REPO_ROOT)}")

    try:
        result = sync_clinic_env_from_secrets(
            api_env_file=api_env,
            update_deployment_credentials=update_deployment_credentials,
        )
    except Exception as exc:
        typer.echo(f"Failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo("Updated POSTGRES_ANALYTICS_* from live RDS credentials (password not printed).")
    if result.rds_master_secret_id:
        typer.echo(f"  Live source: RDS master secret ({result.rds_master_secret_id}).")
    if result.repaired_json_password:
        typer.echo("  Repaired POSTGRES_ANALYTICS_PASSWORD (was a full JSON secret blob).")
    if not result.api_env_changed:
        typer.echo("  api/.env_api_clinic already matched live RDS password.")
    if update_deployment_credentials:
        if result.deployment_credentials_changed:
            typer.echo("  Updated deployment_credentials.json clinic RDS fields.")
        else:
            typer.echo("  deployment_credentials.json already matched live RDS password.")
    typer.echo(
        f"\nNext: mdc status --env clinic --tunnel-db   then   mdc deploy api --env clinic"
    )
