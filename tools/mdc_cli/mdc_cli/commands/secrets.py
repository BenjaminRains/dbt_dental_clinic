"""Sync clinic credentials and rotate the demo Postgres role."""

from __future__ import annotations

import sys
from typing import Optional

import typer

from mdc_cli.paths import API_DIR, REPO_ROOT
from mdc_cli.secrets_catalog import (
    SecretSelectionError,
    format_catalog,
    format_plan,
    parse_selection,
)
from mdc_cli.secrets_manager import (
    clinic_analytics_secret_region,
    clinic_rds_master_secret_id,
    recorded_rotation_dates,
    rotate_demo_postgres_password,
    sync_clinic_env_from_secrets,
)

secrets_app = typer.Typer(
    help="Sync clinic RDS credentials, or rotate the demo Postgres password after confirmation",
)


def _pull_clinic(*, update_deployment_credentials: bool) -> None:
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
    if result.rotated_on:
        typer.echo(f"  Last rotated: {result.rotated_on}.")
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
        "\nNext: mdc status --env clinic --tunnel-db   then   mdc deploy api --env clinic"
    )


def _pull_demo(*, update_deployment_credentials: bool) -> None:
    api_env = API_DIR / ".env_api_demo"
    typer.echo(
        f"SECRETS  rotate demo  -> {api_env.relative_to(REPO_ROOT)} "
        "and deployment_credentials.json"
    )
    try:
        result = rotate_demo_postgres_password(
            api_env_file=api_env,
            update_deployment_credentials=update_deployment_credentials,
        )
    except Exception as exc:
        typer.echo(f"Failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(
        f"Rotated opendental_demo_user on {result.rotated_on} (password not printed)."
    )
    if not result.api_env_changed:
        typer.echo("  api/.env_api_demo already had this password.")
    if update_deployment_credentials:
        if result.deployment_credentials_changed:
            typer.echo("  Updated deployment_credentials.json demo password fields.")
        else:
            typer.echo("  deployment_credentials.json already had this password.")
    typer.echo("\nNext: mdc deploy api --env demo")


def _confirm_plan(keys: list[str]) -> bool:
    typer.echo(format_plan(keys, recorded_rotation_dates()))
    return typer.confirm("Apply this plan?", default=False)


def _run_selected(keys: list[str], *, update_deployment_credentials: bool) -> None:
    if not _confirm_plan(keys):
        typer.echo("Nothing was changed.")
        raise typer.Exit(code=0)
    for key in keys:
        if key == "clinic":
            _pull_clinic(update_deployment_credentials=update_deployment_credentials)
        elif key == "demo":
            _pull_demo(update_deployment_credentials=update_deployment_credentials)


@secrets_app.command("pull")
def secrets_pull(
    target: Optional[str] = typer.Argument(
        None,
        help="clinic (sync, no prompt) or demo (rotate after confirmation). Omit to choose from a list.",
    ),
    update_deployment_credentials: bool = typer.Option(
        True,
        "--update-deployment-credentials/--no-update-deployment-credentials",
        help="Also update deployment_credentials.json (default: on)",
    ),
) -> None:
    """
    With no target, list credentials and wait for a choice.

    ``clinic`` copies the live RDS master secret into local files and does not prompt.
    ``demo`` rotates the demo Postgres role only after a second confirmation.
    """
    if target is None:
        typer.echo(format_catalog(recorded_rotation_dates()))
        if not sys.stdin.isatty():
            typer.echo(
                "\nNo target given and stdin is not a terminal. Nothing was changed."
            )
            typer.echo("Non-interactive clinic sync: mdc secrets pull clinic")
            raise typer.Exit(code=0)
        raw = typer.prompt(
            "Selection",
            default="",
            show_default=False,
        )
        try:
            keys = parse_selection(raw)
        except SecretSelectionError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=2) from exc
        if not keys:
            typer.echo("Nothing was changed.")
            raise typer.Exit(code=0)
        _run_selected(keys, update_deployment_credentials=update_deployment_credentials)
        return

    normalized = target.strip().lower()
    if normalized == "clinic":
        _pull_clinic(update_deployment_credentials=update_deployment_credentials)
        return
    if normalized == "demo":
        _run_selected(["demo"], update_deployment_credentials=update_deployment_credentials)
        return

    typer.echo("Target must be clinic or demo. Omit it to choose from the list.", err=True)
    raise typer.Exit(code=2)
