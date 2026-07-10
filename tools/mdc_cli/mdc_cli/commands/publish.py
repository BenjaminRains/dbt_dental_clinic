"""Publish local analytics schemas to clinic RDS (Phase 3.5b)."""

from __future__ import annotations

import typer

from mdc_cli.paths import REPO_ROOT
from mdc_cli.ps_invoke import invoke_ps_script_file
from mdc_cli.run_helper import is_local_tcp_port_open, load_env_dict_isolated
from mdc_cli.secrets_manager import (
    CLINIC_RDS_PASSWORD_ENV,
    resolve_clinic_rds_password,
)
from mdc_cli.ssm import ClinicDbTunnelError, managed_clinic_db_tunnel
from mdc_cli.stages import require_api_stage

publish_app = typer.Typer(help="Publish local marts schema to clinic RDS")

WORKFLOW_DOC = "docs/deployment/CLINIC_ANALYTICS_WORKFLOW.md"


def _echo_tunnel_missing(tunnel_port: int) -> None:
    typer.echo(
        f"\nClinic RDS is in a private VPC. Publish needs an SSM tunnel on "
        f"127.0.0.1:{tunnel_port} — nothing is listening there yet.\n",
        err=True,
    )
    typer.echo("In a separate terminal (leave it open):", err=True)
    typer.echo("  mdc tunnel clinic-db", err=True)
    typer.echo(
        "\nWait until you see:  Port 5433 opened ... Waiting for connections...",
        err=True,
    )
    typer.echo("Then rerun:", err=True)
    typer.echo(f"  mdc publish analytics --env clinic --tunnel-port {tunnel_port}", err=True)
    typer.echo(f"\nFull workflow: {WORKFLOW_DOC}", err=True)


@publish_app.command("analytics")
def publish_analytics(
    env: str = typer.Option(..., "--env", help="Stage: clinic (RDS publish)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preflight only; no dump/restore"),
    use_direct_rds: bool = typer.Option(
        False,
        "--use-direct-rds",
        help="Connect to RDS endpoint from env (skip localhost tunnel port)",
    ),
    tunnel_port: int = typer.Option(5433, "--tunnel-port", help="Local tunnel port when not direct"),
    skip_tunnel_check: bool = typer.Option(
        False,
        "--skip-tunnel-check",
        help="Skip localhost tunnel preflight (script still checks before restore)",
    ),
    ensure_tunnel: bool = typer.Option(
        False,
        "--ensure-tunnel",
        help="Start background SSM tunnel if localhost port is closed (for Airflow / unattended publish)",
    ),
    use_env_file: bool = typer.Option(
        False,
        "--use-env-file",
        help="Use password from api/.env_api_clinic only (skip Secrets Manager live fetch)",
    ),
) -> None:
    """Copy local marts schema to clinic RDS via pg_dump + pg_restore."""
    require_api_stage(env)
    if env != "clinic":
        typer.echo("mdc publish analytics currently supports --env clinic only.", err=True)
        raise typer.Exit(code=2)

    script = REPO_ROOT / "scripts" / "publish" / "publish_analytics_to_rds.ps1"
    if not script.exists():
        typer.echo(f"Publish script not found: {script.relative_to(REPO_ROOT)}", err=True)
        raise typer.Exit(code=2)

    typer.echo(f"PUBLISH  analytics  clinic  -> {script.relative_to(REPO_ROOT)}")
    if use_direct_rds:
        typer.echo("  Mode: direct RDS endpoint from api/.env_api_clinic")
    else:
        typer.echo(f"  Mode: local tunnel 127.0.0.1:{tunnel_port} (mdc tunnel clinic-db)")
        if ensure_tunnel:
            typer.echo("  Tunnel: --ensure-tunnel (start background SSM session if port closed)")
        elif not skip_tunnel_check:
            if is_local_tcp_port_open("127.0.0.1", tunnel_port):
                typer.echo(f"  Tunnel OK: 127.0.0.1:{tunnel_port}")
            else:
                _echo_tunnel_missing(tunnel_port)
                raise typer.Exit(code=1)

    extra_env: dict[str, str] = {}
    try:
        base_env = load_env_dict_isolated("api", "clinic")
        file_password = (base_env.get("POSTGRES_ANALYTICS_PASSWORD") or "").strip() or None
        resolution = resolve_clinic_rds_password(
            file_password,
            prefer_secrets_manager=not use_env_file,
        )
        extra_env[CLINIC_RDS_PASSWORD_ENV] = resolution.password
        typer.echo(f"  RDS password: {resolution.source}")
        if resolution.warning:
            typer.echo(f"  Warning: {resolution.warning}", err=True)
    except Exception as exc:
        typer.echo(f"Failed to resolve clinic RDS password: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    args: list[str] = ["-TunnelPort", str(tunnel_port)]
    if dry_run:
        args.append("-DryRun")
    if use_direct_rds:
        args.append("-UseDirectRds")

    def _run_publish() -> int:
        return invoke_ps_script_file(script, args, extra_env=extra_env)

    if use_direct_rds or skip_tunnel_check:
        raise typer.Exit(code=_run_publish())

    if ensure_tunnel:
        try:
            with managed_clinic_db_tunnel(
                local_port=str(tunnel_port),
                wait_timeout_seconds=120.0,
            ) as source:
                typer.echo(f"  Tunnel ready ({source}) on 127.0.0.1:{tunnel_port}")
                raise typer.Exit(code=_run_publish())
        except ClinicDbTunnelError as exc:
            typer.echo(f"Failed to start clinic RDS tunnel: {exc}", err=True)
            typer.echo(
                "\nManual fallback: open a terminal and run  mdc tunnel clinic-db  "
                "(keep it open), then rerun publish.",
                err=True,
            )
            raise typer.Exit(code=1) from exc

    raise typer.Exit(code=_run_publish())
