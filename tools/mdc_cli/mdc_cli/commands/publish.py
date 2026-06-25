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
from mdc_cli.stages import require_api_stage

publish_app = typer.Typer(help="Publish local marts schema to clinic RDS")

WORKFLOW_DOC = "docs/CLINIC_ANALYTICS_WORKFLOW.md"


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
        if not skip_tunnel_check:
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

    code = invoke_ps_script_file(script, args, extra_env=extra_env)
    raise typer.Exit(code=code)
