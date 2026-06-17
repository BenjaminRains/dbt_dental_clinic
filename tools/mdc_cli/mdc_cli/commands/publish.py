"""Publish local analytics schemas to clinic RDS (Phase 3.5b)."""

from __future__ import annotations

import typer

from mdc_cli.paths import REPO_ROOT
from mdc_cli.ps_invoke import invoke_ps_script_file
from mdc_cli.stages import require_api_stage

publish_app = typer.Typer(help="Publish local analytics data to clinic RDS")


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
) -> None:
    """Copy local marts/int/staging to clinic RDS via pg_dump + pg_restore."""
    require_api_stage(env)
    if env != "clinic":
        typer.echo("mdc publish analytics currently supports --env clinic only.", err=True)
        raise typer.Exit(code=2)

    script = REPO_ROOT / "scripts" / "publish" / "publish_analytics_to_rds.ps1"
    if not script.exists():
        typer.echo(f"Publish script not found: {script.relative_to(REPO_ROOT)}", err=True)
        raise typer.Exit(code=2)

    args: list[str] = ["-TunnelPort", str(tunnel_port)]
    if dry_run:
        args.append("-DryRun")
    if use_direct_rds:
        args.append("-UseDirectRds")

    typer.echo(f"PUBLISH  analytics  clinic  -> {script.relative_to(REPO_ROOT)}")
    if not use_direct_rds:
        typer.echo(f"  Expect tunnel on 127.0.0.1:{tunnel_port} (mdc tunnel clinic-db)")
    code = invoke_ps_script_file(script, args)
    raise typer.Exit(code=code)
