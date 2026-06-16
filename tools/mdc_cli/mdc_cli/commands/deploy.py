"""Deploy wrappers to existing PowerShell scripts (Phase 4.5)."""

from __future__ import annotations

from pathlib import Path

import typer

from mdc_cli.paths import REPO_ROOT
from mdc_cli.ps_invoke import invoke_ps_function, invoke_ps_script_file
from mdc_cli.stages import require_api_stage

deploy_app = typer.Typer(help="Deployment wrappers (PowerShell)")


@deploy_app.command("api")
def deploy_api(
    env: str = typer.Option(..., "--env", help="Stage: clinic (EC2 deploy)"),
) -> None:
    """Deploy API env to EC2 via deploy_api_file.ps1 when present."""
    require_api_stage(env)
    if env != "clinic":
        typer.echo(
            "mdc deploy api currently supports --env clinic only (EC2 systemd .env).",
            err=True,
        )
        raise typer.Exit(code=2)

    script = REPO_ROOT / "scripts" / "deployment" / "deploy_api_file.ps1"
    env_file = REPO_ROOT / "api" / ".env_api_clinic"
    if not script.exists():
        typer.echo(
            f"Deploy script not found: {script.relative_to(REPO_ROOT)}",
            err=True,
        )
        typer.echo(
            "Use ssm-connect-clinic-api or see docs/DEPLOYMENT_WORKFLOW.md.",
            err=True,
        )
        raise typer.Exit(code=2)
    if not env_file.exists():
        typer.echo(f"Missing env file: {env_file.relative_to(REPO_ROOT)}", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"DEPLOY  api  clinic  -> {script.relative_to(REPO_ROOT)}")
    code = invoke_ps_script_file(
        script,
        ["-FilePath", str(env_file), "-ClinicEnv"],
    )
    raise typer.Exit(code=code)


@deploy_app.command("frontend")
def deploy_frontend() -> None:
    """Deploy frontend via Deploy-Frontend in environment_manager.ps1."""
    typer.echo("DEPLOY  frontend  -> Deploy-Frontend")
    code = invoke_ps_function("Deploy-Frontend")
    raise typer.Exit(code=code)
