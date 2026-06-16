"""Deploy wrappers (Phase 4.5 / 5.3)."""

from __future__ import annotations

from pathlib import Path

import typer

from mdc_cli.deploy_dbt_docs import deploy_dbt_docs
from mdc_cli.deploy_frontend import deploy_frontend_target
from mdc_cli.paths import REPO_ROOT
from mdc_cli.ps_invoke import invoke_ps_script_file
from mdc_cli.stages import require_api_stage, require_dbt_stage

deploy_app = typer.Typer(help="Deployment commands")


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
def deploy_frontend(
    target: str = typer.Option(
        "demo",
        "--target",
        help="Frontend target: demo (portfolio) or clinic (IP-restricted)",
    ),
) -> None:
    """Build and deploy frontend to S3/CloudFront."""
    if target not in ("demo", "clinic"):
        typer.echo("--target must be demo or clinic.", err=True)
        raise typer.Exit(code=2)
    code = deploy_frontend_target(target)
    raise typer.Exit(code=code)


@deploy_app.command("dbt-docs")
def deploy_dbt_docs_cmd(
    env: str = typer.Option(
        "local",
        "--env",
        help="dbt stage for docs generate when target/ is missing",
    ),
    skip_generate: bool = typer.Option(
        False,
        "--skip-generate",
        help="Deploy existing dbt_dental_models/target only (no dbt docs generate)",
    ),
) -> None:
    """Upload dbt docs to demo S3 bucket under dbt-docs/ and invalidate CloudFront."""
    require_dbt_stage(env)
    code = deploy_dbt_docs(env, skip_generate=skip_generate)
    raise typer.Exit(code=code)
