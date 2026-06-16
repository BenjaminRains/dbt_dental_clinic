"""API commands."""

from __future__ import annotations

from typing import Optional

import typer

from mdc_cli.env import validate_api_stage
from mdc_cli.output import finish_validation
from mdc_cli.paths import api_env_file
from mdc_cli.stages import require_api_stage

api_app = typer.Typer(help="API server commands")


def _not_implemented(name: str, phase: str) -> None:
    typer.echo(f"{name} is not implemented yet ({phase}).", err=True)
    raise typer.Exit(code=2)


def _check_api_config(stage: str, *, success_label: str) -> None:
    require_api_stage(stage)
    config_path = api_env_file(stage)
    ok, error = validate_api_stage(stage)
    finish_validation(
        component="api",
        stage=stage,
        config_path=config_path,
        ok=ok,
        error=error,
        success_label=success_label,
    )


@api_app.command("health")
def health(
    env: str = typer.Option(..., "--env", help="Stage: local, demo, clinic, or test"),
) -> None:
    """Config health check for a stage (pydantic settings load; no HTTP in Phase 4.2)."""
    _check_api_config(env, success_label="healthy")


@api_app.command("test-config")
def test_config(
    env: str = typer.Option(..., "--env", help="Stage: local, demo, clinic, or test"),
) -> None:
    """Validate API pydantic settings for a stage."""
    _check_api_config(env, success_label="ok")


@api_app.command("run")
def run(
    env: str = typer.Option(..., "--env", help="Stage: local, demo, clinic, or test"),
    host: Optional[str] = typer.Option(None, help="Override API host"),
    port: Optional[int] = typer.Option(None, help="Override API port"),
) -> None:
    """Run uvicorn with injected env (Phase 4.3)."""
    _not_implemented("mdc api run", "Phase 4.3")
