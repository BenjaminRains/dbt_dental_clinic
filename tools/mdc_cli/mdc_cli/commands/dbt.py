"""dbt commands."""

from __future__ import annotations

import typer

from mdc_cli.dbt_env import dbt_config_path, validate_dbt_stage
from mdc_cli.output import finish_validation
from mdc_cli.stages import require_dbt_stage

dbt_app = typer.Typer(help="dbt project commands")


def _not_implemented(name: str, phase: str) -> None:
    typer.echo(f"{name} is not implemented yet ({phase}).", err=True)
    raise typer.Exit(code=2)


@dbt_app.command("validate")
def validate(
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or demo"),
) -> None:
    """Validate dbt connection env for a stage (profiles.yml env_var sources)."""
    require_dbt_stage(env)
    ok, error = validate_dbt_stage(env)
    finish_validation(
        component="dbt",
        stage=env,
        config_path=dbt_config_path(env),
        ok=ok,
        error=error,
    )


@dbt_app.command("run")
def run(
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or demo"),
) -> None:
    """Run dbt with injected env (Phase 4.3)."""
    _not_implemented("mdc dbt run", "Phase 4.3")


@dbt_app.command("test")
def test(
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or demo"),
) -> None:
    """Run dbt test (Phase 4.3)."""
    _not_implemented("mdc dbt test", "Phase 4.3")


@dbt_app.command("docs")
def docs(
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or demo"),
) -> None:
    """Generate or serve dbt docs (Phase 4.3)."""
    _not_implemented("mdc dbt docs", "Phase 4.3")
