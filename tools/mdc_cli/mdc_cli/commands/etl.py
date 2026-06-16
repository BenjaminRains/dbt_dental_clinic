"""ETL commands (stubs until Phase 4.2–4.3)."""

from __future__ import annotations

from typing import Optional

import typer

from mdc_cli.paths import default_etl_profile

etl_app = typer.Typer(help="ETL pipeline commands")


def _not_implemented(name: str, phase: str) -> None:
    typer.echo(f"{name} is not implemented yet ({phase}).", err=True)
    raise typer.Exit(code=2)


@etl_app.command("validate")
def validate(
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or test"),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="Connection subset: load (repl+analytics) or full (all three)",
    ),
) -> None:
    """Validate ETL pydantic settings for a stage (Phase 4.2)."""
    _not_implemented("mdc etl validate", "Phase 4.2")


@etl_app.command("run")
def run(
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or test"),
    profile: str = typer.Option(
        "full",
        "--profile",
        help="Default full — full pipeline requires source + replication + analytics",
    ),
) -> None:
    """Run ETL pipeline with injected env (Phase 4.3)."""
    _ = default_etl_profile(env)
    _not_implemented("mdc etl run", "Phase 4.3")


@etl_app.command("replicate")
def replicate(
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or test"),
    profile: str = typer.Option("full", "--profile"),
) -> None:
    """Run replication step (Phase 4.3)."""
    _not_implemented("mdc etl replicate", "Phase 4.3")


@etl_app.command("test-connections")
def test_connections(
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or test"),
    profile: str = typer.Option("full", "--profile"),
) -> None:
    """Test database connections (Phase 4.3)."""
    _not_implemented("mdc etl test-connections", "Phase 4.3")
