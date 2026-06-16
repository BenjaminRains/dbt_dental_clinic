"""dbt commands (stubs until Phase 4.2b–4.3)."""

from __future__ import annotations

import typer

dbt_app = typer.Typer(help="dbt project commands")


def _not_implemented(name: str, phase: str) -> None:
    typer.echo(f"{name} is not implemented yet ({phase}).", err=True)
    raise typer.Exit(code=2)


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
