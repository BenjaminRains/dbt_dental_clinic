"""API commands (stubs until Phase 4.2–4.3)."""

from __future__ import annotations

from typing import Optional

import typer

api_app = typer.Typer(help="API server commands")


def _not_implemented(name: str, phase: str) -> None:
    typer.echo(f"{name} is not implemented yet ({phase}).", err=True)
    raise typer.Exit(code=2)


@api_app.command("health")
def health(
    env: str = typer.Option(..., "--env", help="Stage: local, demo, clinic, or test"),
) -> None:
    """Check API config for a stage (Phase 4.2)."""
    _not_implemented("mdc api health", "Phase 4.2")


@api_app.command("test-config")
def test_config(
    env: str = typer.Option(..., "--env", help="Stage: local, demo, clinic, or test"),
) -> None:
    """Validate API pydantic settings for a stage (Phase 4.2)."""
    _not_implemented("mdc api test-config", "Phase 4.2")


@api_app.command("run")
def run(
    env: str = typer.Option(..., "--env", help="Stage: local, demo, clinic, or test"),
    host: Optional[str] = typer.Option(None, help="Override API host"),
    port: Optional[int] = typer.Option(None, help="Override API port"),
) -> None:
    """Run uvicorn with injected env (Phase 4.3)."""
    _not_implemented("mdc api run", "Phase 4.3")
