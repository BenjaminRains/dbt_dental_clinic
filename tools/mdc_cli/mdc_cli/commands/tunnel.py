"""Infrastructure wrappers (stubs until Phase 4.x)."""

from __future__ import annotations

import typer

tunnel_app = typer.Typer(help="SSM port-forward wrappers (PowerShell)")


def _not_implemented(name: str, phase: str) -> None:
    typer.echo(f"{name} is not implemented yet ({phase}).", err=True)
    raise typer.Exit(code=2)


@tunnel_app.command("clinic-db")
def clinic_db() -> None:
    """Open clinic DB tunnel via existing PowerShell script."""
    _not_implemented("mdc tunnel clinic-db", "Phase 4.x")


@tunnel_app.command("close")
def close() -> None:
    """Close active tunnels."""
    _not_implemented("mdc tunnel close", "Phase 4.x")
