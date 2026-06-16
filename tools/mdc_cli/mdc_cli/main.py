"""mdc CLI root application."""

from __future__ import annotations

import typer

from mdc_cli import __version__
from mdc_cli.commands import api, dbt, etl, status, tunnel

app = typer.Typer(
    name="mdc",
    help="Dental clinic monorepo dev CLI — stateless orchestration over pydantic settings.",
    no_args_is_help=True,
)

app.command("status")(status.status)
app.add_typer(api.api_app, name="api")
app.add_typer(etl.etl_app, name="etl")
app.add_typer(dbt.dbt_app, name="dbt")
app.add_typer(tunnel.tunnel_app, name="tunnel")


@app.command("version")
def version_cmd() -> None:
    """Show mdc version."""
    typer.echo(f"mdc {__version__}")
