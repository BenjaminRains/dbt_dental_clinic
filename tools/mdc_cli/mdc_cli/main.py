"""mdc CLI root application."""

from __future__ import annotations

import typer

from mdc_cli import __version__
from mdc_cli.commands import (
    airflow,
    api,
    consult_audio,
    dbt,
    deploy,
    etl,
    frontend,
    publish,
    secrets,
    ssm,
    status,
    tunnel,
)

app = typer.Typer(
    name="mdc",
    help="Dental clinic monorepo dev CLI — stateless orchestration over pydantic settings.",
    no_args_is_help=True,
)

app.command("status")(status.status)
app.add_typer(api.api_app, name="api")
app.add_typer(etl.etl_app, name="etl")
app.add_typer(dbt.dbt_app, name="dbt")
app.add_typer(deploy.deploy_app, name="deploy")
app.add_typer(publish.publish_app, name="publish")
app.add_typer(frontend.frontend_app, name="frontend")
app.add_typer(consult_audio.consult_audio_app, name="consult-audio")
app.add_typer(ssm.ssm_app, name="ssm")
app.add_typer(tunnel.tunnel_app, name="tunnel")
app.add_typer(secrets.secrets_app, name="secrets")
app.add_typer(airflow.airflow_app, name="airflow")


@app.command("version")
def version_cmd() -> None:
    """Show mdc version."""
    typer.echo(f"mdc {__version__}")
