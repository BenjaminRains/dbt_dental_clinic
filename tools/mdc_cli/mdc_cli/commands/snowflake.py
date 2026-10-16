"""Snowflake portfolio warehouse helpers (Snowsight UI + connection check)."""

from __future__ import annotations

import webbrowser

import typer

from mdc_cli.dbt_env import load_dbt_env_dict, validate_dbt_stage
from mdc_cli.output import console
from mdc_cli.paths import dbt_env_file
from mdc_cli.snowflake_ui import snowsight_url

snowflake_app = typer.Typer(help="Snowflake portfolio mini warehouse (demo / synthetic only)")


def _load_sf_env() -> dict[str, str]:
    try:
        return load_dbt_env_dict("snowflake")
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from exc


def _ui_url(env: dict[str, str]) -> str:
    try:
        return snowsight_url(
            env.get("SNOWFLAKE_ACCOUNT", ""),
            override=env.get("SNOWFLAKE_UI_URL"),
        )
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from exc


@snowflake_app.command("open")
def open_ui(
    print_only: bool = typer.Option(
        False,
        "--print",
        help="Print the Snowsight URL without opening a browser",
    ),
) -> None:
    """Open Snowsight for the account in dbt_dental_models/.env_snowflake."""
    env = _load_sf_env()
    url = _ui_url(env)
    console.print(url)
    console.print(
        f"  user={env.get('SNOWFLAKE_USER', '')}  "
        f"role={env.get('SNOWFLAKE_ROLE', '')}  "
        f"db={env.get('SNOWFLAKE_DATABASE', '')}  "
        f"wh={env.get('SNOWFLAKE_WAREHOUSE', '')}"
    )
    if print_only:
        raise typer.Exit(code=0)
    opened = webbrowser.open(url)
    if not opened:
        console.print("[red]Could not open a browser; copy the URL above.[/red]")
        raise typer.Exit(code=1)
    console.print(
        "[green]Opened Snowsight[/green] "
        "(sign in with passkey / SSO in the browser)."
    )
    raise typer.Exit(code=0)


@snowflake_app.command("status")
def status(
    check: bool = typer.Option(
        False,
        "--check",
        help="Also validate the dbt Snowflake connection (key-pair)",
    ),
) -> None:
    """Show Snowflake env summary and Snowsight URL."""
    env = _load_sf_env()
    url = _ui_url(env)
    console.print("Snowflake (portfolio / synthetic)")
    console.print(f"  account:   {env.get('SNOWFLAKE_ACCOUNT', '')}")
    console.print(f"  user:      {env.get('SNOWFLAKE_USER', '')}")
    console.print(f"  role:      {env.get('SNOWFLAKE_ROLE', '')}")
    console.print(f"  warehouse: {env.get('SNOWFLAKE_WAREHOUSE', '')}")
    console.print(f"  database:  {env.get('SNOWFLAKE_DATABASE', '')}")
    console.print(f"  schema:    {env.get('SNOWFLAKE_SCHEMA', '')}")
    key = env.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")
    console.print(f"  key path:  {key or '(not set — password auth?)'}")
    console.print(f"  UI:        {url}")
    console.print("  open:      mdc snowflake open")
    console.print(f"  env file:  {dbt_env_file('snowflake')}")

    if not check:
        raise typer.Exit(code=0)

    ok, error = validate_dbt_stage("snowflake")
    if ok:
        console.print("[green]connection: ok[/green]")
        raise typer.Exit(code=0)
    console.print(f"[red]connection: fail — {error}[/red]")
    raise typer.Exit(code=1)
