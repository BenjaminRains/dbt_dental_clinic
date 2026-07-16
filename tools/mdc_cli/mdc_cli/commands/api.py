"""API commands."""

from __future__ import annotations

from typing import Optional

import typer

from mdc_cli.env import validate_api_stage
from mdc_cli.output import finish_validation
from mdc_cli.paths import API_DIR, api_env_file
from mdc_cli.run_helper import (
    apply_tunnel_db_overrides,
    echo_run_banner,
    load_env_dict_isolated,
    require_component_python,
    run_isolated,
    venv_root_from_python,
)
from mdc_cli.stages import require_api_stage

api_app = typer.Typer(help="API server commands")


def _portal_users_source(settings: dict[str, str]) -> str:
    users_file = (settings.get("CLINIC_PORTAL_USERS_FILE") or "").strip()
    if users_file:
        return users_file
    default_users = API_DIR / "clinic-portal-users.json"
    if default_users.exists():
        return str(default_users)
    if (settings.get("CLINIC_PORTAL_USERS") or "").strip():
        return "env:CLINIC_PORTAL_USERS"
    return "none"


def _portal_secret_present(settings: dict[str, str]) -> bool:
    return bool(
        (settings.get("CLINIC_PORTAL_SESSION_SECRET") or "").strip()
        or (settings.get("CLINIC_API_KEY") or "").strip()
    )


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
    host: Optional[str] = typer.Option(None, help="Override API host (default from env file)"),
    port: Optional[int] = typer.Option(None, help="Override API port (default from env file)"),
    reload: Optional[bool] = typer.Option(
        None,
        "--reload/--no-reload",
        help="Enable uvicorn reload (default: on for local only)",
    ),
    tunnel_db: bool = typer.Option(
        False,
        "--tunnel-db",
        help="Route Postgres to localhost (for mdc tunnel clinic-db). Overrides host/port after loading stage env.",
    ),
    tunnel_port: Optional[int] = typer.Option(
        None,
        "--tunnel-port",
        help="Local tunnel port (default: POSTGRES_PORT env or 5433)",
    ),
) -> None:
    """Run uvicorn with isolated injected env (no api-init required)."""
    require_api_stage(env)
    config_path = api_env_file(env)
    ok, error = validate_api_stage(env)
    if not ok:
        finish_validation(
            component="api",
            stage=env,
            config_path=config_path,
            ok=False,
            error=error,
        )

    settings = load_env_dict_isolated("api", env)
    if tunnel_db:
        settings = apply_tunnel_db_overrides(settings, local_port=tunnel_port)

    if env in ("local", "clinic"):
        has_portal_secret = _portal_secret_present(settings)
        users_source = _portal_users_source(settings)
        typer.echo(
            f"Portal auth preflight: portal_secret={'yes' if has_portal_secret else 'no'} users_file={users_source}"
        )
        if not has_portal_secret:
            typer.echo(
                "Refusing to start API: set CLINIC_PORTAL_SESSION_SECRET or CLINIC_API_KEY for portal login.",
                err=True,
            )
            raise typer.Exit(code=2)

    python = require_component_python("api")
    bind_host = host or settings.get("API_HOST", "0.0.0.0")
    bind_port = str(port or settings.get("API_PORT", "8000"))
    use_reload = reload if reload is not None else env == "local"

    cmd = [
        str(python),
        "-m",
        "uvicorn",
        "main:app",
        "--host",
        bind_host,
        "--port",
        bind_port,
    ]
    if use_reload:
        cmd.append("--reload")

    reload_note = "reload" if use_reload else "no-reload"
    tunnel_note = " tunnel-db->127.0.0.1" if tunnel_db else ""
    echo_run_banner(
        "api",
        env,
        config_path,
        f"-> uvicorn {bind_host}:{bind_port} ({reload_note}{tunnel_note})",
    )
    code = run_isolated(
        settings=settings,
        cmd=cmd,
        cwd=API_DIR,
        venv_root=venv_root_from_python(python),
    )
    raise typer.Exit(code=code)
