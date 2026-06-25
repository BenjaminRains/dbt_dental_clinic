"""mdc status — show config paths, validation, venv discovery, and data freshness."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.text import Text

from mdc_cli.env import validate_component_stage
from mdc_cli.freshness import (
    FreshnessReport,
    collect_freshness_report,
    freshness_stages_for_status,
)
from mdc_cli.secrets_manager import (
    ClinicCredentialSyncReport,
    check_clinic_credential_sync,
)
from mdc_cli.paths import (
    REPO_ROOT,
    ALL_MDC_STAGES,
    ComponentStage,
    discover_component_python,
    iter_status_targets,
)

console = Console()


def _relative_config_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _validation_cell(component: str, target: ComponentStage) -> tuple[str, str]:
    ok, err = validate_component_stage(
        component,
        target.stage,
        profile=target.profile,
    )
    if ok:
        return "ok", ""
    message = (err or "validation failed").splitlines()[0]
    if len(message) > 72:
        message = message[:69] + "..."
    return "fail", message


def render_status_table(env_filter: Optional[str] = None) -> Table:
    table = Table(title="mdc status", show_lines=False)
    table.add_column("Component", style="cyan", no_wrap=True)
    table.add_column("Stage", no_wrap=True)
    table.add_column("Config", overflow="fold")
    table.add_column("File", no_wrap=True)
    table.add_column("Profile", no_wrap=True)
    table.add_column("Valid", no_wrap=True)
    table.add_column("Venv python", overflow="fold")
    table.add_column("Notes", overflow="fold")

    for target in iter_status_targets(env_filter):
        config_display = _relative_config_path(target.config_path)
        file_status = "yes" if target.config_path.exists() else "no"
        valid_status, note = _validation_cell(target.component, target)
        venv_python = discover_component_python(target.component)
        venv_display = _relative_config_path(venv_python) if venv_python else "-"

        table.add_row(
            target.component,
            target.stage,
            config_display,
            file_status,
            target.profile or "-",
            valid_status,
            venv_display,
            note,
        )

    return table


def _format_age(hours: Optional[float]) -> str:
    from mdc_cli.freshness import format_freshness_age

    return format_freshness_age(hours, is_future=bool(hours is not None and hours < 0))


def _format_latest(value: Optional[datetime]) -> str:
    if value is None:
        return "-"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _status_style(status: str) -> str:
    if status == "ok":
        return "[green]ok[/green]"
    if status == "warn":
        return "[yellow]warn[/yellow]"
    if status == "stale":
        return "[red]stale[/red]"
    if status == "n/a":
        return "[dim]n/a[/dim]"
    return status


def render_freshness_table(report: FreshnessReport) -> Table:
    title = f"Data freshness ({report.stage})"
    table = Table(title=title, show_lines=False)
    table.add_column("Probe", style="cyan", no_wrap=True)
    table.add_column("Latest", no_wrap=True)
    table.add_column("Age", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Note", overflow="fold")

    if report.error:
        table.add_row("-", "-", "-", "[red]error[/red]", report.error)
        if report.hint:
            table.add_row("-", "-", "-", "[dim]hint[/dim]", report.hint)
        return table

    if not report.rows:
        table.add_row("-", "-", "-", "[yellow]empty[/yellow]", "No freshness probes returned rows")
        return table

    for row in report.rows:
        table.add_row(
            Text(row.probe, style="cyan"),
            _format_latest(row.latest_at),
            _format_age(row.age_hours),
            _status_style(row.status),
            row.note,
        )
    return table


def _format_secret_label(secret_id: str) -> str:
    """Short label for status output (Rich treats :secret: in ARNs as emoji)."""
    if ":secret:" in secret_id:
        return secret_id.split(":secret:", 1)[1]
    return secret_id


def render_credential_sync_table(report: ClinicCredentialSyncReport) -> Table:
    title = (
        f"Clinic RDS credential sync (live: {_format_secret_label(report.secret_id)})"
    )
    table = Table(title=title, show_lines=False)
    table.add_column("Target", style="cyan", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Note", overflow="fold")

    if report.error:
        table.add_row("-", "[red]error[/red]", report.error)
        return table

    for row in report.rows:
        table.add_row(
            Text(row.target, style="cyan"),
            _status_style(row.status),
            row.note,
        )
    return table


def _print_credential_sync_report() -> ClinicCredentialSyncReport:
    report = check_clinic_credential_sync()
    console.print()
    console.print(render_credential_sync_table(report))
    if report.secrets_manager_ok:
        console.print(
            f"[dim]Live password: RDS master secret ({report.region}). "
            "Publish/status use live SM password by default; sync local files with "
            "mdc secrets pull clinic.[/dim]"
        )
    return report


def _print_freshness_reports(
    stages: list[str],
    *,
    tunnel_db: bool,
    tunnel_port: int,
    prefer_secrets_manager: bool,
    credential_report: Optional[ClinicCredentialSyncReport] = None,
) -> None:
    if not stages:
        return

    console.print()
    for stage in stages:
        report = collect_freshness_report(
            stage,
            tunnel_db=tunnel_db,
            tunnel_port=tunnel_port,
            prefer_secrets_manager=prefer_secrets_manager,
            api_password_malformed=bool(credential_report and credential_report.api_password_malformed),
            api_password_in_sync=credential_report.api_password_in_sync if credential_report else None,
        )
        console.print(render_freshness_table(report))
        if report.database_label and not report.error:
            console.print(f"[dim]Database: {report.database_label}[/dim]")
        if report.password_source:
            console.print(f"[dim]RDS password: {report.password_source}[/dim]")
        if report.password_warning:
            console.print(f"[yellow]{report.password_warning}[/yellow]")
        if report.hint and not report.error:
            console.print(f"[dim]{report.hint}[/dim]")

    console.print(
        "\n[dim]Freshness thresholds: ETL loads warn >36h; mart refresh warn >48h; "
        "business dates warn >72h. Skip with --no-freshness. "
        "Clinic RDS: mdc tunnel clinic-db then --env clinic --tunnel-db. "
        "Rotating RDS password: live fetch from Secrets Manager (mdc secrets pull clinic to update .env).[/dim]"
    )


def status(
    env: Optional[str] = typer.Option(
        None,
        "--env",
        help="Filter to one stage (e.g. local, clinic, test, demo)",
    ),
    no_freshness: bool = typer.Option(
        False,
        "--no-freshness",
        help="Skip analytics database freshness probes",
    ),
    tunnel_db: bool = typer.Option(
        False,
        "--tunnel-db",
        help="Probe clinic RDS via localhost tunnel (mdc tunnel clinic-db)",
    ),
    tunnel_port: int = typer.Option(
        5433,
        "--tunnel-port",
        help="Local tunnel port when using --tunnel-db",
    ),
    use_env_file: bool = typer.Option(
        False,
        "--use-env-file",
        help="Use POSTGRES_ANALYTICS_PASSWORD from api/.env_api_clinic only (skip Secrets Manager live fetch)",
    ),
    no_secrets_check: bool = typer.Option(
        False,
        "--no-secrets-check",
        help="Skip comparing local env files to AWS Secrets Manager",
    ),
) -> None:
    """Show config files, validation, venv paths, and analytics data freshness."""
    if env:
        if env not in ALL_MDC_STAGES:
            raise typer.BadParameter(
                f"Unsupported stage '{env}'. Expected one of: {list(ALL_MDC_STAGES)}"
            )

    console.print(render_status_table(env_filter=env))

    credential_report: Optional[ClinicCredentialSyncReport] = None
    if not no_secrets_check:
        credential_report = _print_credential_sync_report()

    if not no_freshness:
        freshness_stages = freshness_stages_for_status(env)
        _print_freshness_reports(
            freshness_stages,
            tunnel_db=tunnel_db,
            tunnel_port=tunnel_port,
            prefer_secrets_manager=not use_env_file,
            credential_report=credential_report,
        )

    console.print(
        "\n[dim]Configuration authority: api/settings.py, "
        "etl_pipeline/.../settings_v2.py[/dim]"
    )
