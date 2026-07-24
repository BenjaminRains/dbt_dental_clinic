"""Analytics warehouse freshness probes for mdc status."""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from mdc_cli.dbt_env import LOCAL_CLINIC_REQUIRED, load_dbt_env_dict
from mdc_cli.paths import API_DIR
from mdc_cli.secrets_manager import overlay_clinic_rds_credentials
from mdc_cli.run_helper import apply_tunnel_db_overrides, is_local_tcp_port_open, load_env_dict_isolated

# Kept for tests / documentation of ETL table names.
ETL_TABLE_PROBES: tuple[str, ...] = ("payment", "appointment", "procedurelog")

ETL_LOAD_STATUS_SQL = (
    "SELECT MAX(_loaded_at)::text FROM raw.etl_load_status "
    "WHERE table_name = '{table}' AND load_status = 'success'"
)


@dataclass(frozen=True)
class FreshnessProbe:
    name: str
    sql: str
    stages: frozenset[str]
    mart_table: Optional[str] = None
    mart_ts_column: str = "_mart_refreshed_at"
    mart_fallback_column: Optional[str] = None
    business_date: bool = False


FRESHNESS_PROBE_DEFS: tuple[FreshnessProbe, ...] = (
    FreshnessProbe(
        name="etl:payment",
        sql=ETL_LOAD_STATUS_SQL.format(table="payment"),
        stages=frozenset({"local"}),
    ),
    FreshnessProbe(
        name="etl:appointment",
        sql=ETL_LOAD_STATUS_SQL.format(table="appointment"),
        stages=frozenset({"local"}),
    ),
    FreshnessProbe(
        name="etl:procedurelog",
        sql=ETL_LOAD_STATUS_SQL.format(table="procedurelog"),
        stages=frozenset({"local"}),
    ),
    FreshnessProbe(
        name="mart:mart_provider_performance",
        sql="SELECT MAX(_mart_refreshed_at)::text FROM marts.mart_provider_performance",
        stages=frozenset({"local", "clinic"}),
        mart_table="mart_provider_performance",
    ),
    FreshnessProbe(
        name="mart:mart_daily_payments",
        sql="SELECT MAX(_mart_refreshed_at)::text FROM marts.mart_daily_payments",
        stages=frozenset({"local", "clinic"}),
        mart_table="mart_daily_payments",
        mart_fallback_column="payment_date",
    ),
    FreshnessProbe(
        name="business:latest_payment_date",
        sql="SELECT MAX(payment_date)::text FROM marts.mart_daily_payments "
        "WHERE payment_date <= CURRENT_DATE",
        stages=frozenset({"local", "clinic"}),
        business_date=True,
    ),
    FreshnessProbe(
        name="business:latest_production_date",
        sql="SELECT MAX(production_date)::text FROM marts.mart_provider_performance "
        "WHERE production_date <= CURRENT_DATE",
        stages=frozenset({"local", "clinic"}),
        business_date=True,
    ),
)

# Legacy export for tests that import FRESHNESS_PROBES
FRESHNESS_PROBES: tuple[tuple[str, str], ...] = tuple(
    (probe.name, probe.sql) for probe in FRESHNESS_PROBE_DEFS
)


@dataclass(frozen=True)
class FreshnessRow:
    probe: str
    latest_at: Optional[datetime]
    age_hours: Optional[float]
    status: str
    note: str = ""


@dataclass(frozen=True)
class FreshnessReport:
    stage: str
    database_label: str
    rows: list[FreshnessRow]
    error: Optional[str] = None
    hint: Optional[str] = None
    password_source: Optional[str] = None
    password_warning: Optional[str] = None


def probes_for_stage(stage: str) -> tuple[FreshnessProbe, ...]:
    return tuple(p for p in FRESHNESS_PROBE_DEFS if stage in p.stages)


def clinic_etl_skipped_rows() -> list[FreshnessRow]:
    """Clinic RDS receives marts-only publish; ETL tracking lives on local warehouse."""
    return [
        FreshnessRow(
            probe=f"etl:{table}",
            latest_at=None,
            age_hours=None,
            status="n/a",
            note="skipped on clinic RDS (marts-only publish; check local stage for ETL loads)",
        )
        for table in ETL_TABLE_PROBES
    ]


def _parse_timestamp(value: str) -> Optional[datetime]:
    text = (value or "").strip()
    if not text:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
    ):
        try:
            parsed = datetime.strptime(text.replace("+00", "+0000"), fmt)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _age_hours(latest: datetime, *, now: Optional[datetime] = None) -> float:
    """Hours since latest timestamp (negative if latest is in the future)."""
    reference = now or datetime.now(timezone.utc)
    return (reference - latest).total_seconds() / 3600.0


def _classify_probe(probe: str, age_hours: float, *, is_future: bool = False) -> tuple[str, str]:
    if is_future or age_hours < 0:
        if probe.startswith("business:"):
            days_ahead = abs(age_hours) / 24.0
            return (
                "warn",
                f"max business date is {days_ahead:.1f}d in the future — check source / mart data quality",
            )
        return "warn", "future timestamp — check source / mart data quality"

    if probe.startswith("etl:"):
        if age_hours <= 36:
            return "ok", ""
        if age_hours <= 72:
            return "warn", "ETL load older than 36h"
        return "stale", "ETL load older than 72h — run mdc etl run"

    if probe.startswith("mart:"):
        if age_hours <= 48:
            return "ok", ""
        if age_hours <= 168:
            return "warn", "Mart refresh older than 48h — run dbt + publish for clinic RDS"
        return "stale", "Mart refresh older than 7d"

    if probe.startswith("business:"):
        # Age = calendar lag: how long since the latest business date (date-only → start of that UTC day).
        if age_hours <= 72:
            return "ok", ""
        if age_hours <= 120:
            return "warn", "Latest business date is several days behind"
        return "stale", "Business dates look very stale"

    if age_hours <= 48:
        return "ok", ""
    return "warn", ""


def _business_date_age_hours(latest: datetime, *, now: Optional[datetime] = None) -> tuple[float, bool]:
    """
    Return (age_hours, is_future) for business-date probes.

    Business dates are calendar days; compare using UTC midnight of latest vs today so
    "1.9d" means ~2 calendar days since the last payment/production day, not wall-clock
    time since midnight on that date.
    """
    reference = now or datetime.now(timezone.utc)
    latest_day = latest.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    today = reference.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    delta_hours = (today - latest_day).total_seconds() / 3600.0
    return delta_hours, delta_hours < 0


def _probe_age_hours(probe: str, latest: datetime, *, now: Optional[datetime] = None) -> tuple[float, bool]:
    if probe.startswith("business:"):
        return _business_date_age_hours(latest, now=now)
    age = _age_hours(latest, now=now)
    return age, age < 0


def format_freshness_age(hours: Optional[float], *, is_future: bool = False) -> str:
    """Display helper shared with mdc status (negative ages → 'Xd ahead')."""
    if hours is None:
        return "-"
    if is_future or hours < 0:
        ahead = abs(hours)
        if ahead < 24:
            return f"{ahead:.1f}h ahead"
        return f"{ahead / 24:.1f}d ahead"
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


def _auth_failure_hint(
    stage: str,
    *,
    password_source: Optional[str] = None,
    api_password_malformed: bool = False,
    api_password_in_sync: Optional[bool] = None,
) -> Optional[str]:
    if stage != "clinic":
        return None
    if api_password_malformed:
        return (
            "POSTGRES_ANALYTICS_PASSWORD in api/.env_api_clinic contains the full "
            "Secrets Manager JSON blob, not just the password. "
            "Run: pip install -e tools/mdc_cli   then   mdc secrets pull clinic"
        )
    if password_source and password_source.startswith("secrets_manager"):
        if "rds!db" in (password_source or ""):
            return (
                "RDS rejected the live RDS master secret password. "
                "Check RDS rotation errors in AWS Console (Secrets Manager → rds!db-...). "
                "Tunnel must be running: mdc tunnel clinic-db"
            )
        return (
            "RDS rejected the live Secrets Manager password (not the stale .env copy). "
            "Run: mdc secrets pull clinic   then   mdc deploy api --env clinic. "
            "Keep `mdc tunnel clinic-db` open in another terminal."
        )
    if api_password_in_sync is False:
        return (
            "RDS password rejected. Local files differ from Secrets Manager. "
            "Run: pip install -e tools/mdc_cli   then   mdc secrets pull clinic"
        )
    return (
        "RDS password rejected. Clinic RDS uses Secrets Manager rotation (~7 days). "
        "Run: mdc secrets pull clinic   (or rely on live fetch — default for status/publish). "
        "Offline fallback: mdc secrets pull clinic then retry."
    )


def load_freshness_env_dict(
    stage: str,
    *,
    prefer_secrets_manager: bool = True,
) -> tuple[dict[str, str], Optional[str], Optional[str]]:
    """
    Return (env, password_source, password_warning).

    Local warehouse uses dbt env. Clinic RDS uses api/.env_api_clinic plus live
    Secrets Manager password by default (rotation-safe).
    """
    if stage == "local":
        return load_dbt_env_dict("local"), None, None

    api_env_file = API_DIR / ".env_api_clinic"
    if api_env_file.exists():
        env = load_env_dict_isolated("api", "clinic")
        missing = [key for key in LOCAL_CLINIC_REQUIRED if not (env.get(key) or "").strip()]
        if missing and not prefer_secrets_manager:
            raise ValueError(
                f"Missing {', '.join(missing)} in {api_env_file} (required for clinic RDS freshness)."
            )
        if stage == "clinic" and prefer_secrets_manager:
            env, resolution = overlay_clinic_rds_credentials(env, prefer_secrets_manager=True)
            return env, resolution.source, resolution.warning
        if missing:
            raise ValueError(
                f"Missing {', '.join(missing)} in {api_env_file} (required for clinic RDS freshness)."
            )
        return env, "env_file", None

    env = load_dbt_env_dict("clinic")
    if stage == "clinic" and prefer_secrets_manager:
        env, resolution = overlay_clinic_rds_credentials(env, prefer_secrets_manager=True)
        return env, resolution.source, resolution.warning
    return env, "deployment_credentials", None


def _find_psql() -> Optional[str]:
    found = shutil.which("psql")
    if found:
        return found
    for candidate in (
        r"C:\Program Files\PostgreSQL\17\bin\psql.exe",
        r"C:\Program Files\PostgreSQL\16\bin\psql.exe",
        r"C:\Program Files\PostgreSQL\15\bin\psql.exe",
    ):
        if os.path.isfile(candidate):
            return candidate
    return None


def _connection_hint(stage: str, host: str, tunnel_db: bool, tunnel_port: int = 5433) -> Optional[str]:
    if stage != "clinic":
        return None
    if tunnel_db:
        return None
    if host in ("127.0.0.1", "localhost"):
        return None
    h = host.lower()
    if h.endswith(".rds.amazonaws.com") or h == "rds.amazonaws.com":
        if not is_local_tcp_port_open("127.0.0.1", tunnel_port):
            return (
                "Clinic RDS is private. Start `mdc tunnel clinic-db`, then rerun "
                "`mdc status --env clinic --tunnel-db`."
            )
        return (
            f"Tunnel port {tunnel_port} is open. Rerun with `--tunnel-db` to probe clinic RDS."
        )
    return None


def _tunnel_probe_hint(tunnel_db: bool, tunnel_port: int, error: str) -> Optional[str]:
    if not tunnel_db:
        return None
    lowered = error.lower()
    if "connection refused" in lowered or "could not connect" in lowered:
        return (
            f"SSM tunnel not listening on 127.0.0.1:{tunnel_port}. "
            "Open a separate terminal, run: mdc tunnel clinic-db   (keep it open), then retry."
        )
    return None


def _run_psql_scalar(env: dict[str, str], sql: str) -> tuple[Optional[str], Optional[str]]:
    psql = _find_psql()
    if not psql:
        return None, "psql not found on PATH (install PostgreSQL client tools)"

    host = env.get("POSTGRES_ANALYTICS_HOST", "localhost")
    port = str(env.get("POSTGRES_ANALYTICS_PORT", "5432"))
    database = env.get("POSTGRES_ANALYTICS_DB", "opendental_analytics")
    user = env.get("POSTGRES_ANALYTICS_USER", "analytics_user")
    password = env.get("POSTGRES_ANALYTICS_PASSWORD", "")
    sslmode = env.get("POSTGRES_ANALYTICS_SSLMODE", "prefer")

    child_env = os.environ.copy()
    child_env["PGPASSWORD"] = password
    child_env["PGSSLMODE"] = sslmode

    try:
        completed = subprocess.run(
            [
                psql,
                "-h",
                host,
                "-p",
                port,
                "-U",
                user,
                "-d",
                database,
                "-v",
                "ON_ERROR_STOP=1",
                "-t",
                "-A",
                "-c",
                sql,
            ],
            capture_output=True,
            text=True,
            env=child_env,
            timeout=20,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return None, "psql timed out connecting to analytics database"
    except OSError as exc:
        return None, f"psql failed: {exc}"

    if completed.returncode != 0:
        message = (completed.stderr or completed.stdout or "psql error").strip()
        first_line = message.splitlines()[0] if message else "psql error"
        return None, first_line

    value = completed.stdout.strip()
    if not value:
        return None, None
    return value, None


def _column_exists(env: dict[str, str], schema: str, table: str, column: str) -> bool:
    sql = (
        "SELECT 1 FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
        f"AND column_name = '{column}' LIMIT 1"
    )
    value, error = _run_psql_scalar(env, sql)
    return bool(value) and not error


def _resolve_probe_sql(env: dict[str, str], probe: FreshnessProbe) -> tuple[str, Optional[str]]:
    """
    Return (sql, note_suffix).

    Mart probes fall back to a business date column when _mart_refreshed_at is missing
    (e.g. clinic RDS before next dbt run + publish).
    """
    if not probe.mart_table or not probe.mart_fallback_column:
        return probe.sql, None
    if _column_exists(env, "marts", probe.mart_table, probe.mart_ts_column):
        return probe.sql, None
    fallback_sql = (
        f"SELECT MAX({probe.mart_fallback_column})::text "
        f"FROM marts.{probe.mart_table} "
        f"WHERE {probe.mart_fallback_column} <= CURRENT_DATE"
    )
    return (
        fallback_sql,
        f"using {probe.mart_fallback_column} proxy (_mart_refreshed_at missing — run dbt + publish)",
    )


def _run_all_probes(env: dict[str, str], stage: str) -> tuple[list[tuple[str, str, Optional[str]]], list[tuple[str, str]]]:
    rows: list[tuple[str, str, Optional[str]]] = []
    errors: list[tuple[str, str]] = []
    for probe in probes_for_stage(stage):
        sql, note_suffix = _resolve_probe_sql(env, probe)
        value, error = _run_psql_scalar(env, sql)
        if error:
            errors.append((probe.name, error))
            continue
        if value:
            rows.append((probe.name, value, note_suffix))
    return rows, errors


def collect_freshness_report(
    stage: str,
    *,
    tunnel_db: bool = False,
    tunnel_port: int = 5433,
    now: Optional[datetime] = None,
    prefer_secrets_manager: bool = True,
    api_password_malformed: bool = False,
    api_password_in_sync: Optional[bool] = None,
) -> FreshnessReport:
    if stage not in ("local", "clinic"):
        return FreshnessReport(
            stage=stage,
            database_label="",
            rows=[],
            error=f"Freshness checks support stages: local, clinic (got '{stage}')",
        )

    password_source: Optional[str] = None
    password_warning: Optional[str] = None
    try:
        env, password_source, password_warning = load_freshness_env_dict(
            stage,
            prefer_secrets_manager=prefer_secrets_manager,
        )
    except ValueError as exc:
        return FreshnessReport(
            stage=stage,
            database_label="",
            rows=[],
            error=str(exc),
        )
    except RuntimeError as exc:
        return FreshnessReport(
            stage=stage,
            database_label="",
            rows=[],
            error=str(exc),
            hint=_auth_failure_hint(
                stage,
                api_password_malformed=api_password_malformed,
                api_password_in_sync=api_password_in_sync,
            )
            if stage == "clinic"
            else None,
        )

    host = env.get("POSTGRES_ANALYTICS_HOST", "")
    hint = _connection_hint(stage, host, tunnel_db, tunnel_port)
    host_l = host.lower()
    is_rds = host_l.endswith(".rds.amazonaws.com") or host_l == "rds.amazonaws.com"
    if hint and not tunnel_db and stage == "clinic" and is_rds:
        return FreshnessReport(
            stage=stage,
            database_label=f"{env.get('POSTGRES_ANALYTICS_DB', '')} @ {host}",
            rows=[],
            error="Cannot reach clinic RDS without tunnel",
            hint=hint,
        )

    if tunnel_db:
        env = apply_tunnel_db_overrides(env, local_port=tunnel_port)
        host = env.get("POSTGRES_ANALYTICS_HOST", host)

    database_label = f"{env.get('POSTGRES_ANALYTICS_DB', 'opendental_analytics')} @ {host}:{env.get('POSTGRES_ANALYTICS_PORT', '5432')}"
    raw_rows, probe_errors = _run_all_probes(env, stage)
    if not raw_rows and probe_errors:
        first_probe, first_error = probe_errors[0]
        if len(probe_errors) == len(probes_for_stage(stage)):
            auth_hint = (
                _auth_failure_hint(
                    stage,
                    password_source=password_source,
                    api_password_malformed=api_password_malformed,
                    api_password_in_sync=api_password_in_sync,
                )
                if "password authentication failed" in first_error.lower()
                else None
            )
            tunnel_hint = _tunnel_probe_hint(tunnel_db, tunnel_port, first_error)
            return FreshnessReport(
                stage=stage,
                database_label=database_label,
                rows=[],
                error=first_error,
                hint=tunnel_hint or auth_hint or hint,
                password_source=password_source,
                password_warning=password_warning,
            )

    freshness_rows: list[FreshnessRow] = []
    if stage == "clinic":
        freshness_rows.extend(clinic_etl_skipped_rows())

    for probe, latest_text, note_suffix in raw_rows:
        latest = _parse_timestamp(latest_text)
        if latest is None:
            freshness_rows.append(
                FreshnessRow(probe=probe, latest_at=None, age_hours=None, status="unknown", note="unparsed timestamp")
            )
            continue
        age, is_future = _probe_age_hours(probe, latest, now=now)
        status, note = _classify_probe(probe, age, is_future=is_future)
        if note_suffix:
            note = f"{note_suffix}; {note}" if note else note_suffix
        freshness_rows.append(
            FreshnessRow(
                probe=probe,
                latest_at=latest,
                age_hours=age,
                status=status,
                note=note,
            )
        )

    for probe, error in probe_errors:
        hint_note = error
        if "etl_load_status" in error and stage == "local":
            hint_note = (
                f"{error} — run etl_pipeline initialize tracking or mdc etl run --env local"
            )
        elif "_mart_refreshed_at" in error:
            hint_note = f"{error} — run mdc dbt run --env local then publish"
        freshness_rows.append(
            FreshnessRow(
                probe=probe,
                latest_at=None,
                age_hours=None,
                status="error",
                note=hint_note,
            )
        )

    freshness_rows.sort(key=lambda row: row.probe)

    return FreshnessReport(
        stage=stage,
        database_label=database_label,
        rows=freshness_rows,
        hint=hint,
        password_source=password_source,
        password_warning=password_warning,
    )


def freshness_stages_for_status(env_filter: Optional[str]) -> list[str]:
    """Which DB stages to probe from mdc status."""
    if env_filter in ("local", "clinic"):
        return [env_filter]
    if env_filter is None:
        return ["local"]
    return []
