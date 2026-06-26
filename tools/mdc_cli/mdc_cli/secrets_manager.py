"""AWS Secrets Manager helpers for rotating clinic RDS credentials."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from mdc_cli.credentials import _dig, load_deployment_credentials
from mdc_cli.paths import API_DIR, DEPLOYMENT_CREDENTIALS, ETL_DIR, REPO_ROOT
from mdc_cli.process_util import find_executable, run_subprocess_completed

CLINIC_RDS_INSTANCE_DEFAULT = "dental-clinic-analytics"
CLINIC_RDS_PASSWORD_ENV = "MDC_CLINIC_RDS_PASSWORD"


@dataclass(frozen=True)
class ClinicAnalyticsSecret:
    secret_id: str
    region: str
    username: str
    password: str
    host: Optional[str]
    port: Optional[int]
    database: Optional[str]
    raw_secret_string: str = ""


@dataclass(frozen=True)
class PasswordResolution:
    password: str
    source: str
    warning: Optional[str] = None


@dataclass(frozen=True)
class CredentialSyncRow:
    target: str
    status: str
    note: str


@dataclass(frozen=True)
class ClinicCredentialSyncReport:
    secret_id: str
    region: str
    rows: list[CredentialSyncRow]
    secrets_manager_ok: bool
    error: Optional[str] = None
    api_password_malformed: bool = False
    api_password_in_sync: Optional[bool] = None
    rds_master_secret_id: Optional[str] = None


def normalize_clinic_password_value(raw: Optional[str]) -> Optional[str]:
    """
    Return the password string for RDS login.

    Handles the common mistake of pasting the full Secrets Manager JSON blob into
    POSTGRES_ANALYTICS_PASSWORD instead of the password field alone.
    """
    if raw is None:
        return None
    val = raw.strip()
    if not val:
        return None

    for _ in range(2):
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
            val = val[1:-1].strip()

    if not val.startswith("{"):
        return val

    candidates = [val, val.replace('\\"', '"'), val.replace("\\\\", "\\")]
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            password = payload.get("password")
            if password and str(password).strip():
                return str(password).strip()
    return val


def clinic_password_value_is_json_blob(raw: Optional[str]) -> bool:
    if not raw:
        return False
    val = raw.strip()
    if not val.startswith("{"):
        return False
    normalized = normalize_clinic_password_value(val)
    return bool(normalized and normalized != val)


def plain_clinic_password(password: str, *, raw_secret: str = "") -> str:
    """
    Return a single password string suitable for .env and psycopg.

    Handles SM payloads where the password field mistakenly contains the full JSON
    secret blob (same mistake as a bad api/.env_api_clinic paste).
    """
    for candidate in (password, raw_secret):
        if not candidate or not str(candidate).strip():
            continue
        normalized = normalize_clinic_password_value(str(candidate).strip())
        if normalized and not clinic_password_value_is_json_blob(normalized):
            return normalized
    cleaned = normalize_clinic_password_value(password) or password
    if clinic_password_value_is_json_blob(cleaned):
        raise RuntimeError(
            "Secrets Manager secret password field contains a full JSON blob, not a plain "
            "password. Fix the secret in AWS (password key = password string only) or paste "
            "only the password value into api/.env_api_clinic."
        )
    return cleaned


def _finalize_clinic_secret(secret: ClinicAnalyticsSecret) -> ClinicAnalyticsSecret:
    """Ensure secret.password is a plain string, not a nested JSON blob."""
    plain = plain_clinic_password(secret.password, raw_secret=secret.raw_secret_string)
    if plain == secret.password:
        return secret
    return ClinicAnalyticsSecret(
        secret_id=secret.secret_id,
        region=secret.region,
        username=secret.username,
        password=plain,
        host=secret.host,
        port=secret.port,
        database=secret.database,
        raw_secret_string=secret.raw_secret_string,
    )


def clinic_rds_master_secret_id() -> str:
    """Return the RDS master user secret id (ARN or rds!db-... name)."""
    master_arn = fetch_rds_master_user_secret_arn()
    if master_arn:
        return master_arn

    creds = load_deployment_credentials() or {}
    for path in (
        ("backend_api", "clinic_database_reference", "rds", "credentials", "secrets", "opendental_analytics", "secret_name"),
        ("backend_api", "clinic_database_reference", "rds", "credentials", "secrets", "opendental_analytics", "secret_arn"),
        ("clinic_database", "postgresql", "secret_name"),
        ("clinic_database", "postgresql", "secret_arn"),
        ("rds", "credentials", "secrets", "opendental_analytics", "secret_name"),
        ("rds", "credentials", "secrets", "opendental_analytics", "secret_arn"),
    ):
        name = _dig(creds, *path)
        if isinstance(name, str) and name.strip():
            val = name.strip()
            if val.startswith("rds!db") or ":secret:rds!db" in val:
                return val

    raise RuntimeError(
        "Could not resolve RDS master user secret for clinic analytics. "
        "Ensure aws rds describe-db-instances works for the clinic instance, or set "
        "secret_name / secret_arn (rds!db-...) in deployment_credentials.json."
    )


def clinic_analytics_secret_id() -> str:
    """Clinic RDS password authority — the RDS-managed master user secret."""
    return clinic_rds_master_secret_id()


def clinic_analytics_secret_region() -> str:
    creds = load_deployment_credentials() or {}
    region = _dig(creds, "aws", "region")
    if isinstance(region, str) and region.strip():
        return region.strip()
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"


def clinic_rds_instance_identifier() -> str:
    creds = load_deployment_credentials() or {}
    for path in (
        ("backend_api", "clinic_database_reference", "rds", "instance_identifier"),
        ("clinic_database", "postgresql", "instance_identifier"),
        ("rds", "instance_identifier"),
    ):
        value = _dig(creds, *path)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return CLINIC_RDS_INSTANCE_DEFAULT


def _clinic_rds_endpoint_from_creds() -> Optional[str]:
    creds = load_deployment_credentials() or {}
    for path in (
        ("backend_api", "clinic_database_reference", "rds", "endpoint"),
        ("clinic_database", "postgresql", "host"),
        ("rds", "endpoint"),
    ):
        value = _dig(creds, *path)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _run_aws_json(args: list[str]) -> dict[str, Any]:
    aws = find_executable("aws")
    if not aws:
        raise RuntimeError("aws CLI not found on PATH (required for AWS)")
    completed = run_subprocess_completed([aws, *args])
    if completed.returncode != 0:
        message = (completed.stderr or completed.stdout or "aws command failed").strip()
        raise RuntimeError(message.splitlines()[0] if message else "aws command failed")
    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid aws response: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected aws response shape")
    return payload


def fetch_rds_master_user_secret_arn(
    *,
    instance_identifier: Optional[str] = None,
    region: Optional[str] = None,
) -> Optional[str]:
    """
    Return the RDS-managed master secret ARN (rds!db-...) when the instance uses one.

    This secret is rotated by AWS (~7 days). It is the password PostgreSQL actually
    accepts for the master user (analytics_user on clinic RDS).
    """
    resolved_region = region or clinic_analytics_secret_region()
    resolved_id = instance_identifier or clinic_rds_instance_identifier()
    try:
        response = _run_aws_json(
            [
                "rds",
                "describe-db-instances",
                "--db-instance-identifier",
                resolved_id,
                "--region",
                resolved_region,
                "--output",
                "json",
            ]
        )
    except RuntimeError:
        return None

    instances = response.get("DBInstances") or []
    if not instances:
        return None
    master = instances[0].get("MasterUserSecret") or {}
    arn = master.get("SecretArn")
    if isinstance(arn, str) and arn.strip():
        return arn.strip()
    return None



def fetch_clinic_analytics_secret(
    *,
    secret_id: Optional[str] = None,
    region: Optional[str] = None,
) -> ClinicAnalyticsSecret:
    """Fetch current clinic analytics DB secret (handles 7-day rotation)."""
    aws = find_executable("aws")
    if not aws:
        raise RuntimeError("aws CLI not found on PATH (required for Secrets Manager)")

    resolved_id = secret_id or clinic_analytics_secret_id()
    resolved_region = region or clinic_analytics_secret_region()

    completed = run_subprocess_completed(
        [
            aws,
            "secretsmanager",
            "get-secret-value",
            "--secret-id",
            resolved_id,
            "--region",
            resolved_region,
            "--output",
            "json",
        ]
    )
    if completed.returncode != 0:
        message = (completed.stderr or completed.stdout or "aws secretsmanager failed").strip()
        raise RuntimeError(message.splitlines()[0] if message else "aws secretsmanager failed")

    try:
        response = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid aws secretsmanager response: {exc}") from exc

    raw = (response.get("SecretString") or "").strip()
    if not raw and response.get("SecretBinary"):
        import base64

        raw = base64.b64decode(response["SecretBinary"]).decode("utf-8").strip()
    if not raw:
        raise RuntimeError(f"Empty SecretString for {resolved_id}")

    return _finalize_clinic_secret(
        _parse_clinic_analytics_secret_payload(
            raw,
            secret_id=resolved_id,
            region=resolved_region,
        )
    )


def _parse_clinic_analytics_secret_payload(
    raw: str,
    *,
    secret_id: str,
    region: str,
) -> ClinicAnalyticsSecret:
    try:
        payload: dict[str, Any] = json.loads(raw)
    except json.JSONDecodeError:
        # Some secrets store only the password string.
        if not raw or "\n" in raw:
            raise RuntimeError(f"Secret {secret_id} is not valid JSON or plain password text")
        return ClinicAnalyticsSecret(
            secret_id=secret_id,
            region=region,
            username="analytics_user",
            password=raw,
            host=None,
            port=None,
            database=None,
            raw_secret_string=raw,
        )

    password = payload.get("password")
    if not password or not str(password).strip():
        raise RuntimeError(f"Secret {secret_id} has no password field")

    username = str(payload.get("username") or payload.get("user") or "analytics_user")
    host = payload.get("host")
    port_raw = payload.get("port")
    database = payload.get("dbname") or payload.get("database")

    port: Optional[int] = None
    if port_raw is not None and str(port_raw).strip():
        port = int(port_raw)

    return ClinicAnalyticsSecret(
        secret_id=secret_id,
        region=region,
        username=username,
        password=str(password),
        host=str(host).strip() if host else None,
        port=port,
        database=str(database).strip() if database else None,
        raw_secret_string=raw,
    )


def fetch_live_clinic_rds_secret(
    *,
    region: Optional[str] = None,
) -> ClinicAnalyticsSecret:
    """Return the password RDS currently accepts (RDS-managed master user secret)."""
    resolved_region = region or clinic_analytics_secret_region()
    master_arn = fetch_rds_master_user_secret_arn(region=resolved_region)
    if master_arn:
        return fetch_clinic_analytics_secret(secret_id=master_arn, region=resolved_region)

    try:
        configured = clinic_rds_master_secret_id()
    except RuntimeError:
        configured = None
    if configured:
        return fetch_clinic_analytics_secret(secret_id=configured, region=resolved_region)

    raise RuntimeError(
        "No RDS master user secret found for clinic analytics. "
        "Check the dental-clinic-analytics instance in AWS RDS."
    )


def resolve_clinic_rds_password(
    file_password: Optional[str],
    *,
    prefer_secrets_manager: bool = True,
) -> PasswordResolution:
    """Prefer live Secrets Manager password; fall back to env file."""
    if os.environ.get(CLINIC_RDS_PASSWORD_ENV):
        return PasswordResolution(
            password=os.environ[CLINIC_RDS_PASSWORD_ENV],
            source="environment",
        )

    if prefer_secrets_manager:
        try:
            secret = fetch_live_clinic_rds_secret()
            return PasswordResolution(
                password=plain_clinic_password(
                    secret.password,
                    raw_secret=secret.raw_secret_string,
                ),
                source=f"secrets_manager:{secret.secret_id}",
            )
        except Exception as exc:
            if file_password:
                return PasswordResolution(
                    password=file_password,
                    source="env_file",
                    warning=f"Secrets Manager unavailable ({exc}); using api/.env_api_clinic password (may be stale after rotation).",
                )
            raise RuntimeError(
                f"Could not load clinic RDS password from Secrets Manager ({exc}) "
                "and no POSTGRES_ANALYTICS_PASSWORD in api/.env_api_clinic."
            ) from exc

    if not file_password:
        raise RuntimeError("POSTGRES_ANALYTICS_PASSWORD missing in api/.env_api_clinic")
    return PasswordResolution(
        password=file_password,
        source="env_file",
        warning="Using api/.env_api_clinic password only (--use-env-file). May be stale if Secrets Manager rotation ran.",
    )


def overlay_clinic_rds_credentials(
    env: dict[str, str],
    *,
    prefer_secrets_manager: bool = True,
) -> tuple[dict[str, str], Optional[PasswordResolution]]:
    """Return env copy with live RDS password (and optional host/db from secret)."""
    merged = dict(env)
    file_password = normalize_clinic_password_value(
        (merged.get("POSTGRES_ANALYTICS_PASSWORD") or "").strip() or None
    )
    resolution = resolve_clinic_rds_password(
        file_password,
        prefer_secrets_manager=prefer_secrets_manager,
    )
    merged["POSTGRES_ANALYTICS_PASSWORD"] = resolution.password

    if prefer_secrets_manager and resolution.source.startswith("secrets_manager"):
        try:
            secret = fetch_live_clinic_rds_secret()
            if secret.host:
                merged["POSTGRES_ANALYTICS_HOST"] = secret.host
            if secret.port:
                merged["POSTGRES_ANALYTICS_PORT"] = str(secret.port)
            if secret.database:
                merged["POSTGRES_ANALYTICS_DB"] = secret.database
            if secret.username:
                merged["POSTGRES_ANALYTICS_USER"] = secret.username
        except Exception:
            pass

    return merged, resolution


def _format_dotenv_value(value: str, *, key: str = "") -> str:
    """Quote dotenv values that contain characters that break naive KEY=value parsing."""
    if not value:
        return value
    # Password fields must stay a single opaque token — never re-quote JSON blobs.
    if key.upper().endswith("PASSWORD"):
        if any(ch in value for ch in ("\n", "\r")):
            escaped = value.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped}"'
        return value
    if any(ch in value for ch in (' ', '#', '"', "'", "\t", "\n", "\\", "{", "}")):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return value


def _format_dotenv_line(key: str, value: str) -> str:
    return f"{key}={_format_dotenv_value(value, key=key)}"


def update_dotenv_key(path: Path, key: str, value: str) -> bool:
    """Update or append KEY=value in a dotenv file. Returns True if file changed."""
    key_pattern = re.compile(rf"^{re.escape(key)}\s*=", re.IGNORECASE)
    lines: list[str] = []
    if path.is_file():
        lines = path.read_text(encoding="utf-8").splitlines()

    new_line = _format_dotenv_line(key, value)
    replaced = False
    out: list[str] = []
    for line in lines:
        if key_pattern.match(line.strip()):
            out.append(new_line)
            replaced = True
        else:
            out.append(line)

    if not replaced:
        if out and out[-1].strip():
            out.append("")
        out.append(new_line)

    new_text = "\n".join(out).rstrip() + "\n"
    old_text = "\n".join(lines).rstrip() + ("\n" if lines else "")
    if new_text == old_text:
        return False
    path.write_text(new_text, encoding="utf-8")
    return True


def force_update_dotenv_key(path: Path, key: str, value: str) -> bool:
    """Like update_dotenv_key but always writes (skips equality short-circuit)."""
    key_pattern = re.compile(rf"^{re.escape(key)}\s*=", re.IGNORECASE)
    lines: list[str] = []
    if path.is_file():
        lines = path.read_text(encoding="utf-8").splitlines()

    new_line = _format_dotenv_line(key, value)
    replaced = False
    out: list[str] = []
    for line in lines:
        if key_pattern.match(line.strip()):
            out.append(new_line)
            replaced = True
        else:
            out.append(line)

    if not replaced:
        if out and out[-1].strip():
            out.append("")
        out.append(new_line)

    new_text = "\n".join(out).rstrip() + "\n"
    path.write_text(new_text, encoding="utf-8")
    return True


@dataclass(frozen=True)
class ClinicEnvSyncResult:
    secret: ClinicAnalyticsSecret
    api_env_changed: bool
    deployment_credentials_changed: bool
    repaired_json_password: bool
    rds_master_secret_id: Optional[str] = None


def sync_clinic_env_from_secrets(
    *,
    api_env_file: Path,
    update_deployment_credentials: bool = True,
) -> ClinicEnvSyncResult:
    """Write current clinic RDS credentials into api/.env_api_clinic."""
    secret = fetch_live_clinic_rds_secret()
    rds_master_secret_id = fetch_rds_master_user_secret_arn() or secret.secret_id
    if not api_env_file.is_file():
        raise RuntimeError(f"Env file not found: {api_env_file}")

    plain_password = plain_clinic_password(
        secret.password,
        raw_secret=secret.raw_secret_string,
    )

    raw_before = _read_dotenv_value_raw(api_env_file, "POSTGRES_ANALYTICS_PASSWORD")
    repaired_json_password = clinic_password_value_is_json_blob(raw_before)

    api_changed = False
    api_changed |= force_update_dotenv_key(
        api_env_file, "POSTGRES_ANALYTICS_PASSWORD", plain_password
    )
    if secret.host:
        api_changed |= update_dotenv_key(api_env_file, "POSTGRES_ANALYTICS_HOST", secret.host)
    if secret.port:
        api_changed |= update_dotenv_key(api_env_file, "POSTGRES_ANALYTICS_PORT", str(secret.port))
    if secret.database:
        api_changed |= update_dotenv_key(api_env_file, "POSTGRES_ANALYTICS_DB", secret.database)
    if secret.username:
        api_changed |= update_dotenv_key(api_env_file, "POSTGRES_ANALYTICS_USER", secret.username)
    api_changed |= update_dotenv_key(api_env_file, "POSTGRES_ANALYTICS_SSLMODE", "require")

    raw_after = _read_dotenv_value_raw(api_env_file, "POSTGRES_ANALYTICS_PASSWORD")
    if clinic_password_value_is_json_blob(raw_after):
        raise RuntimeError(
            f"Failed to repair POSTGRES_ANALYTICS_PASSWORD in {api_env_file}. "
            "The field still contains a full Secrets Manager JSON blob; edit the file manually "
            "so the value is the password string only, then re-run: mdc secrets pull clinic"
        )

    deployment_credentials_changed = False
    if update_deployment_credentials:
        deployment_credentials_changed = _update_deployment_credentials_clinic_password(secret)

    return ClinicEnvSyncResult(
        secret=secret,
        api_env_changed=api_changed,
        deployment_credentials_changed=deployment_credentials_changed,
        repaired_json_password=repaired_json_password,
        rds_master_secret_id=rds_master_secret_id,
    )


def _update_deployment_credentials_clinic_password(secret: ClinicAnalyticsSecret) -> bool:
    from mdc_cli.paths import DEPLOYMENT_CREDENTIALS

    if not DEPLOYMENT_CREDENTIALS.is_file():
        return False
    old_text = DEPLOYMENT_CREDENTIALS.read_text(encoding="utf-8")
    data = json.loads(old_text)

    for path in (
        ("clinic_database", "postgresql"),
        (
            "backend_api",
            "clinic_database_reference",
            "rds",
            "credentials",
            "secrets",
            "opendental_analytics",
            "current_value",
        ),
    ):
        node = data
        for key in path[:-1]:
            if key not in node or not isinstance(node[key], dict):
                node[key] = {}
            node = node[key]
        leaf = path[-1]
        if leaf not in node or not isinstance(node[leaf], dict):
            node[leaf] = {}
        target = node[leaf]
        target["password"] = plain_clinic_password(
            secret.password,
            raw_secret=secret.raw_secret_string,
        )
        if secret.host:
            target["host"] = secret.host
        if secret.port:
            target["port"] = secret.port
        if secret.database:
            target["dbname"] = secret.database
            target["database"] = secret.database
        if secret.username:
            target["username"] = secret.username
            target["user"] = secret.username

    new_text = json.dumps(data, indent=4) + "\n"
    if new_text == old_text:
        return False
    DEPLOYMENT_CREDENTIALS.write_text(new_text, encoding="utf-8")
    return True


def _unwrap_dotenv_value(val: str) -> str:
    text = val.strip()
    hash_idx = text.find("#")
    if hash_idx >= 0:
        text = text[:hash_idx].strip()
    if text.startswith('"') and text.endswith('"'):
        return text[1:-1]
    if text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    return text


def _read_dotenv_value_raw(path: Path, key: str) -> Optional[str]:
    """Read a dotenv value from disk only (ignore process environment)."""
    if not path.is_file():
        return None
    pattern = re.compile(rf"^{re.escape(key)}\s*=\s*(.+)$", re.IGNORECASE)
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = pattern.match(line)
        if not match:
            continue
        val = _unwrap_dotenv_value(match.group(1))
        return val or None


def _read_password_from_file_only(path: Path, key: str) -> Optional[str]:
    return normalize_clinic_password_value(_read_dotenv_value_raw(path, key))


def _deployment_credentials_clinic_password() -> Optional[str]:
    creds = load_deployment_credentials()
    if not creds:
        return None
    for path in (
        ("clinic_database", "postgresql", "password"),
        (
            "backend_api",
            "clinic_database_reference",
            "rds",
            "credentials",
            "secrets",
            "opendental_analytics",
            "current_value",
            "password",
        ),
    ):
        value = _dig(creds, *path)
        if isinstance(value, str) and value.strip():
            return normalize_clinic_password_value(value.strip())
    return None


def _compare_stored_password(
    target_label: str,
    stored_password: Optional[str],
    live_password: str,
    *,
    malformed_json_blob: bool = False,
) -> CredentialSyncRow:
    if not stored_password:
        return CredentialSyncRow(
            target=target_label,
            status="stale",
            note="password missing — run: mdc secrets pull clinic",
        )
    if stored_password == live_password:
        if malformed_json_blob:
            return CredentialSyncRow(
                target=target_label,
                status="warn",
                note=(
                    "password matches SM but field contains full JSON secret — "
                    "run: mdc secrets pull clinic"
                ),
            )
        return CredentialSyncRow(
            target=target_label,
            status="ok",
            note="matches Secrets Manager",
        )
    if malformed_json_blob:
        return CredentialSyncRow(
            target=target_label,
            status="stale",
            note=(
                "password field contains full JSON secret and does not match SM — "
                "run: mdc secrets pull clinic"
            ),
        )
    return CredentialSyncRow(
        target=target_label,
        status="stale",
        note="differs from Secrets Manager (rotation) — run: mdc secrets pull clinic",
    )


def check_clinic_credential_sync() -> ClinicCredentialSyncReport:
    """
    Compare on-disk clinic RDS passwords to the live RDS master user secret.

    Does not print secrets. Does not require RDS tunnel.
    """
    region = clinic_analytics_secret_region()
    rds_master_secret_id = fetch_rds_master_user_secret_arn()
    api_env = API_DIR / ".env_api_clinic"

    try:
        live = fetch_live_clinic_rds_secret(region=region)
    except Exception as exc:
        return ClinicCredentialSyncReport(
            secret_id=rds_master_secret_id or "rds!db-...",
            region=region,
            rows=[],
            secrets_manager_ok=False,
            error=str(exc).splitlines()[0] if str(exc) else "Secrets Manager fetch failed",
            rds_master_secret_id=rds_master_secret_id,
        )

    rows: list[CredentialSyncRow] = []
    api_password_malformed = False
    api_password_in_sync: Optional[bool] = None
    live_plain = plain_clinic_password(live.password, raw_secret=live.raw_secret_string)

    if api_env.is_file():
        raw_password = _read_dotenv_value_raw(api_env, "POSTGRES_ANALYTICS_PASSWORD")
        file_password = normalize_clinic_password_value(raw_password)
        api_password_malformed = clinic_password_value_is_json_blob(raw_password)
        try:
            target_label = str(api_env.relative_to(REPO_ROOT))
        except ValueError:
            target_label = str(api_env)
        compare_row = _compare_stored_password(
            target_label,
            file_password,
            live_plain,
            malformed_json_blob=api_password_malformed,
        )
        rows.append(compare_row)
        api_password_in_sync = compare_row.status in ("ok", "warn")
    else:
        try:
            missing_label = str(api_env.relative_to(REPO_ROOT))
        except ValueError:
            missing_label = str(api_env)
        rows.append(
            CredentialSyncRow(
                target=missing_label,
                status="warn",
                note="file missing",
            )
        )

    if DEPLOYMENT_CREDENTIALS.is_file():
        creds_password = _deployment_credentials_clinic_password()
        rows.append(
            _compare_stored_password(
                DEPLOYMENT_CREDENTIALS.name,
                creds_password,
                live_plain,
            )
        )

    from mdc_cli.postgres_env import deprecated_etl_analytics_keys

    for stage in ("clinic", "local"):
        stale_keys = deprecated_etl_analytics_keys(stage)
        if stale_keys:
            try:
                target = str((ETL_DIR / f".env_{stage}").relative_to(REPO_ROOT))
            except ValueError:
                target = str(ETL_DIR / f".env_{stage}")
            rows.append(
                CredentialSyncRow(
                    target=target,
                    status="warn",
                    note=(
                        "deprecated POSTGRES_ANALYTICS_* — remove; mdc composes analytics "
                        f"({', '.join(stale_keys[:3])}{'...' if len(stale_keys) > 3 else ''})"
                    ),
                )
            )

    return ClinicCredentialSyncReport(
        secret_id=live.secret_id,
        region=region,
        rows=rows,
        secrets_manager_ok=True,
        api_password_malformed=api_password_malformed,
        api_password_in_sync=api_password_in_sync,
        rds_master_secret_id=rds_master_secret_id or live.secret_id,
    )
