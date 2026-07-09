"""
Live clinic RDS password from Secrets Manager (rotation-safe).

Clinic RDS master user secret (rds!db-...) rotates ~every 7 days. Static copies in
api/.env go stale; this module fetches the current password at runtime with a short TTL
cache so EC2 API / dbt keep working without weekly redeploys.

Disable with API_CLINIC_RDS_LIVE_PASSWORD=0 (tests / offline).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)

DEFAULT_INSTANCE_ID = "dental-clinic-analytics"
DEFAULT_REGION = "us-east-1"
DEFAULT_TTL_SECONDS = 300  # 5 minutes — well under 7-day rotation
LIVE_PASSWORD_ENV = "API_CLINIC_RDS_LIVE_PASSWORD"
SECRET_ARN_ENV = "CLINIC_RDS_MASTER_SECRET_ARN"
INSTANCE_ID_ENV = "CLINIC_RDS_INSTANCE_ID"
TTL_ENV = "API_CLINIC_RDS_PASSWORD_TTL_SECONDS"


@dataclass(frozen=True)
class LiveClinicPassword:
    password: str
    source: str
    secret_id: str


@dataclass
class _CacheEntry:
    password: str
    source: str
    secret_id: str
    fetched_at: float


_lock = threading.Lock()
_cache: Optional[_CacheEntry] = None


def live_password_enabled() -> bool:
    raw = (os.getenv(LIVE_PASSWORD_ENV) or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _ttl_seconds() -> float:
    raw = (os.getenv(TTL_ENV) or "").strip()
    if not raw:
        return float(DEFAULT_TTL_SECONDS)
    try:
        return max(30.0, float(raw))
    except ValueError:
        return float(DEFAULT_TTL_SECONDS)


def _region() -> str:
    return (
        (os.getenv("AWS_REGION") or "").strip()
        or (os.getenv("AWS_DEFAULT_REGION") or "").strip()
        or DEFAULT_REGION
    )


def _instance_identifier() -> str:
    return (os.getenv(INSTANCE_ID_ENV) or "").strip() or DEFAULT_INSTANCE_ID


def _configured_secret_id() -> Optional[str]:
    for key in (SECRET_ARN_ENV, "POSTGRES_ANALYTICS_SECRET_ARN"):
        val = (os.getenv(key) or "").strip()
        if val:
            return val
    return None


def invalidate_clinic_rds_password_cache() -> None:
    """Drop cached password (call after auth failure so the next fetch is fresh)."""
    global _cache
    with _lock:
        _cache = None


def _parse_secret_string(raw: str) -> str:
    val = (raw or "").strip()
    if not val:
        raise RuntimeError("Empty SecretString from Secrets Manager")
    if not val.startswith("{"):
        return val
    try:
        payload: dict[str, Any] = json.loads(val)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid Secrets Manager JSON: {exc}") from exc
    password = payload.get("password")
    if not password or not str(password).strip():
        raise RuntimeError("Secrets Manager payload has no password field")
    # Nested JSON blob mistake
    nested = str(password).strip()
    if nested.startswith("{"):
        try:
            inner = json.loads(nested)
            if isinstance(inner, dict) and inner.get("password"):
                return str(inner["password"]).strip()
        except json.JSONDecodeError:
            pass
    return nested


def _resolve_secret_id(client_rds: Any, client_sm_region: str) -> str:
    configured = _configured_secret_id()
    if configured:
        return configured
    response = client_rds.describe_db_instances(DBInstanceIdentifier=_instance_identifier())
    instances = response.get("DBInstances") or []
    if not instances:
        raise RuntimeError(f"RDS instance not found: {_instance_identifier()}")
    master = instances[0].get("MasterUserSecret") or {}
    arn = master.get("SecretArn")
    if not isinstance(arn, str) or not arn.strip():
        raise RuntimeError(
            f"RDS instance {_instance_identifier()} has no MasterUserSecret "
            f"(region={client_sm_region})"
        )
    return arn.strip()


def _fetch_password_from_aws() -> LiveClinicPassword:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for live clinic RDS password fetch") from exc

    region = _region()
    rds = boto3.client("rds", region_name=region)
    sm = boto3.client("secretsmanager", region_name=region)
    secret_id = _resolve_secret_id(rds, region)
    response = sm.get_secret_value(SecretId=secret_id)
    raw = (response.get("SecretString") or "").strip()
    if not raw and response.get("SecretBinary"):
        import base64

        raw = base64.b64decode(response["SecretBinary"]).decode("utf-8").strip()
    password = _parse_secret_string(raw)
    return LiveClinicPassword(
        password=password,
        source=f"secrets_manager:{secret_id}",
        secret_id=secret_id,
    )


def fetch_live_clinic_rds_password(*, force_refresh: bool = False) -> LiveClinicPassword:
    """
    Return the current clinic RDS master password (TTL-cached).

    Raises RuntimeError when Secrets Manager / RDS cannot be reached.
    """
    global _cache
    now = time.monotonic()
    with _lock:
        if (
            not force_refresh
            and _cache is not None
            and (now - _cache.fetched_at) < _ttl_seconds()
        ):
            return LiveClinicPassword(
                password=_cache.password,
                source=_cache.source,
                secret_id=_cache.secret_id,
            )

    live = _fetch_password_from_aws()
    with _lock:
        _cache = _CacheEntry(
            password=live.password,
            source=live.source,
            secret_id=live.secret_id,
            fetched_at=time.monotonic(),
        )
    logger.info("Loaded clinic RDS password from %s", live.source)
    return live


def resolve_clinic_analytics_password(file_password: str) -> tuple[str, str]:
    """
    Prefer live Secrets Manager password for clinic; fall back to file/env password.

    Returns (password, source_label).
    """
    if not live_password_enabled():
        return file_password, "env_file"

    try:
        live = fetch_live_clinic_rds_password()
        return live.password, live.source
    except Exception as exc:
        if file_password:
            logger.warning(
                "Live clinic RDS password unavailable (%s); using env/file password "
                "(may be stale after Secrets Manager rotation).",
                exc,
            )
            return file_password, "env_file_fallback"
        raise RuntimeError(
            f"Could not load clinic RDS password from Secrets Manager ({exc}) "
            "and no POSTGRES_ANALYTICS_PASSWORD is set."
        ) from exc
