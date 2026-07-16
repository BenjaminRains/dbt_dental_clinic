"""
Clinic portal login (username/password → signed session token).

Credentials: api/clinic-portal-users.json (or CLINIC_PORTAL_USERS_FILE / CLINIC_PORTAL_USERS).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from fastapi import HTTPException, status

logger = logging.getLogger(__name__)

API_DIR = Path(__file__).resolve().parent.parent
DEFAULT_PORTAL_USERS_FILE = API_DIR / "clinic-portal-users.json"

VALID_ROLES = frozenset(
    {"admin", "practice-manager", "owner", "front-desk", "insurance"}
)
SESSION_TTL_SECONDS = 12 * 60 * 60  # 12 hours — clinic shift length


@dataclass(frozen=True)
class PortalUser:
    username: str
    password: str
    role: str
    display_name: str


@dataclass(frozen=True)
class PortalSession:
    username: str
    role: str
    display_name: str
    exp: int


def _portal_enabled() -> bool:
    env = os.getenv("API_ENVIRONMENT", "").strip().lower()
    return env in ("clinic", "local", "test")


def _session_secret() -> str:
    explicit = (os.getenv("CLINIC_PORTAL_SESSION_SECRET") or "").strip()
    if explicit:
        return explicit
    fallback = (os.getenv("CLINIC_API_KEY") or "").strip()
    if fallback:
        return fallback
    raise ValueError(
        "CLINIC_PORTAL_SESSION_SECRET or CLINIC_API_KEY must be set for portal login."
    )


def has_session_secret() -> bool:
    try:
        _session_secret()
    except ValueError:
        return False
    return True


def _portal_users_file_path() -> Optional[Path]:
    explicit = (os.getenv("CLINIC_PORTAL_USERS_FILE") or "").strip()
    if explicit:
        return Path(explicit)
    if DEFAULT_PORTAL_USERS_FILE.is_file():
        return DEFAULT_PORTAL_USERS_FILE
    return None


def portal_users_source() -> str:
    users_file = _portal_users_file_path()
    if users_file is not None:
        return users_file.name
    if (os.getenv("CLINIC_PORTAL_USERS") or "").strip():
        return "env:CLINIC_PORTAL_USERS"
    return "none"


def _load_portal_users_raw() -> list[dict[str, Any]]:
    users_file = _portal_users_file_path()
    if users_file is not None:
        try:
            data = json.loads(users_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"{users_file} is not valid JSON: {exc}") from exc
        if not isinstance(data, list):
            raise ValueError(f"{users_file} must be a JSON array of user objects.")
        return data

    raw = (os.getenv("CLINIC_PORTAL_USERS") or "").strip()
    if not raw:
        return []

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"CLINIC_PORTAL_USERS is not valid JSON: {exc}") from exc

    if not isinstance(data, list):
        raise ValueError("CLINIC_PORTAL_USERS must be a JSON array of user objects.")
    return data


def _parse_portal_users(data: list[dict[str, Any]]) -> list[PortalUser]:
    users: list[PortalUser] = []
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise ValueError(f"CLINIC_PORTAL_USERS[{index}] must be an object.")
        username = str(item.get("username", "")).strip()
        password = str(item.get("password", ""))
        role = str(item.get("role", "")).strip()
        display_name = str(item.get("display_name", "")).strip() or username
        if not username or not password:
            raise ValueError(f"CLINIC_PORTAL_USERS[{index}] requires username and password.")
        if role not in VALID_ROLES:
            raise ValueError(
                f"CLINIC_PORTAL_USERS[{index}] role '{role}' invalid. "
                f"Expected one of: {sorted(VALID_ROLES)}"
            )
        users.append(
            PortalUser(
                username=username,
                password=password,
                role=role,
                display_name=display_name,
            )
        )
    return users


def load_portal_users() -> list[PortalUser]:
    if not _portal_enabled():
        return []

    return _parse_portal_users(_load_portal_users_raw())


def authenticate(username: str, password: str) -> PortalUser:
    users = load_portal_users()
    if not users:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Clinic portal login is not configured.",
        )

    normalized = username.strip().lower()
    for user in users:
        if user.username.lower() != normalized:
            continue
        try:
            matches = len(user.password) == len(password) and secrets.compare_digest(
                user.password, password
            )
        except ValueError:
            matches = False
        if matches:
            return user

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid username or password.",
    )


def create_session_token(user: PortalUser) -> str:
    secret = _session_secret()
    payload = {
        "sub": user.username,
        "role": user.role,
        "name": user.display_name,
        "exp": int(time.time()) + SESSION_TTL_SECONDS,
    }
    payload_b64 = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    sig = hmac.new(
        secret.encode("utf-8"),
        payload_b64.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    return f"{payload_b64}.{sig}"


def verify_session_token(token: str) -> PortalSession:
    if not token or "." not in token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session.",
        )

    payload_b64, sig = token.rsplit(".", 1)
    secret = _session_secret()
    expected = hmac.new(
        secret.encode("utf-8"),
        payload_b64.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    if not secrets.compare_digest(expected, sig):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session.",
        )

    try:
        payload: dict[str, Any] = json.loads(
            base64.urlsafe_b64decode(payload_b64.encode("ascii")).decode("utf-8")
        )
    except (ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session.",
        ) from exc

    exp = int(payload.get("exp", 0))
    if exp < int(time.time()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired. Please sign in again.",
        )

    role = str(payload.get("role", "")).strip()
    username = str(payload.get("sub", "")).strip()
    display_name = str(payload.get("name", "")).strip() or username
    if not username or role not in VALID_ROLES:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session.",
        )

    return PortalSession(
        username=username,
        role=role,
        display_name=display_name,
        exp=exp,
    )


def parse_bearer_token(authorization: Optional[str]) -> str:
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header required.",
        )
    parts = authorization.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header must be Bearer <token>.",
        )
    return parts[1].strip()
