"""Load QuickBooks settings from ``quickbooks/.env_<stage>``.

Stages match the rest of the repo: ``local``, ``clinic``, and ``test``.
``clinic`` is the real company and uses the Intuit production API.
``local`` and ``test`` use the Intuit sandbox. There is no ``production`` stage.

Precedence matches API and ETL: a non-blank process environment value wins
over the stage file. Required values fail fast. Token refresh writes back to
the stage file.
"""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Any


class QuickBooksConfigError(Exception):
    """Raised when the stage file cannot support the requested step."""


class QuickBooksStage(str, Enum):
    LOCAL = "local"
    CLINIC = "clinic"
    TEST = "test"


ENTITIES = ("MDC", "GLIC")

_API_BASE = {
    QuickBooksStage.CLINIC: "https://quickbooks.api.intuit.com",
    QuickBooksStage.LOCAL: "https://sandbox-quickbooks.api.intuit.com",
    QuickBooksStage.TEST: "https://sandbox-quickbooks.api.intuit.com",
}

_APP_KEYS = (
    "QUICKBOOKS_CLIENT_ID",
    "QUICKBOOKS_CLIENT_SECRET",
    "QUICKBOOKS_REDIRECT_URI",
    "QUICKBOOKS_OAUTH_STATE",
    "QUICKBOOKS_INTEGRATION_USER_ROLE",
    "QUICKBOOKS_INTEGRATION_USER_FIRST_NAME",
    "QUICKBOOKS_INTEGRATION_USER_LAST_NAME",
    "QUICKBOOKS_INTEGRATION_USER_EMAIL",
    "QUICKBOOKS_INTEGRATION_USER_PASSWORD",
)

_COMPANY_SUFFIXES = (
    "COMPANY_NAME",
    "REALM_ID",
    "ACCESS_TOKEN",
    "REFRESH_TOKEN",
    "ACCESS_TOKEN_EXPIRES_AT",
    "REFRESH_TOKEN_EXPIRES_AT",
)


def project_root() -> Path:
    """Root ``quickbooks/`` directory."""
    return Path(__file__).resolve().parents[1]


def env_file_path(stage: QuickBooksStage) -> Path:
    return project_root() / f".env_{stage.value}"


def company_key(entity: str, suffix: str) -> str:
    return f"{entity}_QUICKBOOKS_{suffix}"


def known_keys() -> tuple[str, ...]:
    keys = ["QUICKBOOKS_ENVIRONMENT", *_APP_KEYS]
    for entity in ENTITIES:
        keys.extend(company_key(entity, suffix) for suffix in _COMPANY_SUFFIXES)
    return tuple(keys)


def resolve_stage(explicit: str | None = None) -> QuickBooksStage:
    """Resolve stage from ``--env`` or ``QUICKBOOKS_ENVIRONMENT``."""
    raw = explicit if explicit is not None else os.getenv("QUICKBOOKS_ENVIRONMENT")
    if not raw or not str(raw).strip():
        raise QuickBooksConfigError(
            "QUICKBOOKS_ENVIRONMENT is not set. Pass --env or set the variable. "
            "Must be one of: local, clinic, test"
        )
    raw = str(raw).strip()
    if raw in {"production", "prod", "sandbox"}:
        raise QuickBooksConfigError(
            f"Invalid environment '{raw}'. "
            "Use 'clinic' for the clinic QuickBooks company and 'local' for the Intuit sandbox. "
            "Valid environments: local, clinic, test"
        )
    try:
        return QuickBooksStage(raw)
    except ValueError as exc:
        raise QuickBooksConfigError(
            f"Invalid environment '{raw}'. Must be one of: "
            f"{[stage.value for stage in QuickBooksStage]}"
        ) from exc


def _blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _read_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    from dotenv import dotenv_values

    raw = dotenv_values(path) or {}
    return {
        str(key): str(value).strip().strip('"').strip("'")
        for key, value in raw.items()
        if key and value is not None
    }


def _merged_values(path: Path) -> dict[str, str]:
    """Process environment wins. The stage file fills only blank keys."""
    file_values = _read_file(path)
    merged: dict[str, str] = {}
    for key in known_keys():
        from_process = os.getenv(key)
        if from_process is not None and str(from_process).strip():
            merged[key] = str(from_process).strip()
        else:
            merged[key] = file_values.get(key, "")
    return merged


class QuickBooksCredentials:
    """One stage's QuickBooks app, reports user, and company tokens."""

    def __init__(self, stage: QuickBooksStage, path: Path, values: dict[str, str]):
        self.stage = stage
        self.path = path
        self.values = values

    @classmethod
    def load(
        cls,
        *,
        environment: str | None = None,
        path: Path | None = None,
    ) -> "QuickBooksCredentials":
        stage = resolve_stage(environment)
        env_file = path or env_file_path(stage)
        return cls(stage, env_file, _merged_values(env_file))

    def get(self, key: str) -> str:
        return self.values.get(key, "")

    def set_value(self, key: str, value: str) -> None:
        """Remember a rotated token and write it to the stage file."""
        self.values[key] = value
        from dotenv import set_key

        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text(
                f"# Used by: QUICKBOOKS (QUICKBOOKS_ENVIRONMENT={self.stage.value})\n"
                f"QUICKBOOKS_ENVIRONMENT={self.stage.value}\n",
                encoding="utf-8",
            )
        set_key(str(self.path), key, value, quote_mode="always")

    @property
    def api_base(self) -> str:
        return _API_BASE[self.stage]

    @property
    def intuit_key_section(self) -> str:
        """Intuit Keys and credentials section for this stage.

        Clinic uses the Production keys. Local and test use the Development keys.
        """
        if self.stage is QuickBooksStage.CLINIC:
            return "Production"
        return "Development"

    @property
    def client_id(self) -> str:
        return self.get("QUICKBOOKS_CLIENT_ID")

    @property
    def client_secret(self) -> str:
        return self.get("QUICKBOOKS_CLIENT_SECRET")

    @property
    def redirect_uri(self) -> str:
        return self.get("QUICKBOOKS_REDIRECT_URI")

    @property
    def oauth_state(self) -> str:
        return self.get("QUICKBOOKS_OAUTH_STATE")

    def company(self, entity: str) -> dict[str, str]:
        if entity not in ENTITIES:
            raise QuickBooksConfigError(
                f"No company block for {entity}. Known companies: {', '.join(ENTITIES)}"
            )
        return {
            "realm_id": self.get(company_key(entity, "REALM_ID")),
            "access_token": self.get(company_key(entity, "ACCESS_TOKEN")),
            "refresh_token": self.get(company_key(entity, "REFRESH_TOKEN")),
        }

    def missing(self) -> list[str]:
        """Env keys that are still empty. Values are never returned."""
        gaps: list[str] = []
        for key in (
            "QUICKBOOKS_INTEGRATION_USER_EMAIL",
            "QUICKBOOKS_INTEGRATION_USER_PASSWORD",
            "QUICKBOOKS_CLIENT_ID",
            "QUICKBOOKS_CLIENT_SECRET",
            "QUICKBOOKS_REDIRECT_URI",
        ):
            if _blank(self.get(key)):
                gaps.append(key)
        for entity in ENTITIES:
            for suffix in ("REALM_ID", "REFRESH_TOKEN"):
                key = company_key(entity, suffix)
                if _blank(self.get(key)):
                    gaps.append(key)
        return gaps

    def oauth_ready(self) -> bool:
        return not any(
            _blank(self.get(key))
            for key in (
                "QUICKBOOKS_CLIENT_ID",
                "QUICKBOOKS_CLIENT_SECRET",
                "QUICKBOOKS_REDIRECT_URI",
            )
        )

    def reports_ready(self, entity: str) -> bool:
        company = self.company(entity)
        return self.oauth_ready() and not _blank(company["realm_id"]) and not _blank(company["refresh_token"])

    def require_reports_ready(self, entity: str) -> dict[str, str]:
        company = self.company(entity)
        if not self.reports_ready(entity):
            gaps = [
                gap
                for gap in self.missing()
                if gap.startswith("QUICKBOOKS_CLIENT_")
                or gap.startswith("QUICKBOOKS_REDIRECT_")
                or gap.startswith(f"{entity}_")
            ]
            raise QuickBooksConfigError(
                "Reports pull needs the Intuit app and a refresh token for "
                f"{entity}. Still empty: {', '.join(gaps)}"
            )
        return company

    def authorization_url(self, entity: str, state: str) -> str:
        self.company(entity)
        if not self.oauth_ready():
            raise QuickBooksConfigError(
                "Authorization URL needs QUICKBOOKS_CLIENT_ID, QUICKBOOKS_CLIENT_SECRET, "
                "and QUICKBOOKS_REDIRECT_URI"
            )
        from urllib.parse import urlencode

        query = urlencode(
            {
                "client_id": self.client_id,
                "response_type": "code",
                "scope": "com.intuit.quickbooks.accounting",
                "redirect_uri": self.redirect_uri,
                "state": state,
            }
        )
        return f"https://appcenter.intuit.com/connect/oauth2?{query}"

    def status_lines(self) -> list[str]:
        first = self.get("QUICKBOOKS_INTEGRATION_USER_FIRST_NAME")
        last = self.get("QUICKBOOKS_INTEGRATION_USER_LAST_NAME")
        name = " ".join(part for part in (first, last) if part).strip() or "(name not set)"
        role = self.get("QUICKBOOKS_INTEGRATION_USER_ROLE") or "(role not set)"
        loaded = str(self.path)
        if not self.path.is_file():
            loaded += " (file not found)"
        lines = [
            f"QUICKBOOKS_ENVIRONMENT={self.stage.value}, loaded from {loaded}",
            f"intuit host: {self.api_base}",
            f"intuit keys: {self.intuit_key_section} section on Keys and credentials",
            f"integration_user: {name}",
            f"role: {role}",
            "empty fields:",
        ]
        gaps = self.missing()
        if gaps:
            lines.extend(f"  - {gap}" for gap in gaps)
        else:
            lines.append("  - none")
        return lines
