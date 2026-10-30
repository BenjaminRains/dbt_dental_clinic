"""Pull QuickBooks Online reports and persist the rotated refresh token."""

from __future__ import annotations

import json
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

from .credentials import QuickBooksConfigError, QuickBooksCredentials
from .reports import ReportCatalog, ReportSpec

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"


def _basic_auth(client_id: str, client_secret: str) -> str:
    token = b64encode(f"{client_id}:{client_secret}".encode()).decode()
    return f"Basic {token}"


def _request_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
) -> dict[str, Any]:
    request = Request(url, data=body, headers=headers or {}, method=method)
    try:
        with urlopen(request, timeout=60) as response:
            payload = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise QuickBooksConfigError(f"QuickBooks HTTP {exc.code}: {detail}") from exc
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise QuickBooksConfigError("QuickBooks returned a non-object response")
    return data


def _form(data: dict[str, str]) -> bytes:
    return urlencode(data).encode()


class QuickBooksReportsClient:
    def __init__(self, credentials: QuickBooksCredentials, catalog: ReportCatalog):
        self.credentials = credentials
        self.catalog = catalog

    def exchange_code(self, entity: str, callback_url: str) -> None:
        """Trade the OAuth callback for tokens and store them on the company."""
        self.credentials.company(entity)
        if not self.credentials.oauth_ready():
            raise QuickBooksConfigError("App client id and secret are required before token exchange")

        parsed = urlparse(callback_url)
        params = parse_qs(parsed.query)
        code = (params.get("code") or [None])[0]
        realm_id = (params.get("realmId") or [None])[0]
        state = (params.get("state") or [None])[0]
        expected_state = self.credentials.oauth_state
        if not code or not realm_id:
            raise QuickBooksConfigError("Callback URL is missing code or realmId")
        if expected_state and state != expected_state:
            raise QuickBooksConfigError("OAuth state does not match the authorization URL")

        tokens = _request_json(
            TOKEN_URL,
            method="POST",
            headers={
                "Accept": "application/json",
                "Authorization": _basic_auth(self.credentials.client_id, self.credentials.client_secret),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            body=_form(
                {
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": self.credentials.redirect_uri,
                }
            ),
        )
        self._store_tokens(entity, realm_id, tokens)

    def refresh(self, entity: str) -> str:
        company = self.credentials.require_reports_ready(entity)
        tokens = _request_json(
            TOKEN_URL,
            method="POST",
            headers={
                "Accept": "application/json",
                "Authorization": _basic_auth(self.credentials.client_id, self.credentials.client_secret),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            body=_form(
                {
                    "grant_type": "refresh_token",
                    "refresh_token": company["refresh_token"],
                }
            ),
        )
        self._store_tokens(entity, company["realm_id"], tokens)
        return tokens["access_token"]

    def pull_report(
        self,
        entity: str,
        spec: ReportSpec,
        *,
        start_date: str | None,
        end_date: str | None,
        as_of: str | None,
        output_dir: Path,
    ) -> Path:
        company = self.credentials.require_reports_ready(entity)
        access_token = company.get("access_token") or self.refresh(entity)
        params = spec.query_params(
            start_date=start_date,
            end_date=end_date,
            as_of=as_of,
            minor_version=self.catalog.minor_version,
        )
        url = (
            f"{self.credentials.api_base}/v3/company/{company['realm_id']}"
            f"/reports/{spec.api_name}?{urlencode(params)}"
        )
        try:
            report = _request_json(
                url,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {access_token}",
                },
            )
        except QuickBooksConfigError as exc:
            if "HTTP 401" not in str(exc):
                raise
            access_token = self.refresh(entity)
            report = _request_json(
                url,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {access_token}",
                },
            )

        stamp = as_of or f"{start_date}_{end_date}"
        destination = output_dir / entity / f"{spec.key}_{stamp}.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        return destination

    def _store_tokens(self, entity: str, realm_id: str, tokens: dict[str, Any]) -> None:
        from .credentials import company_key

        self.credentials.set_value(company_key(entity, "REALM_ID"), realm_id)
        self.credentials.set_value(company_key(entity, "ACCESS_TOKEN"), tokens["access_token"])
        if tokens.get("refresh_token"):
            self.credentials.set_value(company_key(entity, "REFRESH_TOKEN"), tokens["refresh_token"])
        expires_in = int(tokens.get("expires_in") or 3600)
        self.credentials.set_value(
            company_key(entity, "ACCESS_TOKEN_EXPIRES_AT"),
            (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat(),
        )
        refresh_expires = tokens.get("x_refresh_token_expires_in")
        if refresh_expires:
            self.credentials.set_value(
                company_key(entity, "REFRESH_TOKEN_EXPIRES_AT"),
                (datetime.now(timezone.utc) + timedelta(seconds=int(refresh_expires))).date().isoformat(),
            )
