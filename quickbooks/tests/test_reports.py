"""Unit tests for the QuickBooks reports credential gate and report catalog."""

import json
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

import pytest

from quickbooks.client import QuickBooksReportsClient
from quickbooks.credentials import (
    QuickBooksConfigError,
    QuickBooksCredentials,
    resolve_stage,
)
from quickbooks.reports import load_report_catalog


def _write_env(path: Path, **overrides: str) -> Path:
    values = {
        "QUICKBOOKS_ENVIRONMENT": "test",
        "QUICKBOOKS_INTEGRATION_USER_ROLE": "view_company_reports",
        "QUICKBOOKS_INTEGRATION_USER_FIRST_NAME": "Ada",
        "QUICKBOOKS_INTEGRATION_USER_LAST_NAME": "Lovelace",
        "QUICKBOOKS_INTEGRATION_USER_EMAIL": "reports@example.com",
        "QUICKBOOKS_INTEGRATION_USER_PASSWORD": "secret-value",
        "QUICKBOOKS_CLIENT_ID": "client-id",
        "QUICKBOOKS_CLIENT_SECRET": "client-secret",
        "QUICKBOOKS_REDIRECT_URI": "http://localhost:8080/callback",
        "MDC_QUICKBOOKS_REALM_ID": "123",
        "MDC_QUICKBOOKS_REFRESH_TOKEN": "refresh",
        "MDC_QUICKBOOKS_ACCESS_TOKEN": "",
        "GLIC_QUICKBOOKS_REALM_ID": "456",
        "GLIC_QUICKBOOKS_REFRESH_TOKEN": "glic-refresh",
    }
    values.update(overrides)
    lines = [f"{key}={value}" for key, value in values.items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _load(path: Path, environment: str = "test") -> QuickBooksCredentials:
    return QuickBooksCredentials.load(environment=environment, path=path)


def test_missing_fields_omit_secret_values(tmp_path: Path):
    path = _write_env(tmp_path / ".env_test")
    credentials = _load(path)
    credentials.values["QUICKBOOKS_INTEGRATION_USER_EMAIL"] = ""
    credentials.values["MDC_QUICKBOOKS_REFRESH_TOKEN"] = ""

    gaps = credentials.missing()
    text = "\n".join(credentials.status_lines())

    assert "QUICKBOOKS_INTEGRATION_USER_EMAIL" in gaps
    assert "MDC_QUICKBOOKS_REFRESH_TOKEN" in gaps
    assert "secret-value" not in text
    assert "client-secret" not in text


def test_authorization_url_uses_accounting_scope_and_hides_secret(tmp_path: Path):
    credentials = _load(_write_env(tmp_path / ".env_clinic"), environment="clinic")
    url = credentials.authorization_url("MDC", "state-token")

    assert url.startswith("https://appcenter.intuit.com/connect/oauth2?")
    assert "scope=com.intuit.quickbooks.accounting" in url
    assert "client_id=client-id" in url
    assert "client-secret" not in url
    assert credentials.api_base == "https://quickbooks.api.intuit.com"


def test_catalog_default_reports_match_date_modes():
    catalog = load_report_catalog()

    assert catalog.defaults == (
        "profit_and_loss",
        "balance_sheet",
        "aged_receivables",
        "customer_income",
    )
    assert catalog.get("profit_and_loss").api_name == "ProfitAndLoss"
    assert catalog.get("customer_income").query_params(
        start_date="2026-09-01",
        end_date="2026-09-30",
        as_of=None,
        minor_version=75,
    )["end_date"] == "2026-09-30"
    with pytest.raises(ValueError, match="--as-of"):
        catalog.get("aged_receivables").query_params(
            start_date="2026-09-01",
            end_date="2026-09-30",
            as_of=None,
            minor_version=75,
        )


def test_stage_names_match_the_repo(tmp_path: Path):
    clinic = _load(_write_env(tmp_path / ".env_clinic"), environment="clinic")
    local = _load(_write_env(tmp_path / ".env_local"), environment="local")
    assert clinic.api_base == "https://quickbooks.api.intuit.com"
    assert local.api_base == "https://sandbox-quickbooks.api.intuit.com"
    assert "intuit keys: Production" in "\n".join(clinic.status_lines())
    assert "intuit keys: Development" in "\n".join(local.status_lines())
    with pytest.raises(QuickBooksConfigError, match="clinic"):
        resolve_stage("production")


def test_process_environment_wins_over_stage_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    path = _write_env(tmp_path / ".env_test", QUICKBOOKS_CLIENT_ID="from-file")
    monkeypatch.setenv("QUICKBOOKS_CLIENT_ID", "from-os")
    credentials = _load(path)
    assert credentials.client_id == "from-os"
    assert "from-os" not in "\n".join(credentials.status_lines())
    assert "from-file" not in "\n".join(credentials.status_lines())


def test_exchange_stores_realm_without_keeping_callback_in_status(tmp_path: Path):
    credentials = _load(_write_env(tmp_path / ".env_test"))
    credentials.values["MDC_QUICKBOOKS_REALM_ID"] = ""
    credentials.values["MDC_QUICKBOOKS_REFRESH_TOKEN"] = ""
    credentials.values["QUICKBOOKS_OAUTH_STATE"] = "state-token"
    client = QuickBooksReportsClient(credentials, load_report_catalog())

    def fake_request(url, **kwargs):
        assert "tokens/bearer" in url
        return {
            "access_token": "access-new",
            "refresh_token": "refresh-new",
            "expires_in": 3600,
            "x_refresh_token_expires_in": 86400,
        }

    with patch(
        "quickbooks.client._request_json",
        side_effect=fake_request,
    ):
        client.exchange_code(
            "MDC",
            "http://localhost:8080/callback?code=abc&state=state-token&realmId=999",
        )

    reloaded = QuickBooksCredentials.load(environment="test", path=credentials.path)
    assert reloaded.get("MDC_QUICKBOOKS_REALM_ID") == "999"
    assert reloaded.get("MDC_QUICKBOOKS_REFRESH_TOKEN") == "refresh-new"
    assert "access-new" not in "\n".join(reloaded.status_lines())


def test_pull_refreshes_when_access_token_is_rejected(tmp_path: Path):
    credentials = _load(_write_env(tmp_path / ".env_test"))
    credentials.values["MDC_QUICKBOOKS_ACCESS_TOKEN"] = "stale"
    catalog = load_report_catalog()
    client = QuickBooksReportsClient(credentials, catalog)
    calls = {"n": 0}

    def fake_request(url, **kwargs):
        calls["n"] += 1
        if "tokens/bearer" in url:
            return {"access_token": "fresh", "refresh_token": "refresh-2", "expires_in": 3600}
        if calls["n"] == 1:
            raise QuickBooksConfigError("QuickBooks HTTP 401: expired")
        assert kwargs["headers"]["Authorization"] == "Bearer fresh"
        return {"Header": {"ReportName": "ProfitAndLoss"}, "Rows": {}}

    with patch(
        "quickbooks.client._request_json",
        side_effect=fake_request,
    ):
        written = client.pull_report(
            "MDC",
            catalog.get("profit_and_loss"),
            start_date="2026-09-01",
            end_date="2026-09-30",
            as_of=None,
            output_dir=tmp_path / "out",
        )

    assert written.name == "profit_and_loss_2026-09-01_2026-09-30.json"
    assert json.loads(written.read_text(encoding="utf-8"))["Header"]["ReportName"] == "ProfitAndLoss"
    assert credentials.get("MDC_QUICKBOOKS_ACCESS_TOKEN") == "fresh"


def test_http_error_does_not_include_request_body(tmp_path: Path):
    credentials = _load(_write_env(tmp_path / ".env_test"))
    client = QuickBooksReportsClient(credentials, load_report_catalog())

    class _Boom(HTTPError):
        def __init__(self):
            pass

        def read(self):
            return b"nope"

        code = 400

    with patch("quickbooks.client.urlopen", side_effect=_Boom()):
        with pytest.raises(QuickBooksConfigError, match="HTTP 400"):
            client.refresh("MDC")
