"""Tests for Phase 6 ETL env composition."""

import json
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from mdc_cli import etl_env, paths, postgres_env
from mdc_cli.paths import REPO_ROOT
from mdc_cli.secrets_manager import PasswordResolution


@pytest.fixture
def phase6_layout(tmp_path, monkeypatch):
    etl_dir = tmp_path / "etl_pipeline"
    dbt_dir = tmp_path / "dbt_dental_models"
    etl_dir.mkdir()
    dbt_dir.mkdir()
    monkeypatch.setattr(paths, "ETL_DIR", etl_dir)
    monkeypatch.setattr(paths, "DBT_DIR", dbt_dir)
    creds_path = tmp_path / "deployment_credentials.json"
    monkeypatch.setattr(postgres_env, "DEPLOYMENT_CREDENTIALS", creds_path)

    real_etl_root = str(REPO_ROOT / "etl_pipeline")
    if real_etl_root not in sys.path:
        sys.path.insert(0, real_etl_root)

    return etl_dir, dbt_dir, tmp_path


def _write_clinic_etl_file(etl_dir, *, include_stale_postgres: bool = True) -> None:
    lines = [
        "OPENDENTAL_SOURCE_HOST=192.168.1.10",
        "OPENDENTAL_SOURCE_PORT=3306",
        "OPENDENTAL_SOURCE_DB=opendental",
        "OPENDENTAL_SOURCE_USER=readonly",
        "OPENDENTAL_SOURCE_PASSWORD=src-pass",
        "MYSQL_REPLICATION_HOST=localhost",
        "MYSQL_REPLICATION_PORT=3305",
        "MYSQL_REPLICATION_DB=opendental_replication",
        "MYSQL_REPLICATION_USER=replication_user",
        "MYSQL_REPLICATION_PASSWORD=repl-pass",
    ]
    if include_stale_postgres:
        lines.extend(
            [
                "POSTGRES_ANALYTICS_HOST=localhost",
                "POSTGRES_ANALYTICS_PORT=5432",
                "POSTGRES_ANALYTICS_DB=opendental_analytics",
                "POSTGRES_ANALYTICS_USER=analytics_user",
                "POSTGRES_ANALYTICS_PASSWORD=STALE_PASSWORD",
            ]
        )
    (etl_dir / ".env_clinic").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_clinic_credentials(tmp_path) -> None:
    creds = {
        "clinic_database": {
            "postgresql": {
                "host": "rds.example.com",
                "port": 5432,
                "database": "opendental_analytics",
                "user": "analytics_user",
                "password": "json-password",
                "sslmode": "require",
            }
        }
    }
    (tmp_path / "deployment_credentials.json").write_text(
        json.dumps(creds),
        encoding="utf-8",
    )


@patch("mdc_cli.etl_env.overlay_clinic_rds_credentials")
def test_compose_etl_clinic_uses_json_not_stale_file_password(
    mock_overlay,
    phase6_layout,
):
    etl_dir, _, tmp_path = phase6_layout
    _write_clinic_etl_file(etl_dir, include_stale_postgres=True)
    _write_clinic_credentials(tmp_path)

    def _overlay(env, prefer_secrets_manager=True):
        merged = dict(env)
        merged["POSTGRES_ANALYTICS_PASSWORD"] = "live-secret-password"
        return merged, PasswordResolution(
            password="live-secret-password",
            source="secrets_manager:test",
        )

    mock_overlay.side_effect = _overlay

    env = etl_env.compose_etl_env_dict("clinic", "full", prefer_secrets_manager=False)

    assert env["POSTGRES_ANALYTICS_HOST"] == "rds.example.com"
    assert env["POSTGRES_ANALYTICS_PASSWORD"] == "live-secret-password"
    assert env["POSTGRES_ANALYTICS_PASSWORD"] != "STALE_PASSWORD"
    assert env["POSTGRES_ANALYTICS_SCHEMA"] == "raw"
    assert env["OPENDENTAL_SOURCE_HOST"] == "192.168.1.10"
    assert env.get("PGSSLMODE") == "require"


@patch("mdc_cli.etl_env.overlay_clinic_rds_credentials")
def test_compose_etl_clinic_tunnel_db(mock_overlay, phase6_layout):
    etl_dir, _, tmp_path = phase6_layout
    _write_clinic_etl_file(etl_dir, include_stale_postgres=False)
    _write_clinic_credentials(tmp_path)
    mock_overlay.side_effect = lambda env, prefer_secrets_manager=True: (
        env,
        PasswordResolution(
            password=env["POSTGRES_ANALYTICS_PASSWORD"],
            source="env_file",
        ),
    )

    env = etl_env.compose_etl_env_dict("clinic", "full", tunnel_db=True, tunnel_port=5433)

    assert env["POSTGRES_ANALYTICS_HOST"] == "127.0.0.1"
    assert env["POSTGRES_ANALYTICS_PORT"] == "5433"
    assert env.get("PGSSLMODE") == "prefer"


def test_compose_etl_local_uses_dbt_env_local(phase6_layout):
    etl_dir, dbt_dir, _ = phase6_layout
    (etl_dir / ".env_local").write_text(
        "\n".join(
            [
                "MYSQL_REPLICATION_HOST=localhost",
                "MYSQL_REPLICATION_PORT=3305",
                "MYSQL_REPLICATION_DB=repl",
                "MYSQL_REPLICATION_USER=ru",
                "MYSQL_REPLICATION_PASSWORD=rp",
                "POSTGRES_ANALYTICS_PASSWORD=ignored-stale",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (dbt_dir / ".env_local").write_text(
        "\n".join(
            [
                "POSTGRES_ANALYTICS_HOST=localhost",
                "POSTGRES_ANALYTICS_PORT=5432",
                "POSTGRES_ANALYTICS_DB=opendental_analytics",
                "POSTGRES_ANALYTICS_USER=analytics_user",
                "POSTGRES_ANALYTICS_PASSWORD=warehouse-pass",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    env = etl_env.compose_etl_env_dict("local", "load")

    assert env["POSTGRES_ANALYTICS_PASSWORD"] == "warehouse-pass"
    assert env["MYSQL_REPLICATION_DB"] == "repl"


def test_compose_etl_local_ignores_shell_rds_exports(phase6_layout, monkeypatch):
    """Stale clinic POSTGRES_* in the parent shell must not override local warehouse file."""
    etl_dir, dbt_dir, _ = phase6_layout
    (etl_dir / ".env_local").write_text(
        "\n".join(
            [
                "MYSQL_REPLICATION_HOST=localhost",
                "MYSQL_REPLICATION_PORT=3305",
                "MYSQL_REPLICATION_DB=repl",
                "MYSQL_REPLICATION_USER=ru",
                "MYSQL_REPLICATION_PASSWORD=rp",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (dbt_dir / ".env_local").write_text(
        "\n".join(
            [
                "POSTGRES_ANALYTICS_HOST=localhost",
                "POSTGRES_ANALYTICS_PORT=5432",
                "POSTGRES_ANALYTICS_DB=opendental_analytics",
                "POSTGRES_ANALYTICS_USER=analytics_user",
                "POSTGRES_ANALYTICS_PASSWORD=warehouse-pass",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setenv(
        "POSTGRES_ANALYTICS_HOST",
        "dental-clinic-analytics.c6zwscckihl1.us-east-1.rds.amazonaws.com",
    )
    monkeypatch.setenv("POSTGRES_ANALYTICS_PORT", "5432")
    monkeypatch.setenv("POSTGRES_ANALYTICS_DB", "opendental_analytics")
    monkeypatch.setenv("POSTGRES_ANALYTICS_USER", "analytics_user")
    monkeypatch.setenv("POSTGRES_ANALYTICS_PASSWORD", "clinic-rds-secret")

    env = etl_env.compose_etl_env_dict("local", "load")

    assert env["POSTGRES_ANALYTICS_HOST"] == "localhost"
    assert env["POSTGRES_ANALYTICS_PASSWORD"] == "warehouse-pass"


def test_deprecated_etl_analytics_keys(phase6_layout):
    etl_dir, _, _ = phase6_layout
    _write_clinic_etl_file(etl_dir, include_stale_postgres=True)
    keys = postgres_env.deprecated_etl_analytics_keys("clinic")
    assert "POSTGRES_ANALYTICS_PASSWORD" in keys
