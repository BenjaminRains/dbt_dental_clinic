"""Tests for mdc_cli.dbt_env loader."""

import json

import pytest

from mdc_cli import dbt_env, paths, postgres_env

DBT_ENV_KEYS = (
    "POSTGRES_ANALYTICS_HOST",
    "POSTGRES_ANALYTICS_PORT",
    "POSTGRES_ANALYTICS_DB",
    "POSTGRES_ANALYTICS_USER",
    "POSTGRES_ANALYTICS_PASSWORD",
    "DEMO_POSTGRES_HOST",
    "DEMO_POSTGRES_PORT",
    "DEMO_POSTGRES_DB",
    "DEMO_POSTGRES_USER",
    "DEMO_POSTGRES_PASSWORD",
)


@pytest.fixture
def clean_dbt_os_env(monkeypatch):
    for key in DBT_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    yield


def _write_local_env(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "DBT_DIR", tmp_path)
    env_file = tmp_path / ".env_local"
    env_file.write_text(
        "\n".join(
            [
                "POSTGRES_ANALYTICS_HOST=localhost",
                "POSTGRES_ANALYTICS_PORT=5432",
                "POSTGRES_ANALYTICS_DB=analytics_db",
                "POSTGRES_ANALYTICS_USER=analytics_user",
                "POSTGRES_ANALYTICS_PASSWORD=secret",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return env_file


def test_load_dbt_env_dict_local(tmp_path, monkeypatch, clean_dbt_os_env):
    _write_local_env(tmp_path, monkeypatch)
    data = dbt_env.load_dbt_env_dict("local")
    assert data["DBT_TARGET"] == "local"
    assert data["DBT_PROFILES_DIR"] == str(tmp_path)
    assert data["POSTGRES_ANALYTICS_DB"] == "analytics_db"


def test_load_dbt_env_dict_clinic_from_file(tmp_path, monkeypatch, clean_dbt_os_env):
    monkeypatch.setattr(paths, "DBT_DIR", tmp_path)
    creds_path = tmp_path / "missing.json"
    monkeypatch.setattr(dbt_env, "DEPLOYMENT_CREDENTIALS", creds_path)
    monkeypatch.setattr(postgres_env, "DEPLOYMENT_CREDENTIALS", creds_path)
    env_file = tmp_path / ".env_clinic"
    env_file.write_text(
        "\n".join(
            [
                "POSTGRES_ANALYTICS_HOST=clinic.example.com",
                "POSTGRES_ANALYTICS_PORT=5432",
                "POSTGRES_ANALYTICS_DB=opendental_analytics",
                "POSTGRES_ANALYTICS_USER=analytics_user",
                "POSTGRES_ANALYTICS_PASSWORD=secret",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    data = dbt_env.load_dbt_env_dict("clinic")
    assert data["POSTGRES_ANALYTICS_HOST"] == "clinic.example.com"


def test_load_dbt_env_dict_clinic_from_credentials(tmp_path, monkeypatch, clean_dbt_os_env):
    monkeypatch.setattr(paths, "DBT_DIR", tmp_path)
    creds_path = tmp_path / "deployment_credentials.json"
    monkeypatch.setattr(dbt_env, "DEPLOYMENT_CREDENTIALS", creds_path)
    monkeypatch.setattr(postgres_env, "DEPLOYMENT_CREDENTIALS", creds_path)
    creds_path.write_text(
        json.dumps(
            {
                "clinic_database": {
                    "postgresql": {
                        "host": "rds.example.com",
                        "port": 5432,
                        "database": "opendental_analytics",
                        "user": "analytics_user",
                        "password": "secret",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    data = dbt_env.load_dbt_env_dict("clinic")
    assert data["POSTGRES_ANALYTICS_HOST"] == "rds.example.com"
    assert data["POSTGRES_ANALYTICS_SCHEMA"] == "dbt"
    assert data["POSTGRES_ANALYTICS_SSLMODE"] == "require"


def test_load_dbt_env_dict_clinic_from_backend_api_reference(tmp_path, monkeypatch, clean_dbt_os_env):
    monkeypatch.setattr(paths, "DBT_DIR", tmp_path)
    creds_path = tmp_path / "deployment_credentials.json"
    monkeypatch.setattr(dbt_env, "DEPLOYMENT_CREDENTIALS", creds_path)
    monkeypatch.setattr(postgres_env, "DEPLOYMENT_CREDENTIALS", creds_path)
    creds_path.write_text(
        json.dumps(
            {
                "backend_api": {
                    "clinic_database_reference": {
                        "rds": {
                            "database_connections": {
                                "opendental_analytics": {
                                    "host": "nested.example.com",
                                    "port": 5432,
                                    "database": "opendental_analytics",
                                    "user": "analytics_user",
                                    "password": "secret",
                                }
                            }
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    data = dbt_env.load_dbt_env_dict("clinic")
    assert data["POSTGRES_ANALYTICS_HOST"] == "nested.example.com"


def test_load_dbt_env_dict_demo_localhost_defaults(tmp_path, monkeypatch, clean_dbt_os_env):
    monkeypatch.setattr(paths, "DBT_DIR", tmp_path)
    creds_path = tmp_path / "deployment_credentials.json"
    monkeypatch.setattr(dbt_env, "DEPLOYMENT_CREDENTIALS", creds_path)
    monkeypatch.setattr(postgres_env, "DEPLOYMENT_CREDENTIALS", creds_path)
    monkeypatch.setattr(paths, "DEPLOYMENT_CREDENTIALS", creds_path)
    creds_path.write_text(
        json.dumps(
            {
                "demo_database": {
                    "postgresql": {
                        "database": "opendental_demo",
                        "user": "demo_user",
                        "password": "demo_pass",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    data = dbt_env.load_dbt_env_dict("demo")
    assert data["DEMO_POSTGRES_HOST"] == "localhost"
    assert data["DEMO_POSTGRES_PORT"] == "5434"
    assert data["DEMO_POSTGRES_DB"] == "opendental_demo"


def test_load_dbt_env_dict_local_missing_file(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "DBT_DIR", tmp_path)
    with pytest.raises(ValueError, match="No local warehouse env found"):
        dbt_env.load_dbt_env_dict("local")


def test_local_file_wins_over_os_env(tmp_path, monkeypatch):
    """Local warehouse file is authoritative — shell POSTGRES_* must not override."""
    _write_local_env(tmp_path, monkeypatch)
    monkeypatch.setenv("POSTGRES_ANALYTICS_HOST", "os-host-wins")
    data = dbt_env.load_dbt_env_dict("local")
    assert data["POSTGRES_ANALYTICS_HOST"] == "localhost"
