"""Tests for isolated subprocess env (Phase 4.3)."""

import os

from mdc_cli.run_helper import (
    apply_tunnel_db_overrides,
    build_isolated_child_env,
    load_env_dict_isolated,
    scrub_parent_stage_env,
    _ensure_dbt_target,
)


def test_apply_tunnel_db_overrides_sets_ssl_prefer():
    env = apply_tunnel_db_overrides(
        {
            "POSTGRES_ANALYTICS_HOST": "rds.example.com",
            "POSTGRES_ANALYTICS_PORT": "5432",
            "POSTGRES_ANALYTICS_SSLMODE": "require",
        },
        local_port=5433,
    )
    assert env["POSTGRES_ANALYTICS_HOST"] == "127.0.0.1"
    assert env["POSTGRES_ANALYTICS_PORT"] == "5433"
    assert env["POSTGRES_ANALYTICS_SSLMODE"] == "prefer"
    assert env["PGSSLMODE"] == "prefer"


def test_scrub_parent_stage_env_removes_stage_keys_only(monkeypatch):
    monkeypatch.setenv("POSTGRES_ANALYTICS_HOST", "stale")
    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.setenv("UNRELATED", "keep")

    with scrub_parent_stage_env():
        assert "POSTGRES_ANALYTICS_HOST" not in os.environ
        assert os.environ["UNRELATED"] == "keep"

    assert os.environ["POSTGRES_ANALYTICS_HOST"] == "stale"


def test_build_isolated_child_env_does_not_inherit_stale_stage_vars(monkeypatch):
    monkeypatch.setenv("POSTGRES_ANALYTICS_HOST", "stale-parent")
    monkeypatch.setenv("PATH", "/bin")

    child = build_isolated_child_env(
        {"POSTGRES_ANALYTICS_HOST": "from-settings", "API_ENVIRONMENT": "local"}
    )

    assert child["POSTGRES_ANALYTICS_HOST"] == "from-settings"
    assert child["API_ENVIRONMENT"] == "local"
    assert child["PATH"] == "/bin"


def test_ensure_dbt_target_appends_when_missing():
    assert _ensure_dbt_target(["run"], "clinic") == ["run", "--target", "clinic"]


def test_ensure_dbt_target_respects_existing():
    args = ["run", "--target", "demo"]
    assert _ensure_dbt_target(args, "clinic") == args


def test_is_local_tcp_port_open_false_when_closed():
    from mdc_cli.run_helper import is_local_tcp_port_open

    assert is_local_tcp_port_open("127.0.0.1", 1, timeout=0.2) is False


def test_load_env_dict_isolated_scrubs_before_load(monkeypatch):
    monkeypatch.setenv("API_ENVIRONMENT", "clinic")
    monkeypatch.setenv("POSTGRES_ANALYTICS_HOST", "should-not-win")

    called = {}

    def fake_load(component, stage, profile=None, tunnel_db=False, tunnel_port=None):
        called["host_during_load"] = os.environ.get("POSTGRES_ANALYTICS_HOST")
        return {"API_ENVIRONMENT": stage, "POSTGRES_ANALYTICS_HOST": "from-loader"}

    monkeypatch.setattr("mdc_cli.env.load_env_dict", fake_load)
    result = load_env_dict_isolated("api", "local")

    assert called["host_during_load"] is None
    assert result["POSTGRES_ANALYTICS_HOST"] == "from-loader"
    assert os.environ["POSTGRES_ANALYTICS_HOST"] == "should-not-win"
