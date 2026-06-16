"""Tests for mdc_cli.env helpers."""

from mdc_cli.env import build_child_env


def test_build_child_env_uses_isolated_base(monkeypatch):
    monkeypatch.setenv("PARENT_ONLY", "keep")
    monkeypatch.setenv("SHARED", "parent-value")
    monkeypatch.setenv("POSTGRES_ANALYTICS_HOST", "stale-parent-host")

    child = build_child_env({"SHARED": "child-value", "NEW_KEY": "added"})

    assert child["SHARED"] == "child-value"
    assert child["NEW_KEY"] == "added"
    assert "POSTGRES_ANALYTICS_HOST" not in child
    assert "PARENT_ONLY" not in child
    assert __import__("os").environ["SHARED"] == "parent-value"
