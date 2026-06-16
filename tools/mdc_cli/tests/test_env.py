"""Tests for mdc_cli.env helpers."""

from mdc_cli.env import build_child_env


def test_build_child_env_merges_without_mutating_parent(monkeypatch):
    monkeypatch.setenv("PARENT_ONLY", "keep")
    monkeypatch.setenv("SHARED", "parent-value")

    child = build_child_env({"SHARED": "child-value", "NEW_KEY": "added"})

    assert child["PARENT_ONLY"] == "keep"
    assert child["SHARED"] == "child-value"
    assert child["NEW_KEY"] == "added"
    assert __import__("os").environ["SHARED"] == "parent-value"
