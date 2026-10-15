"""Tests for the secrets pull chooser."""

import pytest

from mdc_cli.secrets_catalog import (
    SecretSelectionError,
    format_catalog,
    format_plan,
    parse_selection,
)


def test_catalog_lists_manual_rows():
    text = format_catalog({"clinic": "2026-10-05"})
    assert "Clinic RDS" in text
    assert "last rotated 2026-10-05" in text
    assert "last rotated unknown" in text
    assert "opendental_demo_user" in text
    assert "Anthropic" in text
    assert "Airflow admin" in text
    assert "[manual]" in text


def test_empty_selection_changes_nothing():
    assert parse_selection("") == []
    assert parse_selection("   ") == []


def test_parse_selection_accepts_numbers_and_names():
    assert parse_selection("2") == ["demo"]
    assert parse_selection("1, 2") == ["clinic", "demo"]
    assert parse_selection("demo clinic") == ["clinic", "demo"]


def test_manual_row_is_rejected():
    with pytest.raises(SecretSelectionError, match="manual"):
        parse_selection("3")
    with pytest.raises(SecretSelectionError, match="manual"):
        parse_selection("airflow")


def test_unknown_selection_is_rejected():
    with pytest.raises(SecretSelectionError, match="Unknown selection"):
        parse_selection("9")


def test_plan_names_files_and_hides_passwords():
    text = format_plan(["clinic", "demo"], {"demo": "2026-10-12"})
    assert "api/.env_api_clinic" in text
    assert "api/.env_api_demo" in text
    assert "last rotated 2026-10-12" in text
    assert "last rotated unknown" in text
    assert "mdc tunnel demo-db" in text
    assert "Passwords are not printed." in text
