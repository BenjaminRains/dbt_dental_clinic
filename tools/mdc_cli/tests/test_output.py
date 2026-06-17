"""Tests for shared CLI output helpers."""

from mdc_cli.output import ascii_cli_text


def test_ascii_cli_text_replaces_unicode_punctuation():
    assert ascii_cli_text("a -> b") == "a -> b"
    assert ascii_cli_text("run -> etl status") == "run -> etl status"
    assert ascii_cli_text("missing - required") == "missing - required"
    assert ascii_cli_text("tunnel-db->127.0.0.1") == "tunnel-db->127.0.0.1"


def test_ascii_cli_text_normalizes_legacy_unicode():
    assert ascii_cli_text("a \u2192 b") == "a -> b"
    assert ascii_cli_text("missing \u2014 run") == "missing - run"

