"""Tests for Snowsight URL helper."""

from __future__ import annotations

import pytest

from mdc_cli.snowflake_ui import snowsight_url


def test_snowsight_url_from_locator():
    assert (
        snowsight_url("igpxtll-rwc63998")
        == "https://igpxtll-rwc63998.snowflakecomputing.com"
    )


def test_snowsight_url_from_host():
    assert (
        snowsight_url("igpxtll-rwc63998.snowflakecomputing.com")
        == "https://igpxtll-rwc63998.snowflakecomputing.com"
    )


def test_snowsight_url_from_https():
    assert (
        snowsight_url("https://igpxtll-rwc63998.snowflakecomputing.com/")
        == "https://igpxtll-rwc63998.snowflakecomputing.com"
    )


def test_snowsight_url_override():
    assert (
        snowsight_url("ignored", override="https://app.snowflake.com/org/acct")
        == "https://app.snowflake.com/org/acct"
    )


def test_snowsight_url_empty():
    with pytest.raises(ValueError):
        snowsight_url("")
