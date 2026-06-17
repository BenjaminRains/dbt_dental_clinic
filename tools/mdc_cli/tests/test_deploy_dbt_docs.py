"""Tests for mdc deploy dbt-docs (Phase 5.3)."""

import re
from unittest.mock import patch

from typer.testing import CliRunner

from mdc_cli.main import app

runner = CliRunner()


def _plain_cli_output(text: str) -> str:
    """Strip Rich ANSI codes so help text assertions are stable in CI."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def test_deploy_dbt_docs_help():
    result = runner.invoke(app, ["deploy", "dbt-docs", "--help"])
    assert result.exit_code == 0
    plain = _plain_cli_output(result.output)
    assert "dbt-docs" in plain
    assert "skip-generate" in plain


@patch("mdc_cli.commands.deploy.deploy_dbt_docs", return_value=0)
def test_deploy_dbt_docs_default_local(mock_deploy):
    result = runner.invoke(app, ["deploy", "dbt-docs"])
    assert result.exit_code == 0
    mock_deploy.assert_called_once_with("local", skip_generate=False)


@patch("mdc_cli.commands.deploy.deploy_dbt_docs", return_value=0)
def test_deploy_dbt_docs_skip_generate(mock_deploy):
    result = runner.invoke(app, ["deploy", "dbt-docs", "--skip-generate"])
    assert result.exit_code == 0
    mock_deploy.assert_called_once_with("local", skip_generate=True)


@patch("mdc_cli.commands.deploy.deploy_dbt_docs", return_value=0)
def test_deploy_dbt_docs_clinic_env(mock_deploy):
    result = runner.invoke(app, ["deploy", "dbt-docs", "--env", "clinic"])
    assert result.exit_code == 0
    mock_deploy.assert_called_once_with("clinic", skip_generate=False)


def test_deploy_dbt_docs_invalid_env():
    result = runner.invoke(app, ["deploy", "dbt-docs", "--env", "test"])
    assert result.exit_code == 2
