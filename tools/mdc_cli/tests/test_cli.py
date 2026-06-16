"""CLI smoke tests."""

from typer.testing import CliRunner

from mdc_cli.main import app

runner = CliRunner()


def test_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "status" in result.stdout
    assert "api" in result.stdout


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "mdc 0.1.0" in result.stdout


def test_status_runs():
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    assert "mdc status" in result.stdout
    assert "api" in result.stdout
    assert "etl" in result.stdout


def test_status_env_filter():
    result = runner.invoke(app, ["status", "--env", "local"])
    assert result.exit_code == 0
    assert "local" in result.stdout
    assert "clinic" not in result.stdout.split("Config")[0]


def test_status_invalid_env():
    result = runner.invoke(app, ["status", "--env", "not-a-stage"])
    assert result.exit_code != 0
    combined = result.stdout + result.stderr
    assert "Unsupported stage" in combined


def test_api_stub_exits_nonzero():
    result = runner.invoke(app, ["api", "run", "--env", "local"])
    assert result.exit_code == 2
    combined = result.stdout + result.stderr
    assert "Phase 4.3" in combined
