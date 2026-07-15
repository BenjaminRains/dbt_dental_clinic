"""Tests for frontend commands (Phase 5.3)."""

from unittest.mock import patch

from typer.testing import CliRunner

from mdc_cli.main import app

runner = CliRunner()


def test_frontend_dev_help():
    result = runner.invoke(app, ["frontend", "dev", "--help"])
    assert result.exit_code == 0
    assert "Vite" in result.output
    assert "--app" in result.output


def _make_frontend_tree(tmp_path):
    repo = tmp_path / "repo"
    frontend = repo / "frontend"
    portfolio = frontend / "apps" / "portfolio"
    portfolio.mkdir(parents=True)
    (frontend / "package.json").write_text("{}", encoding="utf-8")
    (portfolio / "package.json").write_text('{"name":"@mdc/portfolio"}', encoding="utf-8")
    (frontend / "node_modules").mkdir()
    return repo, frontend


@patch("mdc_cli.commands.frontend.find_executable", return_value="/usr/bin/npm")
@patch("mdc_cli.commands.frontend._run_forever", return_value=0)
@patch("mdc_cli.commands.frontend.read_local_api_key_from_pem", return_value="key123")
def test_frontend_dev_runs_npm(mock_key, mock_run, mock_find, tmp_path, monkeypatch):
    repo, frontend = _make_frontend_tree(tmp_path)

    monkeypatch.setattr("mdc_cli.commands.frontend.FRONTEND_DIR", frontend)
    monkeypatch.setattr("mdc_cli.commands.frontend.REPO_ROOT", repo)

    result = runner.invoke(app, ["frontend", "dev"])
    assert result.exit_code == 0
    mock_run.assert_called_once()
    assert mock_run.call_args.args[0] == ["npm", "run", "dev", "-w", "@mdc/portfolio"]
    env_local = frontend / "apps" / "portfolio" / ".env.local"
    assert env_local.is_file()
    assert "VITE_IS_DEMO=true" in env_local.read_text(encoding="utf-8")


@patch("mdc_cli.commands.frontend.find_executable", return_value="/usr/bin/npm")
@patch("mdc_cli.commands.frontend._run_forever", return_value=0)
@patch("mdc_cli.commands.frontend.read_local_api_key_from_pem", return_value="key123")
def test_frontend_dev_clinic_app(mock_key, mock_run, mock_find, tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    frontend = repo / "frontend"
    clinic = frontend / "apps" / "clinic"
    clinic.mkdir(parents=True)
    (frontend / "package.json").write_text("{}", encoding="utf-8")
    (clinic / "package.json").write_text('{"name":"@mdc/clinic"}', encoding="utf-8")
    (frontend / "node_modules").mkdir()

    monkeypatch.setattr("mdc_cli.commands.frontend.FRONTEND_DIR", frontend)
    monkeypatch.setattr("mdc_cli.commands.frontend.REPO_ROOT", repo)

    result = runner.invoke(app, ["frontend", "dev", "--app", "clinic"])
    assert result.exit_code == 0
    mock_run.assert_called_once()
    assert mock_run.call_args.args[0] == ["npm", "run", "dev", "-w", "@mdc/clinic"]
    env_local = clinic / ".env.local"
    assert "VITE_IS_DEMO=false" in env_local.read_text(encoding="utf-8")


@patch("mdc_cli.commands.frontend.find_executable", return_value="/usr/bin/npm")
@patch("mdc_cli.commands.frontend._run_forever", return_value=0)
@patch("mdc_cli.commands.frontend.read_local_api_key_from_pem", return_value="key123")
def test_frontend_dev_npm_install_when_no_node_modules(
    mock_key, mock_run, mock_find, tmp_path, monkeypatch,
):
    repo = tmp_path / "repo"
    frontend = repo / "frontend"
    portfolio = frontend / "apps" / "portfolio"
    portfolio.mkdir(parents=True)
    (frontend / "package.json").write_text("{}", encoding="utf-8")
    (portfolio / "package.json").write_text('{"name":"@mdc/portfolio"}', encoding="utf-8")

    monkeypatch.setattr("mdc_cli.commands.frontend.FRONTEND_DIR", frontend)
    monkeypatch.setattr("mdc_cli.commands.frontend.REPO_ROOT", repo)

    result = runner.invoke(app, ["frontend", "dev"])
    assert result.exit_code == 0
    assert mock_run.call_count == 2
    assert mock_run.call_args_list[0].args[0] == ["npm", "install"]
    assert mock_run.call_args_list[1].args[0] == ["npm", "run", "dev", "-w", "@mdc/portfolio"]


@patch("mdc_cli.commands.deploy.deploy_frontend_target", return_value=0)
def test_deploy_frontend_clinic_target(mock_deploy):
    result = runner.invoke(app, ["deploy", "frontend", "--target", "clinic"])
    assert result.exit_code == 0
    mock_deploy.assert_called_once_with("clinic")


@patch("mdc_cli.commands.deploy.deploy_frontend_target", return_value=0)
def test_deploy_frontend_default_demo(mock_deploy):
    result = runner.invoke(app, ["deploy", "frontend"])
    assert result.exit_code == 0
    mock_deploy.assert_called_once_with("demo")


def test_deploy_frontend_invalid_target():
    result = runner.invoke(app, ["deploy", "frontend", "--target", "local"])
    assert result.exit_code == 2
