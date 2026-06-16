"""Tests for consult-audio CLI commands (Phase 5.4)."""

from unittest.mock import patch

from typer.testing import CliRunner

from mdc_cli.consult_audio_env import ConsultAudioValidation
from mdc_cli.main import app

runner = CliRunner()


def test_consult_audio_validate_help():
    result = runner.invoke(app, ["consult-audio", "validate", "--help"])
    assert result.exit_code == 0


@patch(
    "mdc_cli.commands.consult_audio.validate_consult_audio",
    return_value=ConsultAudioValidation(ok=True, errors=(), warnings=()),
)
def test_consult_audio_validate_ok(mock_validate):
    result = runner.invoke(app, ["consult-audio", "validate"])
    assert result.exit_code == 0
    mock_validate.assert_called_once()


@patch(
    "mdc_cli.commands.consult_audio.validate_consult_audio",
    return_value=ConsultAudioValidation(
        ok=False,
        errors=("venv missing",),
        warnings=(),
    ),
)
def test_consult_audio_validate_fail(mock_validate):
    result = runner.invoke(app, ["consult-audio", "validate"])
    assert result.exit_code == 1


@patch("mdc_cli.commands.consult_audio.install_consult_audio_venv", return_value=0)
def test_consult_audio_install(mock_install):
    result = runner.invoke(app, ["consult-audio", "install"])
    assert result.exit_code == 0
    mock_install.assert_called_once()


@patch("mdc_cli.commands.consult_audio.run_consult_audio_pipeline", return_value=0)
def test_consult_audio_pipeline_status(mock_run):
    result = runner.invoke(app, ["consult-audio", "pipeline", "status"])
    assert result.exit_code == 0
    mock_run.assert_called_once_with("status")


@patch("mdc_cli.commands.consult_audio.run_consult_audio_pipeline", return_value=0)
def test_consult_audio_pipeline_run_chatgpt(mock_run):
    result = runner.invoke(app, ["consult-audio", "pipeline", "run", "--llm", "chatgpt"])
    assert result.exit_code == 0
    mock_run.assert_called_once_with("run", llm="chatgpt")


@patch("mdc_cli.commands.consult_audio.run_consult_audio_module", return_value=0)
def test_consult_audio_analyze(mock_run):
    result = runner.invoke(app, ["consult-audio", "analyze"])
    assert result.exit_code == 0
    mock_run.assert_called_once_with(
        ["scripts/llm_analysis_integration.py", "analyze"],
    )


@patch("mdc_cli.commands.consult_audio.run_consult_audio_module", return_value=0)
def test_consult_audio_run_passthrough(mock_run):
    result = runner.invoke(
        app,
        ["consult-audio", "run", "--", "python", "-m", "consult_audio_pipe.pipeline", "status"],
    )
    assert result.exit_code == 0
    mock_run.assert_called_once_with(
        ["-m", "consult_audio_pipe.pipeline", "status"],
    )


def test_consult_audio_pipeline_invalid_llm():
    result = runner.invoke(app, ["consult-audio", "pipeline", "run", "--llm", "gpt4"])
    assert result.exit_code == 2
