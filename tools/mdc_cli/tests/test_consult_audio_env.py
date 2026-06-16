"""Tests for consult_audio_env (Phase 5.4)."""

from pathlib import Path

from mdc_cli.consult_audio_env import (
    _is_real_secret,
    _parse_dotenv_file,
    load_consult_audio_env_dict,
    validate_consult_audio,
)


def test_is_real_secret_rejects_placeholders():
    assert not _is_real_secret("your_openai_api_key_here")
    assert _is_real_secret("sk-real-key-12345")


def test_parse_dotenv_file(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "OPENAI_API_KEY=from-file\n# comment\nANTHROPIC_API_KEY=anthropic\n",
        encoding="utf-8",
    )
    parsed = _parse_dotenv_file(env_file)
    assert parsed["OPENAI_API_KEY"] == "from-file"
    assert parsed["ANTHROPIC_API_KEY"] == "anthropic"


def test_load_consult_audio_env_os_wins(tmp_path: Path, monkeypatch):
    consult_dir = tmp_path / "consult_audio_pipe"
    consult_dir.mkdir()
    (consult_dir / ".env").write_text("OPENAI_API_KEY=file-key\n", encoding="utf-8")

    monkeypatch.setattr("mdc_cli.consult_audio_env.CONSULT_AUDIO_DIR", consult_dir)
    monkeypatch.setattr(
        "mdc_cli.consult_audio_env.CONSULT_AUDIO_ENV_FILE",
        consult_dir / ".env",
    )
    monkeypatch.setenv("OPENAI_API_KEY", "os-key")

    env = load_consult_audio_env_dict()
    assert env["OPENAI_API_KEY"] == "os-key"


def test_validate_consult_audio_openai_only_warns_anthropic(tmp_path: Path, monkeypatch):
    consult_dir = tmp_path / "consult_audio_pipe"
    consult_dir.mkdir()
    (consult_dir / "requirements.txt").write_text("requests\n", encoding="utf-8")
    (consult_dir / ".env").write_text("OPENAI_API_KEY=sk-test\n", encoding="utf-8")
    venv_python = consult_dir / "venv" / "Scripts" / "python.exe"
    venv_python.parent.mkdir(parents=True)
    venv_python.touch()

    monkeypatch.setattr("mdc_cli.consult_audio_env.CONSULT_AUDIO_DIR", consult_dir)
    monkeypatch.setattr(
        "mdc_cli.consult_audio_env.CONSULT_AUDIO_ENV_FILE",
        consult_dir / ".env",
    )
    monkeypatch.setattr("mdc_cli.consult_audio_env.REQUIREMENTS_FILE", consult_dir / "requirements.txt")
    monkeypatch.setattr(
        "mdc_cli.consult_audio_env.discover_consult_audio_python",
        lambda: venv_python,
    )

    result = validate_consult_audio()
    assert result.ok
    assert any("ANTHROPIC_API_KEY" in w for w in result.warnings)


def test_validate_consult_audio_missing_venv(tmp_path: Path, monkeypatch):
    consult_dir = tmp_path / "consult_audio_pipe"
    consult_dir.mkdir()
    (consult_dir / "requirements.txt").write_text("requests\n", encoding="utf-8")
    (consult_dir / ".env").write_text("OPENAI_API_KEY=sk-test\n", encoding="utf-8")

    monkeypatch.setattr("mdc_cli.consult_audio_env.CONSULT_AUDIO_DIR", consult_dir)
    monkeypatch.setattr(
        "mdc_cli.consult_audio_env.CONSULT_AUDIO_ENV_FILE",
        consult_dir / ".env",
    )
    monkeypatch.setattr("mdc_cli.consult_audio_env.REQUIREMENTS_FILE", consult_dir / "requirements.txt")
    monkeypatch.setattr("mdc_cli.consult_audio_env.discover_consult_audio_python", lambda: None)

    result = validate_consult_audio()
    assert not result.ok
    assert any("install" in e for e in result.errors)
