"""Consult audio pipe venv and .env loading (Phase 5.4)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import typer

from mdc_cli.paths import CONSULT_AUDIO_DIR, discover_consult_audio_python
from mdc_cli.process_util import find_executable, run_subprocess
from mdc_cli.run_helper import run_isolated, venv_root_from_python

CONSULT_AUDIO_ENV_FILE = CONSULT_AUDIO_DIR / ".env"
CONSULT_AUDIO_ENV_TEMPLATE = CONSULT_AUDIO_DIR / ".env.template"
REQUIREMENTS_FILE = CONSULT_AUDIO_DIR / "requirements.txt"
VENV_DIR = CONSULT_AUDIO_DIR / "venv"

API_KEY_VARS = ("OPENAI_API_KEY", "ANTHROPIC_API_KEY")
OPTIONAL_ENV_VARS = ("LOCAL_LLM_URL",)

_PLACEHOLDER_RE = re.compile(
    r"your_|changeme|placeholder|xxx|insert_|replace_",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ConsultAudioValidation:
    ok: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]


def _parse_dotenv_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        hash_idx = val.find("#")
        if hash_idx >= 0:
            val = val[:hash_idx].strip()
        if key and val:
            values[key] = val
    return values


def _is_real_secret(value: str) -> bool:
    stripped = value.strip()
    if not stripped:
        return False
    if _PLACEHOLDER_RE.search(stripped):
        return False
    return True


def load_consult_audio_env_dict() -> dict[str, str]:
    """Child env from consult_audio_pipe/.env; OS env wins (override=False)."""
    file_values = _parse_dotenv_file(CONSULT_AUDIO_ENV_FILE)
    tracked_keys = set(file_values) | set(API_KEY_VARS) | set(OPTIONAL_ENV_VARS)

    env: dict[str, str] = {}
    for key, value in file_values.items():
        env[key] = value
    for key in tracked_keys:
        os_val = os.environ.get(key)
        if os_val:
            env[key] = os_val
    return env


def require_consult_audio_python() -> Path:
    python = discover_consult_audio_python()
    if python is None:
        typer.echo(
            "No consult_audio_pipe/venv found. Run: mdc consult-audio install",
            err=True,
        )
        raise typer.Exit(code=2)
    return python


def _venv_pip(venv_root: Path) -> Path:
    if os.name == "nt":
        return venv_root / "Scripts" / "pip.exe"
    return venv_root / "bin" / "pip"


def install_consult_audio_venv() -> int:
    if not CONSULT_AUDIO_DIR.is_dir():
        typer.echo(f"consult_audio_pipe directory not found: {CONSULT_AUDIO_DIR}", err=True)
        return 1
    if not REQUIREMENTS_FILE.is_file():
        typer.echo(f"Missing {REQUIREMENTS_FILE.name} in consult_audio_pipe/", err=True)
        return 1

    python = find_executable("python") or find_executable("python3")
    if python is None:
        typer.echo("python not found on PATH. Install Python 3.8+.", err=True)
        return 127

    if not VENV_DIR.is_dir():
        typer.echo("Creating consult_audio_pipe/venv...")
        code = run_subprocess(
            [python, "-m", "venv", str(VENV_DIR)],
            cwd=CONSULT_AUDIO_DIR,
        )
        if code != 0:
            typer.echo("Failed to create virtual environment.", err=True)
            return code

    venv_python = discover_consult_audio_python()
    if venv_python is None:
        typer.echo("venv created but python executable not found.", err=True)
        return 1

    venv_root = venv_root_from_python(venv_python)
    pip = _venv_pip(venv_root)
    if not pip.is_file():
        typer.echo(f"pip not found in venv: {pip}", err=True)
        return 1

    typer.echo("Installing consult_audio_pipe dependencies (may take several minutes)...")
    code = run_subprocess(
        [str(pip), "install", "-r", str(REQUIREMENTS_FILE)],
        cwd=CONSULT_AUDIO_DIR,
    )
    if code != 0:
        typer.echo("pip install failed.", err=True)
        return code

    typer.echo("Consult audio venv ready.")
    return 0


def validate_consult_audio() -> ConsultAudioValidation:
    errors: list[str] = []
    warnings: list[str] = []

    if not CONSULT_AUDIO_DIR.is_dir():
        errors.append(f"consult_audio_pipe directory not found: {CONSULT_AUDIO_DIR}")
        return ConsultAudioValidation(ok=False, errors=tuple(errors), warnings=tuple(warnings))

    if not REQUIREMENTS_FILE.is_file():
        errors.append("consult_audio_pipe/requirements.txt not found")

    venv_python = discover_consult_audio_python()
    if venv_python is None:
        errors.append("venv missing — run: mdc consult-audio install")
    else:
        typer.echo(f"venv python: {venv_python}")

    if not CONSULT_AUDIO_ENV_FILE.is_file():
        warnings.append(
            "consult_audio_pipe/.env not found — copy .env.template to .env for API keys"
        )
    else:
        env = load_consult_audio_env_dict()
        has_openai = _is_real_secret(env.get("OPENAI_API_KEY", ""))
        has_anthropic = _is_real_secret(env.get("ANTHROPIC_API_KEY", ""))
        if not has_openai and not has_anthropic:
            errors.append(
                "Set OPENAI_API_KEY or ANTHROPIC_API_KEY in consult_audio_pipe/.env "
                "(or environment)"
            )
        else:
            if has_openai:
                typer.echo("OPENAI_API_KEY: ok")
            else:
                warnings.append(
                    "OPENAI_API_KEY missing — required for pipeline run --llm chatgpt"
                )
            if has_anthropic:
                typer.echo("ANTHROPIC_API_KEY: ok")
            else:
                warnings.append(
                    "ANTHROPIC_API_KEY missing — required for pipeline run (default --llm claude)"
                )

    if find_executable("ffmpeg") is None:
        warnings.append("ffmpeg not on PATH — Whisper transcription may fail")

    ok = not errors
    return ConsultAudioValidation(ok=ok, errors=tuple(errors), warnings=tuple(warnings))


def normalize_run_args(args: list[str]) -> list[str]:
    if not args:
        return args
    first = args[0]
    if first in ("python", "python3") or first.endswith(("python.exe", "/python")):
        return args[1:]
    return args


def run_consult_audio_module(args: list[str]) -> int:
    python = require_consult_audio_python()
    settings = load_consult_audio_env_dict()
    venv_root = venv_root_from_python(python)
    cmd = [str(python), *args]
    return run_isolated(
        settings=settings,
        cmd=cmd,
        cwd=CONSULT_AUDIO_DIR,
        venv_root=venv_root,
    )


def run_consult_audio_pipeline(command: str, *, llm: Optional[str] = None) -> int:
    tail = ["-m", "consult_audio_pipe.pipeline", command]
    if command == "run" and llm:
        tail.append(llm)
    typer.echo(f"consult-audio  {CONSULT_AUDIO_ENV_FILE.name}  -> {' '.join(tail)}")
    return run_consult_audio_module(tail)
