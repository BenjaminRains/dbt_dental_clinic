"""Choices for ``mdc secrets pull`` when no target is given."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SecretChoice:
    key: str
    title: str
    action: str
    summary: str
    selectable: bool


SECRET_CHOICES: tuple[SecretChoice, ...] = (
    SecretChoice(
        key="clinic",
        title="Clinic RDS analytics_user",
        action="sync",
        summary=(
            "Copy the live RDS master secret into api/.env_api_clinic and the "
            "clinic fields in deployment_credentials.json."
        ),
        selectable=True,
    ),
    SecretChoice(
        key="demo",
        title="Demo Postgres opendental_demo_user",
        action="rotate",
        summary=(
            "Set a new password on that role through the demo tunnel, then write "
            "api/.env_api_demo and both demo password fields in "
            "deployment_credentials.json. Leaves the synthetic-generator .env_demo alone."
        ),
        selectable=True,
    ),
    SecretChoice(
        key="anthropic",
        title="Anthropic API key",
        action="manual",
        summary=(
            "Revoke the key in the Anthropic console, then replace "
            "consult_audio_pipe/.env."
        ),
        selectable=False,
    ),
    SecretChoice(
        key="airflow",
        title="Airflow admin",
        action="manual",
        summary=(
            ".env and airflow/.env.native hold different passwords. Change the "
            "admin password in Airflow, then update the env file that creates it."
        ),
        selectable=False,
    ),
)


class SecretSelectionError(ValueError):
    """The selection did not match a runnable row."""


def format_catalog(rotated_on: dict[str, str | None] | None = None) -> str:
    dates = rotated_on or {}
    lines = ["Secrets", ""]
    for index, choice in enumerate(SECRET_CHOICES, start=1):
        when = dates.get(choice.key) or "unknown"
        lines.append(f"  {index}. {choice.title}  [{choice.action}]  last rotated {when}")
        lines.append(f"     {choice.summary}")
    lines.append("")
    lines.append("Select rows to run (example: 1 or 1,2). Empty cancels.")
    return "\n".join(lines)


def format_plan(keys: list[str], rotated_on: dict[str, str | None] | None = None) -> str:
    by_key = {choice.key: choice for choice in SECRET_CHOICES}
    dates = rotated_on or {}
    lines = ["Plan", ""]
    for key in keys:
        choice = by_key[key]
        when = dates.get(key) or "unknown"
        lines.append(f"- {choice.title} (last rotated {when}): {choice.summary}")
        if key == "demo":
            lines.append("  Tunnel: mdc tunnel demo-db (127.0.0.1:5434 unless DEMO_POSTGRES_PORT is set).")
            lines.append("  After the files update: mdc deploy api --env demo")
        if key == "clinic":
            lines.append("  After the files update: mdc deploy api --env clinic")
    lines.append("")
    lines.append("Passwords are not printed.")
    return "\n".join(lines)


def parse_selection(raw: str) -> list[str]:
    """Return selected keys in catalog order. Empty input selects nothing."""
    text = raw.strip().lower()
    if not text:
        return []

    tokens = [part.strip() for part in text.replace(" ", ",").split(",") if part.strip()]
    by_number = {str(index): choice for index, choice in enumerate(SECRET_CHOICES, start=1)}
    by_key = {choice.key: choice for choice in SECRET_CHOICES}

    chosen: list[SecretChoice] = []
    seen: set[str] = set()
    for token in tokens:
        choice = by_number.get(token) or by_key.get(token)
        if choice is None:
            raise SecretSelectionError(
                f"Unknown selection '{token}'. Use a row number or clinic/demo."
            )
        if not choice.selectable:
            raise SecretSelectionError(
                f"{choice.title} is manual. {choice.summary}"
            )
        if choice.key not in seen:
            seen.add(choice.key)
            chosen.append(choice)

    order = [choice.key for choice in SECRET_CHOICES]
    return [key for key in order if key in seen]
