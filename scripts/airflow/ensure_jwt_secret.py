"""Append AIRFLOW__API_AUTH__JWT_SECRET to airflow/.env.native if missing.

Usage (repo root):
  .\\.venv-airflow\\Scripts\\python.exe -S scripts\\airflow\\ensure_jwt_secret.py
"""
from __future__ import annotations

import secrets
from pathlib import Path

ENV = Path(__file__).resolve().parents[2] / "airflow" / ".env.native"
text = ENV.read_text(encoding="utf-8") if ENV.exists() else ""
if "AIRFLOW__API_AUTH__JWT_SECRET=" in text:
    print("already-present")
else:
    secret = secrets.token_urlsafe(32)
    with ENV.open("a", encoding="utf-8") as fh:
        fh.write("\n# Required by Airflow 3.2+ api-server\n")
        fh.write(f"AIRFLOW__API_AUTH__JWT_SECRET={secret}\n")
    print("appended")
