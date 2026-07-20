"""Shared Snowflake connection helper (key-pair preferred; password fallback)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import snowflake.connector


def connect_snowflake(**overrides: Any):
    """
    Connect using env loaded by caller.

    Prefer SNOWFLAKE_PRIVATE_KEY_PATH (passkey-friendly accounts).
    Fall back to SNOWFLAKE_PASSWORD if no key path is set.
    """
    account = os.environ["SNOWFLAKE_ACCOUNT"].strip()
    user = os.environ["SNOWFLAKE_USER"].strip()
    role = os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER").strip()
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "").strip() or None
    database = os.environ.get("SNOWFLAKE_DATABASE", "").strip() or None
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "").strip() or None

    kwargs: dict[str, Any] = {
        "account": account,
        "user": user,
        "role": role,
    }
    if warehouse:
        kwargs["warehouse"] = warehouse
    if database:
        kwargs["database"] = database
    if schema:
        kwargs["schema"] = schema

    key_path = (os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH") or "").strip()
    if key_path:
        path = Path(key_path)
        if not path.is_file():
            raise FileNotFoundError(f"SNOWFLAKE_PRIVATE_KEY_PATH not found: {path}")
        kwargs["private_key_file"] = str(path)
        passphrase = (os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or "").strip()
        if passphrase:
            kwargs["private_key_file_pwd"] = passphrase.encode("utf-8")
    else:
        password = (os.environ.get("SNOWFLAKE_PASSWORD") or "").strip()
        if not password:
            raise ValueError(
                "Set SNOWFLAKE_PRIVATE_KEY_PATH (preferred) or SNOWFLAKE_PASSWORD."
            )
        kwargs["password"] = password

    kwargs.update(overrides)
    return snowflake.connector.connect(**kwargs)
