"""Snowsight URL helpers for the portfolio Snowflake warehouse."""

from __future__ import annotations


def snowsight_url(account: str, *, override: str | None = None) -> str:
    """
    Build a Snowsight URL from SNOWFLAKE_ACCOUNT.

    Accepts locators like ``igpxtll-rwc63998`` or ``orgname-account``, a bare
    ``*.snowflakecomputing.com`` host, or a full ``https://…`` URL.
    ``override`` wins when set (``SNOWFLAKE_UI_URL``).
    """
    if override and override.strip():
        return override.strip().rstrip("/")

    raw = (account or "").strip()
    if not raw:
        raise ValueError("SNOWFLAKE_ACCOUNT is empty")

    if raw.startswith("http://") or raw.startswith("https://"):
        return raw.rstrip("/")

    host = raw
    for suffix in (".snowflakecomputing.com", ".snowflakecomputing.cn"):
        if host.lower().endswith(suffix):
            host = host[: -len(suffix)]
            break
    host = host.strip().rstrip(".")
    if not host:
        raise ValueError(f"Could not parse SNOWFLAKE_ACCOUNT={account!r}")
    return f"https://{host}.snowflakecomputing.com"
