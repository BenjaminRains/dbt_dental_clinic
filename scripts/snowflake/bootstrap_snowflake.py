"""
Run docs/snowflake/sql/01_bootstrap.sql one statement at a time.

Snowflake's Python API rejects multi-statement batches; Snowsight can hit the
same limit depending on how the worksheet is executed.

Usage (from repo root):
  python scripts/snowflake/bootstrap_snowflake.py
  python scripts/snowflake/bootstrap_snowflake.py --dry-run

Requires dbt_dental_models/.env_snowflake with SNOWFLAKE_* set.
Use SNOWFLAKE_ROLE=ACCOUNTADMIN (or equivalent) for the first run.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

try:
    import snowflake.connector
except ImportError:  # pragma: no cover
    snowflake = None


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
DBT_DIR = REPO_ROOT / "dbt_dental_models"
BOOTSTRAP_SQL = REPO_ROOT / "docs" / "snowflake" / "sql" / "01_bootstrap.sql"


def _load_env() -> None:
    if load_dotenv is None:
        raise SystemExit("python-dotenv required. pip install -r scripts/snowflake/requirements.txt")
    env_path = DBT_DIR / ".env_snowflake"
    if not env_path.exists():
        raise SystemExit(f"Missing {env_path}. Copy from .env_snowflake.template.")
    load_dotenv(env_path, override=False, interpolate=False)


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value


def _strip_line_comments(sql_text: str) -> str:
    lines: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines)


def _split_statements(sql_text: str) -> list[str]:
    """Split on semicolons outside single-quoted strings."""
    cleaned = _strip_line_comments(sql_text)
    statements: list[str] = []
    buf: list[str] = []
    in_string = False
    i = 0
    while i < len(cleaned):
        ch = cleaned[i]
        if ch == "'":
            buf.append(ch)
            # SQL escaped quote: ''
            if in_string and i + 1 < len(cleaned) and cleaned[i + 1] == "'":
                buf.append(cleaned[i + 1])
                i += 2
                continue
            in_string = not in_string
            i += 1
            continue
        if ch == ";" and not in_string:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    stmt = "".join(buf).strip()
    if stmt:
        statements.append(stmt)
    return statements


def _user_grants(user: str) -> list[str]:
    # Quote only if needed; Snowflake usernames are typically unquoted identifiers.
    safe = user.replace('"', "")
    return [
        f"GRANT ROLE TRANSFORMER TO USER {safe}",
        f"GRANT ROLE ANALYST TO USER {safe}",
        f"ALTER USER {safe} SET DEFAULT_ROLE = TRANSFORMER",
        f"ALTER USER {safe} SET DEFAULT_WAREHOUSE = WH_DEMO_XS",
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print statements without connecting",
    )
    parser.add_argument(
        "--sql",
        type=Path,
        default=BOOTSTRAP_SQL,
        help="Path to bootstrap SQL file",
    )
    parser.add_argument(
        "--skip-user-grants",
        action="store_true",
        help="Do not GRANT roles to SNOWFLAKE_USER",
    )
    args = parser.parse_args(argv)

    if snowflake is None and not args.dry_run:
        raise SystemExit(
            "snowflake-connector-python required. "
            "pip install -r scripts/snowflake/requirements.txt"
        )

    # Allow importing sibling helper when run as a script
    sys.path.insert(0, str(SCRIPT_DIR))
    from sf_connect import connect_snowflake  # noqa: E402

    _load_env()
    if not args.sql.exists():
        raise SystemExit(f"SQL file not found: {args.sql}")

    statements = _split_statements(args.sql.read_text(encoding="utf-8"))
    user = os.environ.get("SNOWFLAKE_USER", "").strip()
    if user and not args.skip_user_grants:
        statements.extend(_user_grants(user))

    print(f"Statements to run: {len(statements)}")
    if args.dry_run:
        for i, stmt in enumerate(statements, 1):
            preview = " ".join(stmt.split())[:100]
            print(f"  {i:02d}. {preview}...")
        return 0

    _require("SNOWFLAKE_ACCOUNT")
    _require("SNOWFLAKE_USER")
    conn = connect_snowflake()
    try:
        with conn.cursor() as cur:
            for i, stmt in enumerate(statements, 1):
                preview = " ".join(stmt.split())[:90]
                print(f"[{i}/{len(statements)}] {preview}...")
                try:
                    cur.execute(stmt)
                    print("  OK")
                except Exception as exc:  # noqa: BLE001
                    msg = str(exc)
                    print(f"  FAILED: {msg}", file=sys.stderr)
                    raise SystemExit(f"Bootstrap stopped at statement {i}") from exc
    finally:
        conn.close()

    print("Bootstrap complete.")
    print("Next: set SNOWFLAKE_ROLE=TRANSFORMER in dbt_dental_models/.env_snowflake")
    print("Then: mdc dbt validate --env snowflake")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
