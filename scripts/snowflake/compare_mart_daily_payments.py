"""
Compare mart_daily_payments between demo Postgres and Snowflake.

Checks payment_date, net_collections_amount, and payment_count for every date.
Exit 0 when both sides match. Exit 1 when a date or amount differs.

Default Postgres is the EC2 opendental_demo database through the SSM tunnel.
Start that tunnel first and leave it open:

    mdc tunnel demo-db

Usage (from repo root):
    python scripts/snowflake/compare_mart_daily_payments.py
    python scripts/snowflake/compare_mart_daily_payments.py --source local
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
DBT_DIR = REPO_ROOT / "dbt_dental_models"
GENERATOR_DIR = REPO_ROOT / "etl_pipeline" / "synthetic_data_generator"
CREDENTIALS = REPO_ROOT / "deployment_credentials.json"

BLOCKED_DB_NAMES = {"opendental_analytics", "opendental", "opendental_replication"}
MONEY = Decimal("0.01")
QUERY = """
SELECT payment_date::date, net_collections_amount, payment_count
FROM {table}
ORDER BY 1
"""


def _load_snowflake_env() -> None:
    if load_dotenv is None:
        return
    sf_env = DBT_DIR / ".env_snowflake"
    if sf_env.exists():
        load_dotenv(sf_env, override=False, interpolate=False)


def _load_local_demo_env() -> None:
    if load_dotenv is None:
        return
    demo_env = GENERATOR_DIR / ".env_demo"
    if demo_env.exists():
        load_dotenv(demo_env, override=False, interpolate=False)


def _ec2_pg_config() -> dict[str, object]:
    """EC2 demo DB as seen through the local tunnel (localhost:5434)."""
    if not CREDENTIALS.exists():
        raise SystemExit(f"Missing {CREDENTIALS}")
    data = json.loads(CREDENTIALS.read_text(encoding="utf-8"))
    pg = (data.get("demo_database") or {}).get("postgresql") or {}
    database = str(pg.get("database") or "opendental_demo")
    user = str(pg.get("user") or "opendental_demo_user")
    password = pg.get("password")
    if not password:
        raise SystemExit(
            "demo_database.postgresql.password is empty in deployment_credentials.json"
        )
    _refuse_clinic_db(database)
    return {
        "host": "localhost",
        "port": int(os.environ.get("DEMO_TUNNEL_PORT", "5434")),
        "dbname": database,
        "user": user,
        "password": str(password),
    }


def _local_pg_config() -> dict[str, object]:
    _load_local_demo_env()
    database = os.environ.get("DEMO_POSTGRES_DB", "opendental_demo").strip()
    _refuse_clinic_db(database)
    return {
        "host": os.environ.get("DEMO_POSTGRES_HOST", "localhost").strip(),
        "port": int(os.environ.get("DEMO_POSTGRES_PORT", "5432")),
        "dbname": database,
        "user": os.environ.get("DEMO_POSTGRES_USER", "postgres").strip(),
        "password": os.environ.get("DEMO_POSTGRES_PASSWORD", ""),
    }


def _refuse_clinic_db(database: str) -> None:
    if database.lower() in BLOCKED_DB_NAMES or not database.lower().startswith("opendental_demo"):
        raise SystemExit(
            f"Refusing database '{database}'. Parity reads opendental_demo only."
        )


def _money(value: object) -> Decimal:
    if value is None:
        return Decimal("0.00")
    return Decimal(str(value)).quantize(MONEY, rounding=ROUND_HALF_UP)


def _rows(cursor) -> dict[str, tuple[Decimal, int]]:
    out: dict[str, tuple[Decimal, int]] = {}
    for payment_date, amount, count in cursor.fetchall():
        key = payment_date.isoformat()
        out[key] = (_money(amount), int(count or 0))
    return out


def _fetch_postgres(cfg: dict[str, object]) -> dict[str, tuple[Decimal, int]]:
    if psycopg2 is None:
        raise SystemExit("psycopg2 is required. pip install -r scripts/snowflake/requirements.txt")
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
        connect_timeout=15,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(QUERY.format(table="marts.mart_daily_payments"))
            return _rows(cur)
    finally:
        conn.close()


def _fetch_snowflake() -> tuple[str, dict[str, tuple[Decimal, int]]]:
    _load_snowflake_env()
    sys.path.insert(0, str(SCRIPT_DIR))
    from sf_connect import connect_snowflake  # noqa: E402

    database = os.environ.get("SNOWFLAKE_DATABASE", "OPENDENTAL_SF").strip()
    conn = connect_snowflake()
    try:
        with conn.cursor() as cur:
            cur.execute(
                QUERY.format(table=f'"{database}"."marts"."mart_daily_payments"')
            )
            return database, _rows(cur)
    finally:
        conn.close()


def _fmt(side: tuple[Decimal, int] | None) -> str:
    if side is None:
        return "missing"
    amount, count = side
    return f"net={amount} count={count}"


def _sample_keys(keys: list[str]) -> list[str]:
    if len(keys) <= 3:
        return keys
    mid = len(keys) // 2
    return [keys[0], keys[mid], keys[-1]]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        choices=("ec2", "local"),
        default="ec2",
        help="ec2 = tunnel localhost:5434 (default); local = .env_demo Postgres",
    )
    args = parser.parse_args()

    pg_cfg = _ec2_pg_config() if args.source == "ec2" else _local_pg_config()
    print(
        f"Postgres: {pg_cfg['user']}@{pg_cfg['host']}:{pg_cfg['port']}"
        f"/{pg_cfg['dbname']}.marts.mart_daily_payments"
    )
    try:
        pg_rows = _fetch_postgres(pg_cfg)
    except Exception as exc:  # connection errors from either driver
        if args.source == "ec2":
            raise SystemExit(
                f"Could not reach the EC2 demo database via localhost:{pg_cfg['port']}. "
                f"Start `mdc tunnel demo-db` and leave it open.\n{exc}"
            ) from exc
        raise

    sf_database, sf_rows = _fetch_snowflake()
    print(f"Snowflake: {sf_database}.\"marts\".\"mart_daily_payments\"")
    print(f"Rows: postgres={len(pg_rows)} snowflake={len(sf_rows)}")

    dates = sorted(set(pg_rows) | set(sf_rows))
    mismatches: list[str] = []
    for day in dates:
        pg = pg_rows.get(day)
        sf = sf_rows.get(day)
        if pg != sf:
            mismatches.append(
                "  {day}  postgres={pg}  snowflake={sf}".format(
                    day=day,
                    pg=_fmt(pg),
                    sf=_fmt(sf),
                )
            )

    if mismatches:
        print(f"MISMATCH {len(mismatches)} date(s):")
        print("\n".join(mismatches))
        return 1

    print("MATCH all dates.")
    print("payment_date  net_collections_amount  payment_count")
    for day in _sample_keys(dates):
        amount, count = pg_rows[day]
        print(f"  {day}  {amount}  {count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
