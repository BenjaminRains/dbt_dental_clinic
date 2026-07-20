"""
Export Wave 1 payments tables from demo Postgres → Snowflake RAW.

Safety:
  - Refuses non-demo Postgres database names (no clinic PHI path).
  - Synthetic / portfolio use only.

Usage (from repo root):
  copy dbt_dental_models\\.env_snowflake.template dbt_dental_models\\.env_snowflake
  # edit SNOWFLAKE_* secrets; DEMO_POSTGRES_* from synthetic_data_generator/.env_demo
  pip install -r scripts/snowflake/requirements.txt
  python scripts/snowflake/export_demo_to_snowflake.py
  python scripts/snowflake/export_demo_to_snowflake.py --tables payment,claimpayment
  python scripts/snowflake/export_demo_to_snowflake.py --dry-run

  # dbt via mdc (preferred — same env loading as other stages):
  mdc dbt validate --env snowflake
  mdc dbt invoke --env snowflake -- build --select tag:snowflake
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:  # pragma: no cover
    psycopg2 = None
    sql = None

try:
    import snowflake.connector
except ImportError:  # pragma: no cover
    snowflake = None


try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
DBT_DIR = REPO_ROOT / "dbt_dental_models"
GENERATOR_DIR = REPO_ROOT / "etl_pipeline" / "synthetic_data_generator"
EXPORT_TABLES_YML = SCRIPT_DIR / "export_tables.yml"
WAVE1_TABLES = ("payment", "claimpayment")  # fallback if YAML missing

# Hard refuse clinic / analytics PHI databases
BLOCKED_DB_NAMES = {
    "opendental_analytics",
    "opendental",
    "opendental_replication",
}
ALLOWED_DB_PATTERN = re.compile(r"^opendental_demo", re.IGNORECASE)


def _load_env() -> None:
    """
    Load env using the same component-scoped authorities as mdc / ENVIRONMENT_FILES.md.

    Precedence (override=False): process env wins, then:
      1. dbt_dental_models/.env_snowflake  (SNOWFLAKE_* — dbt snowflake stage)
      2. etl_pipeline/synthetic_data_generator/.env_demo  (DEMO_POSTGRES_* for export source)
    """
    if load_dotenv is None:
        if (DBT_DIR / ".env_snowflake").exists() or (GENERATOR_DIR / ".env_demo").exists():
            print(
                "warning: python-dotenv not installed; relying on process env only",
                file=sys.stderr,
            )
        return

    sf_env = DBT_DIR / ".env_snowflake"
    if sf_env.exists():
        load_dotenv(sf_env, override=False, interpolate=False)
    else:
        # Transitional: older scaffold path (do not create new files here)
        legacy = SCRIPT_DIR / ".env_snowflake"
        if legacy.exists():
            print(
                "warning: loading deprecated scripts/snowflake/.env_snowflake; "
                "move secrets to dbt_dental_models/.env_snowflake "
                "(see docs/deployment/ENVIRONMENT_FILES.md)",
                file=sys.stderr,
            )
            load_dotenv(legacy, override=False, interpolate=False)

    demo_env = GENERATOR_DIR / ".env_demo"
    if demo_env.exists():
        load_dotenv(demo_env, override=False, interpolate=False)


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value


def _demo_pg_config() -> dict[str, Any]:
    db = os.environ.get("DEMO_POSTGRES_DB", "opendental_demo").strip()
    if db.lower() in BLOCKED_DB_NAMES:
        raise SystemExit(
            f"Refusing to export from blocked database '{db}'. "
            "Use opendental_demo (synthetic) only."
        )
    if not ALLOWED_DB_PATTERN.match(db):
        raise SystemExit(
            f"Refusing database '{db}'. Name must start with opendental_demo."
        )
    password = os.environ.get("DEMO_POSTGRES_PASSWORD", "")
    user = os.environ.get("DEMO_POSTGRES_USER", "opendental_demo_user")
    if password and not user:
        raise SystemExit("DEMO_POSTGRES_USER is required when password is set")
    return {
        "host": os.environ.get("DEMO_POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("DEMO_POSTGRES_PORT", "5432")),
        "dbname": db,
        "user": user,
        "password": password,
        "schema": os.environ.get("DEMO_POSTGRES_SCHEMA", "raw"),
    }


def _sf_config() -> dict[str, str]:
    """Destination identifiers (auth is handled by sf_connect)."""
    return {
        "account": _require_env("SNOWFLAKE_ACCOUNT"),
        "user": _require_env("SNOWFLAKE_USER"),
        "role": os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_DEMO_XS"),
        "database": os.environ.get("SNOWFLAKE_DATABASE", "OPENDENTAL_SF"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
        "stage": os.environ.get("SNOWFLAKE_STAGE", "DEMO_EXPORT"),
    }


def _pg_type_to_snowflake(data_type: str, udt_name: str) -> str:
    dt = (data_type or "").lower()
    udt = (udt_name or "").lower()
    if udt in {"int2", "int4", "int8", "serial", "bigserial"} or dt in {
        "smallint",
        "integer",
        "bigint",
    }:
        return "NUMBER"
    if udt in {"float4", "float8", "numeric", "decimal", "money"} or dt in {
        "real",
        "double precision",
        "numeric",
        "decimal",
    }:
        return "FLOAT" if udt.startswith("float") or "double" in dt else "NUMBER(38, 6)"
    if udt == "bool" or dt == "boolean":
        return "BOOLEAN"
    if udt == "date" or dt == "date":
        return "DATE"
    if "timestamp" in udt or "timestamp" in dt:
        return "TIMESTAMP_NTZ"
    if udt in {"text", "varchar", "bpchar", "name"} or dt in {
        "text",
        "character varying",
        "character",
    }:
        return "VARCHAR"
    return "VARCHAR"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _fetch_columns(pg_conn, schema: str, table: str) -> list[dict[str, str]]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        rows = cur.fetchall()
    if not rows:
        raise SystemExit(f"No columns found for {schema}.{table}")
    return [
        {
            "name": r[0],
            "data_type": r[1],
            "udt_name": r[2],
            "sf_type": _pg_type_to_snowflake(r[1], r[2]),
        }
        for r in rows
    ]


def _table_exists(pg_conn, schema: str, table: str) -> bool:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def _export_csv(pg_conn, schema: str, table: str, columns: list[dict[str, str]], path: Path) -> int:
    col_idents = sql.SQL(", ").join(sql.Identifier(c["name"]) for c in columns)
    query = sql.SQL("SELECT {cols} FROM {schema}.{table}").format(
        cols=col_idents,
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
    )
    row_count = 0
    with pg_conn.cursor() as cur, path.open("w", newline="", encoding="utf-8") as f:
        cur.execute(query)
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow([c["name"] for c in columns])
        while True:
            batch = cur.fetchmany(5000)
            if not batch:
                break
            for row in batch:
                writer.writerow(["" if v is None else v for v in row])
                row_count += 1
    return row_count


def _create_snowflake_table(
    sf_cur, database: str, schema: str, table: str, columns: list[dict[str, str]]
) -> None:
    col_ddl = ",\n  ".join(f"{_quote_ident(c['name'])} {c['sf_type']}" for c in columns)
    fq = f"{_quote_ident(database)}.{_quote_ident(schema)}.{_quote_ident(table)}"
    sf_cur.execute(f"CREATE OR REPLACE TABLE {fq} (\n  {col_ddl}\n)")


def _load_table(
    sf_conn,
    sf_cfg: dict[str, str],
    table: str,
    columns: list[dict[str, str]],
    csv_path: Path,
) -> int:
    database = sf_cfg["database"]
    schema = sf_cfg["schema"]
    stage = sf_cfg["stage"]
    stage_path = f"@{_quote_ident(database)}.{_quote_ident(schema)}.{_quote_ident(stage)}"
    fq_table = f"{_quote_ident(database)}.{_quote_ident(schema)}.{_quote_ident(table)}"
    remote_name = f"{table}.csv"

    with sf_conn.cursor() as cur:
        cur.execute(f"USE WAREHOUSE {_quote_ident(sf_cfg['warehouse'])}")
        cur.execute(f"USE DATABASE {_quote_ident(database)}")
        cur.execute(f"USE SCHEMA {_quote_ident(schema)}")
        _create_snowflake_table(cur, database, schema, table, columns)
        cur.execute(f"REMOVE {stage_path} PATTERN='.*{re.escape(table)}\\.csv.*'")
        # Keep uncompressed so COPY path matches the local basename.
        put_sql = (
            f"PUT 'file://{csv_path.as_posix()}' {stage_path} "
            "OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
        )
        cur.execute(put_sql)
        col_list = ", ".join(_quote_ident(c["name"]) for c in columns)
        copy_sql = f"""
            COPY INTO {fq_table} ({col_list})
            FROM {stage_path}
            FILES = ('{remote_name}')
            FILE_FORMAT = (
              TYPE = CSV
              PARSE_HEADER = TRUE
              FIELD_OPTIONALLY_ENCLOSED_BY = '"'
              ESCAPE_UNENCLOSED_FIELD = NONE
              EMPTY_FIELD_AS_NULL = TRUE
              NULL_IF = ('')
            )
            MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
            ON_ERROR = ABORT_STATEMENT
        """
        cur.execute(copy_sql)
        cur.execute(f"SELECT COUNT(*) FROM {fq_table}")
        count_row = cur.fetchone()
        return int(count_row[0]) if count_row else 0


def _tables_from_manifest(wave: str | None = None) -> list[str]:
    """Load table list from export_tables.yml (scale path: grow waves in YAML)."""
    if not EXPORT_TABLES_YML.exists():
        return list(WAVE1_TABLES)
    if yaml is None:
        print(
            "warning: PyYAML not installed; using built-in Wave 1 table list. "
            "pip install pyyaml to use export_tables.yml",
            file=sys.stderr,
        )
        return list(WAVE1_TABLES)

    data = yaml.safe_load(EXPORT_TABLES_YML.read_text(encoding="utf-8")) or {}
    waves = data.get("waves") or {}
    wave_name = wave or data.get("default_wave") or "wave1_payments"
    entry = waves.get(wave_name)
    if not entry:
        raise SystemExit(
            f"Unknown export wave '{wave_name}' in {EXPORT_TABLES_YML.name}. "
            f"Known: {', '.join(sorted(waves)) or '(none)'}"
        )
    tables = [str(t).strip().lower() for t in (entry.get("tables") or []) if str(t).strip()]
    if not tables:
        raise SystemExit(f"Wave '{wave_name}' has no tables in {EXPORT_TABLES_YML.name}")
    return tables


def _parse_tables(raw: str | None, wave: str | None = None) -> list[str]:
    if raw:
        tables = [t.strip().lower() for t in raw.split(",") if t.strip()]
    else:
        env_override = os.environ.get("SNOWFLAKE_EXPORT_TABLES", "").strip()
        if env_override:
            tables = [t.strip().lower() for t in env_override.split(",") if t.strip()]
        else:
            tables = _tables_from_manifest(wave)
    if not tables:
        raise SystemExit("No tables specified")
    return tables


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tables",
        help="Comma-separated table names (overrides manifest / SNOWFLAKE_EXPORT_TABLES)",
    )
    parser.add_argument(
        "--wave",
        help="Wave name from export_tables.yml (default: default_wave in that file)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Connect to Postgres, print plans, do not write to Snowflake",
    )
    args = parser.parse_args(argv)

    if psycopg2 is None or sql is None:
        raise SystemExit(
            "psycopg2 is required. pip install -r scripts/snowflake/requirements.txt"
        )
    if not args.dry_run and snowflake is None:
        raise SystemExit(
            "snowflake-connector-python is required. "
            "pip install -r scripts/snowflake/requirements.txt"
        )

    _load_env()
    tables = _parse_tables(args.tables, wave=args.wave)
    pg_cfg = _demo_pg_config()

    print(
        f"Source: postgres://{pg_cfg['host']}:{pg_cfg['port']}/"
        f"{pg_cfg['dbname']}.{pg_cfg['schema']}"
    )
    print(f"Tables: {', '.join(tables)}")
    if args.dry_run:
        print("Mode: dry-run (no Snowflake writes)")

    pg_conn = psycopg2.connect(
        host=pg_cfg["host"],
        port=pg_cfg["port"],
        dbname=pg_cfg["dbname"],
        user=pg_cfg["user"],
        password=pg_cfg["password"],
    )

    sf_conn = None
    sf_cfg = None
    if not args.dry_run:
        sys.path.insert(0, str(SCRIPT_DIR))
        from sf_connect import connect_snowflake  # noqa: E402

        sf_cfg = _sf_config()
        sf_conn = connect_snowflake()
        print(
            f"Dest: snowflake://{sf_cfg['account']}/{sf_cfg['database']}.{sf_cfg['schema']} "
            f"(wh={sf_cfg['warehouse']}, role={sf_cfg['role']})"
        )

    try:
        with tempfile.TemporaryDirectory(prefix="sf_export_") as tmp:
            tmp_dir = Path(tmp)
            for table in tables:
                if not _table_exists(pg_conn, pg_cfg["schema"], table):
                    raise SystemExit(f"Missing source table {pg_cfg['schema']}.{table}")
                columns = _fetch_columns(pg_conn, pg_cfg["schema"], table)
                csv_path = tmp_dir / f"{table}.csv"
                row_count = _export_csv(
                    pg_conn, pg_cfg["schema"], table, columns, csv_path
                )
                print(
                    f"  {table}: exported {row_count:,} rows, "
                    f"{len(columns)} columns -> {csv_path.name}"
                )
                if args.dry_run:
                    print(f"    columns: {', '.join(c['name'] for c in columns)}")
                    continue
                assert sf_conn is not None and sf_cfg is not None
                loaded = _load_table(sf_conn, sf_cfg, table, columns, csv_path)
                print(f"  {table}: loaded {loaded:,} rows into Snowflake RAW")
                if loaded != row_count:
                    print(
                        f"  warning: row count mismatch export={row_count} snowflake={loaded}",
                        file=sys.stderr,
                    )
    finally:
        pg_conn.close()
        if sf_conn is not None:
            sf_conn.close()

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
