#!/usr/bin/env python3
"""Read-only investigation for L0-PAY-001 MySQL vs raw.payment drift.

Identifies phantom rows (deleted in OpenDental, still in raw). It is OK to DELETE those
rows from raw.payment only — never delete from OpenDental source without clinic approval.
Remediation: raw DELETE for confirmed phantoms, Sunday full refresh, or future CDC.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from etl_pipeline.config import DatabaseType
from etl_pipeline.config.script_env import load_script_settings
from etl_pipeline.core.connections import ConnectionFactory

LOOKBACK_DAYS = 30


def main() -> None:
    settings = load_script_settings("local")
    source = ConnectionFactory.get_source_connection(settings)
    analytics = ConnectionFactory.get_analytics_raw_connection(settings)
    source_db = settings.get_database_config(DatabaseType.SOURCE).get("database", "opendental")

    mysql_breakdown = text(
        f"""
        SELECT COUNT(*) AS total_rows,
               SUM(CASE WHEN PayAmt = 0 THEN 1 ELSE 0 END) AS zero_amount_rows,
               SUM(CASE WHEN PayAmt <> 0 THEN 1 ELSE 0 END) AS nonzero_rows,
               ROUND(SUM(PayAmt), 2) AS total_amount,
               ROUND(SUM(CASE WHEN PayAmt <> 0 THEN PayAmt ELSE 0 END), 2) AS nonzero_total
        FROM `{source_db}`.payment
        WHERE PayDate >= DATE_SUB(CURDATE(), INTERVAL :lb DAY)
          AND PayDate > '0001-01-01'
        """
    )
    pg_breakdown = text(
        """
        SELECT COUNT(*) AS total_rows,
               COUNT(*) FILTER (WHERE "PayAmt" = 0) AS zero_amount_rows,
               COUNT(*) FILTER (WHERE "PayAmt" <> 0) AS nonzero_rows,
               ROUND(SUM("PayAmt")::numeric, 2) AS total_amount,
               ROUND(SUM("PayAmt") FILTER (WHERE "PayAmt" <> 0)::numeric, 2) AS nonzero_total
        FROM raw.payment
        WHERE "PayDate"::date >= CURRENT_DATE - make_interval(days => :lb)
          AND "PayDate"::date > '0001-01-01'::date
        """
    )

    with source.connect() as conn:
        mysql = conn.execute(mysql_breakdown, {"lb": LOOKBACK_DAYS}).one()._mapping
    with analytics.connect() as conn:
        pg = conn.execute(pg_breakdown, {"lb": LOOKBACK_DAYS}).one()._mapping

    print("=== Breakdown (30-day PayDate window) ===")
    print("MySQL:", dict(mysql))
    print("raw  :", dict(pg))

    # Export raw PayNums in window for cross-check
    with analytics.connect() as conn:
        raw_nums = {
            int(r[0])
            for r in conn.execute(
                text(
                    """
                    SELECT "PayNum" FROM raw.payment
                    WHERE "PayDate"::date >= CURRENT_DATE - make_interval(days => :lb)
                      AND "PayDate"::date > '0001-01-01'::date
                    """
                ),
                {"lb": LOOKBACK_DAYS},
            )
        }

    with source.connect() as conn:
        mysql_rows = conn.execute(
            text(
                f"""
                SELECT PayNum, PayAmt, PayDate
                FROM `{source_db}`.payment
                WHERE PayDate >= DATE_SUB(CURDATE(), INTERVAL :lb DAY)
                  AND PayDate > '0001-01-01'
                """
            ),
            {"lb": LOOKBACK_DAYS},
        ).fetchall()

    mysql_by_num = {int(r.PayNum): (float(r.PayAmt), str(r.PayDate)) for r in mysql_rows}
    mysql_nums = set(mysql_by_num)

    phantom = raw_nums - mysql_nums
    missing = mysql_nums - raw_nums

    print(f"\n=== Row parity ===")
    print(f"phantom in raw (not in MySQL window): {len(phantom)}")
    print(f"missing from raw (in MySQL window):   {len(missing)}")

    # stale amount: in both but PayAmt differs
    stale = []
    for pay_num in raw_nums & mysql_nums:
        with analytics.connect() as conn:
            raw_row = conn.execute(
                text('SELECT "PayAmt", "PayDate" FROM raw.payment WHERE "PayNum" = :n'),
                {"n": pay_num},
            ).one()
        mysql_amt, mysql_date = mysql_by_num[pay_num]
        raw_amt = float(raw_row.PayAmt)
        if abs(raw_amt - mysql_amt) > 0.001:
            stale.append((pay_num, mysql_amt, raw_amt, mysql_date, raw_row.PayDate))

    print(f"stale PayAmt (both sides, amount mismatch): {len(stale)}")

    if phantom:
        print("\n--- Phantom PayNums (in raw, not MySQL window) ---")
        with analytics.connect() as conn:
            for row in conn.execute(
                text(
                    """
                    SELECT "PayNum", "PayAmt", "PayDate"
                    FROM raw.payment
                    WHERE "PayNum" = ANY(:nums)
                    ORDER BY ABS("PayAmt") DESC
                    """
                ),
                {"nums": list(phantom)},
            ):
                print(dict(row._mapping))

    if stale:
        print("\n--- Stale PayAmt ---")
        for item in sorted(stale, key=lambda x: abs(x[2] - x[1]), reverse=True)[:20]:
            print(f"PayNum={item[0]} mysql=${item[1]} raw=${item[2]} mysql_date={item[3]} raw_date={item[4]}")

    if missing:
        print("\n--- Missing from raw (sample) ---")
        for n in sorted(missing)[:10]:
            print(f"PayNum={n} mysql={mysql_by_num[n]}")

    phantom_amt = 0.0
    if phantom:
        with analytics.connect() as conn:
            phantom_amt = float(
                conn.execute(
                    text(
                        'SELECT COALESCE(SUM("PayAmt"), 0) FROM raw.payment WHERE "PayNum" = ANY(:nums)'
                    ),
                    {"nums": list(phantom)},
                ).scalar()
            )
    stale_delta = sum(r[2] - r[1] for r in stale)
    print(f"\n=== Reconciliation ===")
    print(f"phantom PayAmt sum in raw: ${phantom_amt:.2f}")
    print(f"stale amount delta (raw - mysql): ${stale_delta:.2f}")
    print(f"expected total delta (raw - mysql): ${float(pg['total_amount']) - float(mysql['total_amount']):.2f}")

    print("\n=== Phantom detail (MySQL by PayNum) ===")
    for pay_num in sorted(phantom):
        with source.connect() as conn:
            row = conn.execute(
                text(f"SELECT PayNum, PayAmt, PayDate FROM `{source_db}`.payment WHERE PayNum = :n"),
                {"n": pay_num},
            ).fetchone()
        print(f"PayNum {pay_num}: MySQL={dict(row._mapping) if row else 'DELETED/ABSENT'}")

    source.dispose()
    analytics.dispose()


if __name__ == "__main__":
    main()
