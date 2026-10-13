"""Run the saved Aging of A/R report query against live OpenDental. Totals only."""

from pathlib import Path

from sqlalchemy import text

from etl_pipeline.config.settings import get_settings
from etl_pipeline.core.connections import ConnectionFactory

SQL_PATH = (
    Path(__file__).resolve().parents[1]
    / "compare"
    / "od_aging_report_2026-10-12.sql"
)


def main() -> None:
    sql = SQL_PATH.read_text(encoding="utf-8")
    settings = get_settings()
    engine = ConnectionFactory.get_source_connection(settings)
    with engine.connect() as conn:
        row = conn.execute(text(sql)).mappings().one()
    for key, value in row.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
