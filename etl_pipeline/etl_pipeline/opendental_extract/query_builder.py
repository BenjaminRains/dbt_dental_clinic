"""
Incremental / lookback WHERE builders from tables.yml-shaped dicts.

Pure functions over table config — no Settings, no hop writers.
"""

from __future__ import annotations

from typing import Callable, Dict, List, Optional

TIMESTAMP_WATERMARK_NAMES = frozenset({
    "DateTStamp",
    "SecDateTEdit",
    "SecDateTEntry",
    "DateTEntry",
})


def get_replicator_watermark_column(table_config: Dict) -> Optional[str]:
    col = table_config.get("replicator_watermark_column") or table_config.get(
        "primary_incremental_column"
    )
    if col and col != "none" and str(col).strip():
        return col
    return None


def is_timestamp_watermark_column(column: Optional[str]) -> bool:
    """True when the watermark is a known OpenDental mutation timestamp."""
    return bool(column and column in TIMESTAMP_WATERMARK_NAMES)


def uses_in_place_timestamp_watermark(table_config: Dict) -> bool:
    """
    True only for real in-place mutation sync (non-PK timestamp watermark).

    Guard against ETL-FND-002: sync_profile=in_place_updates with an integer PK
    watermark must not take the datetime loader path (SheetFieldNum > '2026-…').
    """
    watermark = get_replicator_watermark_column(table_config)
    return (
        table_config.get("sync_profile") == "in_place_updates"
        and is_timestamp_watermark_column(watermark)
    )


def get_incremental_columns(table_config: Dict) -> List[str]:
    """
    Columns used for incremental WHERE.

    Mutation tables use timestamp watermark only (not PK in or_logic).
    """
    watermark = get_replicator_watermark_column(table_config)
    if uses_in_place_timestamp_watermark(table_config) and watermark:
        return [watermark]
    return list(table_config.get("incremental_columns") or [])


# Back-compat alias used by MDC loader path
get_loader_incremental_columns = get_incremental_columns


def lookback_resync_enabled(table_config: Dict) -> bool:
    spec = table_config.get("lookback_resync") or {}
    return bool(spec.get("enabled"))


def build_mysql_lookback_predicate(table_config: Dict) -> Optional[str]:
    spec = table_config.get("lookback_resync") or {}
    if not spec.get("enabled"):
        return None
    days = int(spec.get("window_days") or 30)
    columns = spec.get("predicate_columns") or []
    if not columns:
        return None
    parts = [
        f"`{col}` >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)" for col in columns
    ]
    return f"({' OR '.join(parts)})"


def wrap_mysql_incremental_with_lookback_config(
    where_clause: str,
    table_config: Dict,
) -> str:
    lookback = build_mysql_lookback_predicate(table_config)
    if lookback:
        return f"({where_clause}) OR ({lookback})"
    return where_clause


def is_datetime_like(value: object) -> bool:
    if value is None:
        return False
    s = str(value).strip()
    if not s:
        return False
    if "-" in s and (":" in s or len(s) == 10):
        return True
    return False


def is_numeric_value(value: object) -> bool:
    if value is None:
        return False
    s = str(value).strip()
    if not s or is_datetime_like(value):
        return False
    try:
        float(s)
        return True
    except ValueError:
        return False


def build_incremental_where_clause(
    table_config: Dict,
    *,
    last_analytics_load: object = None,
    last_primary_value: object = None,
    is_integer_column: Optional[Callable[[str], bool]] = None,
) -> str:
    """
    Build WHERE clause for incremental SELECT against Open Dental (or a hop).

    MDC loader historically called this for replication → analytics; the same
    predicate shape applies to source → hop extract when the caller injects
    the last cursor values.
    """
    cols = get_incremental_columns(table_config)
    if not cols:
        return "1=1"

    watermark = get_replicator_watermark_column(table_config)

    # Datetime path only when watermark is a real mutation timestamp (ETL-FND-002).
    if uses_in_place_timestamp_watermark(table_config) and watermark:
        threshold = None
        if last_primary_value and is_datetime_like(last_primary_value):
            threshold = last_primary_value
        elif last_analytics_load:
            threshold = last_analytics_load
        if threshold is None:
            return "1=1"
        return (
            f"(`{watermark}` > '{threshold}' "
            f"AND `{watermark}` != '0001-01-01 00:00:00')"
        )

    strategy = table_config.get("incremental_strategy", "or_logic")
    primary_incremental = table_config.get("primary_incremental_column")
    conditions: List[str] = []

    for col in cols:
        if is_integer_column:
            is_int_col = is_integer_column(col)
        else:
            is_int_col = bool(
                col == primary_incremental
                and col
                and not is_timestamp_watermark_column(col)
                and (
                    col.endswith("Num")
                    or col.endswith("ID")
                    or col.endswith("Id")
                )
            )
        if (
            col == primary_incremental
            and last_primary_value
            and is_int_col
            and is_numeric_value(last_primary_value)
        ):
            conditions.append(f"`{col}` > {last_primary_value}")
        elif (
            col == primary_incremental
            and last_primary_value
            and is_int_col
            and is_datetime_like(last_primary_value)
        ):
            continue
        elif last_analytics_load and not (
            is_int_col and col == primary_incremental
        ):
            conditions.append(
                f"(`{col}` > '{last_analytics_load}' "
                f"AND `{col}` != '0001-01-01 00:00:00')"
            )
        elif last_analytics_load and is_int_col and col == primary_incremental:
            continue

    if not conditions:
        return "1=1"
    if strategy == "and_logic":
        return " AND ".join(conditions)
    return " OR ".join(conditions)


# Back-compat name used by MDC PostgresLoader path
build_mysql_loader_where_clause = build_incremental_where_clause
