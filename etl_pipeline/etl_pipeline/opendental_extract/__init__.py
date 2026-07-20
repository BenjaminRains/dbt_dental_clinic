"""
SDK-shaped Open Dental extract surface (Airbyte Track A / gate #0).

Read + classify helpers only. No Settings, analytics loaders, Airflow, or
replication-hop writers. Cursor *logic* lives here; cursor *persistence*
(etl_copy_status / etl_load_status / Airbyte STATE) stays outside.
"""

from .config import SourceMySQLConfig
from .connection import create_source_engine
from .cursor import resolve_watermark_cursor
from .query_builder import (
    TIMESTAMP_WATERMARK_NAMES,
    build_incremental_where_clause,
    build_mysql_lookback_predicate,
    get_incremental_columns,
    get_replicator_watermark_column,
    is_datetime_like,
    is_numeric_value,
    is_timestamp_watermark_column,
    lookback_resync_enabled,
    uses_in_place_timestamp_watermark,
    wrap_mysql_incremental_with_lookback_config,
)
from .row_cleaner import clean_row_data

__all__ = [
    "SourceMySQLConfig",
    "TIMESTAMP_WATERMARK_NAMES",
    "build_incremental_where_clause",
    "build_mysql_lookback_predicate",
    "clean_row_data",
    "create_source_engine",
    "get_incremental_columns",
    "get_replicator_watermark_column",
    "is_datetime_like",
    "is_numeric_value",
    "is_timestamp_watermark_column",
    "lookback_resync_enabled",
    "resolve_watermark_cursor",
    "uses_in_place_timestamp_watermark",
    "wrap_mysql_incremental_with_lookback_config",
]
