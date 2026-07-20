"""
Replica fidelity Phase 2: shared helpers for replicator + loader sync config.

Implementation lives in ``etl_pipeline.opendental_extract`` (Airbyte Track A
extract seam). This module re-exports the public API so existing MDC imports
keep working, and keeps loader-only decisions (streaming upsert) here.
"""

from __future__ import annotations

from typing import Dict

from etl_pipeline.opendental_extract.cursor import resolve_watermark_cursor
from etl_pipeline.opendental_extract.query_builder import (
    TIMESTAMP_WATERMARK_NAMES,
    build_incremental_where_clause,
    build_mysql_loader_where_clause,
    build_mysql_lookback_predicate,
    get_incremental_columns,
    get_loader_incremental_columns,
    get_replicator_watermark_column,
    is_datetime_like,
    is_numeric_value,
    is_timestamp_watermark_column,
    lookback_resync_enabled,
    uses_in_place_timestamp_watermark,
    wrap_mysql_incremental_with_lookback_config,
)

__all__ = [
    "TIMESTAMP_WATERMARK_NAMES",
    "build_incremental_where_clause",
    "build_mysql_loader_where_clause",
    "build_mysql_lookback_predicate",
    "get_incremental_columns",
    "get_loader_incremental_columns",
    "get_replicator_watermark_column",
    "is_datetime_like",
    "is_numeric_value",
    "is_timestamp_watermark_column",
    "lookback_resync_enabled",
    "resolve_watermark_cursor",
    "should_use_streaming_upsert",
    "uses_in_place_timestamp_watermark",
    "wrap_mysql_incremental_with_lookback_config",
]


def should_use_streaming_upsert(table_config: Dict, *, should_truncate: bool) -> bool:
    """MDC loader decision — not part of the extract SDK surface."""
    if should_truncate:
        return False
    return (
        uses_in_place_timestamp_watermark(table_config)
        or lookback_resync_enabled(table_config)
    )
