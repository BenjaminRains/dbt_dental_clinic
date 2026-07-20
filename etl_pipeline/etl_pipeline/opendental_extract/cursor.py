"""Read-side watermark cursor resolution (no persistence)."""

from __future__ import annotations

from typing import Optional


def resolve_watermark_cursor(
    table_name: str,
    last_value: Optional[str],
    stored_column: Optional[str],
    expected_watermark: Optional[str],
) -> Optional[str]:
    """
    Resolve the last cursor value for an incremental read.

    Ignores a stale cursor when the watermark column changed
    (e.g. ProcNum → DateTStamp). Does not read or write tracking tables —
    callers inject ``last_value`` / ``stored_column`` from MDC
    ``etl_copy_status`` / ``etl_load_status`` or from Airbyte stream STATE.
    """
    del table_name  # reserved for logging / future diagnostics
    if last_value is None:
        return None
    if (
        stored_column
        and expected_watermark
        and stored_column != expected_watermark
    ):
        return None
    return last_value
