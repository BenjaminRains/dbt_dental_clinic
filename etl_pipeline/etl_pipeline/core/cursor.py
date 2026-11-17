"""
Read-side extract cursor.

This is the watermark vocabulary for copying out of OpenDental. It does not
write ``etl_copy_status`` or ``etl_load_status``. The replicator persists a
cursor through ``_update_copy_status``. The Postgres loader persists the
matching analytics watermark through:

- ``primary_value`` → ``_update_load_status_with_primary_value``
- ``timestamp_only`` → ``_update_load_status_timestamp_only``
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

TRACKING_PRIMARY_VALUE = "primary_value"
TRACKING_TIMESTAMP_ONLY = "timestamp_only"


@dataclass(frozen=True)
class ExtractCursor:
    """One table's last successful extract position."""

    table_name: str
    watermark_column: Optional[str] = None
    last_value: Optional[object] = None
    rows_copied: int = 0
    copy_status: str = "success"

    def tracking_kind(self) -> Optional[str]:
        """Match the load-status helper this cursor corresponds to."""
        if not self.watermark_column:
            return None
        from etl_pipeline.monitoring.replica_sync_config import (
            is_timestamp_watermark_column,
        )

        if is_timestamp_watermark_column(self.watermark_column):
            return TRACKING_TIMESTAMP_ONLY
        return TRACKING_PRIMARY_VALUE

    def column_changed(self, expected_watermark: Optional[str]) -> bool:
        """True when the stored watermark column is not the one config expects."""
        return bool(
            self.watermark_column
            and expected_watermark
            and self.watermark_column != expected_watermark
        )

    def resolved_value(self, expected_watermark: Optional[str]) -> Optional[object]:
        """Return the stored value, or None when the cursor is missing or stale."""
        if self.last_value is None:
            return None
        if self.column_changed(expected_watermark):
            return None
        return self.last_value

    def advance(
        self,
        *,
        last_value: Optional[object],
        watermark_column: Optional[str],
        rows_copied: int,
        copy_status: str = "success",
    ) -> "ExtractCursor":
        """Next cursor after an extract. Does not write it anywhere."""
        return ExtractCursor(
            table_name=self.table_name,
            watermark_column=watermark_column,
            last_value=last_value,
            rows_copied=rows_copied,
            copy_status=copy_status,
        )
