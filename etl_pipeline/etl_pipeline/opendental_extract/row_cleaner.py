"""Datetime / JSON-safe row normalization for Open Dental extract rows."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Sequence

logger = logging.getLogger(__name__)


def clean_row_data(row: Sequence[Any], columns: Sequence[str], table_name: str):
    """
    Clean and validate row data before emission / hop insert.

    Preserves ints/floats/bools; strips control characters from strings;
    formats MySQL TIME (timedelta) as [-]HH:MM:SS literals.
    """
    try:
        cleaned_row = []
        for i, value in enumerate(row):
            try:
                if value is None:
                    cleaned_row.append(None)
                    continue

                if isinstance(value, str):
                    cleaned_value = "".join(
                        c for c in value if ord(c) >= 32 or c in "\t\n\r"
                    )
                    cleaned_row.append(cleaned_value)
                elif isinstance(value, (int, float, bool)):
                    cleaned_row.append(value)
                elif isinstance(value, timedelta):
                    total_seconds = int(value.total_seconds())
                    sign = "-" if total_seconds < 0 else ""
                    total_seconds = abs(total_seconds)
                    hours, remainder = divmod(total_seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    cleaned_row.append(
                        f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
                    )
                else:
                    try:
                        if hasattr(value, "__str__"):
                            str_value = str(value)
                            if not str_value.startswith("<") or not str_value.endswith(
                                ">"
                            ):
                                cleaned_row.append(str_value)
                            else:
                                col = columns[i] if i < len(columns) else i
                                logger.warning(
                                    "Converting problematic object to None for "
                                    "column %s in %s: %s",
                                    col,
                                    table_name,
                                    str_value,
                                )
                                cleaned_row.append(None)
                        else:
                            col = columns[i] if i < len(columns) else i
                            logger.warning(
                                "Converting object without string representation "
                                "to None for column %s in %s",
                                col,
                                table_name,
                            )
                            cleaned_row.append(None)
                    except Exception:
                        cleaned_row.append(None)

            except Exception as exc:  # noqa: BLE001
                col = columns[i] if i < len(columns) else i
                logger.warning(
                    "Error cleaning value for column %s in %s: %s",
                    col,
                    table_name,
                    exc,
                )
                cleaned_row.append(None)

        return cleaned_row

    except Exception as exc:  # noqa: BLE001
        logger.error("Error cleaning row data for %s: %s", table_name, exc)
        return row
