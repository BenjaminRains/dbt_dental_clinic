"""Unit tests for appointment_service row mapping helpers."""

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

from services.appointment_service import (
    _format_appointment_time,
    _row_to_appointment_detail,
)


def test_format_appointment_time_datetime():
    assert _format_appointment_time(datetime(2025, 3, 15, 14, 30)) == "14:30"


def test_format_appointment_time_iso_string():
    assert _format_appointment_time("2025-03-15T14:30:00") == "14:30"


def test_row_to_appointment_detail_coerces_numeric_types():
    row = SimpleNamespace(
        appointment_id=Decimal("101"),
        patient_id=Decimal("202"),
        provider_id=None,
        appointment_date=datetime(2025, 3, 15).date(),
        appointment_datetime=datetime(2025, 3, 15, 9, 15),
        appointment_type="Hygiene",
        appointment_status="Scheduled",
        is_completed=False,
        is_no_show=False,
        is_broken=False,
        scheduled_production_amount=Decimal("120.50"),
        appointment_length_minutes=Decimal("45.0"),
    )

    detail = _row_to_appointment_detail(row)

    assert detail is not None
    assert detail.appointment_id == 101
    assert detail.patient_id == 202
    assert detail.provider_id == 0
    assert detail.appointment_time == "09:15"
    assert detail.scheduled_production_amount == 120.5
    assert detail.appointment_length_minutes == 45


def test_row_to_appointment_detail_skips_incomplete_rows():
    row = SimpleNamespace(
        appointment_id=1,
        patient_id=None,
        provider_id=3,
        appointment_date=datetime(2025, 3, 15).date(),
        appointment_datetime=None,
        appointment_type=None,
        appointment_status=None,
        is_completed=False,
        is_no_show=False,
        is_broken=False,
        scheduled_production_amount=0,
        appointment_length_minutes=0,
    )

    assert _row_to_appointment_detail(row) is None
