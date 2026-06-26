from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session

CLINIC_TIMEZONE = ZoneInfo("America/Chicago")


def _clinic_today() -> date:
    return datetime.now(CLINIC_TIMEZONE).date()


def get_daily_collections_kpi(
    db: Session,
    payment_date: Optional[date] = None,
) -> dict:
    """
    Daily net collections aligned with OpenDental Daily Payments report.

  Source: marts.mart_daily_payments (see dbt_dental_models/validation/kpi/KPI_VALIDATION_REGISTRY.md).
    """
    target_date = payment_date or _clinic_today()

    query = text(
        """
        SELECT
            payment_date,
            net_collections_amount,
            patient_payment_amount,
            insurance_payment_amount,
            payment_count
        FROM marts.mart_daily_payments
        WHERE payment_date = :payment_date
        """
    )
    row = db.execute(query, {"payment_date": target_date}).fetchone()

    if row is None:
        return {
            "payment_date": target_date,
            "net_collections_amount": 0.0,
            "patient_payment_amount": 0.0,
            "insurance_payment_amount": 0.0,
            "payment_count": 0,
            "has_data": False,
        }

    return {
        "payment_date": row.payment_date,
        "net_collections_amount": float(row.net_collections_amount or 0),
        "patient_payment_amount": float(row.patient_payment_amount or 0),
        "insurance_payment_amount": float(row.insurance_payment_amount or 0),
        "payment_count": int(row.payment_count or 0),
        "has_data": True,
    }


def get_latest_collections_date(db: Session) -> dict:
    """Latest calendar date with at least one payment in mart_daily_payments."""
    query = text(
        """
        SELECT MAX(payment_date) AS payment_date
        FROM marts.mart_daily_payments
        WHERE payment_count > 0
        """
    )
    row = db.execute(query).fetchone()

    if row is None or row.payment_date is None:
        return {"payment_date": None, "has_data": False}

    return {"payment_date": row.payment_date, "has_data": True}
