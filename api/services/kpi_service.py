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


def get_daily_production_kpi(
    db: Session,
    production_date: Optional[date] = None,
) -> dict:
    """
    Daily production day rollup aligned with OpenDental Production by Procedure.

    Source: marts.mart_daily_production_by_procedure
    (see dbt_dental_models/validation/kpi/KPI_VALIDATION_REGISTRY.md).
    """
    target_date = production_date or _clinic_today()

    query = text(
        """
        SELECT
            production_date,
            round(sum(total_fees)::numeric, 2) AS total_fees,
            sum(procedure_quantity)::int AS procedure_quantity,
            count(*)::int AS procedure_code_count
        FROM marts.mart_daily_production_by_procedure
        WHERE production_date = :production_date
        GROUP BY production_date
        """
    )
    row = db.execute(query, {"production_date": target_date}).fetchone()

    if row is None:
        return {
            "production_date": target_date,
            "total_fees": 0.0,
            "procedure_quantity": 0,
            "procedure_code_count": 0,
            "has_data": False,
        }

    return {
        "production_date": row.production_date,
        "total_fees": float(row.total_fees or 0),
        "procedure_quantity": int(row.procedure_quantity or 0),
        "procedure_code_count": int(row.procedure_code_count or 0),
        "has_data": True,
    }


def get_latest_production_date(db: Session) -> dict:
    """Latest calendar date with production activity in mart_daily_production_by_procedure."""
    query = text(
        """
        SELECT MAX(production_date) AS production_date
        FROM marts.mart_daily_production_by_procedure
        WHERE procedure_quantity > 0
        """
    )
    row = db.execute(query).fetchone()

    if row is None or row.production_date is None:
        return {"production_date": None, "has_data": False}

    return {"production_date": row.production_date, "has_data": True}


def get_daily_production_by_code(
    db: Session,
    production_date: Optional[date] = None,
) -> dict:
    """Procedure-code rows for a production_date from mart_daily_production_by_procedure."""
    target_date = production_date or _clinic_today()

    query = text(
        """
        SELECT
            production_date,
            procedure_code,
            procedure_description,
            procedure_category,
            procedure_quantity,
            average_fee,
            total_fees
        FROM marts.mart_daily_production_by_procedure
        WHERE production_date = :production_date
        ORDER BY total_fees DESC, procedure_code
        """
    )
    rows = db.execute(query, {"production_date": target_date}).fetchall()

    if not rows:
        return {
            "production_date": target_date,
            "rows": [],
            "has_data": False,
        }

    return {
        "production_date": target_date,
        "rows": [
            {
                "production_date": r.production_date,
                "procedure_code": r.procedure_code,
                "procedure_description": r.procedure_description,
                "procedure_category": r.procedure_category,
                "procedure_quantity": int(r.procedure_quantity or 0),
                "average_fee": float(r.average_fee or 0),
                "total_fees": float(r.total_fees or 0),
            }
            for r in rows
        ],
        "has_data": True,
    }
