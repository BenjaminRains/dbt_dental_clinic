# api/services/appointment_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Any, List, Optional
from datetime import date, datetime
from decimal import Decimal

from models.appointment import AppointmentSummary, AppointmentDetail


def _coerce_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, Decimal):
        return int(value)
    return int(value)


def _coerce_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _format_appointment_time(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%H:%M")
    if isinstance(value, date):
        return ""
    if isinstance(value, str):
        if "T" in value:
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%H:%M")
            except ValueError:
                pass
        return value[:5] if len(value) >= 5 else value
    if hasattr(value, "strftime"):
        return value.strftime("%H:%M")
    return str(value)


def _format_appointment_date(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _row_to_appointment_detail(row) -> Optional[AppointmentDetail]:
    if row.appointment_id is None or row.patient_id is None or row.appointment_date is None:
        return None

    return AppointmentDetail(
        appointment_id=_coerce_int(row.appointment_id),
        patient_id=_coerce_int(row.patient_id),
        provider_id=_coerce_int(row.provider_id),
        appointment_date=_format_appointment_date(row.appointment_date),
        appointment_time=_format_appointment_time(row.appointment_datetime),
        appointment_type=row.appointment_type or "Unknown",
        appointment_status=row.appointment_status or "Unknown",
        is_completed=bool(row.is_completed),
        is_no_show=bool(row.is_no_show),
        is_broken=bool(row.is_broken),
        scheduled_production_amount=_coerce_float(row.scheduled_production_amount),
        appointment_length_minutes=_coerce_int(row.appointment_length_minutes),
    )


def _rows_to_appointment_details(rows) -> List[AppointmentDetail]:
    details: List[AppointmentDetail] = []
    for row in rows:
        detail = _row_to_appointment_detail(row)
        if detail is not None:
            details.append(detail)
    return details


def get_appointment_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider_id: Optional[int] = None
) -> List[AppointmentSummary]:
    """Get appointment summary and scheduling metrics"""
    query = """
    SELECT 
        fa.appointment_date as date_actual,
        fa.provider_id,
        COUNT(*) as total_appointments,
        SUM(CASE WHEN fa.is_completed THEN 1 ELSE 0 END) as completed_appointments,
        SUM(CASE WHEN fa.is_no_show THEN 1 ELSE 0 END) as no_show_appointments,
        SUM(CASE WHEN fa.is_broken THEN 1 ELSE 0 END) as broken_appointments,
        SUM(CASE WHEN fa.is_new_patient THEN 1 ELSE 0 END) as new_patient_appointments,
        SUM(CASE WHEN fa.is_hygiene_appointment THEN 1 ELSE 0 END) as hygiene_appointments,
        COUNT(DISTINCT fa.patient_id) as unique_patients,
        ROUND(SUM(CASE WHEN fa.is_completed THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 2) as appointment_completion_rate,
        ROUND(SUM(CASE WHEN fa.is_no_show THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 2) as no_show_rate,
        ROUND(SUM(CASE WHEN fa.is_broken THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 2) as cancellation_rate,
        SUM(fa.scheduled_production_amount) as total_scheduled_production,
        SUM(CASE WHEN fa.is_completed THEN fa.scheduled_production_amount ELSE 0 END) as completed_production
    FROM marts.fact_appointment fa
    WHERE fa.appointment_date IS NOT NULL
    """

    params = {}
    if start_date:
        query += " AND fa.appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND fa.appointment_date <= :end_date"
        params['end_date'] = end_date
    if provider_id:
        query += " AND fa.provider_id = :provider_id"
        params['provider_id'] = provider_id

    query += " GROUP BY fa.appointment_date, fa.provider_id ORDER BY fa.appointment_date DESC, fa.provider_id"

    result = db.execute(text(query), params).fetchall()
    summaries: List[AppointmentSummary] = []
    for row in result:
        if row.date_actual is None:
            continue
        summaries.append(
            AppointmentSummary(
                date=_format_appointment_date(row.date_actual),
                provider_id=_coerce_int(row.provider_id),
                total_appointments=_coerce_int(row.total_appointments),
                completed_appointments=_coerce_int(row.completed_appointments),
                no_show_appointments=_coerce_int(row.no_show_appointments),
                broken_appointments=_coerce_int(row.broken_appointments),
                new_patient_appointments=_coerce_int(row.new_patient_appointments),
                hygiene_appointments=_coerce_int(row.hygiene_appointments),
                unique_patients=_coerce_int(row.unique_patients),
                completion_rate=_coerce_float(row.appointment_completion_rate),
                no_show_rate=_coerce_float(row.no_show_rate),
                cancellation_rate=_coerce_float(row.cancellation_rate),
                utilization_rate=0.0,
                scheduled_production=_coerce_float(row.total_scheduled_production),
                completed_production=_coerce_float(row.completed_production),
            )
        )
    return summaries


def get_appointments(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider_id: Optional[int] = None
) -> List[AppointmentDetail]:
    """Get detailed appointment records"""
    query = """
    SELECT 
        fa.appointment_id,
        fa.patient_id,
        fa.provider_id,
        fa.appointment_date,
        fa.appointment_datetime,
        fa.appointment_type,
        fa.appointment_status,
        fa.is_completed,
        fa.is_no_show,
        fa.is_broken,
        fa.scheduled_production_amount,
        fa.appointment_length_minutes
    FROM marts.fact_appointment fa
    WHERE fa.appointment_date IS NOT NULL
    """

    params: dict[str, Any] = {}
    if start_date:
        query += " AND fa.appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND fa.appointment_date <= :end_date"
        params['end_date'] = end_date
    if provider_id:
        query += " AND fa.provider_id = :provider_id"
        params['provider_id'] = provider_id

    query += " ORDER BY fa.appointment_date DESC, fa.appointment_datetime DESC LIMIT :limit OFFSET :skip"
    params['limit'] = limit
    params['skip'] = skip

    result = db.execute(text(query), params).fetchall()
    return _rows_to_appointment_details(result)


def get_appointment_by_id(db: Session, appointment_id: int) -> Optional[AppointmentDetail]:
    """Get a specific appointment by ID"""
    query = """
    SELECT 
        fa.appointment_id,
        fa.patient_id,
        fa.provider_id,
        fa.appointment_date,
        fa.appointment_datetime,
        fa.appointment_type,
        fa.appointment_status,
        fa.is_completed,
        fa.is_no_show,
        fa.is_broken,
        fa.scheduled_production_amount,
        fa.appointment_length_minutes
    FROM marts.fact_appointment fa
    WHERE fa.appointment_id = :appointment_id
    """

    result = db.execute(text(query), {"appointment_id": appointment_id}).fetchone()
    if not result:
        return None

    return _row_to_appointment_detail(result)


def get_today_appointments(db: Session, provider_id: Optional[int] = None) -> List[AppointmentDetail]:
    """Get today's appointments"""
    query = """
    SELECT 
        fa.appointment_id,
        fa.patient_id,
        fa.provider_id,
        fa.appointment_date,
        fa.appointment_datetime,
        fa.appointment_type,
        fa.appointment_status,
        fa.is_completed,
        fa.is_no_show,
        fa.is_broken,
        fa.scheduled_production_amount,
        fa.appointment_length_minutes
    FROM marts.fact_appointment fa
    WHERE fa.appointment_date = CURRENT_DATE
    """

    params: dict[str, Any] = {}
    if provider_id:
        query += " AND fa.provider_id = :provider_id"
        params['provider_id'] = provider_id

    query += " ORDER BY fa.appointment_datetime ASC NULLS LAST, fa.appointment_id ASC"

    result = db.execute(text(query), params).fetchall()
    return _rows_to_appointment_details(result)


def get_upcoming_appointments(db: Session, days: int = 7, provider_id: Optional[int] = None) -> List[AppointmentDetail]:
    """Get upcoming appointments for the next N days (inclusive of today)."""
    days = max(1, min(days, 90))

    query = """
    SELECT 
        fa.appointment_id,
        fa.patient_id,
        fa.provider_id,
        fa.appointment_date,
        fa.appointment_datetime,
        fa.appointment_type,
        fa.appointment_status,
        fa.is_completed,
        fa.is_no_show,
        fa.is_broken,
        fa.scheduled_production_amount,
        fa.appointment_length_minutes
    FROM marts.fact_appointment fa
    WHERE fa.appointment_date BETWEEN CURRENT_DATE AND CURRENT_DATE + :days
    """

    params: dict[str, Any] = {"days": days}
    if provider_id:
        query += " AND fa.provider_id = :provider_id"
        params['provider_id'] = provider_id

    query += " ORDER BY fa.appointment_date ASC, fa.appointment_datetime ASC NULLS LAST, fa.appointment_id ASC"

    result = db.execute(text(query), params).fetchall()
    return _rows_to_appointment_details(result)
