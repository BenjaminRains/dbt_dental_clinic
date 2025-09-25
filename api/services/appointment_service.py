# api/services/appointment_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from datetime import date
from models.appointment import AppointmentSummary, AppointmentDetail, AppointmentCreate, AppointmentUpdate

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
        dp.provider_name,
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
    FROM raw_marts.fact_appointment fa
    LEFT JOIN raw_marts.dim_provider dp ON fa.provider_id = dp.provider_id
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
    
    query += " GROUP BY fa.appointment_date, dp.provider_name ORDER BY fa.appointment_date DESC, dp.provider_name"
    
    result = db.execute(text(query), params).fetchall()
    return [
        AppointmentSummary(
            date=row.date_actual.isoformat(),
            provider_name=row.provider_name or "Unknown",
            total_appointments=row.total_appointments,
            completed_appointments=row.completed_appointments,
            no_show_appointments=row.no_show_appointments,
            broken_appointments=row.broken_appointments,
            new_patient_appointments=row.new_patient_appointments,
            hygiene_appointments=row.hygiene_appointments,
            unique_patients=row.unique_patients,
            completion_rate=float(row.appointment_completion_rate or 0),
            no_show_rate=float(row.no_show_rate or 0),
            cancellation_rate=float(row.cancellation_rate or 0),
            utilization_rate=0.0,  # Not available in fact_appointment
            scheduled_production=float(row.total_scheduled_production or 0),
            completed_production=float(row.completed_production or 0)
        )
        for row in result
    ]

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
        dp.provider_name,
        fa.appointment_date,
        fa.appointment_datetime,
        fa.appointment_type,
        fa.appointment_status,
        fa.is_completed,
        fa.is_no_show,
        fa.is_broken,
        fa.scheduled_production_amount,
        fa.appointment_length_minutes
    FROM raw_marts.fact_appointment fa
    LEFT JOIN raw_marts.dim_provider dp ON fa.provider_id = dp.provider_id
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
    
    query += " ORDER BY fa.appointment_date DESC, fa.appointment_datetime DESC LIMIT :limit OFFSET :skip"
    params['limit'] = limit
    params['skip'] = skip
    
    result = db.execute(text(query), params).fetchall()
    return [
        AppointmentDetail(
            appointment_id=row.appointment_id,
            patient_id=row.patient_id,
            provider_name=row.provider_name or "Unknown",
            appointment_date=row.appointment_date.isoformat(),
            appointment_time=row.appointment_datetime.strftime("%H:%M") if row.appointment_datetime else "",
            appointment_type=row.appointment_type or "Unknown",
            appointment_status=row.appointment_status or "Unknown",
            is_completed=bool(row.is_completed),
            is_no_show=bool(row.is_no_show),
            is_broken=bool(row.is_broken),
            scheduled_production_amount=float(row.scheduled_production_amount or 0),
            appointment_length_minutes=row.appointment_length_minutes or 0
        )
        for row in result
    ]

def get_appointment_by_id(db: Session, appointment_id: int) -> Optional[AppointmentDetail]:
    """Get a specific appointment by ID"""
    query = """
    SELECT 
        fa.appointment_id,
        fa.patient_id,
        dp.provider_name,
        fa.appointment_date,
        fa.appointment_datetime,
        fa.appointment_type,
        fa.appointment_status,
        fa.is_completed,
        fa.is_no_show,
        fa.is_broken,
        fa.scheduled_production_amount,
        fa.appointment_length_minutes
    FROM raw_marts.fact_appointment fa
    LEFT JOIN raw_marts.dim_provider dp ON fa.provider_id = dp.provider_id
    WHERE fa.appointment_id = :appointment_id
    """
    
    result = db.execute(text(query), {"appointment_id": appointment_id}).fetchone()
    if not result:
        return None
    
    return AppointmentDetail(
        appointment_id=result.appointment_id,
        patient_id=result.patient_id,
        provider_name=result.provider_name or "Unknown",
        appointment_date=result.appointment_date.isoformat(),
        appointment_time=result.appointment_datetime.strftime("%H:%M") if result.appointment_datetime else "",
        appointment_type=result.appointment_type or "Unknown",
        appointment_status=result.appointment_status or "Unknown",
        is_completed=bool(result.is_completed),
        is_no_show=bool(result.is_no_show),
        is_broken=bool(result.is_broken),
        scheduled_production_amount=float(result.scheduled_production_amount or 0),
        appointment_length_minutes=result.appointment_length_minutes or 0
    )

def get_today_appointments(db: Session, provider_id: Optional[int] = None) -> List[AppointmentDetail]:
    """Get today's appointments"""
    query = """
    SELECT 
        fa.appointment_id,
        fa.patient_id,
        dp.provider_name,
        fa.appointment_date,
        fa.appointment_datetime,
        fa.appointment_type,
        fa.appointment_status,
        fa.is_completed,
        fa.is_no_show,
        fa.is_broken,
        fa.scheduled_production_amount,
        fa.appointment_length_minutes
    FROM raw_marts.fact_appointment fa
    LEFT JOIN raw_marts.dim_provider dp ON fa.provider_id = dp.provider_id
    WHERE fa.appointment_date = CURRENT_DATE
    """
    
    params = {}
    if provider_id:
        query += " AND fa.provider_id = :provider_id"
        params['provider_id'] = provider_id
    
    query += " ORDER BY fa.appointment_datetime ASC"
    
    result = db.execute(text(query), params).fetchall()
    return [
        AppointmentDetail(
            appointment_id=row.appointment_id,
            patient_id=row.patient_id,
            provider_name=row.provider_name or "Unknown",
            appointment_date=row.appointment_date.isoformat(),
            appointment_time=row.appointment_datetime.strftime("%H:%M") if row.appointment_datetime else "",
            appointment_type=row.appointment_type or "Unknown",
            appointment_status=row.appointment_status or "Unknown",
            is_completed=bool(row.is_completed),
            is_no_show=bool(row.is_no_show),
            is_broken=bool(row.is_broken),
            scheduled_production_amount=float(row.scheduled_production_amount or 0),
            appointment_length_minutes=row.appointment_length_minutes or 0
        )
        for row in result
    ]

def get_upcoming_appointments(db: Session, days: int = 7, provider_id: Optional[int] = None) -> List[AppointmentDetail]:
    """Get upcoming appointments for the next N days"""
    query = """
    SELECT 
        fa.appointment_id,
        fa.patient_id,
        dp.provider_name,
        fa.appointment_date,
        fa.appointment_datetime,
        fa.appointment_type,
        fa.appointment_status,
        fa.is_completed,
        fa.is_no_show,
        fa.is_broken,
        fa.scheduled_production_amount,
        fa.appointment_length_minutes
    FROM raw_marts.fact_appointment fa
    LEFT JOIN raw_marts.dim_provider dp ON fa.provider_id = dp.provider_id
    WHERE fa.appointment_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL ':days days'
    """
    
    params = {"days": days}
    if provider_id:
        query += " AND fa.provider_id = :provider_id"
        params['provider_id'] = provider_id
    
    query += " ORDER BY fa.appointment_date ASC, fa.appointment_datetime ASC"
    
    result = db.execute(text(query), params).fetchall()
    return [
        AppointmentDetail(
            appointment_id=row.appointment_id,
            patient_id=row.patient_id,
            provider_name=row.provider_name or "Unknown",
            appointment_date=row.appointment_date.isoformat(),
            appointment_time=row.appointment_datetime.strftime("%H:%M") if row.appointment_datetime else "",
            appointment_type=row.appointment_type or "Unknown",
            appointment_status=row.appointment_status or "Unknown",
            is_completed=bool(row.is_completed),
            is_no_show=bool(row.is_no_show),
            is_broken=bool(row.is_broken),
            scheduled_production_amount=float(row.scheduled_production_amount or 0),
            appointment_length_minutes=row.appointment_length_minutes or 0
        )
        for row in result
    ]
