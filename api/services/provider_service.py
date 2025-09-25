# api/services/provider_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from datetime import date

def get_provider_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[dict]:
    """Get provider performance summary aggregated from appointment data"""
    
    query = """
    SELECT 
        dp.provider_name,
        COUNT(*) as total_appointments,
        SUM(CASE WHEN fa.is_completed THEN 1 ELSE 0 END) as completed_appointments,
        SUM(CASE WHEN fa.is_no_show THEN 1 ELSE 0 END) as no_show_appointments,
        SUM(CASE WHEN fa.is_broken THEN 1 ELSE 0 END) as broken_appointments,
        COUNT(DISTINCT fa.patient_id) as unique_patients,
        ROUND(SUM(CASE WHEN fa.is_completed THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 2) as completion_rate,
        ROUND(SUM(CASE WHEN fa.is_no_show THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 2) as no_show_rate,
        ROUND(SUM(CASE WHEN fa.is_broken THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 2) as cancellation_rate,
        SUM(fa.scheduled_production_amount) as total_scheduled_production,
        SUM(CASE WHEN fa.is_completed THEN fa.scheduled_production_amount ELSE 0 END) as completed_production,
        ROUND(AVG(fa.scheduled_production_amount), 2) as avg_production_per_appointment
    FROM raw_marts.fact_appointment fa
    LEFT JOIN raw_marts.dim_provider dp ON fa.provider_id = dp.provider_id
    WHERE fa.appointment_date IS NOT NULL
    AND dp.provider_name IS NOT NULL
    """
    
    params = {}
    if start_date:
        query += " AND fa.appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND fa.appointment_date <= :end_date"
        params['end_date'] = end_date
    
    query += " GROUP BY dp.provider_name ORDER BY total_appointments DESC"
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "provider_name": row.provider_name,
            "total_appointments": row.total_appointments,
            "completed_appointments": row.completed_appointments,
            "no_show_appointments": row.no_show_appointments,
            "broken_appointments": row.broken_appointments,
            "unique_patients": row.unique_patients,
            "completion_rate": float(row.completion_rate or 0),
            "no_show_rate": float(row.no_show_rate or 0),
            "cancellation_rate": float(row.cancellation_rate or 0),
            "total_scheduled_production": float(row.total_scheduled_production or 0),
            "completed_production": float(row.completed_production or 0),
            "avg_production_per_appointment": float(row.avg_production_per_appointment or 0)
        }
        for row in result
    ]
