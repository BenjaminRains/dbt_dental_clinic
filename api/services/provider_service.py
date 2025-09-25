# api/services/provider_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from datetime import date

def get_providers(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    active_only: bool = True
) -> List[dict]:
    """Get detailed provider information from dim_provider mart"""
    
    query = """
    SELECT 
        provider_id,
        provider_abbreviation,
        provider_last_name,
        provider_first_name,
        provider_middle_initial,
        provider_suffix,
        provider_preferred_name,
        provider_custom_id,
        fee_schedule_id,
        specialty_id,
        specialty_description,
        provider_status,
        provider_status_description,
        anesthesia_provider_type,
        anesthesia_provider_type_description,
        state_license,
        dea_number,
        blue_cross_id,
        medicaid_id,
        national_provider_id,
        state_rx_id,
        state_where_licensed,
        taxonomy_code_override,
        is_secondary,
        is_hidden,
        is_using_tin,
        has_signature_on_file,
        is_cdanet,
        is_not_person,
        is_instructor,
        is_hidden_report,
        is_erx_enabled,
        provider_color,
        outline_color,
        schedule_note,
        web_schedule_description,
        web_schedule_image_location,
        hourly_production_goal_amount,
        scheduled_days,
        total_available_minutes,
        avg_daily_available_minutes,
        days_off_count,
        avg_minutes_per_scheduled_day,
        availability_percentage,
        provider_status_category,
        provider_type_category,
        provider_specialty_category,
        availability_performance_tier,
        _loaded_at,
        _created_at,
        _updated_at,
        _transformed_at,
        _mart_refreshed_at
    FROM raw_marts.dim_provider
    WHERE provider_id IS NOT NULL
    """
    
    if active_only:
        query += " AND provider_status_category = 'Active'"
    
    query += " ORDER BY provider_last_name, provider_first_name"
    query += f" LIMIT {limit} OFFSET {skip}"
    
    result = db.execute(text(query)).fetchall()
    return [dict(row._mapping) for row in result]

def get_provider_by_id(
    db: Session,
    provider_id: int
) -> Optional[dict]:
    """Get detailed provider information by provider_id"""
    
    query = """
    SELECT 
        provider_id,
        provider_abbreviation,
        provider_last_name,
        provider_first_name,
        provider_middle_initial,
        provider_suffix,
        provider_preferred_name,
        provider_custom_id,
        fee_schedule_id,
        specialty_id,
        specialty_description,
        provider_status,
        provider_status_description,
        anesthesia_provider_type,
        anesthesia_provider_type_description,
        state_license,
        dea_number,
        blue_cross_id,
        medicaid_id,
        national_provider_id,
        state_rx_id,
        state_where_licensed,
        taxonomy_code_override,
        is_secondary,
        is_hidden,
        is_using_tin,
        has_signature_on_file,
        is_cdanet,
        is_not_person,
        is_instructor,
        is_hidden_report,
        is_erx_enabled,
        provider_color,
        outline_color,
        schedule_note,
        web_schedule_description,
        web_schedule_image_location,
        hourly_production_goal_amount,
        scheduled_days,
        total_available_minutes,
        avg_daily_available_minutes,
        days_off_count,
        avg_minutes_per_scheduled_day,
        availability_percentage,
        provider_status_category,
        provider_type_category,
        provider_specialty_category,
        availability_performance_tier,
        _loaded_at,
        _created_at,
        _updated_at,
        _transformed_at,
        _mart_refreshed_at
    FROM raw_marts.dim_provider
    WHERE provider_id = :provider_id
    """
    
    result = db.execute(text(query), {"provider_id": provider_id}).fetchone()
    return dict(result._mapping) if result else None

def get_provider_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[dict]:
    """Get provider performance summary aggregated from appointment data"""
    
    query = """
    SELECT 
        CONCAT(COALESCE(dp.provider_first_name, ''), ' ', COALESCE(dp.provider_last_name, '')) as provider_name,
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
    AND dp.provider_id IS NOT NULL
    AND dp.provider_first_name IS NOT NULL
    AND dp.provider_last_name IS NOT NULL
    """
    
    params = {}
    if start_date:
        query += " AND fa.appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND fa.appointment_date <= :end_date"
        params['end_date'] = end_date
    
    query += " GROUP BY dp.provider_id, dp.provider_first_name, dp.provider_last_name ORDER BY total_appointments DESC"
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "provider_name": str(row.provider_name or ""),
            "total_appointments": int(row.total_appointments or 0),
            "completed_appointments": int(row.completed_appointments or 0),
            "no_show_appointments": int(row.no_show_appointments or 0),
            "broken_appointments": int(row.broken_appointments or 0),
            "unique_patients": int(row.unique_patients or 0),
            "completion_rate": float(row.completion_rate or 0),
            "no_show_rate": float(row.no_show_rate or 0),
            "cancellation_rate": float(row.cancellation_rate or 0),
            "total_scheduled_production": float(row.total_scheduled_production or 0),
            "completed_production": float(row.completed_production or 0),
            "avg_production_per_appointment": float(row.avg_production_per_appointment or 0)
        }
        for row in result
    ]
