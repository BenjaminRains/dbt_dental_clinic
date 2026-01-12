# api/services/provider_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional
from datetime import date
import logging

logger = logging.getLogger(__name__)

def _int_to_hex_color(color_int):
    """Convert integer color value to hex string format (#RRGGBB)"""
    if color_int is None:
        return None
    # Convert signed 32-bit integer to unsigned (handle negative values)
    # Windows color format uses signed integers for ARGB
    if color_int < 0:
        # Convert negative to unsigned 32-bit
        color_int = color_int & 0xFFFFFFFF
    # Extract RGB (skip alpha channel)
    # Format: ARGB -> we want RGB, so take lower 24 bits
    rgb = color_int & 0xFFFFFF
    # Format as #RRGGBB
    return f"#{rgb:06X}"

def _convert_provider_row(row_dict):
    """Convert provider row data to match Pydantic model expectations"""
    # Convert provider_color from integer to hex string
    if 'provider_color' in row_dict:
        row_dict['provider_color'] = _int_to_hex_color(row_dict['provider_color'])
    
    # Convert outline_color from integer to hex string
    if 'outline_color' in row_dict:
        row_dict['outline_color'] = _int_to_hex_color(row_dict['outline_color'])
    
    return row_dict

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
        provider_custom_id,
        fee_schedule_id,
        specialty_id,
        specialty_description,
        provider_status,
        provider_status_description,
        anesthesia_provider_type,
        anesthesia_type_description as anesthesia_provider_type_description,
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
    
    query += " ORDER BY provider_id"
    query += " LIMIT :limit OFFSET :skip"
    
    result = db.execute(text(query), {"limit": limit, "skip": skip}).fetchall()
    # Convert rows to dicts and fix data types
    return [_convert_provider_row(dict(row._mapping)) for row in result]

def get_provider_by_id(
    db: Session,
    provider_id: int
) -> Optional[dict]:
    """Get detailed provider information by provider_id"""
    
    query = """
    SELECT 
        provider_id,
        provider_custom_id,
        fee_schedule_id,
        specialty_id,
        specialty_description,
        provider_status,
        provider_status_description,
        anesthesia_provider_type,
        anesthesia_type_description as anesthesia_provider_type_description,
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
    if result:
        return _convert_provider_row(dict(result._mapping))
    return None

def get_provider_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[dict]:
    """Get provider performance summary aggregated from appointment data"""
    
    # First, verify table and column existence (for debugging)
    try:
        test_cols = db.execute(text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'raw_marts' 
            AND table_name = 'fact_appointment'
            AND column_name IN ('provider_id', 'appointment_date', 'is_completed', 'is_no_show', 'is_broken', 'scheduled_production_amount', 'patient_id')
            ORDER BY column_name
        """)).fetchall()
        logger.info(f"Verified {len(test_cols)} required columns in fact_appointment")
        if len(test_cols) < 7:
            missing = ['provider_id', 'appointment_date', 'is_completed', 'is_no_show', 'is_broken', 'scheduled_production_amount', 'patient_id']
            found = [col.column_name for col in test_cols]
            missing_cols = [c for c in missing if c not in found]
            logger.error(f"Missing columns in fact_appointment: {missing_cols}")
    except Exception as test_error:
        logger.warning(f"Could not verify columns (non-fatal): {str(test_error)}")
    
    # Build query with proper parameter binding
    query = """
    SELECT 
        fa.provider_id,
        COUNT(*)::int as total_appointments,
        SUM(CASE WHEN COALESCE(fa.is_completed, false) = true THEN 1 ELSE 0 END)::int as completed_appointments,
        SUM(CASE WHEN COALESCE(fa.is_no_show, false) = true THEN 1 ELSE 0 END)::int as no_show_appointments,
        SUM(CASE WHEN COALESCE(fa.is_broken, false) = true THEN 1 ELSE 0 END)::int as broken_appointments,
        COUNT(DISTINCT fa.patient_id)::int as unique_patients,
        CASE 
            WHEN COUNT(*) > 0 
            THEN ROUND((SUM(CASE WHEN COALESCE(fa.is_completed, false) = true THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0)::numeric * 100)::numeric, 2)
            ELSE 0.0
        END::numeric as completion_rate,
        CASE 
            WHEN COUNT(*) > 0 
            THEN ROUND((SUM(CASE WHEN COALESCE(fa.is_no_show, false) = true THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0)::numeric * 100)::numeric, 2)
            ELSE 0.0
        END::numeric as no_show_rate,
        CASE 
            WHEN COUNT(*) > 0 
            THEN ROUND((SUM(CASE WHEN COALESCE(fa.is_broken, false) = true THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0)::numeric * 100)::numeric, 2)
            ELSE 0.0
        END::numeric as cancellation_rate,
        COALESCE(SUM(fa.scheduled_production_amount), 0.0)::numeric as total_scheduled_production,
        COALESCE(SUM(CASE WHEN COALESCE(fa.is_completed, false) = true THEN fa.scheduled_production_amount ELSE 0 END), 0.0)::numeric as completed_production,
        CASE 
            WHEN COUNT(*) > 0 
            THEN ROUND(AVG(COALESCE(fa.scheduled_production_amount, 0.0)::numeric), 2)
            ELSE 0.0
        END::numeric as avg_production_per_appointment
    FROM raw_marts.fact_appointment fa
    WHERE fa.appointment_date IS NOT NULL
    AND fa.provider_id IS NOT NULL
    """
    
    params = {}
    if start_date:
        query += " AND fa.appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND fa.appointment_date <= :end_date"
        params['end_date'] = end_date
    
    query += " GROUP BY fa.provider_id ORDER BY total_appointments DESC"
    
    try:
        logger.info(f"Executing provider summary query")
        logger.debug(f"Query: {query}")
        logger.debug(f"Params: {params}")
        
        # Test if table exists first
        test_query = "SELECT COUNT(*) FROM raw_marts.fact_appointment LIMIT 1"
        try:
            test_result = db.execute(text(test_query)).scalar()
            logger.info(f"Table exists, row count check returned: {test_result}")
        except Exception as test_error:
            logger.error(f"Table access test failed: {str(test_error)}")
            raise
        
        result = db.execute(text(query), params).fetchall()
        logger.info(f"Provider summary query returned {len(result)} rows")
        
        if len(result) == 0:
            logger.warning("Provider summary query returned no rows")
            return []
        
        rows = []
        for idx, row in enumerate(result):
            try:
                # Convert row to dict safely
                if hasattr(row, '_mapping'):
                    row_dict = dict(row._mapping)
                else:
                    row_dict = dict(row)
                
                # Ensure all required fields exist with defaults
                provider_row = {
                    "provider_id": int(row_dict.get('provider_id', 0) or 0),
                    "total_appointments": int(row_dict.get('total_appointments', 0) or 0),
                    "completed_appointments": int(row_dict.get('completed_appointments', 0) or 0),
                    "no_show_appointments": int(row_dict.get('no_show_appointments', 0) or 0),
                    "broken_appointments": int(row_dict.get('broken_appointments', 0) or 0),
                    "unique_patients": int(row_dict.get('unique_patients', 0) or 0),
                    "completion_rate": float(row_dict.get('completion_rate', 0) or 0),
                    "no_show_rate": float(row_dict.get('no_show_rate', 0) or 0),
                    "cancellation_rate": float(row_dict.get('cancellation_rate', 0) or 0),
                    "total_scheduled_production": float(row_dict.get('total_scheduled_production', 0) or 0),
                    "completed_production": float(row_dict.get('completed_production', 0) or 0),
                    "avg_production_per_appointment": float(row_dict.get('avg_production_per_appointment', 0) or 0)
                }
                rows.append(provider_row)
            except Exception as row_error:
                logger.error(f"Error processing provider summary row {idx}: {str(row_error)}")
                logger.error(f"Row type: {type(row)}")
                logger.error(f"Row data: {row_dict if 'row_dict' in locals() else 'N/A'}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                # Continue processing other rows instead of failing completely
                continue
        
        logger.info(f"Successfully processed {len(rows)} provider summary rows")
        return rows
        
    except SQLAlchemyError as e:
        error_msg = str(e)
        logger.error(f"SQL error fetching provider summary: {error_msg}")
        logger.error(f"Query: {query}")
        logger.error(f"Params: {params}")
        import traceback
        logger.error(f"SQL Traceback: {traceback.format_exc()}")
        raise
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Unexpected error fetching provider summary: {error_msg}", exc_info=True)
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise
