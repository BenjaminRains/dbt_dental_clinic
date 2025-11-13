# api/services/revenue_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from datetime import date

def get_revenue_trends(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider_id: Optional[int] = None
) -> List[dict]:
    """Get revenue trends over time from mart_revenue_lost"""
    
    query = """
    SELECT 
        appointment_date as date,
        SUM(lost_revenue) as revenue_lost,
        SUM(estimated_recoverable_amount) as recovery_potential,
        COUNT(*) as opportunity_count
    FROM raw_marts.mart_revenue_lost
    WHERE lost_revenue > 0
    """
    
    params = {}
    if start_date:
        query += " AND appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND appointment_date <= :end_date"
        params['end_date'] = end_date
    if provider_id:
        query += " AND provider_id = :provider_id"
        params['provider_id'] = provider_id
    
    query += " GROUP BY appointment_date ORDER BY appointment_date"
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "date": str(row.date),
            "revenue_lost": float(row.revenue_lost or 0),
            "recovery_potential": float(row.recovery_potential or 0),
            "opportunity_count": int(row.opportunity_count or 0)
        }
        for row in result
    ]

def get_revenue_kpi_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> dict:
    """Get revenue KPI summary from mart_revenue_lost"""
    
    query = """
    SELECT 
        SUM(lost_revenue) as total_revenue_lost,
        SUM(estimated_recoverable_amount) as total_recovery_potential,
        AVG(estimated_recoverable_amount) as avg_recovery_potential,
        COUNT(*) as total_opportunities,
        COUNT(DISTINCT patient_id) as affected_patients,
        COUNT(DISTINCT provider_id) as affected_providers
    FROM raw_marts.mart_revenue_lost
    WHERE lost_revenue > 0
    """
    
    params = {}
    if start_date:
        query += " AND appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND appointment_date <= :end_date"
        params['end_date'] = end_date
    
    result = db.execute(text(query), params).fetchone()
    
    if result:
        return {
            "total_revenue_lost": float(result.total_revenue_lost or 0),
            "total_recovery_potential": float(result.total_recovery_potential or 0),
            "avg_recovery_potential": float(result.avg_recovery_potential or 0),
            "total_opportunities": int(result.total_opportunities or 0),
            "affected_patients": int(result.affected_patients or 0),
            "affected_providers": int(result.affected_providers or 0)
        }
    else:
        return {
            "total_revenue_lost": 0.0,
            "total_recovery_potential": 0.0,
            "avg_recovery_potential": 0.0,
            "total_opportunities": 0,
            "affected_patients": 0,
            "affected_providers": 0
        }

def get_revenue_opportunities(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider_id: Optional[int] = None,
    opportunity_type: Optional[str] = None,
    recovery_potential: Optional[str] = None,
    min_priority_score: Optional[int] = None
) -> List[dict]:
    """Get detailed revenue opportunities from mart_revenue_lost"""
    
    query = """
    SELECT 
        date_id,
        opportunity_id,
        appointment_date,
        provider_id,
        clinic_id,
        patient_id,
        appointment_id,
        provider_type_category,
        provider_specialty_category,
        patient_age,
        patient_gender,
        has_insurance_flag,
        patient_specific,
        year,
        month,
        quarter,
        day_name,
        is_weekend,
        is_holiday,
        opportunity_type,
        opportunity_subtype,
        lost_revenue,
        lost_time_minutes,
        missed_procedures,
        opportunity_datetime,
        recovery_potential,
        opportunity_hour,
        time_period,
        revenue_impact_category,
        time_impact_category,
        recovery_timeline,
        recovery_priority_score,
        preventability,
        has_revenue_impact,
        has_time_impact,
        recoverable,
        recent_opportunity,
        appointment_related,
        days_since_opportunity,
        estimated_recoverable_amount,
        _loaded_at,
        _created_at,
        _updated_at,
        _transformed_at,
        _mart_refreshed_at
    FROM raw_marts.mart_revenue_lost
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND appointment_date <= :end_date"
        params['end_date'] = end_date
    if provider_id:
        query += " AND provider_id = :provider_id"
        params['provider_id'] = provider_id
    if opportunity_type:
        query += " AND opportunity_type = :opportunity_type"
        params['opportunity_type'] = opportunity_type
    if recovery_potential:
        query += " AND recovery_potential = :recovery_potential"
        params['recovery_potential'] = recovery_potential
    if min_priority_score is not None:
        query += " AND recovery_priority_score >= :min_priority_score"
        params['min_priority_score'] = min_priority_score
    
    query += " ORDER BY recovery_priority_score DESC, appointment_date DESC"
    query += f" LIMIT {limit} OFFSET {skip}"
    
    result = db.execute(text(query), params).fetchall()
    return [dict(row._mapping) for row in result]

def get_revenue_opportunity_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[dict]:
    """Get revenue opportunity summary by type and category"""
    
    query = """
    SELECT 
        opportunity_type,
        opportunity_subtype,
        COUNT(*) as total_opportunities,
        SUM(lost_revenue) as total_revenue_lost,
        SUM(estimated_recoverable_amount) as total_recovery_potential,
        AVG(recovery_priority_score) as avg_priority_score,
        SUM(CASE WHEN recent_opportunity THEN 1 ELSE 0 END) as recent_opportunities,
        SUM(CASE WHEN recovery_priority_score >= 70 THEN 1 ELSE 0 END) as high_priority_opportunities
    FROM raw_marts.mart_revenue_lost
    WHERE lost_revenue > 0
    """
    
    params = {}
    if start_date:
        query += " AND appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND appointment_date <= :end_date"
        params['end_date'] = end_date
    
    query += " GROUP BY opportunity_type, opportunity_subtype ORDER BY total_revenue_lost DESC"
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "opportunity_type": str(row.opportunity_type),
            "opportunity_subtype": str(row.opportunity_subtype),
            "total_opportunities": int(row.total_opportunities or 0),
            "total_revenue_lost": float(row.total_revenue_lost or 0),
            "total_recovery_potential": float(row.total_recovery_potential or 0),
            "avg_priority_score": float(row.avg_priority_score or 0),
            "recent_opportunities": int(row.recent_opportunities or 0),
            "high_priority_opportunities": int(row.high_priority_opportunities or 0)
        }
        for row in result
    ]

def get_revenue_recovery_plan(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    min_priority_score: int = 50
) -> List[dict]:
    """Get revenue recovery plan with actionable items"""
    
    query = """
    SELECT 
        opportunity_id,
        provider_id,
        patient_id,
        opportunity_type,
        lost_revenue,
        recovery_potential,
        recovery_priority_score as priority_score,
        estimated_recoverable_amount,
        recovery_timeline,
        CASE 
            WHEN opportunity_type = 'Missed Appointment' AND opportunity_subtype = 'No Show' THEN 
                ARRAY['Call patient to reschedule', 'Send follow-up reminder', 'Update patient contact preferences']
            WHEN opportunity_type = 'Missed Appointment' AND opportunity_subtype = 'Cancellation' THEN 
                ARRAY['Reschedule appointment', 'Review cancellation policy', 'Send confirmation reminder']
            WHEN opportunity_type = 'Claim Rejection' THEN 
                ARRAY['Review claim details', 'Resubmit with corrections', 'Contact insurance company']
            WHEN opportunity_type = 'Treatment Plan Delay' THEN 
                ARRAY['Contact patient about treatment plan', 'Schedule consultation', 'Review treatment options']
            WHEN opportunity_type = 'Write Off' THEN 
                ARRAY['Review adjustment reason', 'Verify patient eligibility', 'Consider payment plan']
            ELSE ARRAY['Review opportunity details', 'Determine appropriate action']
        END as recommended_actions
    FROM raw_marts.mart_revenue_lost
    WHERE recovery_priority_score >= :min_priority_score
    AND recoverable = true
    """
    
    params = {"min_priority_score": min_priority_score}
    if start_date:
        query += " AND appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND appointment_date <= :end_date"
        params['end_date'] = end_date
    
    query += " ORDER BY recovery_priority_score DESC, appointment_date DESC LIMIT 50"
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "opportunity_id": int(row.opportunity_id),
            "provider_id": int(row.provider_id) if row.provider_id else None,
            "patient_id": int(row.patient_id),
            "opportunity_type": str(row.opportunity_type),
            "lost_revenue": float(row.lost_revenue or 0),
            "recovery_potential": str(row.recovery_potential),
            "priority_score": int(row.priority_score),
            "recommended_actions": row.recommended_actions,
            "estimated_recovery_amount": float(row.estimated_recoverable_amount or 0),
            "recovery_timeline": str(row.recovery_timeline)
        }
        for row in result
    ]

def get_revenue_opportunity_by_id(
    db: Session,
    opportunity_id: int
) -> Optional[dict]:
    """Get specific revenue opportunity by ID"""
    
    query = """
    SELECT 
        date_id,
        opportunity_id,
        appointment_date,
        provider_id,
        clinic_id,
        patient_id,
        appointment_id,
        provider_type_category,
        provider_specialty_category,
        patient_age,
        patient_gender,
        has_insurance_flag,
        patient_specific,
        year,
        month,
        quarter,
        day_name,
        is_weekend,
        is_holiday,
        opportunity_type,
        opportunity_subtype,
        lost_revenue,
        lost_time_minutes,
        missed_procedures,
        opportunity_datetime,
        recovery_potential,
        opportunity_hour,
        time_period,
        revenue_impact_category,
        time_impact_category,
        recovery_timeline,
        recovery_priority_score,
        preventability,
        has_revenue_impact,
        has_time_impact,
        recoverable,
        recent_opportunity,
        appointment_related,
        days_since_opportunity,
        estimated_recoverable_amount,
        _loaded_at,
        _created_at,
        _updated_at,
        _transformed_at,
        _mart_refreshed_at
    FROM raw_marts.mart_revenue_lost
    WHERE opportunity_id = :opportunity_id
    """
    
    result = db.execute(text(query), {"opportunity_id": opportunity_id}).fetchone()
    return dict(result._mapping) if result else None
