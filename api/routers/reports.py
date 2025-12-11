# Comprehensive reporting endpoints for dental analytics dashboard
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from datetime import datetime, date
import pandas as pd

from database import get_db
from auth.api_key import require_api_key
from api_types import (
    RevenueTrend, RevenueKPISummary, ProviderPerformance, 
    ProviderSummary, ARSummary, DashboardKPIs
)

router = APIRouter(
    prefix="/reports",
    tags=["reports"],
    responses={404: {"description": "Not found"}},
)

# Revenue Analytics Endpoints
@router.get("/revenue/trends", response_model=List[RevenueTrend])
async def get_revenue_trends(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    provider_id: Optional[int] = Query(None, description="Filter by provider"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get revenue trends over time with filtering capabilities"""
    query = """
    SELECT 
        mrl.appointment_date as date_actual,
        SUM(mrl.lost_revenue) as total_revenue_lost,
        SUM(mrl.estimated_recoverable_amount) as total_recovery_potential,
        COUNT(DISTINCT mrl.opportunity_id) as opportunity_count
    FROM raw_marts.mart_revenue_lost mrl
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND mrl.appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND mrl.appointment_date <= :end_date"
        params['end_date'] = end_date
    if provider_id:
        query += " AND mrl.provider_id = :provider_id"
        params['provider_id'] = provider_id
    
    query += """
    GROUP BY mrl.appointment_date
    ORDER BY mrl.appointment_date
    """
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "date": row.date_actual.isoformat(),
            "revenue_lost": float(row.total_revenue_lost or 0),
            "recovery_potential": float(row.total_recovery_potential or 0),
            "opportunity_count": row.opportunity_count
        }
        for row in result
    ]

@router.get("/revenue/kpi-summary", response_model=RevenueKPISummary)
async def get_revenue_kpi_summary(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get key revenue performance indicators"""
    query = """
    SELECT 
        SUM(mrl.lost_revenue) as total_revenue_lost,
        SUM(mrl.estimated_recoverable_amount) as total_recovery_potential,
        AVG(mrl.estimated_recoverable_amount) as avg_recovery_potential,
        COUNT(DISTINCT mrl.opportunity_id) as total_opportunities,
        COUNT(DISTINCT mrl.patient_id) as affected_patients,
        COUNT(DISTINCT mrl.provider_id) as affected_providers
    FROM raw_marts.mart_revenue_lost mrl
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND mrl.appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND mrl.appointment_date <= :end_date"
        params['end_date'] = end_date
    
    result = db.execute(text(query), params).fetchone()
    
    return {
        "total_revenue_lost": float(result.total_revenue_lost or 0),
        "total_recovery_potential": float(result.total_recovery_potential or 0),
        "avg_recovery_potential": float(result.avg_recovery_potential or 0),
        "total_opportunities": result.total_opportunities,
        "affected_patients": result.affected_patients,
        "affected_providers": result.affected_providers
    }

# Provider Performance Endpoints
@router.get("/providers/performance", response_model=List[ProviderPerformance])
async def get_provider_performance(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    provider_id: Optional[int] = Query(None, description="Filter by specific provider"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get comprehensive provider performance metrics"""
    query = """
    SELECT 
        mpp.provider_id,
        mpp.specialty as provider_specialty,
        mpp.production_date as date_actual,
        mpp.total_production as production_amount,
        mpp.total_collections as collection_amount,
        mpp.collection_efficiency as collection_rate,
        mpp.total_unique_patients as patient_count,
        mpp.total_completed_appointments as appointment_count,
        mpp.total_missed_appointments as no_show_count,
        mpp.daily_no_show_rate as no_show_rate,
        round(mpp.total_production / nullif(mpp.total_unique_patients, 0), 2) as avg_production_per_patient,
        round(mpp.total_production / nullif(mpp.total_completed_appointments, 0), 2) as avg_production_per_appointment
    FROM raw_marts.mart_provider_performance mpp
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND mpp.production_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND mpp.production_date <= :end_date"
        params['end_date'] = end_date
    if provider_id:
        query += " AND mpp.provider_id = :provider_id"
        params['provider_id'] = provider_id
    
    query += " ORDER BY mpp.production_date DESC, mpp.provider_id"
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "provider_id": row.provider_id,
            "provider_specialty": row.provider_specialty,
            "date": row.date_actual.isoformat(),
            "production_amount": float(row.production_amount or 0),
            "collection_amount": float(row.collection_amount or 0),
            "collection_rate": float(row.collection_rate or 0),
            "patient_count": row.patient_count,
            "appointment_count": row.appointment_count,
            "no_show_count": row.no_show_count,
            "no_show_rate": float(row.no_show_rate or 0),
            "avg_production_per_patient": float(row.avg_production_per_patient or 0),
            "avg_production_per_appointment": float(row.avg_production_per_appointment or 0)
        }
        for row in result
    ]

@router.get("/providers/summary", response_model=List[ProviderSummary])
async def get_provider_summary(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get aggregated provider performance summary"""
    query = """
    SELECT 
        mpp.provider_id,
        mpp.specialty as provider_specialty,
        SUM(mpp.total_production) as total_production,
        SUM(mpp.total_collections) as total_collection,
        AVG(mpp.collection_efficiency) as avg_collection_rate,
        SUM(mpp.total_unique_patients) as total_patients,
        SUM(mpp.total_completed_appointments) as total_appointments,
        SUM(mpp.total_missed_appointments) as total_no_shows,
        AVG(mpp.daily_no_show_rate) as avg_no_show_rate,
        AVG(round(mpp.total_production / nullif(mpp.total_unique_patients, 0), 2)) as avg_production_per_patient,
        AVG(round(mpp.total_production / nullif(mpp.total_completed_appointments, 0), 2)) as avg_production_per_appointment
    FROM raw_marts.mart_provider_performance mpp
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND mpp.production_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND mpp.production_date <= :end_date"
        params['end_date'] = end_date
    
    query += """
    GROUP BY mpp.provider_id, mpp.specialty
    ORDER BY total_production DESC
    """
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "provider_id": row.provider_id,
            "provider_specialty": row.provider_specialty,
            "total_production": float(row.total_production or 0),
            "total_collection": float(row.total_collection or 0),
            "avg_collection_rate": float(row.avg_collection_rate or 0),
            "total_patients": row.total_patients,
            "total_appointments": row.total_appointments,
            "total_no_shows": row.total_no_shows,
            "avg_no_show_rate": float(row.avg_no_show_rate or 0),
            "avg_production_per_patient": float(row.avg_production_per_patient or 0),
            "avg_production_per_appointment": float(row.avg_production_per_appointment or 0)
        }
        for row in result
    ]

# Appointment Endpoints
@router.get("/appointments/summary", response_model=List[dict])
async def get_appointment_summary(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    provider_id: Optional[int] = Query(None, description="Filter by provider"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
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
    FROM raw_marts.fact_appointment fa
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
    return [
        {
            "date": row.date_actual.isoformat(),
            "provider_id": row.provider_id,
            "total_appointments": row.total_appointments,
            "completed_appointments": row.completed_appointments,
            "no_show_appointments": row.no_show_appointments,
            "broken_appointments": row.broken_appointments,
            "new_patient_appointments": row.new_patient_appointments,
            "hygiene_appointments": row.hygiene_appointments,
            "unique_patients": row.unique_patients,
            "completion_rate": float(row.appointment_completion_rate or 0),
            "no_show_rate": float(row.no_show_rate or 0),
            "cancellation_rate": float(row.cancellation_rate or 0),
            "utilization_rate": 0.0,  # Not available in fact_appointment
            "scheduled_production": float(row.total_scheduled_production or 0),
            "completed_production": float(row.completed_production or 0)
        }
        for row in result
    ]

# Accounts Receivable Endpoints
@router.get("/ar/summary", response_model=List[ARSummary])
async def get_ar_summary(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get accounts receivable summary and aging analysis"""
    query = """
    SELECT 
        mas.snapshot_date as date_actual,
        mas.total_balance as total_ar_balance,
        mas.balance_0_30_days as current_balance,
        (mas.balance_31_60_days + mas.balance_61_90_days + mas.balance_over_90_days) as overdue_balance,
        mas.balance_31_60_days as overdue_30_days,
        mas.balance_61_90_days as overdue_60_days,
        mas.balance_over_90_days as overdue_90_days,
        mas.balance_over_90_days as overdue_120_plus_days,
        round(mas.paid_last_year::numeric / nullif(mas.billed_last_year, 0) * 100, 2) as collection_rate,
        mas.days_since_last_payment as avg_days_to_payment,
        count(distinct mas.patient_id) as patient_count_with_ar,
        mas.insurance_estimate as insurance_ar_balance,
        mas.patient_responsibility as patient_ar_balance
    FROM raw_marts.mart_ar_summary mas
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND mas.snapshot_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND mas.snapshot_date <= :end_date"
        params['end_date'] = end_date
    
    query += " GROUP BY mas.snapshot_date, mas.total_balance, mas.balance_0_30_days, mas.balance_31_60_days, mas.balance_61_90_days, mas.balance_over_90_days, mas.paid_last_year, mas.billed_last_year, mas.days_since_last_payment, mas.insurance_estimate, mas.patient_responsibility ORDER BY mas.snapshot_date DESC"
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "date": row.date_actual.isoformat(),
            "total_ar_balance": float(row.total_ar_balance or 0),
            "current_balance": float(row.current_balance or 0),
            "overdue_balance": float(row.overdue_balance or 0),
            "overdue_30_days": float(row.overdue_30_days or 0),
            "overdue_60_days": float(row.overdue_60_days or 0),
            "overdue_90_days": float(row.overdue_90_days or 0),
            "overdue_120_plus_days": float(row.overdue_120_plus_days or 0),
            "collection_rate": float(row.collection_rate or 0),
            "avg_days_to_payment": float(row.avg_days_to_payment or 0),
            "patient_count_with_ar": row.patient_count_with_ar,
            "insurance_ar_balance": float(row.insurance_ar_balance or 0),
            "patient_ar_balance": float(row.patient_ar_balance or 0)
        }
        for row in result
    ]

# Dashboard KPI Endpoint
@router.get("/dashboard/kpis", response_model=DashboardKPIs)
async def get_dashboard_kpis(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get key performance indicators for dashboard overview"""
    # This would aggregate data from multiple marts for a comprehensive dashboard view
    # For now, let's create a simple version that can be expanded
    
    # Revenue KPIs
    revenue_query = """
    SELECT 
        SUM(mrl.lost_revenue) as total_revenue_lost,
        SUM(mrl.estimated_recoverable_amount) as total_recovery_potential
    FROM raw_marts.mart_revenue_lost mrl
    WHERE 1=1
    """
    
    # Provider KPIs
    provider_query = """
    SELECT 
        COUNT(DISTINCT mpp.provider_id) as active_providers,
        SUM(mpp.total_production) as total_production,
        SUM(mpp.total_collections) as total_collection,
        AVG(mpp.collection_efficiency) as avg_collection_rate
    FROM raw_marts.mart_provider_performance mpp
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        revenue_query += " AND mrl.appointment_date >= :start_date"
        provider_query += " AND mpp.production_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        revenue_query += " AND mrl.appointment_date <= :end_date"
        provider_query += " AND mpp.production_date <= :end_date"
        params['end_date'] = end_date
    
    revenue_result = db.execute(text(revenue_query), params).fetchone()
    provider_result = db.execute(text(provider_query), params).fetchone()
    
    return {
        "revenue": {
            "total_revenue_lost": float(revenue_result.total_revenue_lost or 0),
            "total_recovery_potential": float(revenue_result.total_recovery_potential or 0)
        },
        "providers": {
            "active_providers": provider_result.active_providers,
            "total_production": float(provider_result.total_production or 0),
            "total_collection": float(provider_result.total_collection or 0),
            "avg_collection_rate": float(provider_result.avg_collection_rate or 0)
        }
    }