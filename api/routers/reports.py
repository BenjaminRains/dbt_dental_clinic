# Comprehensive reporting endpoints for dental analytics dashboard
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, date
import pandas as pd

from database import get_db
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
    db: Session = Depends(get_db)
):
    """Get revenue trends over time with filtering capabilities"""
    query = """
    SELECT 
        dd.date_actual,
        SUM(mrl.revenue_lost_amount) as total_revenue_lost,
        SUM(mrl.recovery_potential) as total_recovery_potential,
        COUNT(DISTINCT mrl.opportunity_id) as opportunity_count
    FROM raw_marts.mart_revenue_lost mrl
    JOIN raw_marts.dim_date dd ON mrl.date_id = dd.date_id
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND dd.date_actual >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND dd.date_actual <= :end_date"
        params['end_date'] = end_date
    if provider_id:
        query += " AND mrl.provider_id = :provider_id"
        params['provider_id'] = provider_id
    
    query += """
    GROUP BY dd.date_actual
    ORDER BY dd.date_actual
    """
    
    result = db.execute(query, params).fetchall()
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
    db: Session = Depends(get_db)
):
    """Get key revenue performance indicators"""
    query = """
    SELECT 
        SUM(mrl.revenue_lost_amount) as total_revenue_lost,
        SUM(mrl.recovery_potential) as total_recovery_potential,
        AVG(mrl.recovery_potential) as avg_recovery_potential,
        COUNT(DISTINCT mrl.opportunity_id) as total_opportunities,
        COUNT(DISTINCT mrl.patient_id) as affected_patients,
        COUNT(DISTINCT mrl.provider_id) as affected_providers
    FROM raw_marts.mart_revenue_lost mrl
    JOIN raw_marts.dim_date dd ON mrl.date_id = dd.date_id
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND dd.date_actual >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND dd.date_actual <= :end_date"
        params['end_date'] = end_date
    
    result = db.execute(query, params).fetchone()
    
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
    db: Session = Depends(get_db)
):
    """Get comprehensive provider performance metrics"""
    query = """
    SELECT 
        dp.provider_name,
        dp.provider_specialty,
        mpp.date_actual,
        mpp.production_amount,
        mpp.collection_amount,
        mpp.collection_rate,
        mpp.patient_count,
        mpp.appointment_count,
        mpp.no_show_count,
        mpp.no_show_rate,
        mpp.avg_production_per_patient,
        mpp.avg_production_per_appointment
    FROM raw_marts.mart_provider_performance mpp
    JOIN raw_marts.dim_provider dp ON mpp.provider_id = dp.provider_id
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND mpp.date_actual >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND mpp.date_actual <= :end_date"
        params['end_date'] = end_date
    if provider_id:
        query += " AND mpp.provider_id = :provider_id"
        params['provider_id'] = provider_id
    
    query += " ORDER BY mpp.date_actual DESC, dp.provider_name"
    
    result = db.execute(query, params).fetchall()
    return [
        {
            "provider_name": row.provider_name,
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
    db: Session = Depends(get_db)
):
    """Get aggregated provider performance summary"""
    query = """
    SELECT 
        dp.provider_name,
        dp.provider_specialty,
        SUM(mpp.production_amount) as total_production,
        SUM(mpp.collection_amount) as total_collection,
        AVG(mpp.collection_rate) as avg_collection_rate,
        SUM(mpp.patient_count) as total_patients,
        SUM(mpp.appointment_count) as total_appointments,
        SUM(mpp.no_show_count) as total_no_shows,
        AVG(mpp.no_show_rate) as avg_no_show_rate,
        AVG(mpp.avg_production_per_patient) as avg_production_per_patient,
        AVG(mpp.avg_production_per_appointment) as avg_production_per_appointment
    FROM raw_marts.mart_provider_performance mpp
    JOIN raw_marts.dim_provider dp ON mpp.provider_id = dp.provider_id
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND mpp.date_actual >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND mpp.date_actual <= :end_date"
        params['end_date'] = end_date
    
    query += """
    GROUP BY dp.provider_id, dp.provider_name, dp.provider_specialty
    ORDER BY total_production DESC
    """
    
    result = db.execute(query, params).fetchall()
    return [
        {
            "provider_name": row.provider_name,
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

# Accounts Receivable Endpoints
@router.get("/ar/summary", response_model=List[ARSummary])
async def get_ar_summary(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    db: Session = Depends(get_db)
):
    """Get accounts receivable summary and aging analysis"""
    query = """
    SELECT 
        mas.date_actual,
        mas.total_ar_balance,
        mas.current_balance,
        mas.overdue_balance,
        mas.overdue_30_days,
        mas.overdue_60_days,
        mas.overdue_90_days,
        mas.overdue_120_plus_days,
        mas.collection_rate,
        mas.avg_days_to_payment,
        mas.patient_count_with_ar,
        mas.insurance_ar_balance,
        mas.patient_ar_balance
    FROM raw_marts.mart_ar_summary mas
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND mas.date_actual >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND mas.date_actual <= :end_date"
        params['end_date'] = end_date
    
    query += " ORDER BY mas.date_actual DESC"
    
    result = db.execute(query, params).fetchall()
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
    db: Session = Depends(get_db)
):
    """Get key performance indicators for dashboard overview"""
    # This would aggregate data from multiple marts for a comprehensive dashboard view
    # For now, let's create a simple version that can be expanded
    
    # Revenue KPIs
    revenue_query = """
    SELECT 
        SUM(mrl.revenue_lost_amount) as total_revenue_lost,
        SUM(mrl.recovery_potential) as total_recovery_potential
    FROM raw_marts.mart_revenue_lost mrl
    JOIN raw_marts.dim_date dd ON mrl.date_id = dd.date_id
    WHERE 1=1
    """
    
    # Provider KPIs
    provider_query = """
    SELECT 
        COUNT(DISTINCT mpp.provider_id) as active_providers,
        SUM(mpp.production_amount) as total_production,
        SUM(mpp.collection_amount) as total_collection,
        AVG(mpp.collection_rate) as avg_collection_rate
    FROM raw_marts.mart_provider_performance mpp
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        revenue_query += " AND dd.date_actual >= :start_date"
        provider_query += " AND mpp.date_actual >= :start_date"
        params['start_date'] = start_date
    if end_date:
        revenue_query += " AND dd.date_actual <= :end_date"
        provider_query += " AND mpp.date_actual <= :end_date"
        params['end_date'] = end_date
    
    revenue_result = db.execute(revenue_query, params).fetchone()
    provider_result = db.execute(provider_query, params).fetchone()
    
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