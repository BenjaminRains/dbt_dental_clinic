from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
from database import get_db

router = APIRouter(prefix="/revenue", tags=["revenue"])

# Import services and models
from services.revenue_service import (
    get_revenue_trends,
    get_revenue_kpi_summary,
    get_revenue_opportunities,
    get_revenue_opportunity_summary,
    get_revenue_recovery_plan,
    get_revenue_opportunity_by_id
)
from models.revenue import (
    RevenueTrend,
    RevenueKPISummary,
    RevenueOpportunity,
    RevenueOpportunitySummary,
    RevenueRecoveryPlan
)

@router.get("/trends", response_model=List[RevenueTrend])
async def get_revenue_trends_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for trend analysis"),
    end_date: Optional[date] = Query(None, description="End date for trend analysis"),
    provider_id: Optional[int] = Query(None, description="Filter by specific provider"),
    db: Session = Depends(get_db)
):
    """Get revenue trends over time from mart_revenue_lost"""
    return get_revenue_trends(db, start_date, end_date, provider_id)

@router.get("/kpi-summary", response_model=RevenueKPISummary)
async def get_revenue_kpi_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for KPI analysis"),
    end_date: Optional[date] = Query(None, description="End date for KPI analysis"),
    db: Session = Depends(get_db)
):
    """Get revenue KPI summary for dashboard display"""
    return get_revenue_kpi_summary(db, start_date, end_date)

@router.get("/opportunities", response_model=List[RevenueOpportunity])
async def get_revenue_opportunities_endpoint(
    skip: int = Query(0, description="Number of records to skip"),
    limit: int = Query(100, description="Maximum number of records to return"),
    start_date: Optional[date] = Query(None, description="Start date for opportunity analysis"),
    end_date: Optional[date] = Query(None, description="End date for opportunity analysis"),
    provider_id: Optional[int] = Query(None, description="Filter by specific provider"),
    opportunity_type: Optional[str] = Query(None, description="Filter by opportunity type"),
    recovery_potential: Optional[str] = Query(None, description="Filter by recovery potential"),
    min_priority_score: Optional[int] = Query(None, description="Minimum priority score filter"),
    db: Session = Depends(get_db)
):
    """Get detailed revenue opportunities from mart_revenue_lost"""
    return get_revenue_opportunities(
        db, skip, limit, start_date, end_date, 
        provider_id, opportunity_type, recovery_potential, min_priority_score
    )

@router.get("/opportunities/summary", response_model=List[RevenueOpportunitySummary])
async def get_revenue_opportunity_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for summary analysis"),
    end_date: Optional[date] = Query(None, description="End date for summary analysis"),
    db: Session = Depends(get_db)
):
    """Get revenue opportunity summary by type and category"""
    return get_revenue_opportunity_summary(db, start_date, end_date)

@router.get("/recovery-plan", response_model=List[RevenueRecoveryPlan])
async def get_revenue_recovery_plan_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for recovery plan"),
    end_date: Optional[date] = Query(None, description="End date for recovery plan"),
    min_priority_score: int = Query(50, description="Minimum priority score for recovery plan"),
    db: Session = Depends(get_db)
):
    """Get revenue recovery plan with actionable items"""
    return get_revenue_recovery_plan(db, start_date, end_date, min_priority_score)

@router.get("/opportunities/{opportunity_id}", response_model=RevenueOpportunity)
async def get_revenue_opportunity_by_id_endpoint(
    opportunity_id: int,
    db: Session = Depends(get_db)
):
    """Get specific revenue opportunity by ID"""
    opportunity = get_revenue_opportunity_by_id(db, opportunity_id)
    if opportunity is None:
        raise HTTPException(status_code=404, detail="Revenue opportunity not found")
    return opportunity
