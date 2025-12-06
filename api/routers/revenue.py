from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
from database import get_db
from auth.api_key import require_api_key
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/revenue", tags=["revenue"])

# Import services and models
from services.revenue_service import (
    get_revenue_trends,
    get_revenue_kpi_summary,
    get_revenue_opportunities,
    get_revenue_opportunity_summary,
    get_revenue_recovery_plan,
    get_revenue_opportunity_by_id,
    get_revenue_lost_summary,
    get_revenue_lost_opportunity,
    get_lost_appointments_detail
)
from models.revenue import (
    RevenueTrend,
    RevenueKPISummary,
    RevenueOpportunity,
    RevenueOpportunitySummary,
    RevenueRecoveryPlan,
    RevenueLostSummary,
    RevenueLostOpportunity,
    LostAppointmentDetail
)

@router.get("/trends", response_model=List[RevenueTrend])
async def get_revenue_trends_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for trend analysis"),
    end_date: Optional[date] = Query(None, description="End date for trend analysis"),
    provider_id: Optional[int] = Query(None, description="Filter by specific provider"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get revenue trends over time from mart_revenue_lost"""
    return get_revenue_trends(db, start_date, end_date, provider_id)

@router.get("/kpi-summary", response_model=RevenueKPISummary)
async def get_revenue_kpi_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for KPI analysis"),
    end_date: Optional[date] = Query(None, description="End date for KPI analysis"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
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
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get detailed revenue opportunities from mart_revenue_lost"""
    try:
        print(f"[DEBUG] Revenue opportunities endpoint called: opportunity_type={opportunity_type}, min_priority_score={min_priority_score}")
        logger.info(f"Revenue opportunities endpoint called: opportunity_type={opportunity_type}, min_priority_score={min_priority_score}")
        result = get_revenue_opportunities(
            db, skip, limit, start_date, end_date, 
            provider_id, opportunity_type, recovery_potential, min_priority_score
        )
        print(f"[DEBUG] Revenue opportunities endpoint returning {len(result)} results")
        logger.info(f"Revenue opportunities endpoint returning {len(result)} results")
        return result
    except Exception as e:
        error_msg = str(e)
        print(f"[ERROR] Error in revenue opportunities endpoint: {error_msg}")
        import traceback
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        logger.error(f"Error in revenue opportunities endpoint: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching revenue opportunities: {error_msg}")

@router.get("/opportunities/summary", response_model=List[RevenueOpportunitySummary])
async def get_revenue_opportunity_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for summary analysis"),
    end_date: Optional[date] = Query(None, description="End date for summary analysis"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get revenue opportunity summary by type and category"""
    return get_revenue_opportunity_summary(db, start_date, end_date)

@router.get("/recovery-plan", response_model=List[RevenueRecoveryPlan])
async def get_revenue_recovery_plan_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for recovery plan"),
    end_date: Optional[date] = Query(None, description="End date for recovery plan"),
    min_priority_score: int = Query(50, description="Minimum priority score for recovery plan"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get revenue recovery plan with actionable items"""
    try:
        return get_revenue_recovery_plan(db, start_date, end_date, min_priority_score)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching recovery plan: {str(e)}")

@router.get("/opportunities/{opportunity_id}", response_model=RevenueOpportunity)
async def get_revenue_opportunity_by_id_endpoint(
    opportunity_id: int,
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get specific revenue opportunity by ID"""
    opportunity = get_revenue_opportunity_by_id(db, opportunity_id)
    if opportunity is None:
        raise HTTPException(status_code=404, detail="Revenue opportunity not found")
    return opportunity

# PBN-style Revenue Lost endpoints
@router.get("/lost-summary", response_model=RevenueLostSummary)
async def get_revenue_lost_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get PBN-style Revenue Lost summary (Appmts Lost $, Recovered $, Lost Appmts %)"""
    try:
        return get_revenue_lost_summary(db, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching revenue lost summary: {str(e)}")

@router.get("/lost-opportunity", response_model=RevenueLostOpportunity)
async def get_revenue_lost_opportunity_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get PBN-style Opportunity metrics (Failed %, Cancelled %, Re-appnt %)"""
    try:
        return get_revenue_lost_opportunity(db, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching revenue lost opportunity: {str(e)}")

@router.get("/lost-appointments", response_model=List[LostAppointmentDetail])
async def get_lost_appointments_detail_endpoint(
    skip: int = Query(0, description="Number of records to skip"),
    limit: int = Query(100, description="Maximum number of records to return"),
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    status: Optional[str] = Query(None, description="Filter by status: 'Failed' or 'Cancelled'"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get detailed list of cancelled/failed appointments"""
    try:
        return get_lost_appointments_detail(db, skip, limit, start_date, end_date, status)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching lost appointments detail: {str(e)}")
