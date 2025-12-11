# api/routers/ar.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
from database import get_db
from auth.api_key import require_api_key

router = APIRouter(prefix="/ar", tags=["accounts-receivable"])

# Import services and models
from services.ar_service import (
    get_ar_kpi_summary,
    get_ar_aging_summary,
    get_ar_priority_queue,
    get_ar_risk_distribution,
    get_ar_aging_trends,
    get_available_snapshot_dates
)
from models.ar import (
    ARKPISummary,
    ARAgingSummary,
    ARPriorityQueueItem,
    ARRiskDistribution,
    ARAgingTrend
)

@router.get("/kpi-summary", response_model=ARKPISummary)
async def get_ar_kpi_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for KPI analysis"),
    end_date: Optional[date] = Query(None, description="End date for KPI analysis"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get AR KPI summary for dashboard"""
    try:
        return get_ar_kpi_summary(db, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching AR KPI summary: {str(e)}")

@router.get("/aging-summary", response_model=List[ARAgingSummary])
async def get_ar_aging_summary_endpoint(
    snapshot_date: Optional[date] = Query(None, description="Specific snapshot date, or latest if not provided"),
    start_date: Optional[date] = Query(None, description="Start date for date range filter"),
    end_date: Optional[date] = Query(None, description="End date for date range filter"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get AR aging summary by bucket"""
    try:
        return get_ar_aging_summary(db, snapshot_date, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching AR aging summary: {str(e)}")

@router.get("/priority-queue", response_model=List[ARPriorityQueueItem])
async def get_ar_priority_queue_endpoint(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of records to return"),
    min_priority_score: Optional[int] = Query(None, ge=0, le=100, description="Minimum priority score threshold"),
    risk_category: Optional[str] = Query(None, description="Filter by aging risk category"),
    min_balance: Optional[float] = Query(None, ge=0, description="Minimum total balance threshold"),
    provider_id: Optional[int] = Query(None, description="Filter by specific provider"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get AR priority queue sorted by collection priority"""
    try:
        return get_ar_priority_queue(
            db, skip, limit, min_priority_score, 
            risk_category, min_balance, provider_id
        )
    except Exception as e:
        # Log full error for debugging, but return generic message to client
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error fetching AR priority queue: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while fetching data. Please try again later.")

@router.get("/risk-distribution", response_model=List[ARRiskDistribution])
async def get_ar_risk_distribution_endpoint(
    snapshot_date: Optional[date] = Query(None, description="Specific snapshot date, or latest if not provided"),
    start_date: Optional[date] = Query(None, description="Start date for date range filter"),
    end_date: Optional[date] = Query(None, description="End date for date range filter"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get risk category distribution"""
    try:
        return get_ar_risk_distribution(db, snapshot_date, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching AR risk distribution: {str(e)}")

@router.get("/aging-trends", response_model=List[ARAgingTrend])
async def get_ar_aging_trends_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for trend analysis"),
    end_date: Optional[date] = Query(None, description="End date for trend analysis"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get AR aging trends over time"""
    try:
        return get_ar_aging_trends(db, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching AR aging trends: {str(e)}")

@router.get("/snapshot-dates")
async def get_snapshot_dates_endpoint(
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get list of available snapshot dates from mart_ar_summary"""
    try:
        return get_available_snapshot_dates(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching snapshot dates: {str(e)}")

