# api/routers/treatment_acceptance.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
from database import get_db
from auth.api_key import require_api_key

router = APIRouter(prefix="/treatment-acceptance", tags=["treatment-acceptance"])

# Import services and models
from services.treatment_acceptance_service import (
    get_treatment_acceptance_kpi_summary,
    get_treatment_acceptance_summary,
    get_treatment_acceptance_trends,
    get_treatment_acceptance_provider_performance
)
from models.treatment_acceptance import (
    TreatmentAcceptanceKPISummary,
    TreatmentAcceptanceSummary,
    TreatmentAcceptanceTrend,
    TreatmentAcceptanceProviderPerformance
)

@router.get("/kpi-summary", response_model=TreatmentAcceptanceKPISummary)
async def get_treatment_acceptance_kpi_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for KPI analysis"),
    end_date: Optional[date] = Query(None, description="End date for KPI analysis"),
    provider_id: Optional[int] = Query(None, description="Filter by provider"),
    clinic_id: Optional[int] = Query(None, description="Filter by clinic"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get Treatment Acceptance KPI summary for dashboard"""
    try:
        result = get_treatment_acceptance_kpi_summary(db, start_date, end_date, provider_id, clinic_id)
        return TreatmentAcceptanceKPISummary(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching Treatment Acceptance KPI summary: {str(e)}")

@router.get("/summary", response_model=List[TreatmentAcceptanceSummary])
async def get_treatment_acceptance_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    provider_id: Optional[int] = Query(None, description="Filter by provider"),
    clinic_id: Optional[int] = Query(None, description="Filter by clinic"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get Treatment Acceptance daily summary"""
    try:
        results = get_treatment_acceptance_summary(db, start_date, end_date, provider_id, clinic_id)
        return [TreatmentAcceptanceSummary(**row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching Treatment Acceptance summary: {str(e)}")

@router.get("/trends", response_model=List[TreatmentAcceptanceTrend])
async def get_treatment_acceptance_trends_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for trend analysis"),
    end_date: Optional[date] = Query(None, description="End date for trend analysis"),
    provider_id: Optional[int] = Query(None, description="Filter by provider"),
    clinic_id: Optional[int] = Query(None, description="Filter by clinic"),
    group_by: str = Query("month", description="Group by period: day, week, month"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get Treatment Acceptance trends over time"""
    try:
        results = get_treatment_acceptance_trends(db, start_date, end_date, provider_id, clinic_id, group_by)
        return [TreatmentAcceptanceTrend(**row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching Treatment Acceptance trends: {str(e)}")

@router.get("/provider-performance", response_model=List[TreatmentAcceptanceProviderPerformance])
async def get_treatment_acceptance_provider_performance_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    clinic_id: Optional[int] = Query(None, description="Filter by clinic"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get Treatment Acceptance provider performance breakdown"""
    try:
        results = get_treatment_acceptance_provider_performance(db, start_date, end_date, clinic_id)
        return [TreatmentAcceptanceProviderPerformance(**row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching Treatment Acceptance provider performance: {str(e)}")

