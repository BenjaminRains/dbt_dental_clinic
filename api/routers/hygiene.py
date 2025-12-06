# api/routers/hygiene.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from datetime import date
from database import get_db
from auth.api_key import require_api_key

router = APIRouter(prefix="/hygiene", tags=["hygiene"])

# Import services and models
from services.hygiene_service import get_hygiene_retention_summary
from models.hygiene import HygieneRetentionSummary

@router.get("/retention-summary", response_model=HygieneRetentionSummary)
async def get_hygiene_retention_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for analysis (ISO format: YYYY-MM-DD). Defaults to 12 months ago if not provided."),
    end_date: Optional[date] = Query(None, description="End date for analysis (ISO format: YYYY-MM-DD). Defaults to today if not provided."),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """
    Get Hygiene Retention KPI summary for dashboard
    
    Returns all 6 KPIs:
    - Recall Current %: % of active patients current on recall
    - Hyg Pre-Appointment Any %: % of hygiene patients who scheduled next appointment
    - Hyg Patients Seen: Unique count of patients with hygiene procedures
    - Hyg Pts Re-appntd: Count of unique patients who scheduled next appointment after hygiene
    - Recall Overdue %: % of active recall patients who are overdue
    - Not on Recall %: % of all patients not enrolled in recall programs
    """
    try:
        result = get_hygiene_retention_summary(db, start_date, end_date)
        return HygieneRetentionSummary(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching Hygiene Retention summary: {str(e)}")

