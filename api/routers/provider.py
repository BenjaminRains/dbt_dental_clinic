from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
from database import get_db

router = APIRouter(prefix="/providers", tags=["providers"])

# Import services
from services.provider_service import get_provider_summary

@router.get("/summary", response_model=List[dict])
async def get_provider_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    db: Session = Depends(get_db)
):
    """Get provider performance summary aggregated from appointment data"""
    return get_provider_summary(db, start_date, end_date)
