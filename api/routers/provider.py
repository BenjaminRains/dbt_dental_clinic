from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
from database import get_db

router = APIRouter(prefix="/providers", tags=["providers"])

# Import services and models
from services.provider_service import get_provider_summary, get_providers, get_provider_by_id
from models.provider import ProviderSummary, Provider

@router.get("/", response_model=List[Provider])
async def get_providers_endpoint(
    skip: int = Query(0, description="Number of records to skip"),
    limit: int = Query(100, description="Maximum number of records to return"),
    active_only: bool = Query(True, description="Return only active providers"),
    db: Session = Depends(get_db)
):
    """Get detailed provider information from dim_provider mart"""
    return get_providers(db, skip=skip, limit=limit, active_only=active_only)

@router.get("/summary", response_model=List[ProviderSummary])
async def get_provider_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    db: Session = Depends(get_db)
):
    """Get provider performance summary aggregated from appointment data"""
    return get_provider_summary(db, start_date, end_date)

@router.get("/{provider_id}", response_model=Provider)
async def get_provider_by_id_endpoint(
    provider_id: int,
    db: Session = Depends(get_db)
):
    """Get detailed provider information by provider_id"""
    provider = get_provider_by_id(db, provider_id)
    if provider is None:
        raise HTTPException(status_code=404, detail="Provider not found")
    return provider
