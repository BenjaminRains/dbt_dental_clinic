from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
from database import get_db
from auth.api_key import require_api_key

router = APIRouter(prefix="/appointments", tags=["appointments"])

# Import models and services
from models.appointment import AppointmentSummary, AppointmentDetail, AppointmentCreate, AppointmentUpdate
from services.appointment_service import (
    get_appointment_summary,
    get_appointments,
    get_appointment_by_id,
    get_today_appointments,
    get_upcoming_appointments
)

@router.get("/summary", response_model=List[AppointmentSummary])
async def get_appointment_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    provider_id: Optional[int] = Query(None, description="Filter by provider"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get appointment summary and scheduling metrics"""
    return get_appointment_summary(db, start_date, end_date, provider_id)

@router.get("/today", response_model=List[AppointmentDetail])
async def get_today_appointments_endpoint(
    provider_id: Optional[int] = Query(None, description="Filter by provider"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get today's appointments"""
    return get_today_appointments(db, provider_id)

@router.get("/upcoming", response_model=List[AppointmentDetail])
async def get_upcoming_appointments_endpoint(
    days: int = Query(7, description="Number of days to look ahead"),
    provider_id: Optional[int] = Query(None, description="Filter by provider"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get upcoming appointments for the next N days"""
    return get_upcoming_appointments(db, days, provider_id)

@router.get("/", response_model=List[AppointmentDetail])
async def get_appointments_endpoint(
    skip: int = Query(0, description="Number of records to skip"),
    limit: int = Query(100, description="Maximum number of records to return"),
    start_date: Optional[date] = Query(None, description="Start date filter"),
    end_date: Optional[date] = Query(None, description="End date filter"),
    provider_id: Optional[int] = Query(None, description="Filter by provider"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get detailed appointment records"""
    return get_appointments(db, skip, limit, start_date, end_date, provider_id)


@router.get("/{appointment_id}", response_model=AppointmentDetail)
async def get_appointment_endpoint(
    appointment_id: int,
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get a specific appointment by ID"""
    return get_appointment_by_id(db, appointment_id)