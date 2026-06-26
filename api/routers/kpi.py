from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from auth.api_key import require_api_key
from database import get_db
from models.kpi import DailyCollectionsKPI, LatestCollectionsDate
from services.kpi_service import get_daily_collections_kpi, get_latest_collections_date

router = APIRouter(prefix="/kpi", tags=["kpi"])


@router.get("/daily-collections/latest-date", response_model=LatestCollectionsDate)
async def get_latest_collections_date_endpoint(
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key),
):
    """Most recent date with collections in mart_daily_payments."""
    try:
        result = get_latest_collections_date(db)
        return LatestCollectionsDate(**result)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching latest collections date: {str(e)}",
        ) from e


@router.get("/daily-collections", response_model=DailyCollectionsKPI)
async def get_daily_collections_kpi_endpoint(
    payment_date: Optional[date] = Query(
        None,
        description="Calendar date (PayDate / CheckDate). Defaults to today.",
    ),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key),
):
    """
    Daily net collections from mart_daily_payments.

    Validated against OpenDental Daily Payments (see validation/kpi/KPI_VALIDATION_REGISTRY.md).
    """
    try:
        result = get_daily_collections_kpi(db, payment_date)
        return DailyCollectionsKPI(**result)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching daily collections KPI: {str(e)}",
        ) from e
