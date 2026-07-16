from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from auth.api_key import require_api_key
from database import get_db
from models.kpi import (
    DailyCollectionsKPI,
    DailyProductionByCodeResponse,
    DailyProductionKPI,
    LatestCollectionsDate,
    LatestProductionDate,
)
from services.kpi_service import (
    get_daily_collections_kpi,
    get_daily_production_by_code,
    get_daily_production_kpi,
    get_latest_collections_date,
    get_latest_production_date,
)

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


@router.get("/daily-production/latest-date", response_model=LatestProductionDate)
async def get_latest_production_date_endpoint(
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key),
):
    """Most recent date with production in mart_daily_production_by_procedure."""
    try:
        result = get_latest_production_date(db)
        return LatestProductionDate(**result)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching latest production date: {str(e)}",
        ) from e


@router.get("/daily-production", response_model=DailyProductionKPI)
async def get_daily_production_kpi_endpoint(
    production_date: Optional[date] = Query(
        None,
        description="Calendar date (DateComplete). Defaults to today.",
    ),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key),
):
    """
    Daily production day rollup from mart_daily_production_by_procedure.

    Validated against OpenDental Daily → Production by Procedure
    (see validation/kpi/KPI_VALIDATION_REGISTRY.md).
    """
    try:
        result = get_daily_production_kpi(db, production_date)
        return DailyProductionKPI(**result)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching daily production KPI: {str(e)}",
        ) from e


@router.get("/daily-production/by-code", response_model=DailyProductionByCodeResponse)
async def get_daily_production_by_code_endpoint(
    production_date: Optional[date] = Query(
        None,
        description="Calendar date (DateComplete). Defaults to today.",
    ),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key),
):
    """Procedure-code detail for a production date (OD report grain)."""
    try:
        result = get_daily_production_by_code(db, production_date)
        return DailyProductionByCodeResponse(**result)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching daily production by code: {str(e)}",
        ) from e
