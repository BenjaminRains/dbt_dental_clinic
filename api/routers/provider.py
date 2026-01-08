from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
import logging
from database import get_db
from auth.api_key import require_api_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/providers", 
    tags=["providers"],
    dependencies=[Depends(require_api_key)]  # All endpoints in this router require API key
)

# Import services and models
from services.provider_service import get_provider_summary, get_providers, get_provider_by_id
from models.provider import ProviderSummary, Provider

@router.get("/", response_model=List[Provider])
async def get_providers_endpoint(
    skip: int = Query(0, description="Number of records to skip"),
    limit: int = Query(100, description="Maximum number of records to return"),
    active_only: bool = Query(True, description="Return only active providers"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get detailed provider information from dim_provider mart"""
    try:
        return get_providers(db, skip=skip, limit=limit, active_only=active_only)
    except Exception as e:
        import traceback
        error_msg = str(e)
        logger.error(f"Error fetching providers: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching providers: {error_msg}")

@router.get("/summary", response_model=List[ProviderSummary])
async def get_provider_summary_endpoint(
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get provider performance summary aggregated from appointment data"""
    logger.info(f"Provider summary endpoint called with start_date={start_date}, end_date={end_date}")
    try:
        result = get_provider_summary(db, start_date, end_date)
        logger.info(f"Provider summary endpoint returning {len(result)} providers")
        return result
    except Exception as e:
        import traceback
        error_msg = str(e)
        full_traceback = traceback.format_exc()
        
        # Log full error details
        logger.error(f"Error fetching provider summary: {error_msg}")
        logger.error(f"Full traceback:\n{full_traceback}")
        
        # Check error type and provide helpful message
        error_lower = error_msg.lower()
        if 'does not exist' in error_lower or 'relation' in error_lower:
            detail_msg = "Provider data tables not found. Please ensure fact_appointment table exists."
        elif 'permission denied' in error_lower:
            detail_msg = "Permission error. Please check access configuration."
        elif 'column' in error_lower and 'does not exist' in error_lower:
            detail_msg = "Table schema mismatch. Please rebuild fact_appointment table."
        elif 'syntax error' in error_lower or 'invalid' in error_lower:
            detail_msg = f"Query error: {error_msg[:200]}"  # Show first 200 chars of SQL error
        else:
            # Include more detail for debugging
            detail_msg = f"Error fetching provider summary: {error_msg[:300]}"
        
        raise HTTPException(status_code=500, detail=detail_msg)

@router.get("/{provider_id}", response_model=Provider)
async def get_provider_by_id_endpoint(
    provider_id: int,
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get detailed provider information by provider_id"""
    provider = get_provider_by_id(db, provider_id)
    if provider is None:
        raise HTTPException(status_code=404, detail="Provider not found")
    return provider
