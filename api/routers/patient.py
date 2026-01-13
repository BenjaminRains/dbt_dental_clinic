# pydantic models for request/response validation
# patient related endpoints.

# api/routers/patients.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
import logging

from models.patient import Patient, PaginatedPatients, TopPatientBalance
from services import patient_service
from database import get_db
from auth.api_key import require_api_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/patients",
    tags=["patients"],
    responses={404: {"description": "Not found"}},
    dependencies=[Depends(require_api_key)],  # All endpoints in this router require API key
)

@router.get("/", response_model=PaginatedPatients)
def read_patients(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get paginated list of patients - requires API key"""
    try:
        result = patient_service.get_patients(db, skip=skip, limit=limit)
        return result
    except Exception as e:
        import traceback
        error_msg = str(e)
        logger.error(f"Error fetching patients: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching patients: {error_msg}")

@router.get("/top-balances", response_model=List[TopPatientBalance])
def get_top_patient_balances(
    limit: int = Query(10, ge=1, le=100, description="Number of top patients to return"),
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get top N patients by total balance - requires API key"""
    try:
        logger.info(f"Fetching top {limit} patient balances")
        balances = patient_service.get_top_patient_balances(db, limit=limit)
        
        # Log response data for debugging
        if balances:
            logger.info(f"Returning {len(balances)} patient balances")
            # Log first item's data types
            first_item = balances[0]
            logger.info(f"First balance item types: {[(k, type(v).__name__, v) for k, v in first_item.items()]}")
            logger.info(f"First balance item patient_id: {first_item.get('patient_id')} (type: {type(first_item.get('patient_id')).__name__})")
            
            # Manually validate before returning to catch validation errors early
            try:
                from models.patient import TopPatientBalance
                validated_balances = [TopPatientBalance(**item) for item in balances]
                logger.info("Successfully validated all patient balances with Pydantic")
                return [item.model_dump() for item in validated_balances]
            except Exception as validation_error:
                logger.error(f"Pydantic validation failed: {validation_error}")
                logger.error(f"First item that failed: {first_item}")
                import traceback
                logger.error(f"Validation traceback:\n{traceback.format_exc()}")
                raise  # Re-raise to trigger the ValidationError handler
        else:
            logger.warning("No patient balances returned")
        
        return balances
    except Exception as e:
        import traceback
        error_msg = str(e)
        full_traceback = traceback.format_exc()
        
        # Enhanced error logging
        logger.error(f"Error fetching top patient balances: {error_msg}")
        logger.error(f"Full traceback:\n{full_traceback}")
        
        # Check if it's a Pydantic validation error
        if "ValidationError" in str(type(e)) or "validation" in error_msg.lower():
            logger.error("This appears to be a Pydantic validation error")
            logger.error("This usually means the data returned doesn't match the expected model")
        
        # Provide more detailed error message
        detail_msg = f"Error fetching top patient balances: {error_msg}"
        if len(error_msg) > 500:
            detail_msg = f"Error fetching top patient balances: {error_msg[:500]}... (truncated)"
        
        raise HTTPException(status_code=500, detail=detail_msg)

@router.get("/{patient_id}", response_model=Patient)
def read_patient(
    patient_id: int, 
    db: Session = Depends(get_db),
    _api_key: dict = Depends(require_api_key)
):
    """Get patient by ID - requires API key"""
    try:
        patient = patient_service.get_patient_by_id(db, patient_id=patient_id)
        if patient is None:
            raise HTTPException(status_code=404, detail="Patient not found")
        return patient
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_msg = str(e)
        logger.error(f"Error fetching patient {patient_id}: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching patient: {error_msg}")