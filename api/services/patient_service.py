# api/services/patient_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text

def get_patients(db: Session, skip: int = 0, limit: int = 100):
    # Get total count first
    count_query = text("SELECT COUNT(*) as total FROM raw_marts.dim_patient")
    count_result = db.execute(count_query)
    total = count_result.fetchone().total
    
    # Get paginated results
    query = text("""
        SELECT * FROM raw_marts.dim_patient
        LIMIT :limit OFFSET :skip
    """)
    result = db.execute(query, {"skip": skip, "limit": limit})
    patients = result.fetchall()
    
    return {
        "patients": [dict(row._mapping) for row in patients],
        "total": total
    }

def get_patient_by_id(db: Session, patient_id: int):
    query = text("""
        SELECT * FROM raw_marts.dim_patient
        WHERE patient_id = :patient_id
    """)
    result = db.execute(query, {"patient_id": patient_id})
    return result.fetchone()