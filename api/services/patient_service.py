# api/services/patient_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text

def get_patients(db: Session, skip: int = 0, limit: int = 100):
    # Direct SQL query to your DBT model
    query = text("""
        SELECT * FROM analytics.mart_patients
        LIMIT :limit OFFSET :skip
    """)
    result = db.execute(query, {"skip": skip, "limit": limit})
    return result.fetchall()

def get_patient_by_id(db: Session, patient_id: int):
    query = text("""
        SELECT * FROM analytics.mart_patients
        WHERE patient_id = :patient_id
    """)
    result = db.execute(query, {"patient_id": patient_id})
    return result.fetchone()