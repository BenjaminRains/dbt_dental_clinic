"""
Debug why Recall Current % is now 0.00%
"""

import sys
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from config import get_config

def get_db_session():
    config = get_config()
    DATABASE_URL = config.get_database_url()
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()

def test_recall_current_issue():
    db = get_db_session()
    try:
        print("="*80)
        print("DEBUGGING RECALL CURRENT % (now showing 0.00%)")
        print("="*80)
        
        # Check recall records with future dates
        query1 = """
        SELECT 
            COUNT(DISTINCT patient_id) as total_recall_patients,
            COUNT(DISTINCT CASE WHEN date_due > CURRENT_DATE THEN patient_id END) as future_date_patients,
            COUNT(DISTINCT CASE WHEN date_due IS NULL THEN patient_id END) as null_date_patients,
            COUNT(DISTINCT CASE WHEN date_due <= CURRENT_DATE THEN patient_id END) as past_date_patients,
            MIN(date_due) as min_date_due,
            MAX(date_due) as max_date_due
        FROM raw_intermediate.int_recall_management
        WHERE is_disabled = false
            AND is_valid_recall = true
        """
        result1 = db.execute(text(query1)).fetchone()
        print(f"Recall Records Breakdown:")
        print(f"  Total recall patients: {result1.total_recall_patients}")
        print(f"  Patients with future date_due: {result1.future_date_patients}")
        print(f"  Patients with NULL date_due: {result1.null_date_patients}")
        print(f"  Patients with past date_due: {result1.past_date_patients}")
        print(f"  Min date_due: {result1.min_date_due}")
        print(f"  Max date_due: {result1.max_date_due}")
        print()
        
        # Check active patients with recall
        query2 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall AS (
            SELECT DISTINCT 
                irm.patient_id,
                CASE 
                    WHEN irm.date_due IS NOT NULL AND irm.date_due > CURRENT_DATE 
                    THEN true 
                    ELSE false 
                END as is_current_on_recall
            FROM raw_intermediate.int_recall_management irm
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT CASE WHEN pwr.is_current_on_recall THEN ap.patient_id END) as current_on_recall,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NOT NULL THEN ap.patient_id END) as has_recall_record,
            (COUNT(DISTINCT CASE WHEN pwr.is_current_on_recall THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as recall_current_percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        """
        result2 = db.execute(text(query2)).fetchone()
        print(f"Active Patients with Recall:")
        print(f"  Total active patients: {result2.total_active}")
        print(f"  Has recall record: {result2.has_recall_record}")
        print(f"  Current on recall (future date_due): {result2.current_on_recall}")
        print(f"  Recall Current %: {result2.recall_current_percent:.2f}%")
        print()
        
        # Check what the old logic would give (just having a recall record)
        query3 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall AS (
            SELECT DISTINCT patient_id
            FROM raw_intermediate.int_recall_management
            WHERE is_disabled = false
                AND is_valid_recall = true
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT pwr.patient_id) as has_recall_record,
            (COUNT(DISTINCT pwr.patient_id)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as recall_current_percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        """
        result3 = db.execute(text(query3)).fetchone()
        print(f"Old Logic (just having recall record):")
        print(f"  Total active patients: {result3.total_active}")
        print(f"  Has recall record: {result3.has_recall_record}")
        print(f"  Recall Current %: {result3.recall_current_percent:.2f}%")
        print()
        
    finally:
        db.close()

if __name__ == "__main__":
    test_recall_current_issue()

