"""
Test if PBN uses only active patients (not all patients) for Not on Recall %
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

def test_not_on_recall_active_only():
    db = get_db_session()
    try:
        print("="*80)
        print("TESTING 'NOT ON RECALL %' WITH ACTIVE PATIENTS ONLY")
        print("="*80)
        print("PBN Target: 20% not on recall")
        print("Current (all patients): 80.80% not on recall")
        print("Maybe PBN uses only active patients as denominator?")
        print()
        
        # Test 1: All patients (current logic)
        query1 = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_with_periodic_exam AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0140', 'D0150', 'D0220', 'D0272', 'D0274', 'D0330')
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM all_patients ap
        LEFT JOIN patients_with_periodic_exam pwe ON ap.patient_id = pwe.patient_id
        """
        result1 = db.execute(text(query1)).fetchone()
        print(f"Test 1 - All patients (current):")
        print(f"  Not on recall: {result1.not_on_recall_count}")
        print(f"  Total patients: {result1.total_patients}")
        print(f"  Not on Recall %: {result1.not_on_recall_percent:.2f}%")
        print()
        
        # Test 2: Only active patients (visited in past 18 months)
        query2 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_periodic_exam AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0140', 'D0150', 'D0220', 'D0272', 'D0274', 'D0330')
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM active_patients ap
        LEFT JOIN patients_with_periodic_exam pwe ON ap.patient_id = pwe.patient_id
        """
        result2 = db.execute(text(query2)).fetchone()
        print(f"Test 2 - Active patients only (visited in past 18 months):")
        print(f"  Not on recall: {result2.not_on_recall_count}")
        print(f"  Total patients: {result2.total_patients}")
        print(f"  Not on Recall %: {result2.not_on_recall_percent:.2f}%")
        print()
        
        # Test 3: Active patients with periodic exam in past 18 months
        query3 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recent_periodic_exam AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0140', 'D0150', 'D0220', 'D0272', 'D0274', 'D0330')
                AND ipc.procedure_date >= CURRENT_DATE - INTERVAL '18 months'
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM active_patients ap
        LEFT JOIN patients_with_recent_periodic_exam pwe ON ap.patient_id = pwe.patient_id
        """
        result3 = db.execute(text(query3)).fetchone()
        print(f"Test 3 - Active patients, exam in past 18 months:")
        print(f"  Not on recall: {result3.not_on_recall_count}")
        print(f"  Total patients: {result3.total_patients}")
        print(f"  Not on Recall %: {result3.not_on_recall_percent:.2f}%")
        print()
        
        # Test 4: Breakdown
        query4 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_any_periodic_exam AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0140', 'D0150', 'D0220', 'D0272', 'D0274', 'D0330')
        ),
        patients_with_recent_periodic_exam AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0140', 'D0150', 'D0220', 'D0272', 'D0274', 'D0330')
                AND ipc.procedure_date >= CURRENT_DATE - INTERVAL '18 months'
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM active_patients) as active_patients,
            (SELECT COUNT(DISTINCT patient_id) FROM patients_with_any_periodic_exam 
             WHERE patient_id IN (SELECT patient_id FROM active_patients)) as active_with_any_exam,
            (SELECT COUNT(DISTINCT patient_id) FROM patients_with_recent_periodic_exam 
             WHERE patient_id IN (SELECT patient_id FROM active_patients)) as active_with_recent_exam
        """
        result4 = db.execute(text(query4)).fetchone()
        print(f"Breakdown:")
        print(f"  Active patients: {result4.active_patients}")
        print(f"  Active with ANY periodic exam (ever): {result4.active_with_any_exam}")
        print(f"  Active with recent periodic exam (18 months): {result4.active_with_recent_exam}")
        print()
        
        print("="*80)
        print("CLOSEST TO PBN'S 20%:")
        print("="*80)
        results = [
            ("Test 1 (all patients)", result1.not_on_recall_percent),
            ("Test 2 (active patients, any exam)", result2.not_on_recall_percent),
            ("Test 3 (active patients, recent exam)", result3.not_on_recall_percent),
        ]
        results.sort(key=lambda x: abs(x[1] - 20))
        for name, percent in results[:3]:
            print(f"  {name}: {percent:.2f}% (diff: {abs(percent - 20):.2f}%)")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_not_on_recall_active_only()

