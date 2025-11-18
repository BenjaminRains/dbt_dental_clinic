"""
Test alternative definitions for "Not on Recall"
Maybe PBN checks for recall records OR exams, or uses a different patient set
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

def test_not_on_recall_alternative():
    db = get_db_session()
    try:
        print("="*80)
        print("TESTING ALTERNATIVE DEFINITIONS FOR 'NOT ON RECALL'")
        print("="*80)
        print("PBN Target: 20% not on recall")
        print("Active patients only: 0.73% (too low)")
        print("All patients: 80.80% (too high)")
        print("Maybe PBN uses a different definition?")
        print()
        
        # Test 1: All patients, but "on recall" = has recall record OR has periodic exam
        query1 = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_with_recall_record AS (
            SELECT DISTINCT patient_id
            FROM raw_intermediate.int_recall_management
            WHERE is_disabled = false
                AND is_valid_recall = true
        ),
        patients_with_periodic_exam AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0140', 'D0150', 'D0220', 'D0272', 'D0274', 'D0330')
        ),
        patients_on_recall AS (
            SELECT patient_id FROM patients_with_recall_record
            UNION
            SELECT patient_id FROM patients_with_periodic_exam
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN por.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN por.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM all_patients ap
        LEFT JOIN patients_on_recall por ON ap.patient_id = por.patient_id
        """
        result1 = db.execute(text(query1)).fetchone()
        print(f"Test 1 - All patients, 'on recall' = recall record OR periodic exam:")
        print(f"  Not on recall: {result1.not_on_recall_count}")
        print(f"  Total patients: {result1.total_patients}")
        print(f"  Not on Recall %: {result1.not_on_recall_percent:.2f}%")
        print()
        
        # Test 2: Maybe PBN only counts patients with at least one appointment?
        query2 = """
        WITH patients_with_appointments AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_with_periodic_exam AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0140', 'D0150', 'D0220', 'D0272', 'D0274', 'D0330')
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN pwa.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT pwa.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN pwa.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT pwa.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM patients_with_appointments pwa
        LEFT JOIN patients_with_periodic_exam pwe ON pwa.patient_id = pwe.patient_id
        """
        result2 = db.execute(text(query2)).fetchone()
        print(f"Test 2 - Patients with at least one appointment (ever):")
        print(f"  Not on recall: {result2.not_on_recall_count}")
        print(f"  Total patients: {result2.total_patients}")
        print(f"  Not on Recall %: {result2.not_on_recall_percent:.2f}%")
        print()
        
        # Test 3: Maybe PBN uses a different exam code set?
        # Let's check if excluding some codes helps
        query3 = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_with_basic_exam AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0140', 'D0150')  -- Only basic exams, no X-rays
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM all_patients ap
        LEFT JOIN patients_with_basic_exam pwe ON ap.patient_id = pwe.patient_id
        """
        result3 = db.execute(text(query3)).fetchone()
        print(f"Test 3 - All patients, only basic exams (D0120, D0140, D0150, no X-rays):")
        print(f"  Not on recall: {result3.not_on_recall_count}")
        print(f"  Total patients: {result3.total_patients}")
        print(f"  Not on Recall %: {result3.not_on_recall_percent:.2f}%")
        print()
        
        # Test 4: Breakdown to understand the data
        query4 = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_with_appointments AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_with_periodic_exam AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0140', 'D0150', 'D0220', 'D0272', 'D0274', 'D0330')
        ),
        patients_with_recall_record AS (
            SELECT DISTINCT patient_id
            FROM raw_intermediate.int_recall_management
            WHERE is_disabled = false
                AND is_valid_recall = true
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM all_patients) as total_patients,
            (SELECT COUNT(DISTINCT patient_id) FROM patients_with_appointments) as patients_with_appt,
            (SELECT COUNT(DISTINCT patient_id) FROM patients_with_periodic_exam) as patients_with_exam,
            (SELECT COUNT(DISTINCT patient_id) FROM patients_with_recall_record) as patients_with_recall,
            (SELECT COUNT(DISTINCT patient_id) FROM all_patients 
             WHERE patient_id NOT IN (SELECT patient_id FROM patients_with_appointments)) as patients_no_appt,
            (SELECT COUNT(DISTINCT patient_id) FROM all_patients 
             WHERE patient_id NOT IN (SELECT patient_id FROM patients_with_periodic_exam)) as patients_no_exam
        """
        result4 = db.execute(text(query4)).fetchone()
        print(f"Breakdown:")
        print(f"  Total patients: {result4.total_patients}")
        print(f"  Patients with appointments (ever): {result4.patients_with_appt}")
        print(f"  Patients with periodic exam (ever): {result4.patients_with_exam}")
        print(f"  Patients with recall record: {result4.patients_with_recall}")
        print(f"  Patients with NO appointments: {result4.patients_no_appt}")
        print(f"  Patients with NO exams: {result4.patients_no_exam}")
        print()
        
        print("="*80)
        print("CLOSEST TO PBN'S 20%:")
        print("="*80)
        results = [
            ("Test 1 (recall OR exam)", result1.not_on_recall_percent),
            ("Test 2 (patients with appointments)", result2.not_on_recall_percent),
            ("Test 3 (basic exams only)", result3.not_on_recall_percent),
        ]
        results.sort(key=lambda x: abs(x[1] - 20))
        for name, percent in results[:3]:
            print(f"  {name}: {percent:.2f}% (diff: {abs(percent - 20):.2f}%)")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_not_on_recall_alternative()

