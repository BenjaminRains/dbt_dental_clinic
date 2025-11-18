"""
Test different exam code combinations for "Not on Recall"
Maybe PBN only counts basic exams (D0120, D0140, D0150) or only completed exams
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

def test_not_on_recall_exam_codes():
    db = get_db_session()
    try:
        print("="*80)
        print("TESTING DIFFERENT EXAM CODE COMBINATIONS FOR 'NOT ON RECALL'")
        print("="*80)
        print("PBN Target: 20% not on recall")
        print("Current: 0.73% (too low)")
        print()
        
        # Test 1: All exam codes (current)
        query1 = """
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
        result1 = db.execute(text(query1)).fetchone()
        print(f"Test 1 - All exam codes (current):")
        print(f"  Not on recall: {result1.not_on_recall_count}")
        print(f"  Total patients: {result1.total_patients}")
        print(f"  Not on Recall %: {result1.not_on_recall_percent:.2f}%")
        print()
        
        # Test 2: Only basic exams (D0120, D0140, D0150) - no X-rays
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
            WHERE pc.procedure_code IN ('D0120', 'D0140', 'D0150')  -- Only basic exams
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
        print(f"Test 2 - Only basic exams (D0120, D0140, D0150, no X-rays):")
        print(f"  Not on recall: {result2.not_on_recall_count}")
        print(f"  Total patients: {result2.total_patients}")
        print(f"  Not on Recall %: {result2.not_on_recall_percent:.2f}%")
        print()
        
        # Test 3: Only completed exams (status = 2)
        query3 = """
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
                AND ipc.procedure_status = 2  -- Only completed
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM active_patients ap
        LEFT JOIN patients_with_periodic_exam pwe ON ap.patient_id = pwe.patient_id
        """
        result3 = db.execute(text(query3)).fetchone()
        print(f"Test 3 - All exam codes, only completed (status = 2):")
        print(f"  Not on recall: {result3.not_on_recall_count}")
        print(f"  Total patients: {result3.total_patients}")
        print(f"  Not on Recall %: {result3.not_on_recall_percent:.2f}%")
        print()
        
        # Test 4: Only basic exams, only completed
        query4 = """
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
            WHERE pc.procedure_code IN ('D0120', 'D0140', 'D0150')  -- Only basic exams
                AND ipc.procedure_status = 2  -- Only completed
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwe.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM active_patients ap
        LEFT JOIN patients_with_periodic_exam pwe ON ap.patient_id = pwe.patient_id
        """
        result4 = db.execute(text(query4)).fetchone()
        print(f"Test 4 - Only basic exams (D0120, D0140, D0150), only completed:")
        print(f"  Not on recall: {result4.not_on_recall_count}")
        print(f"  Total patients: {result4.total_patients}")
        print(f"  Not on Recall %: {result4.not_on_recall_percent:.2f}%")
        print()
        
        # Test 5: Breakdown by exam code
        query5 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN ipc.patient_id IN (SELECT patient_id FROM active_patients) 
                                AND pc.procedure_code = 'D0120' THEN ipc.patient_id END) as d0120_patients,
            COUNT(DISTINCT CASE WHEN ipc.patient_id IN (SELECT patient_id FROM active_patients) 
                                AND pc.procedure_code = 'D0140' THEN ipc.patient_id END) as d0140_patients,
            COUNT(DISTINCT CASE WHEN ipc.patient_id IN (SELECT patient_id FROM active_patients) 
                                AND pc.procedure_code = 'D0150' THEN ipc.patient_id END) as d0150_patients,
            COUNT(DISTINCT CASE WHEN ipc.patient_id IN (SELECT patient_id FROM active_patients) 
                                AND pc.procedure_code IN ('D0120', 'D0140', 'D0150') THEN ipc.patient_id END) as basic_exam_patients,
            COUNT(DISTINCT CASE WHEN ipc.patient_id IN (SELECT patient_id FROM active_patients) 
                                AND pc.procedure_code IN ('D0272', 'D0274', 'D0330') THEN ipc.patient_id END) as xray_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0140', 'D0150', 'D0272', 'D0274', 'D0330')
        """
        result5 = db.execute(text(query5)).fetchone()
        print(f"Breakdown by exam code:")
        print(f"  D0120 patients: {result5.d0120_patients}")
        print(f"  D0140 patients: {result5.d0140_patients}")
        print(f"  D0150 patients: {result5.d0150_patients}")
        print(f"  Basic exam patients (D0120/D0140/D0150): {result5.basic_exam_patients}")
        print(f"  X-ray patients (D0272/D0274/D0330): {result5.xray_patients}")
        print()
        
        print("="*80)
        print("CLOSEST TO PBN'S 20%:")
        print("="*80)
        results = [
            ("Test 1 (all codes)", result1.not_on_recall_percent),
            ("Test 2 (basic only)", result2.not_on_recall_percent),
            ("Test 3 (all codes, completed)", result3.not_on_recall_percent),
            ("Test 4 (basic, completed)", result4.not_on_recall_percent),
        ]
        results.sort(key=lambda x: abs(x[1] - 20))
        for name, percent in results[:3]:
            print(f"  {name}: {percent:.2f}% (diff: {abs(percent - 20):.2f}%)")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_not_on_recall_exam_codes()

