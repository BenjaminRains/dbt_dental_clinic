"""
Test final adjustments to get from 4.44% to 20% for Not on Recall %
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

def test_not_on_recall_final():
    db = get_db_session()
    try:
        print("="*80)
        print("FINAL TESTING FOR 'NOT ON RECALL %' (4.44% -> 20%)")
        print("="*80)
        
        # Test 1: Current logic (all statuses)
        query1 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall_service AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D1110', 'D1120', 'D1208', 'D4910')
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall_service pws ON ap.patient_id = pws.patient_id
        """
        result1 = db.execute(text(query1)).fetchone()
        print(f"Test 1 - Current logic (all statuses):")
        print(f"  Not on recall: {result1.not_on_recall_count}")
        print(f"  Total patients: {result1.total_patients}")
        print(f"  Not on Recall %: {result1.not_on_recall_percent:.2f}%")
        print()
        
        # Test 2: Only completed procedures (status = 2)
        query2 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall_service AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D1110', 'D1120', 'D1208', 'D4910')
                AND ipc.procedure_status = 2  -- Only completed
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall_service pws ON ap.patient_id = pws.patient_id
        """
        result2 = db.execute(text(query2)).fetchone()
        print(f"Test 2 - Only completed procedures (status = 2):")
        print(f"  Not on recall: {result2.not_on_recall_count}")
        print(f"  Total patients: {result2.total_patients}")
        print(f"  Not on Recall %: {result2.not_on_recall_percent:.2f}%")
        print()
        
        # Test 3: Check recall records instead of procedures
        query3 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall_record AS (
            SELECT DISTINCT patient_id
            FROM raw_intermediate.int_recall_management
            WHERE is_disabled = false
                AND is_valid_recall = true
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall_record pwr ON ap.patient_id = pwr.patient_id
        """
        result3 = db.execute(text(query3)).fetchone()
        print(f"Test 3 - Recall records (not procedures):")
        print(f"  Not on recall: {result3.not_on_recall_count}")
        print(f"  Total patients: {result3.total_patients}")
        print(f"  Not on Recall %: {result3.not_on_recall_percent:.2f}%")
        print()
        
        # Test 4: Breakdown by code
        query4 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN ipc.patient_id IN (SELECT patient_id FROM active_patients) 
                                AND pc.procedure_code = 'D1110' THEN ipc.patient_id END) as d1110_patients,
            COUNT(DISTINCT CASE WHEN ipc.patient_id IN (SELECT patient_id FROM active_patients) 
                                AND pc.procedure_code = 'D1120' THEN ipc.patient_id END) as d1120_patients,
            COUNT(DISTINCT CASE WHEN ipc.patient_id IN (SELECT patient_id FROM active_patients) 
                                AND pc.procedure_code = 'D1208' THEN ipc.patient_id END) as d1208_patients,
            COUNT(DISTINCT CASE WHEN ipc.patient_id IN (SELECT patient_id FROM active_patients) 
                                AND pc.procedure_code = 'D4910' THEN ipc.patient_id END) as d4910_patients,
            COUNT(DISTINCT CASE WHEN ipc.patient_id IN (SELECT patient_id FROM active_patients) 
                                AND pc.procedure_code IN ('D1110', 'D1120', 'D1208', 'D4910') THEN ipc.patient_id END) as any_recall_service_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D1110', 'D1120', 'D1208', 'D4910')
        """
        result4 = db.execute(text(query4)).fetchone()
        print(f"Breakdown by code:")
        print(f"  D1110 patients: {result4.d1110_patients}")
        print(f"  D1120 patients: {result4.d1120_patients}")
        print(f"  D1208 patients: {result4.d1208_patients}")
        print(f"  D4910 patients: {result4.d4910_patients}")
        print(f"  Any recall service: {result4.any_recall_service_patients}")
        print()
        
        print("="*80)
        print("CLOSEST TO PBN'S 20%:")
        print("="*80)
        results = [
            ("Test 1 (all statuses)", result1.not_on_recall_percent),
            ("Test 2 (completed only)", result2.not_on_recall_percent),
            ("Test 3 (recall records)", result3.not_on_recall_percent),
        ]
        results.sort(key=lambda x: abs(x[1] - 20))
        for name, percent in results[:3]:
            print(f"  {name}: {percent:.2f}% (diff: {abs(percent - 20):.2f}%)")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_not_on_recall_final()

