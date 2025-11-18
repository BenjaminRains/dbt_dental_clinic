"""
Refine X-ray inclusion to get from 2081 to 2073 (8 too many)
Maybe we need to exclude certain X-rays or be more selective
"""

import sys
import os
from datetime import date
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

def test_refine_xrays():
    db = get_db_session()
    try:
        print("="*80)
        print("REFINING X-RAY INCLUSION (2081 -> 2073, need to exclude 8)")
        print("="*80)
        
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        # Test 1: Current - all X-rays
        query1 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0272', 'D0274', 'D0330')
            AND ipc.procedure_status = 2  -- Completed
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result1 = db.execute(text(query1), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 1 - All X-rays included: {result1.total_patients} (diff: {abs(result1.total_patients - 2073)})")
        
        # Test 2: Only D0272 and D0274 (bitewing X-rays, commonly done with hygiene)
        query2 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0272', 'D0274')
            AND ipc.procedure_status = 2  -- Completed
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result2 = db.execute(text(query2), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 2 - Exclude D0330 (panoramic): {result2.total_patients} (diff: {abs(result2.total_patients - 2073)})")
        
        # Test 3: Only D0272 (bitewing, 2 films)
        query3 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0272')
            AND ipc.procedure_status = 2  -- Completed
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 3 - Only D0272 (bitewing 2 films): {result3.total_patients} (diff: {abs(result3.total_patients - 2073)})")
        
        # Test 4: X-rays only if linked to appointments with hygienist_id
        query4 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        LEFT JOIN raw_marts.fact_appointment fa ON ipc.appointment_id = fa.appointment_id
        WHERE (
            -- Hygiene codes always included
            pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
            OR (
                -- X-rays only if linked to hygienist appointment
                pc.procedure_code IN ('D0272', 'D0274', 'D0330')
                AND fa.hygienist_id IS NOT NULL
                AND fa.hygienist_id != 0
            )
        )
            AND ipc.procedure_status = 2  -- Completed
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result4 = db.execute(text(query4), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 4 - X-rays only if linked to hygienist appointment: {result4.total_patients} (diff: {abs(result4.total_patients - 2073)})")
        
        # Test 5: Breakdown by X-ray code
        query5 = """
        SELECT 
            COUNT(DISTINCT CASE WHEN pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180') THEN ipc.patient_id END) as hygiene_codes,
            COUNT(DISTINCT CASE WHEN pc.procedure_code = 'D0272' THEN ipc.patient_id END) as d0272_only,
            COUNT(DISTINCT CASE WHEN pc.procedure_code = 'D0274' THEN ipc.patient_id END) as d0274_only,
            COUNT(DISTINCT CASE WHEN pc.procedure_code = 'D0330' THEN ipc.patient_id END) as d0330_only,
            COUNT(DISTINCT CASE WHEN pc.procedure_code IN ('D0272', 'D0274', 'D0330') THEN ipc.patient_id END) as all_xrays
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0272', 'D0274', 'D0330')
            AND ipc.procedure_status = 2  -- Completed
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nBreakdown:")
        print(f"  Hygiene codes only: {result5.hygiene_codes}")
        print(f"  D0272 only: {result5.d0272_only}")
        print(f"  D0274 only: {result5.d0274_only}")
        print(f"  D0330 only: {result5.d0330_only}")
        print(f"  All X-rays: {result5.all_xrays}")
        
        # Test 6: What if we exclude patients who ONLY have X-rays (no hygiene codes)?
        query6 = """
        WITH patients_with_hygiene_codes AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_status = 2
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
        ),
        patients_with_xrays AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0272', 'D0274', 'D0330')
                AND ipc.procedure_status = 2
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM patients_with_hygiene_codes) +
            (SELECT COUNT(DISTINCT CASE WHEN px.patient_id NOT IN (SELECT patient_id FROM patients_with_hygiene_codes) 
                                       THEN px.patient_id END) FROM patients_with_xrays px) as total_patients
        """
        result6 = db.execute(text(query6), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nTest 6 - Hygiene codes + X-rays (only if no hygiene codes): {result6.total_patients} (diff: {abs(result6.total_patients - 2073)})")
        
        print("\n" + "="*80)
        print("CLOSEST MATCHES:")
        print("="*80)
        results = [
            ("Test 1 (all X-rays)", result1.total_patients),
            ("Test 2 (exclude D0330)", result2.total_patients),
            ("Test 3 (only D0272)", result3.total_patients),
            ("Test 4 (X-rays only if linked to hygienist)", result4.total_patients),
            ("Test 6 (X-rays only if no hygiene codes)", result6.total_patients),
        ]
        results.sort(key=lambda x: abs(x[1] - 2073))
        for name, count in results[:3]:
            print(f"  {name}: {count} (diff: {abs(count - 2073)})")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_refine_xrays()

