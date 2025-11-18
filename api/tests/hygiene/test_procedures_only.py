"""
Test if PBN uses ONLY procedures (not appointments) for Hyg Patients Seen
Based on the plan: "Primary: int_procedure_complete - Hygiene procedures identified by is_hygiene = true"
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

def test_procedures_only():
    db = get_db_session()
    try:
        print("="*80)
        print("TESTING IF PBN USES ONLY PROCEDURES (NOT APPOINTMENTS)")
        print("="*80)
        print("PBN Plan says: 'Primary: int_procedure_complete - Hygiene procedures'")
        print("Maybe PBN doesn't combine with appointments at all?")
        print()
        
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        # Test 1: Only procedures with is_hygiene = true (PBN's primary method)
        query1 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        WHERE ipc.is_hygiene = true
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result1 = db.execute(text(query1), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 1 - Procedures with is_hygiene = true: {result1.total_patients} (diff: {abs(result1.total_patients - 2073)})")
        
        # Test 2: Only procedures with specific codes (our current approach)
        query2 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result2 = db.execute(text(query2), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 2 - Procedures with specific codes: {result2.total_patients} (diff: {abs(result2.total_patients - 2073)})")
        
        # Test 3: Procedures with is_hygiene = true OR specific codes
        query3 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        LEFT JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE (ipc.is_hygiene = true 
               OR pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180'))
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 3 - Procedures with is_hygiene = true OR specific codes: {result3.total_patients} (diff: {abs(result3.total_patients - 2073)})")
        
        # Test 4: Only completed procedures with specific codes (procedure_status = 2)
        query4 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
            AND ipc.procedure_status = 2  -- Status 2 = Completed
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result4 = db.execute(text(query4), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 4 - Completed procedures with specific codes: {result4.total_patients} (diff: {abs(result4.total_patients - 2073)})")
        
        # Test 5: Procedures linked to appointments (any appointment)
        query5 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
            AND ipc.appointment_id IS NOT NULL
        """
        result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 5 - Procedures linked to appointments: {result5.total_patients} (diff: {abs(result5.total_patients - 2073)})")
        
        # Test 6: Procedures linked to completed appointments
        query6 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        INNER JOIN raw_marts.fact_appointment fa ON ipc.appointment_id = fa.appointment_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
            AND fa.is_completed = true  -- Appointment is completed
        """
        result6 = db.execute(text(query6), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 6 - Procedures linked to completed appointments: {result6.total_patients} (diff: {abs(result6.total_patients - 2073)})")
        
        # Test 7: Breakdown
        query7 = """
        SELECT 
            COUNT(DISTINCT CASE WHEN ipc.is_hygiene = true THEN ipc.patient_id END) as is_hygiene_true,
            COUNT(DISTINCT CASE WHEN pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180') THEN ipc.patient_id END) as specific_codes,
            COUNT(DISTINCT CASE WHEN ipc.is_hygiene = true OR pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180') THEN ipc.patient_id END) as either,
            COUNT(DISTINCT CASE WHEN ipc.appointment_id IS NOT NULL THEN ipc.patient_id END) as with_appointment,
            COUNT(DISTINCT CASE WHEN ipc.appointment_id IS NULL THEN ipc.patient_id END) as without_appointment
        FROM raw_intermediate.int_procedure_complete ipc
        LEFT JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE (ipc.is_hygiene = true 
               OR pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180'))
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result7 = db.execute(text(query7), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nBreakdown:")
        print(f"  is_hygiene = true: {result7.is_hygiene_true}")
        print(f"  Specific codes: {result7.specific_codes}")
        print(f"  Either: {result7.either}")
        print(f"  With appointment: {result7.with_appointment}")
        print(f"  Without appointment: {result7.without_appointment}")
        
        print("\n" + "="*80)
        print("CLOSEST MATCH:")
        print("="*80)
        results = [
            ("Test 1 (is_hygiene=true)", result1.total_patients),
            ("Test 2 (specific codes)", result2.total_patients),
            ("Test 3 (either)", result3.total_patients),
            ("Test 4 (completed + codes)", result4.total_patients),
            ("Test 5 (linked to appt)", result5.total_patients),
            ("Test 6 (linked to completed appt)", result6.total_patients),
        ]
        results.sort(key=lambda x: abs(x[1] - 2073))
        for name, count in results[:3]:
            print(f"  {name}: {count} (diff: {abs(count - 2073)})")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_procedures_only()

