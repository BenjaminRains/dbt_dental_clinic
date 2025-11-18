"""
Test combinations to get from 2024 (completed procedures) to 2073
We're only 49 patients short!
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

def test_completed_procedures_plus():
    db = get_db_session()
    try:
        print("="*80)
        print("TESTING TO GET FROM 2024 TO 2073 (49 PATIENTS SHORT)")
        print("="*80)
        
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        # Test 1: Completed procedures + appointments (excluding overlap)
        query1 = """
        WITH completed_procedures AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_status = 2  -- Completed
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
        ),
        hygiene_appointments AS (
            SELECT DISTINCT fa.patient_id
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        ),
        combined AS (
            SELECT patient_id FROM completed_procedures
            UNION
            SELECT patient_id FROM hygiene_appointments
        )
        SELECT COUNT(DISTINCT patient_id) as total_patients
        FROM combined
        """
        result1 = db.execute(text(query1), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 1 - Completed procedures + appointments: {result1.total_patients} (diff: {abs(result1.total_patients - 2073)})")
        
        # Test 2: Completed procedures + completed appointments only
        query2 = """
        WITH completed_procedures AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_status = 2  -- Completed
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
        ),
        completed_appointments AS (
            SELECT DISTINCT fa.patient_id
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND fa.is_completed = true
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        ),
        combined AS (
            SELECT patient_id FROM completed_procedures
            UNION
            SELECT patient_id FROM completed_appointments
        )
        SELECT COUNT(DISTINCT patient_id) as total_patients
        FROM combined
        """
        result2 = db.execute(text(query2), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 2 - Completed procedures + completed appointments: {result2.total_patients} (diff: {abs(result2.total_patients - 2073)})")
        
        # Test 3: What if we include procedures with status 1 or 6 (treatment planned/ordered)?
        query3 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
            AND ipc.procedure_status IN (1, 2, 6)  -- Treatment planned, completed, or ordered
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 3 - Procedures with status 1, 2, or 6: {result3.total_patients} (diff: {abs(result3.total_patients - 2073)})")
        
        # Test 4: What if we include additional procedure codes?
        query4 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0272', 'D0274', 'D0330')  -- Adding X-rays
            AND ipc.procedure_status = 2  -- Completed
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result4 = db.execute(text(query4), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 4 - Completed procedures + X-rays: {result4.total_patients} (diff: {abs(result4.total_patients - 2073)})")
        
        # Test 5: Breakdown to see what we're missing
        query5 = """
        WITH completed_procedures AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_status = 2  -- Completed
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
        ),
        hygiene_appointments AS (
            SELECT DISTINCT fa.patient_id
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        ),
        appointments_not_in_procedures AS (
            SELECT DISTINCT ha.patient_id
            FROM hygiene_appointments ha
            WHERE NOT EXISTS (
                SELECT 1 FROM completed_procedures cp WHERE cp.patient_id = ha.patient_id
            )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM completed_procedures) as completed_procedure_patients,
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) as appointment_patients,
            (SELECT COUNT(DISTINCT patient_id) FROM appointments_not_in_procedures) as appointments_only_patients
        """
        result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nBreakdown:")
        print(f"  Completed procedure patients: {result5.completed_procedure_patients}")
        print(f"  Appointment patients: {result5.appointment_patients}")
        print(f"  Appointments NOT in procedures: {result5.appointments_only_patients}")
        print(f"  If we add appointments-only: {result5.completed_procedure_patients + result5.appointments_only_patients} (diff: {abs((result5.completed_procedure_patients + result5.appointments_only_patients) - 2073)})")
        
        # Test 6: What if PBN uses procedures with appointment_id (linked to appointments)?
        query6 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
            AND ipc.procedure_status = 2  -- Completed
            AND ipc.appointment_id IS NOT NULL  -- Linked to appointment
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result6 = db.execute(text(query6), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 6 - Completed procedures linked to appointments: {result6.total_patients} (diff: {abs(result6.total_patients - 2073)})")
        
        # Test 7: What if we exclude procedures without appointment_id?
        query7 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as total_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
            AND ipc.procedure_status = 2  -- Completed
            AND (ipc.appointment_id IS NOT NULL OR EXISTS (
                -- Or patient has appointment with hygienist_id on same date
                SELECT 1 FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ipc.patient_id
                    AND (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                    AND fa.appointment_date = ipc.procedure_date
            ))
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result7 = db.execute(text(query7), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 7 - Completed procedures (with appointment_id OR matching hygienist appointment date): {result7.total_patients} (diff: {abs(result7.total_patients - 2073)})")
        
        print("\n" + "="*80)
        print("CLOSEST MATCHES:")
        print("="*80)
        results = [
            ("Test 1 (completed procedures + appointments)", result1.total_patients),
            ("Test 2 (completed procedures + completed appointments)", result2.total_patients),
            ("Test 3 (status 1,2,6)", result3.total_patients),
            ("Test 4 (with X-rays)", result4.total_patients),
            ("Test 6 (linked to appointments)", result6.total_patients),
            ("Test 7 (with appointment_id OR matching date)", result7.total_patients),
        ]
        results.sort(key=lambda x: abs(x[1] - 2073))
        for name, count in results[:3]:
            print(f"  {name}: {count} (diff: {abs(count - 2073)})")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_completed_procedures_plus()

