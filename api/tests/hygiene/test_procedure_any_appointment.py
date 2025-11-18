"""
Test if PBN counts procedures linked to ANY appointment (not just hygienist appointments)
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

def test_procedure_any_appointment():
    db = get_db_session()
    try:
        print("="*80)
        print("TESTING PROCEDURES LINKED TO ANY APPOINTMENT")
        print("="*80)
        print("Target: 2073 patients")
        print("Appointments with hygienist_id: 1352")
        print("Need: 721 more patients from procedures")
        print()
        
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        # Test 1: Appointments + procedures linked to ANY appointment (excluding those in hygienist appointments)
        query1 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT fa.patient_id, fa.appointment_date as hygiene_date
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        ),
        hygiene_procedures AS (
            SELECT DISTINCT ipc.patient_id, ipc.procedure_date as hygiene_date
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND ipc.appointment_id IS NOT NULL  -- Linked to ANY appointment
                AND NOT EXISTS (
                    -- Exclude if already counted in hygienist appointments (same patient + date)
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                        AND ha.hygiene_date = ipc.procedure_date
                )
        ),
        hygiene_patients AS (
            SELECT DISTINCT patient_id FROM hygiene_appointments
            UNION
            SELECT DISTINCT patient_id FROM hygiene_procedures
        )
        SELECT COUNT(DISTINCT patient_id) as total_patients
        FROM hygiene_patients
        """
        result1 = db.execute(text(query1), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 1 - Appointments + procedures linked to ANY appointment: {result1.total_patients} (diff: {abs(result1.total_patients - 2073)})")
        
        # Test 2: Same but exclude procedures where the linked appointment has hygienist_id
        query2 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT fa.patient_id, fa.appointment_date as hygiene_date
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        ),
        hygiene_procedures AS (
            SELECT DISTINCT ipc.patient_id, ipc.procedure_date as hygiene_date
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            LEFT JOIN raw_marts.fact_appointment fa ON ipc.appointment_id = fa.appointment_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND ipc.appointment_id IS NOT NULL
                AND (fa.hygienist_id IS NULL OR fa.hygienist_id = 0)  -- Appointment does NOT have hygienist_id
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                        AND ha.hygiene_date = ipc.procedure_date
                )
        ),
        hygiene_patients AS (
            SELECT DISTINCT patient_id FROM hygiene_appointments
            UNION
            SELECT DISTINCT patient_id FROM hygiene_procedures
        )
        SELECT COUNT(DISTINCT patient_id) as total_patients
        FROM hygiene_patients
        """
        result2 = db.execute(text(query2), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 2 - Appointments + procedures from non-hygienist appointments: {result2.total_patients} (diff: {abs(result2.total_patients - 2073)})")
        
        # Test 3: Breakdown of procedures
        query3 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT fa.patient_id, fa.appointment_date as hygiene_date
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        ),
        all_procedures AS (
            SELECT DISTINCT ipc.patient_id, ipc.procedure_date as hygiene_date, ipc.appointment_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
        ),
        procedures_with_appt AS (
            SELECT DISTINCT ap.patient_id
            FROM all_procedures ap
            WHERE ap.appointment_id IS NOT NULL
        ),
        procedures_without_appt AS (
            SELECT DISTINCT ap.patient_id
            FROM all_procedures ap
            WHERE ap.appointment_id IS NULL
        ),
        procedures_in_hygienist_appts AS (
            SELECT DISTINCT ap.patient_id
            FROM all_procedures ap
            INNER JOIN raw_marts.fact_appointment fa ON ap.appointment_id = fa.appointment_id
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
        ),
        procedures_not_in_hygienist_appts AS (
            SELECT DISTINCT ap.patient_id
            FROM all_procedures ap
            WHERE NOT EXISTS (
                SELECT 1 FROM hygiene_appointments ha 
                WHERE ha.patient_id = ap.patient_id
                    AND ha.hygiene_date = ap.hygiene_date
            )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) as appointment_patients,
            (SELECT COUNT(DISTINCT patient_id) FROM all_procedures) as all_procedure_patients,
            (SELECT COUNT(DISTINCT patient_id) FROM procedures_with_appt) as procedure_patients_with_appt,
            (SELECT COUNT(DISTINCT patient_id) FROM procedures_without_appt) as procedure_patients_without_appt,
            (SELECT COUNT(DISTINCT patient_id) FROM procedures_in_hygienist_appts) as procedure_patients_in_hygienist_appts,
            (SELECT COUNT(DISTINCT patient_id) FROM procedures_not_in_hygienist_appts) as procedure_patients_not_in_hygienist_appts
        """
        result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nBreakdown:")
        print(f"  Appointment patients: {result3.appointment_patients}")
        print(f"  All procedure patients: {result3.all_procedure_patients}")
        print(f"  Procedure patients (with appointment): {result3.procedure_patients_with_appt}")
        print(f"  Procedure patients (without appointment): {result3.procedure_patients_without_appt}")
        print(f"  Procedure patients (in hygienist appointments): {result3.procedure_patients_in_hygienist_appts}")
        print(f"  Procedure patients (NOT in hygienist appointments): {result3.procedure_patients_not_in_hygienist_appts}")
        print(f"\n  If we use appointments + procedures NOT in hygienist appointments:")
        print(f"    Total: {result3.appointment_patients + result3.procedure_patients_not_in_hygienist_appts} (diff: {abs((result3.appointment_patients + result3.procedure_patients_not_in_hygienist_appts) - 2073)})")
        
        # Test 4: What if PBN uses a UNION approach (appointments OR procedures, not both)?
        query4 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT fa.patient_id
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        ),
        hygiene_procedures AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
        ),
        hygiene_patients AS (
            SELECT patient_id FROM hygiene_appointments
            UNION
            SELECT patient_id FROM hygiene_procedures
        )
        SELECT COUNT(DISTINCT patient_id) as total_patients
        FROM hygiene_patients
        """
        result4 = db.execute(text(query4), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nTest 4 - UNION (appointments OR procedures, no exclusion): {result4.total_patients} (diff: {abs(result4.total_patients - 2073)})")
        
        # Test 5: What if we need to filter procedures by completion status?
        query5 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT fa.patient_id, fa.appointment_date as hygiene_date
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        ),
        hygiene_procedures AS (
            SELECT DISTINCT ipc.patient_id, ipc.procedure_date as hygiene_date
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND ipc.is_completed = true  -- Only completed procedures
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                        AND ha.hygiene_date = ipc.procedure_date
                )
        ),
        hygiene_patients AS (
            SELECT DISTINCT patient_id FROM hygiene_appointments
            UNION
            SELECT DISTINCT patient_id FROM hygiene_procedures
        )
        SELECT COUNT(DISTINCT patient_id) as total_patients
        FROM hygiene_patients
        """
        result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 5 - Appointments + completed procedures: {result5.total_patients} (diff: {abs(result5.total_patients - 2073)})")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_procedure_any_appointment()

