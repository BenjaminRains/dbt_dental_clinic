"""
Test if PBN counts procedures linked to appointments with hygienist_id
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

def test_procedure_appointment_link():
    db = get_db_session()
    try:
        print("="*80)
        print("TESTING PROCEDURES LINKED TO APPOINTMENTS WITH HYGIENIST_ID")
        print("="*80)
        
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        # Test 1: Appointments + procedures linked to appointments with hygienist_id
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
            INNER JOIN raw_marts.fact_appointment fa ON ipc.appointment_id = fa.appointment_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)  -- Linked to appointment with hygienist
                AND NOT EXISTS (
                    -- Exclude if already counted in appointments (same patient + date)
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
        print(f"Test 1 - Appointments + procedures linked to hygienist appointments: {result1.total_patients} (diff from 2073: {abs(result1.total_patients - 2073)})")
        
        # Test 2: Same but with date matching (procedure date = appointment date)
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
            INNER JOIN raw_marts.fact_appointment fa ON ipc.appointment_id = fa.appointment_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND ipc.procedure_date = fa.appointment_date  -- Procedure date matches appointment date
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
        print(f"Test 2 - Same + procedure date = appointment date: {result2.total_patients} (diff from 2073: {abs(result2.total_patients - 2073)})")
        
        # Test 3: Count breakdown
        query3 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT fa.patient_id, fa.appointment_date as hygiene_date
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        ),
        procedures_linked_to_hygienist_appts AS (
            SELECT DISTINCT ipc.patient_id, ipc.procedure_date as hygiene_date
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            INNER JOIN raw_marts.fact_appointment fa ON ipc.appointment_id = fa.appointment_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
        ),
        procedures_not_in_appointments AS (
            SELECT DISTINCT p.patient_id
            FROM procedures_linked_to_hygienist_appts p
            WHERE NOT EXISTS (
                SELECT 1 FROM hygiene_appointments ha 
                WHERE ha.patient_id = p.patient_id
                    AND ha.hygiene_date = p.hygiene_date
            )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) as appointment_patients,
            (SELECT COUNT(DISTINCT patient_id) FROM procedures_linked_to_hygienist_appts) as all_linked_procedure_patients,
            (SELECT COUNT(DISTINCT patient_id) FROM procedures_not_in_appointments) as unique_procedure_patients,
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) + 
            (SELECT COUNT(DISTINCT patient_id) FROM procedures_not_in_appointments) as combined_total
        """
        result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nBreakdown:")
        print(f"  Appointment patients: {result3.appointment_patients}")
        print(f"  All procedure patients (linked to hygienist appts): {result3.all_linked_procedure_patients}")
        print(f"  Unique procedure patients (not in appointments): {result3.unique_procedure_patients}")
        print(f"  Combined total: {result3.combined_total}")
        print(f"  PBN target: 2073")
        print(f"  Difference: {result3.combined_total - 2073}")
        
        # Test 4: What if we only count procedures on different dates than appointments?
        query4 = """
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
            INNER JOIN raw_marts.fact_appointment fa ON ipc.appointment_id = fa.appointment_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND ipc.procedure_date != fa.appointment_date  -- Different date than appointment
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
        result4 = db.execute(text(query4), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nTest 4 - Procedures on different dates than appointments: {result4.total_patients} (diff from 2073: {abs(result4.total_patients - 2073)})")
        
        # Test 5: What if PBN only counts appointments, and procedures are a separate count?
        # Maybe they're not combining them at all?
        query5 = """
        SELECT 
            (SELECT COUNT(DISTINCT fa.patient_id) 
             FROM raw_marts.fact_appointment fa
             WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                 AND fa.appointment_date >= :start_date
                 AND fa.appointment_date <= :end_date) as appointment_patients,
            (SELECT COUNT(DISTINCT ipc.patient_id)
             FROM raw_intermediate.int_procedure_complete ipc
             INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
             WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                 AND ipc.procedure_date >= :start_date
                 AND ipc.procedure_date <= :end_date) as procedure_patients
        """
        result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nTest 5 - Separate counts (not combined):")
        print(f"  Appointment patients: {result5.appointment_patients}")
        print(f"  Procedure patients: {result5.procedure_patients}")
        print(f"  If PBN uses appointments only: {result5.appointment_patients} (diff: {abs(result5.appointment_patients - 2073)})")
        print(f"  If PBN uses procedures only: {result5.procedure_patients} (diff: {abs(result5.procedure_patients - 2073)})")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_procedure_appointment_link()

