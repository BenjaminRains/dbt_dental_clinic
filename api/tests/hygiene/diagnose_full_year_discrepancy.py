"""
Diagnose why we're getting 2909 patients vs PBN's 2073 for full year 2025
We need to find what filters PBN might be applying
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

def diagnose_discrepancy():
    db = get_db_session()
    try:
        print("="*80)
        print("DIAGNOSING FULL YEAR 2025 DISCREPANCY")
        print("="*80)
        print("Our count: 2909 patients")
        print("PBN count: 2073 patients")
        print("Difference: 836 patients (40.3% too many)")
        print()
        
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        # Test 1: Current logic (appointments + procedures)
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
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        ),
        hygiene_patients AS (
            SELECT DISTINCT patient_id, hygiene_date FROM hygiene_appointments
            UNION
            SELECT DISTINCT patient_id, hygiene_date FROM hygiene_procedures
        )
        SELECT COUNT(DISTINCT patient_id) as total_patients
        FROM hygiene_patients
        """
        result1 = db.execute(text(query1), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 1 - Current logic (all appointments + procedures): {result1.total_patients}")
        
        # Test 2: Only completed appointments
        query2 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT fa.patient_id, fa.appointment_date as hygiene_date
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND fa.is_completed = true
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
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        ),
        hygiene_patients AS (
            SELECT DISTINCT patient_id, hygiene_date FROM hygiene_appointments
            UNION
            SELECT DISTINCT patient_id, hygiene_date FROM hygiene_procedures
        )
        SELECT COUNT(DISTINCT patient_id) as total_patients
        FROM hygiene_patients
        """
        result2 = db.execute(text(query2), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 2 - Only completed appointments: {result2.total_patients}")
        
        # Test 3: Exclude broken/no-show appointments
        query3 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT fa.patient_id, fa.appointment_date as hygiene_date
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND (fa.is_broken = false OR fa.is_broken IS NULL)
                AND (fa.is_no_show = false OR fa.is_no_show IS NULL)
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
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        ),
        hygiene_patients AS (
            SELECT DISTINCT patient_id, hygiene_date FROM hygiene_appointments
            UNION
            SELECT DISTINCT patient_id, hygiene_date FROM hygiene_procedures
        )
        SELECT COUNT(DISTINCT patient_id) as total_patients
        FROM hygiene_patients
        """
        result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 3 - Exclude broken/no-show: {result3.total_patients}")
        
        # Test 4: Completed + exclude broken/no-show
        query4 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT fa.patient_id, fa.appointment_date as hygiene_date
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND fa.is_completed = true
                AND (fa.is_broken = false OR fa.is_broken IS NULL)
                AND (fa.is_no_show = false OR fa.is_no_show IS NULL)
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
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        ),
        hygiene_patients AS (
            SELECT DISTINCT patient_id, hygiene_date FROM hygiene_appointments
            UNION
            SELECT DISTINCT patient_id, hygiene_date FROM hygiene_procedures
        )
        SELECT COUNT(DISTINCT patient_id) as total_patients
        FROM hygiene_patients
        """
        result4 = db.execute(text(query4), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 4 - Completed + exclude broken/no-show: {result4.total_patients}")
        
        # Test 5: Only appointments (no procedures)
        query5 = """
        SELECT COUNT(DISTINCT fa.patient_id) as total_patients
        FROM raw_marts.fact_appointment fa
        WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
            AND fa.appointment_date >= :start_date
            AND fa.appointment_date <= :end_date
        """
        result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 5 - Only appointments (no procedures): {result5.total_patients}")
        
        # Test 6: Only completed appointments (no procedures)
        query6 = """
        SELECT COUNT(DISTINCT fa.patient_id) as total_patients
        FROM raw_marts.fact_appointment fa
        WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
            AND fa.is_completed = true
            AND fa.appointment_date >= :start_date
            AND fa.appointment_date <= :end_date
        """
        result6 = db.execute(text(query6), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"Test 6 - Only completed appointments (no procedures): {result6.total_patients}")
        
        # Test 7: Check appointment status breakdown
        query7 = """
        SELECT 
            COUNT(DISTINCT fa.patient_id) as total_patients,
            COUNT(DISTINCT CASE WHEN fa.is_completed = true THEN fa.patient_id END) as completed_patients,
            COUNT(DISTINCT CASE WHEN fa.is_broken = true THEN fa.patient_id END) as broken_patients,
            COUNT(DISTINCT CASE WHEN fa.is_no_show = true THEN fa.patient_id END) as no_show_patients,
            COUNT(DISTINCT CASE WHEN fa.is_completed = false AND (fa.is_broken = false OR fa.is_broken IS NULL) AND (fa.is_no_show = false OR fa.is_no_show IS NULL) THEN fa.patient_id END) as scheduled_patients
        FROM raw_marts.fact_appointment fa
        WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
            AND fa.appointment_date >= :start_date
            AND fa.appointment_date <= :end_date
        """
        result7 = db.execute(text(query7), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nAppointment Status Breakdown:")
        print(f"  Total patients: {result7.total_patients}")
        print(f"  Completed: {result7.completed_patients}")
        print(f"  Broken: {result7.broken_patients}")
        print(f"  No-show: {result7.no_show_patients}")
        print(f"  Scheduled (not completed): {result7.scheduled_patients}")
        
        # Test 8: Procedures breakdown
        query8 = """
        SELECT 
            COUNT(DISTINCT ipc.patient_id) as total_procedure_patients,
            COUNT(DISTINCT CASE WHEN EXISTS (
                SELECT 1 FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ipc.patient_id
                    AND (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
            ) THEN ipc.patient_id END) as patients_already_in_appointments
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result8 = db.execute(text(query8), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nProcedures Breakdown:")
        print(f"  Total procedure patients: {result8.total_procedure_patients}")
        print(f"  Already in appointments: {result8.patients_already_in_appointments}")
        print(f"  Unique from procedures only: {result8.total_procedure_patients - result8.patients_already_in_appointments}")
        
        print("\n" + "="*80)
        print("ANALYSIS:")
        print("="*80)
        print(f"Target: 2073 patients")
        print(f"Current: {result1.total_patients} patients")
        print(f"\nClosest matches:")
        if abs(result2.total_patients - 2073) < abs(result1.total_patients - 2073):
            print(f"  ✅ Test 2 (completed only): {result2.total_patients} (diff: {abs(result2.total_patients - 2073)})")
        if abs(result3.total_patients - 2073) < abs(result1.total_patients - 2073):
            print(f"  ✅ Test 3 (exclude broken/no-show): {result3.total_patients} (diff: {abs(result3.total_patients - 2073)})")
        if abs(result4.total_patients - 2073) < abs(result1.total_patients - 2073):
            print(f"  ✅ Test 4 (completed + exclude broken/no-show): {result4.total_patients} (diff: {abs(result4.total_patients - 2073)})")
        if abs(result6.total_patients - 2073) < abs(result1.total_patients - 2073):
            print(f"  ✅ Test 6 (only completed appointments, no procedures): {result6.total_patients} (diff: {abs(result6.total_patients - 2073)})")
        
    finally:
        db.close()

if __name__ == "__main__":
    diagnose_discrepancy()

