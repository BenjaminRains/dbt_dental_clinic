"""
Test different ways to filter procedures to match PBN's 2073 count
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

def test_procedure_filters():
    db = get_db_session()
    try:
        print("="*80)
        print("TESTING PROCEDURE FILTERS TO MATCH PBN'S 2073")
        print("="*80)
        print("Appointments only: 1352")
        print("Procedures only (unique): 1557")
        print("Combined: 2909")
        print("PBN target: 2073")
        print("Need to reduce by: 836 patients")
        print()
        
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        # Test 1: Procedures linked to appointments
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
                AND ipc.appointment_id IS NOT NULL  -- Only procedures linked to appointments
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
        print("   [count omitted] Test 1 - Procedures linked to appointments only")
        
        # Test 2: Procedures from completed appointments only
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
                AND fa.is_completed = true  -- Only from completed appointments
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
        print("   [count omitted] Test 2 - Procedures from completed appointments only")
        
        # Test 3: Only appointments + procedures that match appointment dates
        query3 = """
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
                AND EXISTS (
                    -- Procedure date matches an appointment date for this patient
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                        AND ha.hygiene_date = ipc.procedure_date
                )
                AND NOT EXISTS (
                    -- But not already counted in appointments
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                        AND ha.hygiene_date = ipc.procedure_date
                )
        )
        SELECT COUNT(DISTINCT patient_id) as total_patients
        FROM (
            SELECT patient_id FROM hygiene_appointments
            UNION
            SELECT patient_id FROM hygiene_procedures
        ) combined
        """
        result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchone()
        print("   [count omitted] Test 3 - Procedures matching appointment dates")
        
        # Test 4: Only appointments (no procedures at all)
        query4 = """
        SELECT COUNT(DISTINCT fa.patient_id) as total_patients
        FROM raw_marts.fact_appointment fa
        WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
            AND fa.appointment_date >= :start_date
            AND fa.appointment_date <= :end_date
        """
        result4 = db.execute(text(query4), {"start_date": start_date, "end_date": end_date}).fetchone()
        print("   [count omitted] Test 4 - Only appointments (no procedures)")
        
        # Test 5: Appointments + procedures from appointments with hygienist_id (even if appointment not completed)
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
            INNER JOIN raw_marts.fact_appointment fa ON ipc.appointment_id = fa.appointment_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)  -- From appointments with hygienist
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                        AND ha.hygiene_date = ipc.procedure_date
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
        result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchone()
        print("   [count omitted] Test 5 - Appointments + procedures from hygienist appointments")
        
        # Test 6: Check if PBN might be using a different date range calculation
        # Maybe they count procedures/appointments differently
        query6 = """
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
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) as appointment_patients,
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures) as procedure_patients,
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) + 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures) as combined_total
        """
        result6 = db.execute(text(query6), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nBreakdown:")
        print("   [count omitted] Appointment patients")
        print("   [count omitted] Procedure patients (unique)")
        print("   [count omitted] Combined")
        print(f"  PBN target: 2073")
        print("   [count omitted] Difference")
        print("   [count omitted] If we need")
        
        # Test 7: What if PBN only counts procedures that are NOT in appointments with hygienist_id?
        # But maybe they use a different matching logic
        query7 = """
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
                    -- Exclude if patient has ANY appointment with hygienist in date range
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
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
        result7 = db.execute(text(query7), {"start_date": start_date, "end_date": end_date}).fetchone()
        print("   [count omitted] \nTest 7 - Current logic (appointments + procedures not in appointments)")
        
        print("\n" + "="*80)
        print("KEY INSIGHT:")
        print("="*80)
        print("   [count omitted] Appointments")
        print("   [count omitted] To reach 2073, we need")
        print("   [count omitted] Currently getting")
        print("   [count omitted] Need to reduce procedures by")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_procedure_filters()

