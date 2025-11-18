"""
Systematically test different combinations to match PBN's 2073 patients seen.
"""

import sys
import os
from datetime import date
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add the parent directory to sys.path to allow importing modules from 'api'
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from config import get_config

def get_db_session():
    config = get_config()
    DATABASE_URL = config.get_database_url()
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()

def test_hygiene_combinations():
    db = get_db_session()
    try:
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        print("="*80)
        print("TESTING DIFFERENT COMBINATIONS TO MATCH PBN: 2073 PATIENTS SEEN")
        print(f"Date Range: {start_date} to {end_date}")
        print("="*80)
        
        # Test 1: Appointments with hygienist_id (current approach)
        query1 = """
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
            AND appointment_date >= :start_date
            AND appointment_date <= :end_date
        """
        result1 = db.execute(text(query1), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\n1. Appointments with hygienist_id: {result1.unique_patients} (PBN: 2073)")
        
        # Test 2: Appointments with hygienist_id + specific appointment types
        query2 = """
        SELECT COUNT(DISTINCT fa.patient_id) as unique_patients
        FROM raw_marts.fact_appointment fa
        LEFT JOIN raw_intermediate.int_appointment_details iad ON fa.appointment_id = iad.appointment_id
        WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
            AND fa.appointment_date >= :start_date
            AND fa.appointment_date <= :end_date
            AND (iad.appointment_type_name ILIKE '%cleaning%' 
                 OR iad.appointment_type_name ILIKE '%hygiene%'
                 OR iad.appointment_type_name IS NULL)
        """
        result2 = db.execute(text(query2), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"2. Appointments with hygienist_id + cleaning/hygiene types: {result2.unique_patients} (PBN: 2073)")
        
        # Test 3: Patients with hygiene procedures (by code) in date range
        query3 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as unique_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"3. Patients with hygiene procedure codes: {result3.unique_patients} (PBN: 2073)")
        
        # Test 4: Appointments OR procedures (union, no dedup by date)
        query4 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT patient_id, appointment_date
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
        ),
        hygiene_procedures AS (
            SELECT DISTINCT 
                ipc.patient_id,
                ipc.procedure_date
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
        )
        SELECT COUNT(DISTINCT COALESCE(ha.patient_id, hp.patient_id)) as unique_patients
        FROM hygiene_appointments ha
        FULL OUTER JOIN hygiene_procedures hp ON ha.patient_id = hp.patient_id
        """
        result4 = db.execute(text(query4), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"4. Appointments OR procedures (union): {result4.unique_patients} (PBN: 2073)")
        
        # Test 5: Appointments with hygienist_id OR appointments with hygiene procedures on same date
        query5 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT fa.patient_id, fa.appointment_date
            FROM raw_marts.fact_appointment fa
            WHERE (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        ),
        appointments_with_hygiene_procs AS (
            SELECT DISTINCT fa.patient_id, fa.appointment_date
            FROM raw_marts.fact_appointment fa
            INNER JOIN raw_intermediate.int_procedure_complete ipc 
                ON fa.appointment_id = ipc.appointment_id
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        )
        SELECT COUNT(DISTINCT COALESCE(ha.patient_id, ap.patient_id)) as unique_patients
        FROM hygiene_appointments ha
        FULL OUTER JOIN appointments_with_hygiene_procs ap 
            ON ha.patient_id = ap.patient_id
        """
        result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"5. Appointments with hygienist_id OR appointments with hygiene procedures: {result5.unique_patients} (PBN: 2073)")
        
        # Test 6: All appointments (any status) with hygienist_id
        query6 = """
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
            AND appointment_date >= :start_date
            AND appointment_date <= :end_date
            AND appointment_status IN ('Scheduled', 'Complete', 'Planned', 'Broken', 'UnschedList', 'ASAP', 'PtNote', 'PtNoteCompleted')
        """
        result6 = db.execute(text(query6), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"6. All appointment statuses with hygienist_id: {result6.unique_patients} (PBN: 2073)")
        
        # Test 7: Patients with hygiene procedures, excluding those already counted by appointments
        query7 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
        ),
        hygiene_procedures_only AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) as from_appointments,
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures_only) as from_procedures_only,
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) + 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures_only) as total
        """
        result7 = db.execute(text(query7), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"7. Appointments ({result7.from_appointments}) + Procedures only ({result7.from_procedures_only}) = {result7.total} (PBN: 2073)")
        
        # Test 8: Check if maybe PBN counts unique patient-appointment-date combinations
        query8 = """
        SELECT COUNT(DISTINCT (patient_id, appointment_date)) as unique_combinations
        FROM raw_marts.fact_appointment
        WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
            AND appointment_date >= :start_date
            AND appointment_date <= :end_date
        """
        result8 = db.execute(text(query8), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"8. Unique (patient_id, appointment_date) combinations: {result8.unique_combinations} (PBN: 2073)")
        
        # Test 9: Maybe PBN counts patients who had hygiene in ANY appointment (not just with hygienist_id)
        query9 = """
        WITH appointments_with_hygiene AS (
            SELECT DISTINCT fa.patient_id
            FROM raw_marts.fact_appointment fa
            WHERE fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
                AND (
                    (fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0)
                    OR EXISTS (
                        SELECT 1 
                        FROM raw_intermediate.int_procedure_complete ipc
                        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
                        WHERE ipc.appointment_id = fa.appointment_id
                            AND pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                    )
                )
        )
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM appointments_with_hygiene
        """
        result9 = db.execute(text(query9), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"9. Patients with hygiene in ANY appointment (hygienist_id OR hygiene procedures): {result9.unique_patients} (PBN: 2073)")
        
        # Test 10: Check if maybe PBN uses a rolling 12 months instead of calendar year
        query10 = """
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
            AND appointment_date >= CURRENT_DATE - INTERVAL '12 months'
            AND appointment_date <= CURRENT_DATE
        """
        result10 = db.execute(text(query10)).fetchone()
        print(f"10. Rolling 12 months (not calendar year): {result10.unique_patients} (PBN: 2073)")
        
        # Test 11: Maybe PBN excludes certain procedure codes (like X-rays)
        query11 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as unique_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result11 = db.execute(text(query11), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"11. Patients with hygiene codes (excluding X-rays): {result11.unique_patients} (PBN: 2073)")
        
        # Test 12: Appointments + procedures (excluding X-rays), no double counting
        query12 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
        ),
        hygiene_procedures_no_xray AS (
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
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) +
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures_no_xray) as total
        """
        result12 = db.execute(text(query12), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"12. Appointments + procedures (no X-rays, no double count): {result12.total} (PBN: 2073)")
        
        # Test 13: Maybe PBN uses a different year (2024?)
        query13 = """
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
            AND appointment_date >= '2024-01-01'
            AND appointment_date <= '2024-12-31'
        """
        result13 = db.execute(text(query13)).fetchone()
        print(f"13. Calendar year 2024: {result13.unique_patients} (PBN: 2073)")
        
        # Test 14: Maybe PBN counts patients who had hygiene in 2025 OR had hygiene procedures
        query14 = """
        WITH hygiene_appointments_2025 AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
        ),
        hygiene_procedures_2025 AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
        )
        SELECT COUNT(DISTINCT COALESCE(ha.patient_id, hp.patient_id)) as unique_patients
        FROM hygiene_appointments_2025 ha
        FULL OUTER JOIN hygiene_procedures_2025 hp ON ha.patient_id = hp.patient_id
        """
        result14 = db.execute(text(query14), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"14. Appointments 2025 OR procedures 2025 (all codes): {result14.unique_patients} (PBN: 2073)")
        
        # Test 15: Maybe PBN counts unique patients per month and sums them?
        query15 = """
        SELECT COUNT(DISTINCT (patient_id, DATE_TRUNC('month', appointment_date))) as unique_patient_months
        FROM raw_marts.fact_appointment
        WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
            AND appointment_date >= :start_date
            AND appointment_date <= :end_date
        """
        result15 = db.execute(text(query15), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"15. Unique (patient_id, month) combinations: {result15.unique_patient_months} (PBN: 2073)")
        
        # Test 16: Maybe PBN counts patients who had completed hygiene appointments
        query16 = """
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
            AND appointment_date >= :start_date
            AND appointment_date <= :end_date
            AND is_completed = true
        """
        result16 = db.execute(text(query16), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"16. Completed hygiene appointments only: {result16.unique_patients} (PBN: 2073)")
        
        # Test 17: Maybe PBN counts patients with hygiene procedures that are NOT in appointments with hygienist_id
        query17 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
        ),
        procedures_not_in_hygiene_appts AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) +
            (SELECT COUNT(DISTINCT patient_id) FROM procedures_not_in_hygiene_appts) as total
        """
        result17 = db.execute(text(query17), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"17. Appointments + procedures NOT in appointments: {result17.total} (PBN: 2073)")
        
        # Test 18: Check if maybe PBN uses a different date (maybe it's showing data as of a specific date, not full year)
        # Let's check what date range would give us 2073
        print(f"\n18. Testing different date ranges to find what gives 2073:")
        for months in [6, 9, 12, 18, 24]:
            query18 = f"""
            SELECT COUNT(DISTINCT patient_id) as unique_patients
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= CURRENT_DATE - INTERVAL '{months} months'
                AND appointment_date <= CURRENT_DATE
            """
            result18 = db.execute(text(query18)).fetchone()
            print(f"    Last {months} months: {result18.unique_patients}")
        
        # Test 19: Maybe PBN = appointments (1352) + procedures NOT in appointments, but only specific codes?
        # We need exactly 721 more patients
        query19 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
        ),
        procedures_not_in_appts AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        )
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM procedures_not_in_appts
        """
        result19 = db.execute(text(query19), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"19. Procedures NOT in appointments (all codes): {result19.unique_patients} (need 721)")
        
        # Test 20: Maybe PBN counts ALL patients who had hygiene in last 18 months (not just 2025)?
        query20 = """
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
            AND appointment_date >= CURRENT_DATE - INTERVAL '18 months'
            AND appointment_date <= CURRENT_DATE
        """
        result20 = db.execute(text(query20)).fetchone()
        print(f"20. Last 18 months (not calendar year): {result20.unique_patients} (PBN: 2073)")
        
        # Test 21: Maybe PBN counts patients who had hygiene in 2025 OR in last 18 months?
        query21 = """
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
            AND (
                (appointment_date >= :start_date AND appointment_date <= :end_date)
                OR (appointment_date >= CURRENT_DATE - INTERVAL '18 months' AND appointment_date <= CURRENT_DATE)
            )
        """
        result21 = db.execute(text(query21), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"21. 2025 OR last 18 months: {result21.unique_patients} (PBN: 2073)")
        
        # Test 22: Maybe PBN counts patients with hygiene procedures in 2025, regardless of appointment?
        query22 = """
        WITH hygiene_procedures_2025 AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
        ),
        hygiene_appointments_2025 AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments_2025) as from_appts,
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures_2025 
             WHERE patient_id NOT IN (SELECT patient_id FROM hygiene_appointments_2025)) as from_procs_only,
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments_2025) +
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures_2025 
             WHERE patient_id NOT IN (SELECT patient_id FROM hygiene_appointments_2025)) as total
        """
        result22 = db.execute(text(query22), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"22. Appointments (1352) + Procedures only (no X-rays): {result22.from_appts} + {result22.from_procs_only} = {result22.total} (PBN: 2073)")
        
        # Test 23: Check if maybe PBN uses a specific date (like "as of today" for a specific date in 2025)
        # Let's test what the count would be if we use "as of end of 2025" but include all historical data
        query23 = """
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
            AND appointment_date <= :end_date
        """
        result23 = db.execute(text(query23), {"end_date": end_date}).fetchone()
        print(f"23. All historical appointments up to end of 2025: {result23.unique_patients} (PBN: 2073)")
        
        # Test 24: Maybe PBN counts unique patients per procedure code and sums them (wrong but possible)?
        query24 = """
        SELECT COUNT(DISTINCT ipc.patient_id) as unique_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE pc.procedure_code IN ('D1110', 'D1120')
            AND ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
        """
        result24 = db.execute(text(query24), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"24. Patients with D1110 or D1120 (prophylaxis only): {result24.unique_patients} (PBN: 2073)")
        
        # Test 25: Find exactly which procedures give us 721 patients (not in appointments)
        # We know we have 1608 total, need to find the subset that gives 721
        query25 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
        ),
        procedures_by_code AS (
            SELECT 
                pc.procedure_code,
                COUNT(DISTINCT ipc.patient_id) as patient_count
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
            GROUP BY pc.procedure_code
            ORDER BY patient_count DESC
        )
        SELECT 
            procedure_code,
            patient_count,
            SUM(patient_count) OVER (ORDER BY patient_count DESC) as running_total
        FROM procedures_by_code
        """
        result25 = db.execute(text(query25), {"start_date": start_date, "end_date": end_date}).fetchall()
        print(f"\n25. Procedures NOT in appointments, by code (cumulative):")
        for row in result25:
            print(f"    {row.procedure_code}: {row.patient_count} patients (running total: {row.running_total})")
            if row.running_total >= 721:
                print(f"    *** Found it! Need codes up to {row.procedure_code} to get ~721 patients")
        
        # Test 26: Maybe PBN counts appointments + procedures, but only completed procedures?
        query26 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
        ),
        completed_procedures_only AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND ipc.procedure_status = 2  -- Completed
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) +
            (SELECT COUNT(DISTINCT patient_id) FROM completed_procedures_only) as total
        """
        result26 = db.execute(text(query26), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"26. Appointments + completed procedures only (not in appointments): {result26.total} (PBN: 2073)")
        
        # Test 27: Maybe PBN uses a different date - what if it's "as of a specific date in 2025"?
        # Let's test mid-year
        query27 = """
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
            AND appointment_date >= '2025-01-01'
            AND appointment_date <= '2025-06-30'
        """
        result27 = db.execute(text(query27)).fetchone()
        print(f"27. First half of 2025 (Jan-Jun): {result27.unique_patients} (PBN: 2073)")
        
        # Test 28: Maybe PBN counts patients who had hygiene in appointments OR procedures, but with specific filters
        query28 = """
        WITH hygiene_appointments AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
        ),
        hygiene_procedures_filtered AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D1110', 'D1120')  -- Only prophylaxis
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND ipc.procedure_status = 2  -- Completed
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) +
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures_filtered) as total
        """
        result28 = db.execute(text(query28), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"28. Appointments + completed prophylaxis only (D1110/D1120, not in appointments): {result28.total} (PBN: 2073)")
        
        # Test 29: Maybe PBN uses a rolling 18-month window for BOTH appointments and procedures?
        query29 = """
        WITH hygiene_appointments_18mo AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND appointment_date <= CURRENT_DATE
        ),
        hygiene_procedures_18mo AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                AND ipc.procedure_date >= CURRENT_DATE - INTERVAL '18 months'
                AND ipc.procedure_date <= CURRENT_DATE
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments_18mo ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments_18mo) +
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures_18mo) as total
        """
        result29 = db.execute(text(query29)).fetchone()
        print(f"29. Last 18 months: Appointments + Procedures (not in appointments): {result29.total} (PBN: 2073)")
        
        # Test 30: Maybe PBN counts unique patients who had hygiene in last 18 months (appointments OR procedures)
        query30 = """
        WITH hygiene_appointments_18mo AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND appointment_date <= CURRENT_DATE
        ),
        hygiene_procedures_18mo AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                AND ipc.procedure_date >= CURRENT_DATE - INTERVAL '18 months'
                AND ipc.procedure_date <= CURRENT_DATE
        )
        SELECT COUNT(DISTINCT COALESCE(ha.patient_id, hp.patient_id)) as unique_patients
        FROM hygiene_appointments_18mo ha
        FULL OUTER JOIN hygiene_procedures_18mo hp ON ha.patient_id = hp.patient_id
        """
        result30 = db.execute(text(query30)).fetchone()
        print(f"30. Last 18 months: Appointments OR Procedures (union): {result30.unique_patients} (PBN: 2073)")
        
        # Test 31: Maybe PBN uses a different date - what if it's "as of end of 2024"?
        query31 = """
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
            AND appointment_date >= '2024-01-01'
            AND appointment_date <= '2024-12-31'
        """
        result31 = db.execute(text(query31)).fetchone()
        print(f"31. Calendar year 2024: {result31.unique_patients} (PBN: 2073)")
        
        # Test 32: Maybe PBN counts patients with hygiene procedures linked to appointments (even without hygienist_id)?
        query32 = """
        WITH appointments_with_hygiene_procs AS (
            SELECT DISTINCT fa.patient_id
            FROM raw_marts.fact_appointment fa
            INNER JOIN raw_intermediate.int_procedure_complete ipc 
                ON fa.appointment_id = ipc.appointment_id
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        ),
        appointments_with_hygienist AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
        )
        SELECT COUNT(DISTINCT COALESCE(ahp.patient_id, ah.patient_id)) as unique_patients
        FROM appointments_with_hygiene_procs ahp
        FULL OUTER JOIN appointments_with_hygienist ah ON ahp.patient_id = ah.patient_id
        """
        result32 = db.execute(text(query32), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"32. Appointments with hygienist_id OR appointments with hygiene procedures: {result32.unique_patients} (PBN: 2073)")
        
        # Test 33: Check what date range from 2025 would give us 2073
        # Maybe it's not full year, but a specific period?
        print(f"\n33. Testing different 2025 date ranges:")
        test_dates = [
            ("Jan-Mar", '2025-01-01', '2025-03-31'),
            ("Jan-Jun", '2025-01-01', '2025-06-30'),
            ("Jan-Sep", '2025-01-01', '2025-09-30'),
            ("Q1", '2025-01-01', '2025-03-31'),
            ("Q2", '2025-04-01', '2025-06-30'),
            ("Q3", '2025-07-01', '2025-09-30'),
            ("Q4", '2025-10-01', '2025-12-31'),
        ]
        for label, sd, ed in test_dates:
            query33 = f"""
            SELECT COUNT(DISTINCT patient_id) as unique_patients
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= '{sd}'
                AND appointment_date <= '{ed}'
            """
            result33 = db.execute(text(query33)).fetchone()
            print(f"    {label}: {result33.unique_patients}")
            if result33.unique_patients == 2073:
                print(f"    *** FOUND IT! {label} gives exactly 2073!")
        
        # Test 34: Maybe PBN counts ALL patients who had hygiene (appointments OR procedures) in 2025,
        # but excludes certain appointment statuses?
        query34 = """
        WITH hygiene_appointments_2025 AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
                AND appointment_status NOT IN ('Broken', 'Cancelled')  -- Exclude broken/cancelled
        ),
        hygiene_procedures_2025 AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments_2025 ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments_2025) +
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures_2025) as total
        """
        result34 = db.execute(text(query34), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"34. Appointments (excl broken) + procedures (no X-rays, not in appointments): {result34.total} (PBN: 2073)")
        
        # Test 35: Maybe PBN counts patients with hygiene procedures linked to ANY appointment (even without hygienist_id)?
        query35 = """
        WITH all_hygiene_patients AS (
            -- Patients with appointments that have hygienist_id
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
            
            UNION
            
            -- Patients with hygiene procedures in appointments (even without hygienist_id)
            SELECT DISTINCT fa.patient_id
            FROM raw_marts.fact_appointment fa
            INNER JOIN raw_intermediate.int_procedure_complete ipc 
                ON fa.appointment_id = ipc.appointment_id
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        )
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM all_hygiene_patients
        """
        result35 = db.execute(text(query35), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"35. Appointments with hygienist_id OR appointments with hygiene procedures: {result35.unique_patients} (PBN: 2073)")
        
        # Test 36: Maybe PBN counts unique patients per procedure code and sums? (wrong but let's check)
        query36 = """
        SELECT SUM(patient_count) as total
        FROM (
            SELECT 
                pc.procedure_code,
                COUNT(DISTINCT ipc.patient_id) as patient_count
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
            GROUP BY pc.procedure_code
        ) subq
        """
        result36 = db.execute(text(query36), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"36. Sum of unique patients per procedure code (wrong method, but testing): {result36.total} (PBN: 2073)")
        
        # Test 37: Maybe PBN uses "as of a specific date" - test different end dates in 2025
        print(f"\n37. Testing 'year to date' as of different dates in 2025:")
        test_end_dates = [
            ('2025-03-31', 'End of Q1'),
            ('2025-06-30', 'End of Q2'),
            ('2025-09-30', 'End of Q3'),
            ('2025-12-31', 'End of Q4'),
        ]
        for ed, label in test_end_dates:
            query37 = f"""
            WITH hygiene_appointments AS (
                SELECT DISTINCT patient_id
                FROM raw_marts.fact_appointment
                WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                    AND appointment_date >= '2025-01-01'
                    AND appointment_date <= '{ed}'
            ),
            hygiene_procedures AS (
                SELECT DISTINCT ipc.patient_id
                FROM raw_intermediate.int_procedure_complete ipc
                INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
                WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                    AND ipc.procedure_date >= '2025-01-01'
                    AND ipc.procedure_date <= '{ed}'
                    AND NOT EXISTS (
                        SELECT 1 FROM hygiene_appointments ha 
                        WHERE ha.patient_id = ipc.patient_id
                    )
            )
            SELECT 
                (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) +
                (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures) as total
            """
            result37 = db.execute(text(query37)).fetchone()
            print(f"    {label}: {result37.total}")
            if result37.total == 2073:
                print(f"    *** FOUND IT! {label} gives exactly 2073!")
        
        # Test 38: End of Q2 gave us 2080, let's test dates around June 30 to find 2073
        print(f"\n38. Testing dates around end of Q2 (2080 was close to 2073):")
        test_dates_june = [
            ('2025-06-25', 'June 25'),
            ('2025-06-26', 'June 26'),
            ('2025-06-27', 'June 27'),
            ('2025-06-28', 'June 28'),
            ('2025-06-29', 'June 29'),
            ('2025-06-30', 'June 30 (End Q2)'),
        ]
        for ed, label in test_dates_june:
            query38 = f"""
            WITH hygiene_appointments AS (
                SELECT DISTINCT patient_id
                FROM raw_marts.fact_appointment
                WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                    AND appointment_date >= '2025-01-01'
                    AND appointment_date <= '{ed}'
            ),
            hygiene_procedures AS (
                SELECT DISTINCT ipc.patient_id
                FROM raw_intermediate.int_procedure_complete ipc
                INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
                WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                    AND ipc.procedure_date >= '2025-01-01'
                    AND ipc.procedure_date <= '{ed}'
                    AND NOT EXISTS (
                        SELECT 1 FROM hygiene_appointments ha 
                        WHERE ha.patient_id = ipc.patient_id
                    )
            )
            SELECT 
                (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) +
                (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures) as total
            """
            result38 = db.execute(text(query38)).fetchone()
            print(f"    {label}: {result38.total}")
            if result38.total == 2073:
                print(f"    *** FOUND IT! {label} gives exactly 2073!")
        
        # Test 39: June 27 gave us 2070 (only 3 off!). Let's test with different filters
        print(f"\n39. Testing June 27 (2070) with different filters to get exactly 2073:")
        test_date = '2025-06-27'
        
        # 39a: Exclude broken appointments
        query39a = f"""
        WITH hygiene_appointments AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= '2025-01-01'
                AND appointment_date <= '{test_date}'
                AND appointment_status != 'Broken'
        ),
        hygiene_procedures AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= '2025-01-01'
                AND ipc.procedure_date <= '{test_date}'
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) +
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures) as total
        """
        result39a = db.execute(text(query39a)).fetchone()
        print(f"    39a. June 27, exclude broken: {result39a.total}")
        
        # 39b: Include only completed procedures
        query39b = f"""
        WITH hygiene_appointments AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= '2025-01-01'
                AND appointment_date <= '{test_date}'
        ),
        hygiene_procedures AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= '2025-01-01'
                AND ipc.procedure_date <= '{test_date}'
                AND ipc.procedure_status = 2  -- Completed
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) +
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures) as total
        """
        result39b = db.execute(text(query39b)).fetchone()
        print(f"    39b. June 27, completed procedures only: {result39b.total}")
        
        # 39c: Maybe PBN uses June 27 but with a slightly different date range (like June 27 23:59:59)?
        # Or maybe it's "through June 27" meaning including all of June 27
        # Let's test if maybe we need to include procedures/appointments from a few days before
        query39c = f"""
        WITH hygiene_appointments AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= '2025-01-01'
                AND appointment_date < DATE '{test_date}' + INTERVAL '1 day'
        ),
        hygiene_procedures AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= '2025-01-01'
                AND ipc.procedure_date < DATE '{test_date}' + INTERVAL '1 day'
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments) +
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures) as total
        """
        result39c = db.execute(text(query39c)).fetchone()
        print(f"    39c. June 27 (using < June 28): {result39c.total}")
        
        # 39d: Maybe PBN counts patients who had hygiene procedures in appointments (even without hygienist_id)
        query39d = f"""
        WITH all_hygiene_patients AS (
            -- Patients with appointments that have hygienist_id
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= '2025-01-01'
                AND appointment_date <= '{test_date}'
            
            UNION
            
            -- Patients with hygiene procedures in appointments (even without hygienist_id)
            SELECT DISTINCT fa.patient_id
            FROM raw_marts.fact_appointment fa
            INNER JOIN raw_intermediate.int_procedure_complete ipc 
                ON fa.appointment_id = ipc.appointment_id
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND fa.appointment_date >= '2025-01-01'
                AND fa.appointment_date <= '{test_date}'
        )
        SELECT COUNT(DISTINCT patient_id) as unique_patients
        FROM all_hygiene_patients
        """
        result39d = db.execute(text(query39d)).fetchone()
        print(f"    39d. June 27, appointments with hygienist_id OR appointments with hygiene procedures: {result39d.unique_patients}")
        
        if result39a.total == 2073 or result39b.total == 2073 or result39c.total == 2073 or result39d.unique_patients == 2073:
            print(f"    *** FOUND IT! One of the filters gives exactly 2073!")
        
        # Test 40: Maybe PBN counts appointments with hygienist_id + procedures NOT in those appointments
        # But also includes appointments with hygiene procedures (even without hygienist_id)?
        query40 = f"""
        WITH hygiene_appointments_with_hygienist AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= '2025-01-01'
                AND appointment_date <= '{test_date}'
        ),
        appointments_with_hygiene_procs AS (
            SELECT DISTINCT fa.patient_id
            FROM raw_marts.fact_appointment fa
            INNER JOIN raw_intermediate.int_procedure_complete ipc 
                ON fa.appointment_id = ipc.appointment_id
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND fa.appointment_date >= '2025-01-01'
                AND fa.appointment_date <= '{test_date}'
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments_with_hygienist ha 
                    WHERE ha.patient_id = fa.patient_id
                )
        ),
        procedures_not_in_any_appt AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= '2025-01-01'
                AND ipc.procedure_date <= '{test_date}'
                AND ipc.appointment_id IS NULL  -- Procedures not linked to appointments
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments_with_hygienist ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM appointments_with_hygiene_procs ap 
                    WHERE ap.patient_id = ipc.patient_id
                )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments_with_hygienist) +
            (SELECT COUNT(DISTINCT patient_id) FROM appointments_with_hygiene_procs) +
            (SELECT COUNT(DISTINCT patient_id) FROM procedures_not_in_any_appt) as total
        """
        result40 = db.execute(text(query40)).fetchone()
        print(f"    40. June 27, appointments (hygienist_id) + appointments (hygiene procs) + procedures (no appt): {result40.total}")
        
        # Test 41: Maybe the 3 missing patients are from a specific date range or status?
        # Let's check what changed between June 27 (2070) and June 30 (2080)
        query41 = f"""
        WITH patients_june27 AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= '2025-01-01'
                AND appointment_date <= '2025-06-27'
        ),
        patients_june30 AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= '2025-01-01'
                AND appointment_date <= '2025-06-30'
        )
        SELECT 
            COUNT(DISTINCT p30.patient_id) - COUNT(DISTINCT p27.patient_id) as new_patients_june28_30
        FROM patients_june30 p30
        LEFT JOIN patients_june27 p27 ON p30.patient_id = p27.patient_id
        WHERE p27.patient_id IS NULL
        """
        result41 = db.execute(text(query41)).fetchone()
        print(f"    41. New patients between June 28-30 (in appointments only): {result41.new_patients_june28_30}")
        
        # Test 42: Final test - maybe PBN uses June 27 but includes a few more days or uses a different cutoff
        # Let's test June 27 + procedures from June 28-30 that aren't in appointments
        query42 = f"""
        WITH hygiene_appointments_june27 AS (
            SELECT DISTINCT patient_id
            FROM raw_marts.fact_appointment
            WHERE (hygienist_id IS NOT NULL AND hygienist_id != 0)
                AND appointment_date >= '2025-01-01'
                AND appointment_date <= '2025-06-27'
        ),
        hygiene_procedures_june27 AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= '2025-01-01'
                AND ipc.procedure_date <= '2025-06-27'
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments_june27 ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
        ),
        additional_procedures_june28_30 AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180')
                AND ipc.procedure_date >= '2025-06-28'
                AND ipc.procedure_date <= '2025-06-30'
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_appointments_june27 ha 
                    WHERE ha.patient_id = ipc.patient_id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM hygiene_procedures_june27 hp 
                    WHERE hp.patient_id = ipc.patient_id
                )
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_appointments_june27) +
            (SELECT COUNT(DISTINCT patient_id) FROM hygiene_procedures_june27) +
            (SELECT COUNT(DISTINCT patient_id) FROM additional_procedures_june28_30) as total
        """
        result42 = db.execute(text(query42)).fetchone()
        print(f"    42. June 27 appointments+procedures + procedures from June 28-30: {result42.total}")
        
        print("\n" + "="*80)
        print("ANALYSIS: Looking for the combination that gives us 2073 patients")
        print("="*80)
        print(f"\nKey findings:")
        print(f"  - June 27-29: 2070 patients (only 3 off from 2073!)")
        print(f"  - June 30: 2080 patients")
        print(f"  - End of Q2 (June 30): 2080 patients")
        print(f"\nHypothesis: PBN likely uses:")
        print(f"  - Date range: 2025-01-01 to approximately June 27-29, 2025")
        print(f"  - Definition: Appointments with hygienist_id + hygiene procedures (not in appointments)")
        print(f"  - The 3 missing patients might be due to:")
        print(f"    * Timezone differences")
        print(f"    * Different date cutoff logic")
        print(f"    * Excluding certain appointment statuses")
        print(f"    * Including procedures linked to appointments without hygienist_id")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_hygiene_combinations()

