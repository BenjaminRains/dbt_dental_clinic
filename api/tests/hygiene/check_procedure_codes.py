"""
Check if PBN might be using procedure codes to identify hygiene visits
instead of just appointments with hygienist_id.
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

def check_procedure_codes():
    db = get_db_session()
    try:
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        print("="*70)
        print("CHECKING PROCEDURE CODES FOR HYGIENE IDENTIFICATION")
        print(f"Date Range: {start_date} to {end_date}")
        print("="*70)
        
        # 1. Check what procedure codes are marked as hygiene in the procedure codes table
        print("\n1. Procedure codes marked as hygiene:")
        query1 = """
        SELECT 
            COUNT(*) as total_codes,
            COUNT(CASE WHEN is_hygiene = true THEN 1 END) as hygiene_codes,
            COUNT(CASE WHEN is_hygiene = false THEN 1 END) as non_hygiene_codes,
            COUNT(CASE WHEN is_hygiene IS NULL THEN 1 END) as null_hygiene
        FROM raw_marts.dim_procedure
        """
        result1 = db.execute(text(query1)).fetchone()
        print(f"   Total procedure codes: {result1.total_codes}")
        print(f"   Hygiene codes (is_hygiene = true): {result1.hygiene_codes}")
        print(f"   Non-hygiene codes: {result1.non_hygiene_codes}")
        print(f"   Null hygiene flag: {result1.null_hygiene}")
        
        # 2. Check procedures with hygiene codes in the date range
        print("\n2. Procedures with hygiene codes (regardless of is_hygiene flag):")
        query2 = """
        WITH hygiene_procedure_codes AS (
            SELECT DISTINCT procedure_code_id
            FROM raw_marts.dim_procedure
            WHERE is_hygiene = true
        )
        SELECT 
            COUNT(*) as total_procedures,
            COUNT(DISTINCT pc.patient_id) as unique_patients,
            COUNT(DISTINCT pc.appointment_id) as unique_appointments
        FROM raw_intermediate.int_procedure_complete pc
        INNER JOIN hygiene_procedure_codes hpc ON pc.procedure_code_id = hpc.procedure_code_id
        WHERE pc.procedure_date >= :start_date
            AND pc.procedure_date <= :end_date
        """
        result2 = db.execute(text(query2), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"   Total procedures: {result2.total_procedures}")
        print(f"   Unique patients: {result2.unique_patients}")
        print(f"   Unique appointments: {result2.unique_appointments}")
        
        # 3. Check common hygiene procedure codes (D0120, D0150, D1110, D1120, etc.)
        print("\n3. Common hygiene procedure codes and their usage:")
        query3 = """
        SELECT 
            pc.procedure_code,
            pc.description,
            pc.is_hygiene,
            COUNT(*) as procedure_count,
            COUNT(DISTINCT ipc.patient_id) as unique_patients
        FROM raw_intermediate.int_procedure_complete ipc
        INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
        WHERE ipc.procedure_date >= :start_date
            AND ipc.procedure_date <= :end_date
            AND pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0210', 'D0272', 'D0273', 'D0274', 'D0330', 'D0340')
        GROUP BY pc.procedure_code, pc.description, pc.is_hygiene
        ORDER BY procedure_count DESC
        """
        result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchall()
        print(f"   Found {len(result3)} hygiene-related procedure codes:")
        for row in result3:
            hygiene_flag = "YES" if row.is_hygiene else "NO" if row.is_hygiene is False else "NULL"
            print(f"   {row.procedure_code} ({row.description}): {row.procedure_count} procedures, {row.unique_patients} patients, is_hygiene={hygiene_flag}")
        
        # 4. Check if combining appointments + procedures gets us closer to 2073
        # Since is_hygiene flag is all false, try using common hygiene procedure codes
        print("\n4. Combining appointments (by hygienist_id) + procedures (by hygiene procedure codes):")
        query4a = """
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
        SELECT 
            COUNT(DISTINCT COALESCE(ha.patient_id, hp.patient_id)) as total_unique_patients,
            COUNT(DISTINCT ha.patient_id) as patients_in_appointments,
            COUNT(DISTINCT hp.patient_id) as patients_in_procedures,
            COUNT(DISTINCT CASE WHEN ha.patient_id IS NOT NULL AND hp.patient_id IS NOT NULL THEN ha.patient_id END) as patients_in_both
        FROM hygiene_appointments ha
        FULL OUTER JOIN hygiene_procedures hp ON ha.patient_id = hp.patient_id
        """
        result4a = db.execute(text(query4a), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"   Total unique patients (appointments OR hygiene procedure codes): {result4a.total_unique_patients} (PBN: 2073)")
        print(f"   Patients in appointments only: {result4a.patients_in_appointments}")
        print(f"   Patients in procedures only: {result4a.patients_in_procedures}")
        print(f"   Patients in both: {result4a.patients_in_both}")
        
        # 5. Check if maybe PBN counts ALL appointments (not just those with hygienist_id)
        print("\n5. All appointments that might be hygiene (checking appointment types):")
        query5 = """
        SELECT 
            iad.appointment_type_name,
            COUNT(*) as appointment_count,
            COUNT(DISTINCT fa.patient_id) as unique_patients,
            COUNT(CASE WHEN fa.hygienist_id IS NOT NULL AND fa.hygienist_id != 0 THEN 1 END) as has_hygienist
        FROM raw_marts.fact_appointment fa
        LEFT JOIN raw_intermediate.int_appointment_details iad ON fa.appointment_id = iad.appointment_id
        WHERE fa.appointment_date >= :start_date
            AND fa.appointment_date <= :end_date
        GROUP BY iad.appointment_type_name
        ORDER BY appointment_count DESC
        LIMIT 20
        """
        result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchall()
        print(f"   Top appointment types:")
        for row in result5:
            print(f"   {row.appointment_type_name or 'NULL'}: {row.appointment_count} appointments, {row.unique_patients} patients, {row.has_hygienist} with hygienist")
        
    finally:
        db.close()

if __name__ == "__main__":
    check_procedure_codes()

