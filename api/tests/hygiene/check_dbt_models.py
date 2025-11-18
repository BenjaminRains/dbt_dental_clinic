"""
Diagnostic script to check dbt model data for hygiene appointments and procedures.
This helps identify if we're missing any data or using incorrect filters.
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

def check_dbt_models():
    db = get_db_session()
    try:
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        print("="*70)
        print("CHECKING DBT MODELS FOR HYGIENE DATA")
        print(f"Date Range: {start_date} to {end_date}")
        print("="*70)
        
        # 1. Check int_procedure_complete for hygiene procedures
        print("\n1. int_procedure_complete (hygiene procedures):")
        query1 = """
        SELECT 
            COUNT(*) as total_procedures,
            COUNT(DISTINCT patient_id) as unique_patients,
            COUNT(DISTINCT appointment_id) as unique_appointments,
            COUNT(CASE WHEN is_hygiene = true THEN 1 END) as hygiene_procedures,
            COUNT(CASE WHEN is_hygiene = false THEN 1 END) as non_hygiene_procedures,
            COUNT(CASE WHEN is_hygiene IS NULL THEN 1 END) as null_hygiene
        FROM raw_intermediate.int_procedure_complete
        WHERE procedure_date >= :start_date
            AND procedure_date <= :end_date
        """
        result1 = db.execute(text(query1), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"   Total procedures: {result1.total_procedures}")
        print(f"   Unique patients: {result1.unique_patients}")
        print(f"   Unique appointments: {result1.unique_appointments}")
        print(f"   Hygiene procedures (is_hygiene = true): {result1.hygiene_procedures}")
        print(f"   Non-hygiene procedures (is_hygiene = false): {result1.non_hygiene_procedures}")
        print(f"   Null hygiene flag: {result1.null_hygiene}")
        
        # 2. Check fact_appointment for hygiene appointments
        print("\n2. fact_appointment (hygiene appointments):")
        query2 = """
        SELECT 
            COUNT(*) as total_appointments,
            COUNT(DISTINCT patient_id) as unique_patients,
            COUNT(CASE WHEN is_hygiene_appointment = true THEN 1 END) as is_hygiene_true,
            COUNT(CASE WHEN is_hygiene_appointment = false THEN 1 END) as is_hygiene_false,
            COUNT(CASE WHEN is_hygiene_appointment IS NULL THEN 1 END) as is_hygiene_null,
            COUNT(CASE WHEN hygienist_id IS NOT NULL AND hygienist_id != 0 THEN 1 END) as has_hygienist,
            COUNT(CASE WHEN (is_hygiene_appointment = true OR hygienist_id IS NOT NULL) THEN 1 END) as hygiene_by_either,
            COUNT(CASE WHEN (is_hygiene_appointment = true OR hygienist_id IS NOT NULL) AND is_completed = true THEN 1 END) as hygiene_completed,
            COUNT(CASE WHEN (is_hygiene_appointment = true OR hygienist_id IS NOT NULL) AND is_completed = false THEN 1 END) as hygiene_not_completed,
            COUNT(CASE WHEN (is_hygiene_appointment = true OR hygienist_id IS NOT NULL) AND is_completed IS NULL THEN 1 END) as hygiene_completed_null
        FROM raw_marts.fact_appointment
        WHERE appointment_date >= :start_date
            AND appointment_date <= :end_date
        """
        result2 = db.execute(text(query2), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"   Total appointments: {result2.total_appointments}")
        print(f"   Unique patients: {result2.unique_patients}")
        print(f"   is_hygiene_appointment = true: {result2.is_hygiene_true}")
        print(f"   is_hygiene_appointment = false: {result2.is_hygiene_false}")
        print(f"   is_hygiene_appointment IS NULL: {result2.is_hygiene_null}")
        print(f"   has_hygienist_id (not null and != 0): {result2.has_hygienist}")
        print(f"   Hygiene by either flag (is_hygiene_appointment OR hygienist_id): {result2.hygiene_by_either}")
        print(f"   Hygiene + completed: {result2.hygiene_completed}")
        print(f"   Hygiene + not completed: {result2.hygiene_not_completed}")
        print(f"   Hygiene + completed IS NULL: {result2.hygiene_completed_null}")
        
        # 3. Check overlap between procedures and appointments
        print("\n3. Overlap between procedures and appointments:")
        query3 = """
        WITH hygiene_procedures AS (
            SELECT DISTINCT patient_id, procedure_date
            FROM raw_intermediate.int_procedure_complete
            WHERE is_hygiene = true
                AND procedure_date >= :start_date
                AND procedure_date <= :end_date
        ),
        hygiene_appointments AS (
            SELECT DISTINCT patient_id, appointment_date
            FROM raw_marts.fact_appointment
            WHERE (is_hygiene_appointment = true OR hygienist_id IS NOT NULL)
                AND appointment_date >= :start_date
                AND appointment_date <= :end_date
        )
        SELECT 
            COUNT(DISTINCT hp.patient_id) as patients_in_procedures,
            COUNT(DISTINCT ha.patient_id) as patients_in_appointments,
            COUNT(DISTINCT CASE WHEN ha.patient_id IS NOT NULL THEN hp.patient_id END) as patients_in_both,
            COUNT(DISTINCT CASE WHEN ha.patient_id IS NULL THEN hp.patient_id END) as patients_only_procedures,
            COUNT(DISTINCT CASE WHEN hp.patient_id IS NULL THEN ha.patient_id END) as patients_only_appointments
        FROM hygiene_procedures hp
        FULL OUTER JOIN hygiene_appointments ha 
            ON hp.patient_id = ha.patient_id
            AND hp.procedure_date = ha.appointment_date
        """
        result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"   Patients in procedures: {result3.patients_in_procedures}")
        print(f"   Patients in appointments: {result3.patients_in_appointments}")
        print(f"   Patients in both: {result3.patients_in_both}")
        print(f"   Patients only in procedures: {result3.patients_only_procedures}")
        print(f"   Patients only in appointments: {result3.patients_only_appointments}")
        
        # 4. Check appointment status distribution for hygiene appointments
        print("\n4. Appointment status for hygiene appointments:")
        query4 = """
        SELECT 
            appointment_status,
            COUNT(*) as count,
            COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE (is_hygiene_appointment = true OR hygienist_id IS NOT NULL)
            AND appointment_date >= :start_date
            AND appointment_date <= :end_date
        GROUP BY appointment_status
        ORDER BY count DESC
        """
        result4 = db.execute(text(query4), {"start_date": start_date, "end_date": end_date}).fetchall()
        for row in result4:
            print(f"   Status '{row.appointment_status}': {row.count} appointments, {row.unique_patients} patients")
        
        # 5. Check if PBN might be counting ALL hygiene appointments (not just completed)
        print("\n5. Hygiene appointments by completion status:")
        query5 = """
        SELECT 
            is_completed,
            COUNT(*) as appointment_count,
            COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE (is_hygiene_appointment = true OR hygienist_id IS NOT NULL)
            AND appointment_date >= :start_date
            AND appointment_date <= :end_date
        GROUP BY is_completed
        ORDER BY is_completed DESC NULLS LAST
        """
        result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchall()
        for row in result5:
            status = "True" if row.is_completed else "False" if row.is_completed is False else "NULL"
            print(f"   is_completed = {status}: {row.appointment_count} appointments, {row.unique_patients} patients")
        
        # 6. Check if there are appointments with hygienist_id but is_hygiene_appointment = false
        print("\n6. Hygiene identification patterns:")
        query6 = """
        SELECT 
            CASE 
                WHEN is_hygiene_appointment = true AND (hygienist_id IS NOT NULL AND hygienist_id != 0) THEN 'Both flags'
                WHEN is_hygiene_appointment = true AND (hygienist_id IS NULL OR hygienist_id = 0) THEN 'Only is_hygiene_appointment'
                WHEN is_hygiene_appointment = false AND (hygienist_id IS NOT NULL AND hygienist_id != 0) THEN 'Only hygienist_id'
                ELSE 'Neither'
            END as identification_pattern,
            COUNT(*) as appointment_count,
            COUNT(DISTINCT patient_id) as unique_patients
        FROM raw_marts.fact_appointment
        WHERE appointment_date >= :start_date
            AND appointment_date <= :end_date
        GROUP BY identification_pattern
        ORDER BY appointment_count DESC
        """
        result6 = db.execute(text(query6), {"start_date": start_date, "end_date": end_date}).fetchall()
        for row in result6:
            print(f"   {row.identification_pattern}: {row.appointment_count} appointments, {row.unique_patients} patients")
        
    finally:
        db.close()

if __name__ == "__main__":
    check_dbt_models()

