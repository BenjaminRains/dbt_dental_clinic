"""
Priority investigations to match PBN's numbers
Tests the most promising hypotheses first
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

def test_priority_investigations():
    db = get_db_session()
    try:
        print("="*80)
        print("PRIORITY INVESTIGATIONS - Most Promising Tests")
        print("="*80)
        
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        # ============================================================
        # TEST 1: Hyg Pts Re-appntd - Exclude broken/no-show appointments
        # ============================================================
        print("\n" + "="*80)
        print("TEST 1: Hyg Pts Re-appntd - Exclude broken/no-show appointments")
        print("="*80)
        query1 = """
        WITH hygiene_patients AS (
            SELECT DISTINCT ipc.patient_id, ipc.procedure_date as hygiene_date
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0272', 'D0274', 'D0330')
                AND ipc.procedure_status = 2
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
        )
        SELECT COALESCE(COUNT(DISTINCT hp.patient_id)::integer, 0) as reappointed_count
        FROM hygiene_patients hp
        WHERE EXISTS (
            SELECT 1 
            FROM raw_marts.fact_appointment fa2
            WHERE fa2.patient_id = hp.patient_id
                AND fa2.appointment_date > hp.hygiene_date
                AND fa2.appointment_date > CURRENT_DATE
                AND (fa2.is_broken = false OR fa2.is_broken IS NULL)  -- Exclude broken
                AND (fa2.is_no_show = false OR fa2.is_no_show IS NULL)  -- Exclude no-show
        )
        """
        result1 = db.execute(text(query1), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"  Hyg Pts Re-appntd (exclude broken/no-show): {result1.reappointed_count} (PBN: 1051, diff: {abs(result1.reappointed_count - 1051)})")
        
        # ============================================================
        # TEST 2: Not on Recall - Exclude X-rays
        # ============================================================
        print("\n" + "="*80)
        print("TEST 2: Not on Recall - Exclude X-rays from recall codes")
        print("="*80)
        query2 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall_service AS (
            SELECT DISTINCT ipc.patient_id
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D1110', 'D1120', 'D1208', 'D4910', 'D0120')  -- Exclude X-rays
                AND ipc.procedure_status = 2
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall_service pws ON ap.patient_id = pws.patient_id
        """
        result2 = db.execute(text(query2)).fetchone()
        not_on_recall_2 = float(result2.not_on_recall_percent) if result2.not_on_recall_percent else 0.0
        print(f"  Not on Recall % (exclude X-rays): {not_on_recall_2:.2f}% (PBN: 20%, diff: {abs(not_on_recall_2 - 20):.2f}%)")
        
        # ============================================================
        # TEST 3: Recall Current - Include scheduled appointments
        # ============================================================
        print("\n" + "="*80)
        print("TEST 3: Recall Current - Include patients with scheduled appointments")
        print("="*80)
        query3 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall AS (
            SELECT DISTINCT irm.patient_id
            FROM raw_intermediate.int_recall_management irm
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        ),
        patients_with_scheduled_appt AS (
            SELECT DISTINCT fa.patient_id
            FROM raw_marts.fact_appointment fa
            WHERE fa.appointment_date > CURRENT_DATE
                AND fa.patient_id IN (SELECT patient_id FROM patients_with_recall)
        ),
        patients_current_on_recall AS (
            SELECT patient_id FROM patients_with_recall
            UNION
            SELECT patient_id FROM patients_with_scheduled_appt
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pcr.patient_id IS NOT NULL THEN ap.patient_id END)::numeric as current_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_active,
            (COUNT(DISTINCT CASE WHEN pcr.patient_id IS NOT NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as recall_current_percent
        FROM active_patients ap
        LEFT JOIN patients_current_on_recall pcr ON ap.patient_id = pcr.patient_id
        """
        result3 = db.execute(text(query3)).fetchone()
        recall_current_3 = float(result3.recall_current_percent) if result3.recall_current_percent else 0.0
        print(f"  Recall Current % (with scheduled appointments): {recall_current_3:.2f}% (PBN: 53.4%, diff: {abs(recall_current_3 - 53.4):.2f}%)")
        
        # ============================================================
        # TEST 4: Recall Current - Include compliance_status = 'Compliant' or 'Due Soon'
        # ============================================================
        print("\n" + "="*80)
        print("TEST 4: Recall Current - Include compliance_status = 'Compliant' or 'Due Soon'")
        print("="*80)
        query4 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_current_on_recall AS (
            SELECT DISTINCT irm.patient_id
            FROM raw_intermediate.int_recall_management irm
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
                AND irm.compliance_status IN ('Compliant', 'Due Soon')
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pcr.patient_id IS NOT NULL THEN ap.patient_id END)::numeric as current_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_active,
            (COUNT(DISTINCT CASE WHEN pcr.patient_id IS NOT NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as recall_current_percent
        FROM active_patients ap
        LEFT JOIN patients_current_on_recall pcr ON ap.patient_id = pcr.patient_id
        """
        result4 = db.execute(text(query4)).fetchone()
        recall_current_4 = float(result4.recall_current_percent) if result4.recall_current_percent else 0.0
        print(f"  Recall Current % (Compliant or Due Soon): {recall_current_4:.2f}% (PBN: 53.4%, diff: {abs(recall_current_4 - 53.4):.2f}%)")
        
        # ============================================================
        # TEST 5: Hyg Pre-Appointment % - Exclude broken/no-show
        # ============================================================
        print("\n" + "="*80)
        print("TEST 5: Hyg Pre-Appointment % - Exclude broken/no-show appointments")
        print("="*80)
        query5 = """
        WITH hygiene_patients AS (
            SELECT DISTINCT ipc.patient_id, ipc.procedure_date as hygiene_date
            FROM raw_intermediate.int_procedure_complete ipc
            INNER JOIN raw_marts.dim_procedure pc ON ipc.procedure_code_id = pc.procedure_code_id
            WHERE pc.procedure_code IN ('D0120', 'D0150', 'D1110', 'D1120', 'D0180', 'D0272', 'D0274', 'D0330')
                AND ipc.procedure_status = 2
                AND ipc.procedure_date >= :start_date
                AND ipc.procedure_date <= :end_date
        ),
        patients_with_next_appt AS (
            SELECT DISTINCT hp.patient_id
            FROM hygiene_patients hp
            WHERE EXISTS (
                SELECT 1 
                FROM raw_marts.fact_appointment fa2
                WHERE fa2.patient_id = hp.patient_id
                    AND fa2.appointment_date > hp.hygiene_date
                    AND fa2.appointment_date > CURRENT_DATE
                    AND (fa2.is_broken = false OR fa2.is_broken IS NULL)
                    AND (fa2.is_no_show = false OR fa2.is_no_show IS NULL)
            )
        )
        SELECT 
            COALESCE(COUNT(DISTINCT pwna.patient_id)::numeric, 0) as reappointed_count,
            COALESCE(COUNT(DISTINCT hp.patient_id)::numeric, 0) as total_hygiene,
            CASE 
                WHEN COUNT(DISTINCT hp.patient_id) > 0
                THEN (COUNT(DISTINCT pwna.patient_id)::numeric / NULLIF(COUNT(DISTINCT hp.patient_id)::numeric, 0)) * 100
                ELSE 0
            END as hyg_pre_appointment_percent
        FROM hygiene_patients hp
        LEFT JOIN patients_with_next_appt pwna ON hp.patient_id = pwna.patient_id
        """
        result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchone()
        hyg_pre_appt_5 = float(result5.hyg_pre_appointment_percent) if result5.hyg_pre_appointment_percent else 0.0
        print(f"  Hyg Pre-Appointment % (exclude broken/no-show): {hyg_pre_appt_5:.2f}% (PBN: 50.7%, diff: {abs(hyg_pre_appt_5 - 50.7):.2f}%)")
        
        # ============================================================
        # SUMMARY
        # ============================================================
        print("\n" + "="*80)
        print("SUMMARY - Closest Matches:")
        print("="*80)
        results = [
            ("Test 1 (Hyg Pts Re-appntd, exclude broken/no-show)", result1.reappointed_count, 1051),
            ("Test 2 (Not on Recall, exclude X-rays)", not_on_recall_2, 20),
            ("Test 3 (Recall Current, with scheduled appts)", recall_current_3, 53.4),
            ("Test 4 (Recall Current, Compliant/Due Soon)", recall_current_4, 53.4),
            ("Test 5 (Hyg Pre-Appt %, exclude broken/no-show)", hyg_pre_appt_5, 50.7),
        ]
        
        # Sort by difference from target
        for name, value, target in sorted(results, key=lambda x: abs(float(x[1]) - x[2])):
            if name.startswith("Test 1"):
                diff = abs(float(value) - target)
                print(f"  {name}: {value} (target: {target}, diff: {diff})")
            else:
                diff = abs(float(value) - target)
                print(f"  {name}: {float(value):.2f}% (target: {target}%, diff: {diff:.2f}%)")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_priority_investigations()

