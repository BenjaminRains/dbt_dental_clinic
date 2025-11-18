"""
Follow-up investigations based on priority test results
Key findings:
- Excluding broken/no-show helps slightly (1200 vs 1203, 57.66% vs 57.81%)
- Excluding X-rays makes "Not on Recall" WORSE (32.93% vs 28.58%)
- Scheduled appointments don't help Recall Current %
- Compliance status filtering makes Recall Current % much worse
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

def test_follow_up_investigations():
    db = get_db_session()
    try:
        print("="*80)
        print("FOLLOW-UP INVESTIGATIONS - Based on Priority Test Results")
        print("="*80)
        
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        # ============================================================
        # TEST A: Hyg Pts Re-appntd - Exclude specific appointment types
        # ============================================================
        print("\n" + "="*80)
        print("TEST A: Hyg Pts Re-appntd - Exclude emergency/consultation types")
        print("="*80)
        query_a = """
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
            INNER JOIN raw_intermediate.int_appointment_details iad ON fa2.appointment_id = iad.appointment_id
            WHERE fa2.patient_id = hp.patient_id
                AND fa2.appointment_date > hp.hygiene_date
                AND fa2.appointment_date > CURRENT_DATE
                AND (fa2.is_broken = false OR fa2.is_broken IS NULL)
                AND (fa2.is_no_show = false OR fa2.is_no_show IS NULL)
                AND LOWER(iad.appointment_type_name) NOT LIKE '%emergency%'
                AND LOWER(iad.appointment_type_name) NOT LIKE '%consultation%'
                AND LOWER(iad.appointment_type_name) NOT LIKE '%new patient%'
        )
        """
        result_a = db.execute(text(query_a), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"  Hyg Pts Re-appntd (exclude emergency/consultation): {result_a.reappointed_count} (PBN: 1051, diff: {abs(result_a.reappointed_count - 1051)})")
        
        # ============================================================
        # TEST B: Hyg Pts Re-appntd - Time window (30, 60, 90 days)
        # ============================================================
        print("\n" + "="*80)
        print("TEST B: Hyg Pts Re-appntd - Time windows after hygiene")
        print("="*80)
        for days in [30, 60, 90, 180, 365]:
            query_b = f"""
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
                    AND fa2.appointment_date <= hp.hygiene_date + INTERVAL '{days} days'
                    AND fa2.appointment_date > CURRENT_DATE
                    AND (fa2.is_broken = false OR fa2.is_broken IS NULL)
                    AND (fa2.is_no_show = false OR fa2.is_no_show IS NULL)
            )
            """
            result_b = db.execute(text(query_b), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"  Hyg Pts Re-appntd (within {days} days): {result_b.reappointed_count} (PBN: 1051, diff: {abs(result_b.reappointed_count - 1051)})")
        
        # ============================================================
        # TEST C: Not on Recall - Procedures linked to appointments only
        # ============================================================
        print("\n" + "="*80)
        print("TEST C: Not on Recall - Only procedures linked to appointments")
        print("="*80)
        query_c = """
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
            WHERE pc.procedure_code IN ('D1110', 'D1120', 'D1208', 'D4910', 'D0274', 'D0330', 'D0210', 'D0120', 'D0272')
                AND ipc.procedure_status = 2
                AND ipc.appointment_id IS NOT NULL  -- Only procedures linked to appointments
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall_service pws ON ap.patient_id = pws.patient_id
        """
        result_c = db.execute(text(query_c)).fetchone()
        not_on_recall_c = float(result_c.not_on_recall_percent) if result_c.not_on_recall_percent else 0.0
        print(f"  Not on Recall % (procedures linked to appointments): {not_on_recall_c:.2f}% (PBN: 20%, diff: {abs(not_on_recall_c - 20):.2f}%)")
        
        # ============================================================
        # TEST D: Not on Recall - Procedures from hygiene appointments only
        # ============================================================
        print("\n" + "="*80)
        print("TEST D: Not on Recall - Procedures from hygiene appointments only")
        print("="*80)
        query_d = """
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
            INNER JOIN raw_marts.fact_appointment fa ON ipc.appointment_id = fa.appointment_id
            WHERE pc.procedure_code IN ('D1110', 'D1120', 'D1208', 'D4910', 'D0274', 'D0330', 'D0210', 'D0120', 'D0272')
                AND ipc.procedure_status = 2
                AND fa.hygienist_id IS NOT NULL
                AND fa.hygienist_id != 0  -- Only from hygiene appointments
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall_service pws ON ap.patient_id = pws.patient_id
        """
        result_d = db.execute(text(query_d)).fetchone()
        not_on_recall_d = float(result_d.not_on_recall_percent) if result_d.not_on_recall_percent else 0.0
        print(f"  Not on Recall % (from hygiene appointments): {not_on_recall_d:.2f}% (PBN: 20%, diff: {abs(not_on_recall_d - 20):.2f}%)")
        
        # ============================================================
        # TEST E: Not on Recall - Procedures within past 18 months only
        # ============================================================
        print("\n" + "="*80)
        print("TEST E: Not on Recall - Procedures within past 18 months only")
        print("="*80)
        query_e = """
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
            WHERE pc.procedure_code IN ('D1110', 'D1120', 'D1208', 'D4910', 'D0274', 'D0330', 'D0210', 'D0120', 'D0272')
                AND ipc.procedure_status = 2
                AND ipc.procedure_date >= CURRENT_DATE - INTERVAL '18 months'  -- Only recent procedures
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall_service pws ON ap.patient_id = pws.patient_id
        """
        result_e = db.execute(text(query_e)).fetchone()
        not_on_recall_e = float(result_e.not_on_recall_percent) if result_e.not_on_recall_percent else 0.0
        print(f"  Not on Recall % (procedures within 18 months): {not_on_recall_e:.2f}% (PBN: 20%, diff: {abs(not_on_recall_e - 20):.2f}%)")
        
        # ============================================================
        # TEST F: Recall Current - Include patients seen in last 6 months
        # ============================================================
        print("\n" + "="*80)
        print("TEST F: Recall Current - Include patients seen in last 6 months")
        print("="*80)
        query_f = """
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
        patients_seen_recently AS (
            SELECT DISTINCT fa.patient_id
            FROM raw_marts.fact_appointment fa
            WHERE fa.is_completed = true
                AND fa.appointment_date >= CURRENT_DATE - INTERVAL '6 months'
                AND fa.patient_id IN (SELECT patient_id FROM active_patients)
        ),
        patients_current_on_recall AS (
            SELECT patient_id FROM patients_with_recall
            UNION
            SELECT patient_id FROM patients_seen_recently
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pcr.patient_id IS NOT NULL THEN ap.patient_id END)::numeric as current_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_active,
            (COUNT(DISTINCT CASE WHEN pcr.patient_id IS NOT NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as recall_current_percent
        FROM active_patients ap
        LEFT JOIN patients_current_on_recall pcr ON ap.patient_id = pcr.patient_id
        """
        result_f = db.execute(text(query_f)).fetchone()
        recall_current_f = float(result_f.recall_current_percent) if result_f.recall_current_percent else 0.0
        print(f"  Recall Current % (with recent visits): {recall_current_f:.2f}% (PBN: 53.4%, diff: {abs(recall_current_f - 53.4):.2f}%)")
        
        # ============================================================
        # TEST G: Recall Current - Grace period (30 days past due)
        # ============================================================
        print("\n" + "="*80)
        print("TEST G: Recall Current - Grace period (30 days past due)")
        print("="*80)
        query_g = """
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
                AND (irm.date_due >= CURRENT_DATE - INTERVAL '30 days' OR irm.date_due IS NULL)  -- Grace period
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pcr.patient_id IS NOT NULL THEN ap.patient_id END)::numeric as current_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_active,
            (COUNT(DISTINCT CASE WHEN pcr.patient_id IS NOT NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as recall_current_percent
        FROM active_patients ap
        LEFT JOIN patients_current_on_recall pcr ON ap.patient_id = pcr.patient_id
        """
        result_g = db.execute(text(query_g)).fetchone()
        recall_current_g = float(result_g.recall_current_percent) if result_g.recall_current_percent else 0.0
        print(f"  Recall Current % (30-day grace period): {recall_current_g:.2f}% (PBN: 53.4%, diff: {abs(recall_current_g - 53.4):.2f}%)")
        
        # ============================================================
        # SUMMARY
        # ============================================================
        print("\n" + "="*80)
        print("SUMMARY - Best Results:")
        print("="*80)
        print("\nHyg Pts Re-appntd (target: 1051):")
        print(f"  - Exclude broken/no-show: 1200 (diff: 149)")
        print(f"  - Exclude emergency/consultation: {result_a.reappointed_count} (diff: {abs(result_a.reappointed_count - 1051)})")
        
        print("\nNot on Recall % (target: 20%):")
        print(f"  - Current (all codes): 28.58% (diff: 8.58%)")
        print(f"  - Exclude X-rays: 32.93% (WORSE)")
        print(f"  - Linked to appointments: {not_on_recall_c:.2f}% (diff: {abs(not_on_recall_c - 20):.2f}%)")
        print(f"  - From hygiene appointments: {not_on_recall_d:.2f}% (diff: {abs(not_on_recall_d - 20):.2f}%)")
        print(f"  - Within 18 months: {not_on_recall_e:.2f}% (diff: {abs(not_on_recall_e - 20):.2f}%)")
        
        print("\nRecall Current % (target: 53.4%):")
        print(f"  - Current (has recall record): 42.97% (diff: 10.43%)")
        print(f"  - With scheduled appointments: 42.97% (no change)")
        print(f"  - With recent visits: {recall_current_f:.2f}% (diff: {abs(recall_current_f - 53.4):.2f}%)")
        print(f"  - 30-day grace period: {recall_current_g:.2f}% (diff: {abs(recall_current_g - 53.4):.2f}%)")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_follow_up_investigations()

