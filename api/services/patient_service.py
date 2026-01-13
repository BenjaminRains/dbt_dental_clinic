# api/services/patient_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, date
import json
import logging

logger = logging.getLogger(__name__)

def _convert_patient_row(row_dict):
    """Convert patient row data to match Pydantic model expectations"""
    # Convert age_category from string to numeric (PII removal: age -> numeric age_category)
    if 'age' in row_dict and row_dict['age'] is not None:
        age = row_dict['age']
        if age <= 17:
            row_dict['age_category'] = 1  # Minor (0-17)
        elif age <= 34:
            row_dict['age_category'] = 2  # Young Adult (18-34)
        elif age <= 54:
            row_dict['age_category'] = 3  # Middle Aged (35-54)
        else:
            row_dict['age_category'] = 4  # Older Adult (55+)
    elif 'age_category' in row_dict:
        # Convert string age_category to numeric if it exists
        age_cat_str = str(row_dict.get('age_category', '')).lower()
        if 'minor' in age_cat_str:
            row_dict['age_category'] = 1
        elif 'young' in age_cat_str or (age_cat_str and '18' in age_cat_str):
            row_dict['age_category'] = 2
        elif 'middle' in age_cat_str or (age_cat_str and '35' in age_cat_str):
            row_dict['age_category'] = 3
        elif 'senior' in age_cat_str or 'older' in age_cat_str or (age_cat_str and '55' in age_cat_str):
            row_dict['age_category'] = 4
        else:
            row_dict['age_category'] = None
    
    # Remove age field (PII)
    if 'age' in row_dict:
        del row_dict['age']
    
    # Convert datetime to date for date-only fields
    # SQLAlchemy may return DATE columns as datetime objects
    if 'first_visit_date' in row_dict and row_dict['first_visit_date'] is not None:
        if isinstance(row_dict['first_visit_date'], datetime):
            row_dict['first_visit_date'] = row_dict['first_visit_date'].date()
        elif isinstance(row_dict['first_visit_date'], str):
            # Handle string dates from database
            try:
                dt = datetime.fromisoformat(row_dict['first_visit_date'].replace('Z', '+00:00'))
                row_dict['first_visit_date'] = dt.date()
            except (ValueError, AttributeError):
                pass
    
    if 'admit_date' in row_dict and row_dict['admit_date'] is not None:
        if isinstance(row_dict['admit_date'], datetime):
            row_dict['admit_date'] = row_dict['admit_date'].date()
        elif isinstance(row_dict['admit_date'], str):
            # Handle string dates from database
            try:
                dt = datetime.fromisoformat(row_dict['admit_date'].replace('Z', '+00:00'))
                row_dict['admit_date'] = dt.date()
            except (ValueError, AttributeError):
                pass
    
    # Ensure age_category is an integer, not a string
    if 'age_category' in row_dict and row_dict['age_category'] is not None:
        try:
            row_dict['age_category'] = int(row_dict['age_category'])
        except (ValueError, TypeError):
            # If conversion fails, set to None
            row_dict['age_category'] = None
    
    # Convert linked_patient_ids to list
    if 'linked_patient_ids' in row_dict:
        if row_dict['linked_patient_ids'] is None:
            row_dict['linked_patient_ids'] = None
        elif isinstance(row_dict['linked_patient_ids'], list):
            # Already a list, ensure items are strings
            row_dict['linked_patient_ids'] = [str(item) for item in row_dict['linked_patient_ids']]
        elif isinstance(row_dict['linked_patient_ids'], str):
            # Try to parse as JSON first, then fall back to single value
            try:
                parsed = json.loads(row_dict['linked_patient_ids'])
                if isinstance(parsed, list):
                    row_dict['linked_patient_ids'] = [str(item) for item in parsed]
                else:
                    row_dict['linked_patient_ids'] = [str(parsed)]
            except (json.JSONDecodeError, TypeError):
                # Single string value, make it a list
                row_dict['linked_patient_ids'] = [row_dict['linked_patient_ids']]
        else:
            # Integer or other type - convert to single-item list of strings
            row_dict['linked_patient_ids'] = [str(row_dict['linked_patient_ids'])]
    
    # Convert link_types to list
    if 'link_types' in row_dict:
        if row_dict['link_types'] is None:
            row_dict['link_types'] = None
        elif isinstance(row_dict['link_types'], list):
            # Already a list, ensure items are strings
            row_dict['link_types'] = [str(item) for item in row_dict['link_types']]
        elif isinstance(row_dict['link_types'], str):
            # Try to parse as JSON first, then fall back to single value
            try:
                parsed = json.loads(row_dict['link_types'])
                if isinstance(parsed, list):
                    row_dict['link_types'] = [str(item) for item in parsed]
                else:
                    row_dict['link_types'] = [str(parsed)]
            except (json.JSONDecodeError, TypeError):
                # Single string value, make it a list
                row_dict['link_types'] = [row_dict['link_types']]
        else:
            # Integer or other type - convert to single-item list of strings
            row_dict['link_types'] = [str(row_dict['link_types'])]
    
    return row_dict

def get_patients(db: Session, skip: int = 0, limit: int = 100):
    # Get total count first
    count_query = text("SELECT COUNT(*) as total FROM raw_marts.dim_patient")
    count_result = db.execute(count_query)
    total = count_result.fetchone().total
    
    # Get paginated results
    query = text("""
        SELECT * FROM raw_marts.dim_patient
        LIMIT :limit OFFSET :skip
    """)
    result = db.execute(query, {"skip": skip, "limit": limit})
    patients = result.fetchall()
    
    # Convert rows to dicts and fix data types
    # The _convert_patient_row function handles date conversions
    patient_list = [_convert_patient_row(dict(row._mapping)) for row in patients]
    
    return {
        "patients": patient_list,
        "total": total
    }

def get_patient_by_id(db: Session, patient_id: int):
    query = text("""
        SELECT * FROM raw_marts.dim_patient
        WHERE patient_id = :patient_id
    """)
    result = db.execute(query, {"patient_id": patient_id})
    row = result.fetchone()
    if row:
        # The _convert_patient_row function handles date conversions
        return _convert_patient_row(dict(row._mapping))
    return None

def get_top_patient_balances(db: Session, limit: int = 10):
    """Get top N patients by total balance from AR summary"""
    query = text("""
        WITH latest_snapshots AS (
            SELECT DISTINCT ON (mas.patient_id::integer)
                mas.patient_id::integer as patient_id,
                mas.total_balance,
                mas.balance_0_30_days,
                mas.balance_31_60_days,
                mas.balance_61_90_days,
                mas.balance_over_90_days,
                mas.aging_risk_category,
                mas.days_since_last_payment,
                mas.payment_recency,
                mas.snapshot_date
            FROM raw_marts.mart_ar_summary mas
            WHERE mas.total_balance > 0
            ORDER BY mas.patient_id::integer, mas.snapshot_date DESC
        )
        SELECT 
            ls.patient_id::integer as patient_id,
            ls.total_balance,
            ls.balance_0_30_days,
            ls.balance_31_60_days,
            ls.balance_61_90_days,
            ls.balance_over_90_days,
            ls.aging_risk_category,
            ls.days_since_last_payment,
            ls.payment_recency
        FROM latest_snapshots ls
        ORDER BY ls.total_balance DESC
        LIMIT :limit
    """)
    result = db.execute(query, {"limit": limit})
    rows = result.fetchall()
    
    # Convert rows to dicts and ensure patient_id is integer (safety net for type issues)
    converted_rows = []
    for row in rows:
        row_dict = dict(row._mapping)
        # Ensure patient_id is integer (handle case where DB returns string)
        if 'patient_id' in row_dict and row_dict['patient_id'] is not None:
            # Log the type before conversion for debugging
            original_type = type(row_dict['patient_id']).__name__
            original_value = row_dict['patient_id']
            
            try:
                row_dict['patient_id'] = int(row_dict['patient_id'])
                # Log if conversion was needed
                if original_type != 'int':
                    logger.info(f"Converted patient_id from {original_type} to int: {original_value} -> {row_dict['patient_id']}")
            except (ValueError, TypeError) as e:
                # Log warning but skip invalid rows
                logger.error(f"Could not convert patient_id to int: {original_value} (type: {original_type}), error: {e}")
                continue
        converted_rows.append(row_dict)
    
    logger.info(f"Returning {len(converted_rows)} patient balances")
    return converted_rows