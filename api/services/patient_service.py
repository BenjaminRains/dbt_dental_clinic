# api/services/patient_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
import json

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
        return _convert_patient_row(dict(row._mapping))
    return None