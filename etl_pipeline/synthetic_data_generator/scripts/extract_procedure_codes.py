"""
Extract Procedure Codes from Production Database
================================================

Extracts procedure codes from opendental_analytics production database
and saves them to a JSON file for use in synthetic data generation.

Procedure codes (CDT codes) are standardized dental codes, not PII.
"""

import psycopg2
import json
import os
import sys
from typing import List, Dict

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Default production database connection (can be overridden via environment variables)
PROD_DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'opendental_analytics'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', '')
}

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), '../data/procedure_codes.json')


def extract_procedure_codes() -> List[Dict]:
    """Extract procedure codes from production database"""
    print("Connecting to production database...")
    print(f"  Host: {PROD_DB_CONFIG['host']}:{PROD_DB_CONFIG['port']}")
    print(f"  Database: {PROD_DB_CONFIG['database']}")
    
    try:
        conn = psycopg2.connect(**PROD_DB_CONFIG)
        cur = conn.cursor()
        
        query = """
        SELECT 
            "CodeNum",
            "ProcCode",
            "Descript",
            "AbbrDesc",
            "ProcCat",
            "IsHygiene",
            "IsProsth",
            "DateTStamp"
        FROM raw.procedurecode
        ORDER BY "CodeNum";
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        
        procedure_codes = []
        for row in rows:
            procedure_codes.append({
                'CodeNum': row[0],
                'ProcCode': row[1],
                'Descript': row[2],
                'AbbrDesc': row[3] if row[3] else '',
                'ProcCat': row[4] if row[4] else 0,
                'IsHygiene': bool(row[5]) if row[5] is not None else False,
                'IsProsth': bool(row[6]) if row[6] is not None else False,
                'DateTStamp': row[7].isoformat() if row[7] else None
            })
        
        cur.close()
        conn.close()
        
        print(f"✅ Extracted {len(procedure_codes)} procedure codes")
        return procedure_codes
        
    except Exception as e:
        print(f"❌ Error extracting procedure codes: {e}")
        if "password" in str(e).lower():
            print("\nHint: Set POSTGRES_PASSWORD environment variable")
        if "connection" in str(e).lower():
            print("\nHint: Check database connection settings")
        sys.exit(1)


def save_to_json(procedure_codes: List[Dict], output_file: str):
    """Save procedure codes to JSON file"""
    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(procedure_codes, f, indent=2, default=str)
    
    print(f"✅ Saved to: {output_file}")


def main():
    """Main execution"""
    print("=" * 60)
    print("PROCEDURE CODE EXTRACTION")
    print("=" * 60)
    print()
    
    # Extract procedure codes
    procedure_codes = extract_procedure_codes()
    
    # Save to JSON
    save_to_json(procedure_codes, OUTPUT_FILE)
    
    print()
    print("=" * 60)
    print("✅ Extraction complete!")
    print("=" * 60)
    print(f"\nProcedure codes are now available for synthetic data generation.")
    print(f"File: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()

