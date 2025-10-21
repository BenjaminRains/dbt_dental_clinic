#!/usr/bin/env python3
"""
Audit Table Row Counts - Compare Replication vs Analytics

This script compares row counts between the MySQL replication database
and PostgreSQL analytics database to identify tables with data mismatches
that may be affected by the incremental loading stale state bug.

Usage:
    cd etl_pipeline
    pipenv run python scripts/audit_table_row_counts.py

Output:
    Tables with >10% row count mismatch, sorted by severity
"""

import sys
from pathlib import Path
from sqlalchemy import text
from tabulate import tabulate

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl_pipeline.config import get_settings, DatabaseType
from etl_pipeline.core.connections import ConnectionFactory

def get_analytics_row_count(analytics_engine, table_name):
    """Get row count from analytics database."""
    try:
        with analytics_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM raw.{table_name}"))
            return result.scalar() or 0
    except Exception as e:
        print(f"  Error getting analytics count for {table_name}: {e}")
        return None

def get_replication_row_count(replication_engine, table_name, replication_db):
    """Get row count from replication database."""
    try:
        with replication_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`"))
            return result.scalar() or 0
    except Exception as e:
        print(f"  Error getting replication count for {table_name}: {e}")
        return None

def main():
    """Main audit function."""
    print("=" * 80)
    print("TABLE ROW COUNT AUDIT: Replication vs Analytics")
    print("=" * 80)
    print()
    
    # Get settings and database connections
    print("Initializing connections...")
    settings = get_settings()
    replication_engine = ConnectionFactory.get_replication_connection(settings)
    analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
    
    # Get replication database name
    replication_config = settings.get_database_config(DatabaseType.REPLICATION)
    replication_db = replication_config.get('database', 'opendental_replication')
    
    print(f"Replication DB: {replication_db}")
    print(f"Analytics Schema: raw")
    print()
    
    # Get list of tables from analytics
    print("Discovering tables in analytics...")
    with analytics_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'raw' 
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN ('etl_load_status', 'etl_transform_status')
            ORDER BY table_name
        """))
        analytics_tables = [row[0] for row in result]
    
    print(f"Found {len(analytics_tables)} tables in analytics")
    print()
    
    # Compare row counts
    print("Comparing row counts...")
    mismatches = []
    all_results = []
    
    for i, table_name in enumerate(analytics_tables, 1):
        if i % 20 == 0:
            print(f"  Progress: {i}/{len(analytics_tables)} tables checked...")
        
        analytics_count = get_analytics_row_count(analytics_engine, table_name)
        replication_count = get_replication_row_count(replication_engine, table_name, replication_db)
        
        if analytics_count is None or replication_count is None:
            continue
        
        # Calculate mismatch
        missing_rows = replication_count - analytics_count
        if replication_count > 0:
            load_percentage = round(100.0 * analytics_count / replication_count, 2)
        else:
            load_percentage = 100.0 if analytics_count == 0 else 0.0
        
        # Store all results
        all_results.append({
            'table_name': table_name,
            'replication': replication_count,
            'analytics': analytics_count,
            'missing': missing_rows,
            'pct_loaded': load_percentage
        })
        
        # Flag significant mismatches (>10% missing OR any rows missing if replication < 100)
        is_significant_mismatch = False
        if missing_rows > 0:
            if replication_count < 100:
                # For small tables, any mismatch is significant
                is_significant_mismatch = True
            elif missing_rows > replication_count * 0.1:
                # For larger tables, >10% missing is significant
                is_significant_mismatch = True
        
        if is_significant_mismatch:
            mismatches.append({
                'table_name': table_name,
                'replication': replication_count,
                'analytics': analytics_count,
                'missing': missing_rows,
                'pct_loaded': load_percentage,
                'severity': 'CRITICAL' if load_percentage < 50 else 'HIGH' if load_percentage < 90 else 'MEDIUM'
            })
    
    print(f"  Completed: {len(analytics_tables)} tables checked")
    print()
    
    # Sort by severity and missing rows
    mismatches.sort(key=lambda x: (
        0 if x['severity'] == 'CRITICAL' else 1 if x['severity'] == 'HIGH' else 2,
        -x['missing']
    ))
    
    # Display results
    print("=" * 80)
    print(f"MISMATCHED TABLES: {len(mismatches)} tables with row count discrepancies")
    print("=" * 80)
    print()
    
    if mismatches:
        print(tabulate(mismatches, headers='keys', tablefmt='grid', floatfmt='.2f'))
        print()
        print("SEVERITY BREAKDOWN:")
        critical = sum(1 for m in mismatches if m['severity'] == 'CRITICAL')
        high = sum(1 for m in mismatches if m['severity'] == 'HIGH')
        medium = sum(1 for m in mismatches if m['severity'] == 'MEDIUM')
        print(f"  CRITICAL (<50% loaded): {critical} tables")
        print(f"  HIGH (50-90% loaded): {high} tables")
        print(f"  MEDIUM (90-100% loaded): {medium} tables")
        print()
        
        # Calculate total missing rows
        total_missing = sum(m['missing'] for m in mismatches)
        print(f"TOTAL MISSING ROWS ACROSS ALL TABLES: {total_missing:,}")
    else:
        print("✅ No row count mismatches found! All tables are in sync.")
    
    print()
    print("=" * 80)
    
    # Save detailed results to file
    output_file = Path(__file__).parent.parent / 'logs' / 'table_audit_results.txt'
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w') as f:
        f.write("TABLE ROW COUNT AUDIT RESULTS\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Total tables checked: {len(analytics_tables)}\n")
        f.write(f"Tables with mismatches: {len(mismatches)}\n\n")
        
        if mismatches:
            f.write("MISMATCHED TABLES:\n")
            f.write(tabulate(mismatches, headers='keys', tablefmt='grid', floatfmt='.2f'))
            f.write("\n\n")
        
        f.write("\nALL TABLES (Complete List):\n")
        all_results.sort(key=lambda x: -x['missing'])
        f.write(tabulate(all_results, headers='keys', tablefmt='grid', floatfmt='.2f'))
    
    print(f"Detailed results saved to: {output_file}")
    print()

if __name__ == '__main__':
    main()