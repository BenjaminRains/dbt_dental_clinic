#!/usr/bin/env python3
"""
Database Analysis Report Generator

This script analyzes the database comparison data and generates a markdown report.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import os
from datetime import datetime
import sys

# Add the etl_pipeline directory to the path
sys.path.append(str(Path(__file__).parent.parent))

def load_database_data():
    """Load the CSV files from the most recent export directory."""
    # Find the most recent export directory
    data_dir = Path("database_data")  # Fixed: relative to etl_pipeline directory
    export_dirs = list(data_dir.glob("export_*"))
    
    if not export_dirs:
        print("❌ No export directories found. Run compare_databases.py first.")
        return None, None, None
    
    latest_export = max(export_dirs)
    print(f"📁 Using data from: {latest_export}")
    
    # Load the CSV files
    source_df = pd.read_csv(latest_export / "opendental_tables.csv")
    replication_df = pd.read_csv(latest_export / "opendental_replication_tables.csv")
    analytics_df = pd.read_csv(latest_export / "opendental_analytics_tables.csv")
    
    print(f"✅ Source: {len(source_df)} tables loaded")
    print(f"✅ Replication: {len(replication_df)} tables loaded")
    print(f"✅ Analytics: {len(analytics_df)} tables loaded")
    
    return source_df, replication_df, analytics_df

def analyze_differences(source_df, replication_df, analytics_df):
    """Analyze differences between databases."""
    # Get table sets
    source_tables = set(source_df['table_name'].tolist())
    replication_tables = set(replication_df['table_name'].tolist())
    analytics_tables = set(analytics_df['table_name'].tolist())
    
    # Calculate differences
    missing_in_replication = source_tables - replication_tables
    extra_in_replication = replication_tables - source_tables
    missing_in_analytics = source_tables - analytics_tables
    extra_in_analytics = analytics_tables - source_tables
    
    return {
        'source_tables': source_tables,
        'replication_tables': replication_tables,
        'analytics_tables': analytics_tables,
        'missing_in_replication': missing_in_replication,
        'extra_in_replication': extra_in_replication,
        'missing_in_analytics': missing_in_analytics,
        'extra_in_analytics': extra_in_analytics
    }

def generate_markdown_report(source_df, replication_df, analytics_df, analysis_results):
    """Generate a comprehensive markdown report."""
    
    report_content = f"""# Database Comparison Analysis Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Overview
- **Source (opendental):** {len(analysis_results['source_tables'])} tables (ground truth)
- **Replication (opendental_replication):** {len(analysis_results['replication_tables'])} tables
- **Analytics (opendental_analytics):** {len(analysis_results['analytics_tables'])} tables

## Missing Tables Analysis

### ❌ Missing in Replication ({len(analysis_results['missing_in_replication'])} tables)
"""

    if analysis_results['missing_in_replication']:
        missing_repl_df = source_df[source_df['table_name'].isin(analysis_results['missing_in_replication'])]
        report_content += """
| Table Name | Row Count | Size (MB) |
|------------|-----------|-----------|
"""
        for _, row in missing_repl_df.iterrows():
            report_content += f"| {row['table_name']} | {row['row_count']:,} | {row['size_mb']:.2f} |\n"
    else:
        report_content += "✅ All source tables are in replication!\n"

    report_content += f"""

### ❌ Missing in Analytics ({len(analysis_results['missing_in_analytics'])} tables)
"""

    if analysis_results['missing_in_analytics']:
        missing_analytics_df = source_df[source_df['table_name'].isin(analysis_results['missing_in_analytics'])]
        report_content += """
| Table Name | Row Count | Size (MB) |
|------------|-----------|-----------|
"""
        for _, row in missing_analytics_df.iterrows():
            report_content += f"| {row['table_name']} | {row['row_count']:,} | {row['size_mb']:.2f} |\n"
    else:
        report_content += "✅ All source tables are in analytics!\n"

    report_content += f"""

## Extra Tables Analysis

### ➕ Extra in Replication ({len(analysis_results['extra_in_replication'])} tables)
"""

    if analysis_results['extra_in_replication']:
        extra_repl_df = replication_df[replication_df['table_name'].isin(analysis_results['extra_in_replication'])]
        report_content += """
| Table Name | Row Count | Size (MB) |
|------------|-----------|-----------|
"""
        for _, row in extra_repl_df.iterrows():
            report_content += f"| {row['table_name']} | {row['row_count']:,} | {row['size_mb']:.2f} |\n"
    else:
        report_content += "✅ No extra tables in replication!\n"

    report_content += f"""

### ➕ Extra in Analytics ({len(analysis_results['extra_in_analytics'])} tables)
"""

    if analysis_results['extra_in_analytics']:
        extra_analytics_df = analytics_df[analytics_df['table_name'].isin(analysis_results['extra_in_analytics'])]
        report_content += """
| Table Name | Schema | Row Count | Size (MB) |
|------------|--------|-----------|-----------|
"""
        for _, row in extra_analytics_df.iterrows():
            schema = row.get('schema_name', 'N/A')
            report_content += f"| {row['table_name']} | {schema} | {row['row_count']:,} | {row['size_mb']:.2f} |\n"
    else:
        report_content += "✅ No extra tables in analytics!\n"

    # Add summary statistics
    report_content += f"""

## Summary Statistics

| Metric | Count |
|--------|-------|
| Source Tables | {len(analysis_results['source_tables'])} |
| Missing in Replication | {len(analysis_results['missing_in_replication'])} |
| Extra in Replication | {len(analysis_results['extra_in_replication'])} |
| Missing in Analytics | {len(analysis_results['missing_in_analytics'])} |
| Extra in Analytics | {len(analysis_results['extra_in_analytics'])} |

## Largest Missing Tables

### Top 10 by Size (Missing in Replication)
"""

    if analysis_results['missing_in_replication']:
        missing_repl_df = source_df[source_df['table_name'].isin(analysis_results['missing_in_replication'])]
        largest_missing = missing_repl_df.nlargest(10, 'size_mb')
        report_content += """
| Table Name | Row Count | Size (MB) |
|------------|-----------|-----------|
"""
        for _, row in largest_missing.iterrows():
            report_content += f"| {row['table_name']} | {row['row_count']:,} | {row['size_mb']:.2f} |\n"

    report_content += f"""

## Recommendations

1. **ETL Pipeline Issues**: {len(analysis_results['missing_in_replication'])} tables failed to replicate
2. **Data Quality**: {len(analysis_results['extra_in_replication'])} unexpected tables in replication
3. **Analytics Pipeline**: {len(analysis_results['missing_in_analytics'])} tables missing from analytics
4. **Schema Management**: {len(analysis_results['extra_in_analytics'])} extra tables in analytics

## Next Steps

1. Investigate why {len(analysis_results['missing_in_replication'])} tables failed to replicate
2. Review {len(analysis_results['extra_in_replication'])} unexpected tables in replication
3. Fix analytics pipeline for {len(analysis_results['missing_in_analytics'])} missing tables
4. Clean up {len(analysis_results['extra_in_analytics'])} extra tables in analytics
"""

    return report_content

def main():
    """Main function to generate the database analysis report."""
    print("📊 Database Analysis Report Generator")
    print("=" * 50)
    
    # Load data
    source_df, replication_df, analytics_df = load_database_data()
    
    if source_df is None:
        return
    
    # Analyze differences
    print("\n🔍 Analyzing database differences...")
    analysis_results = analyze_differences(source_df, replication_df, analytics_df)
    
    # Generate report
    print("\n📝 Generating markdown report...")
    report_content = generate_markdown_report(source_df, replication_df, analytics_df, analysis_results)
    
    # Save report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = Path("database_data") / f"database_analysis_report_{timestamp}.md"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"✅ Report saved to: {report_file.absolute()}")
    print(f"📊 Analysis complete!")
    
    # Print summary
    print(f"\n📋 Summary:")
    print(f"  - Source tables: {len(analysis_results['source_tables'])}")
    print(f"  - Missing in replication: {len(analysis_results['missing_in_replication'])}")
    print(f"  - Extra in replication: {len(analysis_results['extra_in_replication'])}")
    print(f"  - Missing in analytics: {len(analysis_results['missing_in_analytics'])}")
    print(f"  - Extra in analytics: {len(analysis_results['extra_in_analytics'])}")

if __name__ == "__main__":
    main() 