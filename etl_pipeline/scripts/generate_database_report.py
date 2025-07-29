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
    
    # Categorize missing tables by size and potential impact
    def categorize_missing_tables(missing_set, source_df):
        missing_df = source_df[source_df['table_name'].isin(missing_set)].copy()
        
        # Categorize by size
        large_tables = missing_df[missing_df['size_mb'] > 10]
        medium_tables = missing_df[(missing_df['size_mb'] > 1) & (missing_df['size_mb'] <= 10)]
        small_tables = missing_df[missing_df['size_mb'] <= 1]
        
        # Categorize by row count
        high_volume = missing_df[missing_df['row_count'] > 10000]
        medium_volume = missing_df[(missing_df['row_count'] > 1000) & (missing_df['row_count'] <= 10000)]
        low_volume = missing_df[missing_df['row_count'] <= 1000]
        
        return {
            'large_tables': large_tables,
            'medium_tables': medium_tables,
            'small_tables': small_tables,
            'high_volume': high_volume,
            'medium_volume': medium_volume,
            'low_volume': low_volume,
            'all_missing': missing_df
        }
    
    # Categorize extra tables
    def categorize_extra_tables(extra_set, df):
        extra_df = df[df['table_name'].isin(extra_set)].copy()
        
        # Check if they're ETL/system tables
        etl_tables = extra_df[extra_df['table_name'].str.startswith('etl_')]
        system_tables = extra_df[~extra_df['table_name'].str.startswith('etl_')]
        
        return {
            'etl_tables': etl_tables,
            'system_tables': system_tables,
            'all_extra': extra_df
        }
    
    return {
        'source_tables': source_tables,
        'replication_tables': replication_tables,
        'analytics_tables': analytics_tables,
        'missing_in_replication': missing_in_replication,
        'extra_in_replication': extra_in_replication,
        'missing_in_analytics': missing_in_analytics,
        'extra_in_analytics': extra_in_analytics,
        'missing_replication_categories': categorize_missing_tables(missing_in_replication, source_df),
        'missing_analytics_categories': categorize_missing_tables(missing_in_analytics, source_df),
        'extra_replication_categories': categorize_extra_tables(extra_in_replication, replication_df),
        'extra_analytics_categories': categorize_extra_tables(extra_in_analytics, analytics_df)
    }

def generate_markdown_report(source_df, replication_df, analytics_df, analysis_results):
    """Generate a comprehensive markdown report."""
    
    report_content = f"""# Database Comparison Analysis Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Overview
- **Source (opendental):** {len(analysis_results['source_tables'])} tables (ground truth)
- **Replication (opendental_replication):** {len(analysis_results['replication_tables'])} tables
- **Analytics (opendental_analytics):** {len(analysis_results['analytics_tables'])} tables

## Critical Issues Summary

### Missing Tables Impact
- **High Impact (Large/High Volume):** {len(analysis_results['missing_replication_categories']['large_tables']) + len(analysis_results['missing_replication_categories']['high_volume'])} tables
- **Medium Impact:** {len(analysis_results['missing_replication_categories']['medium_tables']) + len(analysis_results['missing_replication_categories']['medium_volume'])} tables
- **Low Impact:** {len(analysis_results['missing_replication_categories']['small_tables']) + len(analysis_results['missing_replication_categories']['low_volume'])} tables

### Extra Tables Analysis
- **ETL/System Tables:** {len(analysis_results['extra_replication_categories']['etl_tables']) + len(analysis_results['extra_analytics_categories']['etl_tables'])} tables (expected)
- **Unexpected Tables:** {len(analysis_results['extra_replication_categories']['system_tables']) + len(analysis_results['extra_analytics_categories']['system_tables'])} tables (need investigation)

## Missing Tables Analysis

### Missing in Replication ({len(analysis_results['missing_in_replication'])} tables)

#### High Priority - Large Tables (>10MB)
"""

    large_missing = analysis_results['missing_replication_categories']['large_tables']
    if not large_missing.empty:
        report_content += """
| Table Name | Row Count | Size (MB) | Impact Level |
|------------|-----------|-----------|--------------|
"""
        for _, row in large_missing.iterrows():
            impact = "CRITICAL" if row['size_mb'] > 50 else "HIGH"
            report_content += f"| {row['table_name']} | {row['row_count']:,} | {row['size_mb']:.2f} | {impact} |\n"
    else:
        report_content += "No large tables missing!\n"

    report_content += f"""

#### Medium Priority - Medium Tables (1-10MB)
"""

    medium_missing = analysis_results['missing_replication_categories']['medium_tables']
    if not medium_missing.empty:
        report_content += """
| Table Name | Row Count | Size (MB) | Impact Level |
|------------|-----------|-----------|--------------|
"""
        for _, row in medium_missing.iterrows():
            report_content += f"| {row['table_name']} | {row['row_count']:,} | {row['size_mb']:.2f} | MEDIUM |\n"
    else:
        report_content += "No medium tables missing!\n"

    report_content += f"""

#### Low Priority - Small Tables (≤1MB)
"""

    small_missing = analysis_results['missing_replication_categories']['small_tables']
    if not small_missing.empty:
        report_content += """
| Table Name | Row Count | Size (MB) | Notes |
|------------|-----------|-----------|-------|
"""
        for _, row in small_missing.iterrows():
            notes = "Empty table" if row['row_count'] == 0 else "Small config table"
            report_content += f"| {row['table_name']} | {row['row_count']:,} | {row['size_mb']:.2f} | {notes} |\n"
    else:
        report_content += "No small tables missing!\n"

    report_content += f"""

### Missing in Analytics ({len(analysis_results['missing_in_analytics'])} tables)

#### High Priority - Large Tables (>10MB)
"""

    large_missing_analytics = analysis_results['missing_analytics_categories']['large_tables']
    if not large_missing_analytics.empty:
        report_content += """
| Table Name | Row Count | Size (MB) | Impact Level |
|------------|-----------|-----------|--------------|
"""
        for _, row in large_missing_analytics.iterrows():
            impact = "CRITICAL" if row['size_mb'] > 50 else "HIGH"
            report_content += f"| {row['table_name']} | {row['row_count']:,} | {row['size_mb']:.2f} | {impact} |\n"
    else:
        report_content += "No large tables missing!\n"

    report_content += f"""

#### Medium Priority - Medium Tables (1-10MB)
"""

    medium_missing_analytics = analysis_results['missing_analytics_categories']['medium_tables']
    if not medium_missing_analytics.empty:
        report_content += """
| Table Name | Row Count | Size (MB) | Impact Level |
|------------|-----------|-----------|--------------|
"""
        for _, row in medium_missing_analytics.iterrows():
            report_content += f"| {row['table_name']} | {row['row_count']:,} | {row['size_mb']:.2f} | MEDIUM |\n"
    else:
        report_content += "No medium tables missing!\n"

    report_content += f"""

#### Low Priority - Small Tables (≤1MB)
"""

    small_missing_analytics = analysis_results['missing_analytics_categories']['small_tables']
    if not small_missing_analytics.empty:
        report_content += """
| Table Name | Row Count | Size (MB) | Notes |
|------------|-----------|-----------|-------|
"""
        for _, row in small_missing_analytics.iterrows():
            notes = "Empty table" if row['row_count'] == 0 else "Small config table"
            report_content += f"| {row['table_name']} | {row['row_count']:,} | {row['size_mb']:.2f} | {notes} |\n"
    else:
        report_content += "No small tables missing!\n"

    report_content += f"""

## Extra Tables Analysis

### Extra in Replication ({len(analysis_results['extra_in_replication'])} tables)

#### ETL/System Tables (Expected)
"""

    etl_extra_repl = analysis_results['extra_replication_categories']['etl_tables']
    if not etl_extra_repl.empty:
        report_content += """
| Table Name | Row Count | Size (MB) | Purpose |
|------------|-----------|-----------|---------|
"""
        for _, row in etl_extra_repl.iterrows():
            purpose = "ETL tracking" if "status" in row['table_name'] else "ETL metadata"
            report_content += f"| {row['table_name']} | {row['row_count']:,} | {row['size_mb']:.2f} | {purpose} |\n"
    else:
        report_content += "No ETL tables found!\n"

    report_content += f"""

#### Unexpected Tables (Need Investigation)
"""

    unexpected_repl = analysis_results['extra_replication_categories']['system_tables']
    if not unexpected_repl.empty:
        report_content += """
| Table Name | Row Count | Size (MB) | Action Needed |
|------------|-----------|-----------|---------------|
"""
        for _, row in unexpected_repl.iterrows():
            report_content += f"| {row['table_name']} | {row['row_count']:,} | {row['size_mb']:.2f} | Investigate |\n"
    else:
        report_content += "No unexpected tables!\n"

    report_content += f"""

### Extra in Analytics ({len(analysis_results['extra_in_analytics'])} tables)

#### ETL/System Tables (Expected)
"""

    etl_extra_analytics = analysis_results['extra_analytics_categories']['etl_tables']
    if not etl_extra_analytics.empty:
        report_content += """
| Table Name | Schema | Row Count | Size (MB) | Purpose |
|------------|--------|-----------|-----------|---------|
"""
        for _, row in etl_extra_analytics.iterrows():
            schema = row.get('schema_name', 'N/A')
            purpose = "ETL tracking" if "status" in row['table_name'] else "ETL metrics"
            report_content += f"| {row['table_name']} | {schema} | {row['row_count']:,} | {row['size_mb']:.2f} | {purpose} |\n"
    else:
        report_content += "No ETL tables found!\n"

    report_content += f"""

#### Unexpected Tables (Need Investigation)
"""

    unexpected_analytics = analysis_results['extra_analytics_categories']['system_tables']
    if not unexpected_analytics.empty:
        report_content += """
| Table Name | Schema | Row Count | Size (MB) | Action Needed |
|------------|--------|-----------|-----------|---------------|
"""
        for _, row in unexpected_analytics.iterrows():
            schema = row.get('schema_name', 'N/A')
            report_content += f"| {row['table_name']} | {schema} | {row['row_count']:,} | {row['size_mb']:.2f} | Investigate |\n"
    else:
        report_content += "No unexpected tables!\n"

    # Add detailed summary statistics
    report_content += f"""

## Detailed Summary Statistics

### Missing Tables by Impact Level
| Impact Level | Replication | Analytics | Total |
|--------------|-------------|-----------|-------|
| Critical (>50MB) | {len(analysis_results['missing_replication_categories']['large_tables'][analysis_results['missing_replication_categories']['large_tables']['size_mb'] > 50])} | {len(analysis_results['missing_analytics_categories']['large_tables'][analysis_results['missing_analytics_categories']['large_tables']['size_mb'] > 50])} | {len(analysis_results['missing_replication_categories']['large_tables'][analysis_results['missing_replication_categories']['large_tables']['size_mb'] > 50]) + len(analysis_results['missing_analytics_categories']['large_tables'][analysis_results['missing_analytics_categories']['large_tables']['size_mb'] > 50])} |
| High (10-50MB) | {len(analysis_results['missing_replication_categories']['large_tables'][analysis_results['missing_replication_categories']['large_tables']['size_mb'] <= 50])} | {len(analysis_results['missing_analytics_categories']['large_tables'][analysis_results['missing_analytics_categories']['large_tables']['size_mb'] <= 50])} | {len(analysis_results['missing_replication_categories']['large_tables'][analysis_results['missing_replication_categories']['large_tables']['size_mb'] <= 50]) + len(analysis_results['missing_analytics_categories']['large_tables'][analysis_results['missing_analytics_categories']['large_tables']['size_mb'] <= 50])} |
| Medium (1-10MB) | {len(analysis_results['missing_replication_categories']['medium_tables'])} | {len(analysis_results['missing_analytics_categories']['medium_tables'])} | {len(analysis_results['missing_replication_categories']['medium_tables']) + len(analysis_results['missing_analytics_categories']['medium_tables'])} |
| Low (≤1MB) | {len(analysis_results['missing_replication_categories']['small_tables'])} | {len(analysis_results['missing_analytics_categories']['small_tables'])} | {len(analysis_results['missing_replication_categories']['small_tables']) + len(analysis_results['missing_analytics_categories']['small_tables'])} |

### Extra Tables by Type
| Table Type | Replication | Analytics | Total |
|------------|-------------|-----------|-------|
| ETL/System (Expected) | {len(analysis_results['extra_replication_categories']['etl_tables'])} | {len(analysis_results['extra_analytics_categories']['etl_tables'])} | {len(analysis_results['extra_replication_categories']['etl_tables']) + len(analysis_results['extra_analytics_categories']['etl_tables'])} |
| Unexpected (Need Investigation) | {len(analysis_results['extra_replication_categories']['system_tables'])} | {len(analysis_results['extra_analytics_categories']['system_tables'])} | {len(analysis_results['extra_replication_categories']['system_tables']) + len(analysis_results['extra_analytics_categories']['system_tables'])} |

## Action Items by Priority

### Critical Priority (Immediate Action Required)
1. **Large missing tables in replication:** {len(analysis_results['missing_replication_categories']['large_tables'])} tables
   - These represent significant data loss and must be fixed immediately
   - Focus on tables >50MB first

2. **Large missing tables in analytics:** {len(analysis_results['missing_analytics_categories']['large_tables'])} tables
   - Critical business data missing from analytics
   - Impacts reporting and decision-making

### High Priority (This Week)
1. **Medium-sized missing tables:** {len(analysis_results['missing_replication_categories']['medium_tables']) + len(analysis_results['missing_analytics_categories']['medium_tables'])} tables
   - Important operational data missing
   - Schedule investigation and fixes

2. **Unexpected extra tables:** {len(analysis_results['extra_replication_categories']['system_tables']) + len(analysis_results['extra_analytics_categories']['system_tables'])} tables
   - Investigate source and purpose
   - Determine if they should be removed or documented

### Low Priority (This Month)
1. **Small missing tables:** {len(analysis_results['missing_replication_categories']['small_tables']) + len(analysis_results['missing_analytics_categories']['small_tables'])} tables
   - Mostly configuration and definition tables
   - Low impact but should be addressed for completeness

## Next Steps

### Immediate (Today)
1. Investigate why {len(analysis_results['missing_replication_categories']['large_tables'])} large tables failed to replicate
2. Review ETL pipeline logs for replication failures
3. Alert stakeholders about missing critical data

### This Week
1. Fix replication pipeline for missing large/medium tables
2. Update analytics pipeline to include missing tables
3. Clean up unexpected extra tables
4. Document ETL/system tables as expected

### This Month
1. Implement monitoring for table replication success
2. Create automated alerts for missing critical tables
3. Establish regular database comparison reports
4. Document table categorization and impact levels
"""

    return report_content

def main():
    """Main function to generate the database analysis report."""
    print("Database Analysis Report Generator")
    print("=" * 50)
    
    # Load data
    source_df, replication_df, analytics_df = load_database_data()
    
    if source_df is None:
        return
    
    # Analyze differences
    print("\nAnalyzing database differences...")
    analysis_results = analyze_differences(source_df, replication_df, analytics_df)
    
    # Generate report
    print("\nGenerating markdown report...")
    report_content = generate_markdown_report(source_df, replication_df, analytics_df, analysis_results)
    
    # Save report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = Path("database_data") / f"database_analysis_report_{timestamp}.md"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"Report saved to: {report_file.absolute()}")
    print(f"Analysis complete!")
    
    # Print enhanced summary
    print(f"\nCRITICAL ISSUES SUMMARY")
    print(f"=" * 50)
    
    # Missing tables by impact
    large_missing_repl = len(analysis_results['missing_replication_categories']['large_tables'])
    large_missing_analytics = len(analysis_results['missing_analytics_categories']['large_tables'])
    medium_missing_repl = len(analysis_results['missing_replication_categories']['medium_tables'])
    medium_missing_analytics = len(analysis_results['missing_analytics_categories']['medium_tables'])
    small_missing_repl = len(analysis_results['missing_replication_categories']['small_tables'])
    small_missing_analytics = len(analysis_results['missing_analytics_categories']['small_tables'])
    
    # Extra tables by type
    etl_extra_repl = len(analysis_results['extra_replication_categories']['etl_tables'])
    etl_extra_analytics = len(analysis_results['extra_analytics_categories']['etl_tables'])
    unexpected_repl = len(analysis_results['extra_replication_categories']['system_tables'])
    unexpected_analytics = len(analysis_results['extra_analytics_categories']['system_tables'])
    
    print(f"\nMISSING TABLES BY IMPACT:")
    print(f"  Critical (>10MB): {large_missing_repl + large_missing_analytics} tables")
    print(f"    - Replication: {large_missing_repl} tables")
    print(f"    - Analytics: {large_missing_analytics} tables")
    print(f"  Medium (1-10MB): {medium_missing_repl + medium_missing_analytics} tables")
    print(f"    - Replication: {medium_missing_repl} tables")
    print(f"    - Analytics: {medium_missing_analytics} tables")
    print(f"  Low (≤1MB): {small_missing_repl + small_missing_analytics} tables")
    print(f"    - Replication: {small_missing_repl} tables")
    print(f"    - Analytics: {small_missing_analytics} tables")
    
    print(f"\nEXTRA TABLES BY TYPE:")
    print(f"  ETL/System (Expected): {etl_extra_repl + etl_extra_analytics} tables")
    print(f"    - Replication: {etl_extra_repl} tables")
    print(f"    - Analytics: {etl_extra_analytics} tables")
    print(f"  Unexpected (Need Investigation): {unexpected_repl + unexpected_analytics} tables")
    print(f"    - Replication: {unexpected_repl} tables")
    print(f"    - Analytics: {unexpected_analytics} tables")
    
    print(f"\nACTION ITEMS BY PRIORITY:")
    print(f"  Critical Priority (Immediate):")
    print(f"    - Large missing tables in replication: {large_missing_repl} tables")
    print(f"    - Large missing tables in analytics: {large_missing_analytics} tables")
    print(f"  High Priority (This Week):")
    print(f"    - Medium missing tables: {medium_missing_repl + medium_missing_analytics} tables")
    print(f"    - Unexpected extra tables: {unexpected_repl + unexpected_analytics} tables")
    print(f"  Low Priority (This Month):")
    print(f"    - Small missing tables: {small_missing_repl + small_missing_analytics} tables")
    
    # Show top critical missing tables
    if large_missing_repl > 0:
        print(f"\nTOP CRITICAL MISSING TABLES (Replication):")
        large_missing_df = analysis_results['missing_replication_categories']['large_tables']
        for _, row in large_missing_df.head(5).iterrows():
            impact = "CRITICAL" if row['size_mb'] > 50 else "HIGH"
            print(f"    - {row['table_name']}: {row['row_count']:,} rows, {row['size_mb']:.2f}MB ({impact})")
    
    if large_missing_analytics > 0:
        print(f"\nTOP CRITICAL MISSING TABLES (Analytics):")
        large_missing_analytics_df = analysis_results['missing_analytics_categories']['large_tables']
        for _, row in large_missing_analytics_df.head(5).iterrows():
            impact = "CRITICAL" if row['size_mb'] > 50 else "HIGH"
            print(f"    - {row['table_name']}: {row['row_count']:,} rows, {row['size_mb']:.2f}MB ({impact})")
    
    print(f"\nNEXT STEPS:")
    print(f"  1. Investigate why {large_missing_repl + large_missing_analytics} large tables failed to replicate")
    print(f"  2. Review ETL pipeline logs for replication failures")
    print(f"  3. Alert stakeholders about missing critical data")
    print(f"  4. Fix replication pipeline for missing large/medium tables")
    print(f"  5. Update analytics pipeline to include missing tables")

if __name__ == "__main__":
    main() 