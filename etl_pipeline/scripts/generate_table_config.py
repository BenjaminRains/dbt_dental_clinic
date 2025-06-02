"""
Script to generate tables.yaml configuration using schema discovery.
This script will:
1. Discover all tables in the source database
2. Analyze their schemas and characteristics
3. Find suitable incremental columns
4. Generate the appropriate configuration

Optimized to minimize load on source database by:
- Batching information_schema queries
- Caching results
- Using single queries where possible
- Connection pooling
- Smart table analysis
"""
import os
import yaml
from typing import Dict, List, Set, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from etl_pipeline.core.schema_discovery import SchemaDiscovery
import logging
from collections import defaultdict
from tqdm import tqdm
import time
from datetime import datetime
import json
from tabulate import tabulate
import networkx as nx
from etl_pipeline.core.connections import ConnectionFactory

class ProgressTracker:
    """Class to track and report progress of schema discovery."""
    def __init__(self, total_tables: int):
        self.total_tables = total_tables
        self.start_time = time.time()
        self.processed_tables = 0
        self.failed_tables = []
        self.successful_tables = []
        
    def update(self, table_name: str, success: bool = True):
        """Update progress for a processed table."""
        self.processed_tables += 1
        if success:
            self.successful_tables.append(table_name)
        else:
            self.failed_tables.append(table_name)
            
    def get_progress(self) -> Dict:
        """Get current progress statistics."""
        elapsed_time = time.time() - self.start_time
        success_rate = (len(self.successful_tables) / self.processed_tables * 100) if self.processed_tables > 0 else 0
        estimated_remaining = (elapsed_time / self.processed_tables * (self.total_tables - self.processed_tables)) if self.processed_tables > 0 else 0
        
        return {
            'processed': self.processed_tables,
            'total': self.total_tables,
            'success_rate': success_rate,
            'elapsed_time': elapsed_time,
            'estimated_remaining': estimated_remaining,
            'failed_tables': self.failed_tables,
            'successful_tables': self.successful_tables
        }

def get_all_tables(engine, database: str) -> List[str]:
    """
    Get all tables from the database.
    
    Args:
        engine: SQLAlchemy engine
        database: Database name
        
    Returns:
        List[str]: List of table names
    """
    with engine.connect() as conn:
        query = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = :db_name 
            AND table_type = 'BASE TABLE'
        """)
        result = conn.execute(query.bindparams(db_name=database))
        return [row[0] for row in result]

def batch_get_schemas(engine, database: str, tables: List[str], pbar) -> Dict[str, Dict]:
    """
    Get schema information for multiple tables in batches.
    
    Args:
        engine: SQLAlchemy engine
        database: Database name
        tables: List of table names
        pbar: Progress bar object
        
    Returns:
        Dict[str, Dict]: Schema information for each table
    """
    schemas = {}
    batch_size = 50  # Process tables in batches to avoid memory issues
    
    # Get columns information
    pbar.set_description("Fetching column information")
    with engine.connect() as conn:
        for i in range(0, len(tables), batch_size):
            batch = tables[i:i + batch_size]
            query = text("""
                SELECT 
                    table_name,
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    extra
                FROM information_schema.columns
                WHERE table_schema = :db_name
                AND table_name IN :table_names
            """)
            result = conn.execute(query.bindparams(
                db_name=database,
                table_names=tuple(batch)
            ))
            
            for row in result:
                if row.table_name not in schemas:
                    schemas[row.table_name] = {
                        'columns': [],
                        'indexes': [],
                        'foreign_keys': [],
                        'incremental_columns': []
                    }
                schemas[row.table_name]['columns'].append(row)
    
    # Get index information
    pbar.set_description("Fetching index information")
    with engine.connect() as conn:
        for i in range(0, len(tables), batch_size):
            batch = tables[i:i + batch_size]
            query = text("""
                SELECT 
                    table_name,
                    index_name,
                    non_unique,
                    GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns
                FROM information_schema.statistics
                WHERE table_schema = :db_name
                AND table_name IN :table_names
                GROUP BY table_name, index_name, non_unique
            """)
            result = conn.execute(query.bindparams(
                db_name=database,
                table_names=tuple(batch)
            ))
            
            for row in result:
                schemas[row.table_name]['indexes'].append(row)
    
    # Get foreign key information
    pbar.set_description("Fetching foreign key information")
    with engine.connect() as conn:
        for i in range(0, len(tables), batch_size):
            batch = tables[i:i + batch_size]
            query = text("""
                SELECT 
                    table_name,
                    column_name,
                    referenced_table_name,
                    referenced_column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = :db_name
                AND table_name IN :table_names
                AND referenced_table_name IS NOT NULL
            """)
            result = conn.execute(query.bindparams(
                db_name=database,
                table_names=tuple(batch)
            ))
            
            for row in result:
                schemas[row.table_name]['foreign_keys'].append(row)
    
    # Identify incremental columns
    pbar.set_description("Identifying incremental columns")
    for table_name, schema in schemas.items():
        for column in schema['columns']:
            if (column.data_type in ('datetime', 'timestamp') or
                'auto_increment' in (column.extra or '').lower()):
                schema['incremental_columns'].append(column)
    
    pbar.update(1)
    return schemas

def generate_summary_report(progress_tracker: ProgressTracker, config: Dict, output_path: str) -> None:
    """
    Generate a summary report of the configuration generation process.
    
    Args:
        progress_tracker: Progress tracker object
        config: Generated configuration
        output_path: Path to the output configuration file
    """
    progress = progress_tracker.get_progress()
    
    # Calculate statistics
    total_tables = len(config['source_tables'])
    critical_tables = sum(1 for info in config['source_tables'].values() 
                         if info.get('table_importance') == 'critical')
    important_tables = sum(1 for info in config['source_tables'].values() 
                          if info.get('table_importance') == 'important')
    audit_tables = sum(1 for info in config['source_tables'].values() 
                      if info.get('table_importance') == 'audit')
    reference_tables = sum(1 for info in config['source_tables'].values() 
                          if info.get('table_importance') == 'reference')
    
    # Generate report
    report = f"""
Configuration Generation Summary
==============================

Processed Tables: {progress['processed']}/{progress['total']}
Success Rate: {progress['success_rate']:.1f}%
Total Time: {progress['elapsed_time']:.1f} seconds

Table Classification
------------------
Critical Tables: {critical_tables}
Important Tables: {important_tables}
Audit Tables: {audit_tables}
Reference Tables: {reference_tables}
Standard Tables: {total_tables - critical_tables - important_tables - audit_tables - reference_tables}

Failed Tables
------------
{chr(10).join(f"- {table}" for table in progress['failed_tables']) if progress['failed_tables'] else "None"}

Output
------
Configuration written to: {output_path}
"""
    
    # Write report to file
    report_path = output_path.replace('.yaml', '_report.txt')
    with open(report_path, 'w') as f:
        f.write(report)
    
    # Print report to console
    logger.info("\n" + report)

# Constants
EXCLUDED_PATTERNS = ['tmp_', 'temp_', 'backup_', '#', 'test_']
CORE_TABLES = {
    'patient', 'appointment', 'procedurelog', 'payment', 'claim', 
    'treatment', 'recall', 'insurance'
}
IMPORTANT_TABLES = {
    'provider', 'operatory', 'definition', 'preference', 'userod',
    'schedule', 'scheduleop', 'timeadjust', 'claimproc'
}
AUDIT_TABLES = {
    'securitylog', 'emailmessage', 'commlog', 'histappointment',
    'patientnote', 'treatplan', 'document'
}
SYSTEM_TABLES = {
    'procedurecode', 'icd9', 'icd10', 'cpt', 'fee', 'feesched',
    'carrier', 'insplan', 'benefit', 'covcat'
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'etl_pipeline/logs/schema_discovery_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_optimized_source_engine():
    """Get source engine optimized for schema discovery."""
    return create_engine(
        ConnectionFactory.get_source_connection(readonly=True).url,
        poolclass=QueuePool,
        pool_size=2,  # Limit concurrent connections
        pool_pre_ping=True,
        connect_args={
            "read_timeout": 30,
            "write_timeout": 30,
            "connect_timeout": 10
        }
    )

def analyze_table_relationships(engine, db_name: str, tables: List[str]) -> Dict[str, Dict]:
    """Analyze table relationships and dependencies."""
    table_metrics = {}
    
    with engine.connect() as conn:
        # Get all foreign key relationships
        query = text("""
            SELECT 
                table_name,
                referenced_table_name
            FROM information_schema.key_column_usage
            WHERE table_schema = :db_name
            AND referenced_table_name IS NOT NULL
            AND table_name IN :tables
        """)
        
        results = conn.execute(query.bindparams(db_name=db_name, tables=tuple(tables))).fetchall()
        
        # Initialize metrics for all tables
        for table in tables:
            table_metrics[table] = {
                'referenced_by_count': 0,
                'references_count': 0,
                'pagerank_score': 0.0,
                'is_core_reference': False
            }
        
        # If no foreign keys found, return basic metrics
        if not results:
            return table_metrics
        
        # Build relationship graph
        graph = nx.DiGraph()
        for row in results:
            if row.table_name in tables and row.referenced_table_name in tables:
                graph.add_edge(row.table_name, row.referenced_table_name)
        
        # Calculate metrics
        for table in tables:
            if table in graph:
                table_metrics[table].update({
                    'referenced_by_count': graph.in_degree(table),
                    'references_count': graph.out_degree(table),
                    'pagerank_score': nx.pagerank(graph).get(table, 0.0)
                })
        
        # Identify core reference tables (tables referenced by many others)
        if table_metrics:
            reference_counts = [m['referenced_by_count'] for m in table_metrics.values()]
            if reference_counts:
                reference_threshold = sorted(reference_counts, reverse=True)[:10]
                if reference_threshold:
                    threshold = reference_threshold[-1]
                    for table, metrics in table_metrics.items():
                        metrics['is_core_reference'] = metrics['referenced_by_count'] >= threshold
        
        return table_metrics

def analyze_table_usage(source_engine, source_db: str, tables: List[str]) -> Dict[str, Dict]:
    """
    Analyze table usage patterns based on:
    1. Update frequency (auto_increment gap)
    2. Presence of audit columns
    3. Table size and growth
    """
    with source_engine.connect() as conn:
        usage_query = text("""
            SELECT 
                t.table_name,
                t.table_rows,
                t.auto_increment,
                t.data_length,
                t.index_length,
                -- Check for audit columns
                (SELECT COUNT(*) FROM information_schema.columns c 
                 WHERE c.table_schema = t.table_schema 
                 AND c.table_name = t.table_name 
                 AND c.column_name IN ('DateTStamp', 'DateModified', 'SecDateTEdit')) AS has_audit_columns,
                -- Check for timestamp columns
                (SELECT COUNT(*) FROM information_schema.columns c 
                 WHERE c.table_schema = t.table_schema 
                 AND c.table_name = t.table_name 
                 AND c.data_type IN ('datetime', 'timestamp')) AS has_timestamp_columns,
                -- Check for soft delete
                (SELECT COUNT(*) FROM information_schema.columns c 
                 WHERE c.table_schema = t.table_schema 
                 AND c.table_name = t.table_name 
                 AND c.column_name IN ('IsDeleted', 'IsHidden', 'IsArchived')) AS has_soft_delete
            FROM information_schema.tables t
            WHERE t.table_schema = :db_name
            AND t.table_name IN :table_names
            AND t.table_type = 'BASE TABLE'
        """)
        
        results = conn.execute(usage_query.bindparams(
            db_name=source_db,
            table_names=tuple(tables)
        )).fetchall()
        
        usage_metrics = {}
        for row in results:
            # Calculate update frequency
            update_frequency = 'low'
            if row.auto_increment and row.table_rows:
                churn_rate = (row.auto_increment - row.table_rows) / row.table_rows * 100
                if churn_rate > 50:
                    update_frequency = 'high'
                elif churn_rate > 10:
                    update_frequency = 'medium'
            
            # Calculate table size
            size_mb = (row.data_length + row.index_length) / 1024 / 1024
            
            usage_metrics[row.table_name] = {
                'row_count': row.table_rows or 0,
                'size_mb': round(size_mb, 2),
                'has_audit_columns': bool(row.has_audit_columns),
                'has_timestamp_columns': bool(row.has_timestamp_columns),
                'has_soft_delete': bool(row.has_soft_delete),
                'update_frequency': update_frequency
            }
        
        return usage_metrics

def determine_table_importance(relationships: Dict[str, Dict], usage: Dict[str, Dict]) -> Dict[str, str]:
    """
    Determine table importance based on relationship and usage analysis.
    Returns a mapping of table names to importance levels.
    """
    importance = {}
    
    for table in relationships.keys():
        rel_metrics = relationships[table]
        usage_metrics = usage[table]
        
        # Calculate importance score
        score = 0
        
        # Relationship-based scoring
        score += rel_metrics['referenced_by_count'] * 2  # Weight references to this table
        score += rel_metrics['pagerank_score'] * 100     # Weight PageRank score
        if rel_metrics['is_core_reference']:
            score += 50  # Bonus for core reference tables
        
        # Usage-based scoring
        if usage_metrics['has_audit_columns']:
            score += 30  # Tables with audit columns are important
        if usage_metrics['has_timestamp_columns']:
            score += 20  # Tables with timestamps are likely important
        if usage_metrics['update_frequency'] == 'high':
            score += 25  # Frequently updated tables are important
        if usage_metrics['size_mb'] > 100:
            score += 15  # Large tables are likely important
        
        # Determine importance level based on score
        if score >= 100:
            importance[table] = 'critical'
        elif score >= 50:
            importance[table] = 'important'
        elif usage_metrics['has_audit_columns'] or usage_metrics['has_timestamp_columns']:
            importance[table] = 'audit'
        elif rel_metrics['is_core_reference']:
            importance[table] = 'reference'
        else:
            importance[table] = 'standard'
    
    return importance

def analyze_table_characteristics(source_engine, source_db: str, tables: List[str]) -> Dict[str, Dict]:
    """
    Analyze table characteristics to make smarter configuration decisions.
    This helps optimize batch sizes and extraction strategies.
    """
    # Analyze relationships
    logger.info("Analyzing table relationships...")
    relationships = analyze_table_relationships(source_engine, source_db, tables)
    
    # Analyze usage patterns
    logger.info("Analyzing table usage patterns...")
    usage = analyze_table_usage(source_engine, source_db, tables)
    
    # Determine table importance
    logger.info("Determining table importance...")
    importance = determine_table_importance(relationships, usage)
    
    # Combine all metrics
    table_analysis = {}
    for table in tables:
        table_analysis[table] = {
            'size_mb': usage[table]['size_mb'],
            'row_count': usage[table]['row_count'],
            'update_frequency': usage[table]['update_frequency'],
            'has_audit_columns': usage[table]['has_audit_columns'],
            'has_timestamp_columns': usage[table]['has_timestamp_columns'],
            'has_soft_delete': usage[table]['has_soft_delete'],
            'referenced_by_count': relationships[table]['referenced_by_count'],
            'references_count': relationships[table]['references_count'],
            'pagerank_score': relationships[table]['pagerank_score'],
            'is_core_reference': relationships[table]['is_core_reference'],
            'table_importance': importance[table],
            # Determine optimal batch size based on table characteristics
            'recommended_batch_size': get_recommended_batch_size(usage[table]),
            'extraction_strategy': get_extraction_strategy(usage[table], importance[table])
        }
    
    return table_analysis

def get_recommended_batch_size(usage_metrics: Dict) -> int:
    """Determine optimal batch size based on table characteristics."""
    size_mb = usage_metrics['size_mb']
    update_freq = usage_metrics['update_frequency']
    
    if size_mb < 1:  # Small tables
        return 5000
    elif size_mb < 100:  # Medium tables
        return 2000 if update_freq == 'high' else 3000
    elif size_mb < 1000:  # Large tables
        return 1000 if update_freq == 'high' else 2000
    else:  # Very large tables
        return 500 if update_freq == 'high' else 1000

def get_extraction_strategy(usage_metrics: Dict, importance: str) -> str:
    """Determine optimal extraction strategy based on table characteristics."""
    size_mb = usage_metrics['size_mb']
    update_freq = usage_metrics['update_frequency']
    
    if size_mb < 1:
        return 'full_table'
    elif size_mb < 100:
        return 'chunked'
    elif size_mb < 1000:
        return 'chunked_parallel' if update_freq == 'high' else 'chunked'
    else:
        return 'streaming'

def classify_table_importance(table_name: str) -> str:
    """Classify OpenDental tables by business importance."""
    table_lower = table_name.lower()
    
    if any(core in table_lower for core in CORE_TABLES):
        return 'critical'
    elif any(imp in table_lower for imp in IMPORTANT_TABLES):
        return 'important'  
    elif any(audit in table_lower for audit in AUDIT_TABLES):
        return 'audit'
    elif any(sys in table_lower for sys in SYSTEM_TABLES):
        return 'reference'
    else:
        return 'standard'

def get_column_priority(column_name: str, data_type: str, extra: str) -> int:
    """Determine priority of a column for incremental loading."""
    priority = 0
    
    # Handle SQLAlchemy result row
    if hasattr(column_name, 'column_name'):
        column_name = column_name.column_name
    if hasattr(data_type, 'data_type'):
        data_type = data_type.data_type
    if hasattr(extra, 'extra'):
        extra = extra.extra
    
    # Prefer timestamp columns
    if data_type in ('datetime', 'timestamp'):
        priority += 100
        # Prefer audit columns
        if any(name in column_name.lower() for name in ['modified', 'updated', 'created']):
            priority += 50
    
    # Prefer auto-increment columns
    if extra and 'auto_increment' in str(extra).lower():
        priority += 75
    
    # Prefer date columns
    if data_type == 'date':
        priority += 25
    
    return priority

def generate_validation_rules(table_name: str, schema_info: Dict, analysis: Dict) -> List[Dict]:
    """Generate appropriate validation rules based on table characteristics."""
    rules = []
    
    # Get primary key columns for not_null validation
    pk_columns = []
    for idx in schema_info['indexes']:
        if not idx.non_unique and 'PRIMARY' in (idx.index_name or '').upper():
            pk_columns.extend(idx.columns.split(','))
    
    if pk_columns:
        rules.append({
            'name': 'not_null',
            'columns': [col.strip() for col in pk_columns]
        })
    
    # Add foreign key validation
    for fk in schema_info['foreign_keys']:
        rules.append({
            'name': 'relationships',
            'columns': [fk.column_name],
            'references': f"{fk.referenced_table_name}.{fk.referenced_column_name}"
        })
    
    # Add business-specific validations based on table importance
    importance = analysis.get('table_importance', 'standard')
    if importance == 'critical':
        # Add stricter validation for critical tables
        rules.append({
            'name': 'row_count_change',
            'threshold': 0.1,  # Alert if row count changes by more than 10%
            'severity': 'error'
        })
    
    return rules

def get_max_extraction_time(analysis: Dict) -> int:
    """Calculate appropriate timeout based on table size."""
    size_mb = analysis.get('size_mb', 0)
    
    if size_mb < 10:
        return 5  # 5 minutes for small tables
    elif size_mb < 100:
        return 15  # 15 minutes for medium tables
    elif size_mb < 1000:
        return 60  # 1 hour for large tables
    else:
        return 180  # 3 hours for very large tables

def get_quality_threshold(analysis: Dict) -> float:
    """Set data quality thresholds based on table importance."""
    importance = analysis.get('table_importance', 'standard')
    
    thresholds = {
        'critical': 0.99,    # 99% quality required
        'important': 0.95,   # 95% quality required  
        'audit': 0.90,       # 90% quality required
        'reference': 0.98,   # 98% quality required (reference data should be clean)
        'standard': 0.85     # 85% quality required
    }
    
    return thresholds.get(importance, 0.85)

def has_circular_dependencies(dependencies: Dict[str, List[str]]) -> bool:
    """Check for circular dependencies in table relationships."""
    graph = nx.DiGraph()
    
    # Add edges to the graph
    for table, deps in dependencies.items():
        for dep in deps:
            graph.add_edge(table, dep)
    
    try:
        # Find cycles
        cycles = list(nx.simple_cycles(graph))
        return len(cycles) > 0
    except nx.NetworkXNoCycle:
        return False

def validate_generated_config(config: Dict) -> List[str]:
    """Validate the generated configuration for common issues."""
    issues = []
    
    # Check for circular dependencies
    dependencies = {table: info.get('dependencies', []) 
                   for table, info in config['source_tables'].items()}
    
    if has_circular_dependencies(dependencies):
        issues.append("Circular dependencies detected in table relationships")
    
    # Check for missing incremental columns on large tables
    for table, info in config['source_tables'].items():
        if (info.get('estimated_rows', 0) > 10000 and 
            not info.get('incremental_column')):
            issues.append(f"Large table {table} has no incremental column")
    
    # Check for unrealistic batch sizes
    for table, info in config['source_tables'].items():
        batch_size = info.get('batch_size', 1000)
        size_mb = info.get('estimated_size_mb', 0)
        
        if size_mb > 100 and batch_size > 5000:
            issues.append(f"Table {table} has large batch size for its size")
    
    return issues

def generate_smart_table_config(table_name: str, schema_info: Dict, analysis: Dict) -> Dict:
    """Generate configuration using both schema discovery and table analysis."""
    try:
        # Get analysis data
        table_analysis = analysis.get(table_name, {})
        
        # Find the best incremental column
        incremental_columns = schema_info['incremental_columns']
        best_incremental = None
        
        if incremental_columns:
            # Sort by priority and pick the best one
            sorted_columns = sorted(incremental_columns, 
                                  key=lambda x: get_column_priority(x.column_name, x.data_type, x.extra))
            best_incremental = sorted_columns[0].column_name
        
        # Get foreign keys for dependencies
        dependencies = list(set(fk.referenced_table_name for fk in schema_info['foreign_keys']))
        
        # Generate validation rules based on table analysis
        validation_rules = generate_validation_rules(table_name, schema_info, table_analysis)
        
        # Generate source table config with smart defaults
        source_config = {
            'incremental_column': best_incremental,
            'batch_size': table_analysis.get('recommended_batch_size', 1000),
            'extraction_strategy': table_analysis.get('extraction_strategy', 'chunked'),
            'is_modeled': table_analysis.get('table_importance') in ['critical', 'important'],
            'table_importance': table_analysis.get('table_importance', 'standard'),
            'estimated_size_mb': table_analysis.get('size_mb', 0),
            'estimated_rows': table_analysis.get('row_count', 0),
            'is_append_only': table_analysis.get('is_append_only', False),
            'validation_rules': validation_rules,
            'dependencies': dependencies,
            'monitoring': {
                'alert_on_failure': table_analysis.get('table_importance') in ['critical', 'important'],
                'max_extraction_time_minutes': get_max_extraction_time(table_analysis),
                'data_quality_threshold': get_quality_threshold(table_analysis)
            }
        }
        
        # Generate staging config
        staging_config = {
            'schema': 'staging',
            'indexes': [
                {
                    'columns': idx.columns.split(',') if hasattr(idx, 'columns') else [idx.column_name],
                    'type': 'primary' if not idx.non_unique else 'btree'
                }
                for idx in schema_info['indexes']
            ],
            'cleanup_after_load': not table_analysis.get('is_append_only', False)
        }
        
        # Generate target config
        target_config = {
            'schema': 'analytics',
            'indexes': staging_config['indexes'],
            'partitioning': {
                'type': 'range',
                'column': best_incremental,
                'interval': '1 month'
            } if best_incremental else None
        }
        
        return {
            'source': source_config,
            'staging': staging_config,
            'target': target_config
        }
        
    except Exception as e:
        logger.error(f"Error generating smart config for {table_name}: {str(e)}")
        return None

def main():
    """Main function to generate tables.yaml configuration."""
    try:
        # Create logs directory if it doesn't exist
        os.makedirs('etl_pipeline/logs', exist_ok=True)
        
        # Get optimized database connections
        source_engine = get_optimized_source_engine()
        target_engine = ConnectionFactory.get_staging_connection()  # Use staging MySQL as target
        
        # Get all tables
        tables = get_all_tables(source_engine, 'opendental')
        # Filter out excluded tables
        tables = [t for t in tables if not any(pattern in t.lower() for pattern in EXCLUDED_PATTERNS)]
        total_tables = len(tables)
        logger.info(f"Found {total_tables} tables in source database")
        
        # Initialize progress tracker
        progress_tracker = ProgressTracker(total_tables)
        
        # Create progress bar for schema discovery
        with tqdm(total=total_tables, desc="Schema Discovery") as pbar:
            # Batch get all schema information
            logger.info("Fetching schema information for all tables...")
            schemas = batch_get_schemas(source_engine, 'opendental', tables, pbar)
            
            # Analyze table characteristics
            logger.info("Analyzing table characteristics...")
            table_analysis = analyze_table_characteristics(source_engine, 'opendental', tables)
            
            # Generate configurations
            config = {
                'source_tables': {},
                'staging_tables': {},
                'target_tables': {}
            }
            
            # Process tables in batches to avoid memory issues
            batch_size = 50
            for i in range(0, len(tables), batch_size):
                batch = tables[i:i + batch_size]
                logger.info(f"Processing tables {i+1} to {min(i+batch_size, len(tables))}...")
                
                for table_name in batch:
                    try:
                        table_config = generate_smart_table_config(table_name, schemas[table_name], table_analysis)
                        if table_config:
                            config['source_tables'][table_name] = table_config['source']
                            config['staging_tables'][table_name] = table_config['staging']
                            config['target_tables'][table_name] = table_config['target']
                            progress_tracker.update(table_name, success=True)
                        else:
                            progress_tracker.update(table_name, success=False)
                    except Exception as e:
                        logger.error(f"Error processing table {table_name}: {str(e)}")
                        progress_tracker.update(table_name, success=False)
                    
                    pbar.update(1)
                    
                    # Log progress every 10 tables
                    if progress_tracker.processed_tables % 10 == 0:
                        progress = progress_tracker.get_progress()
                        logger.info(
                            f"Progress: {progress['processed']}/{progress['total']} tables "
                            f"({progress['success_rate']:.1f}% success) - "
                            f"Elapsed: {progress['elapsed_time']:.1f}s - "
                            f"Remaining: {progress['estimated_remaining']:.1f}s"
                        )
        
        # Validate configuration
        logger.info("Validating generated configuration...")
        issues = validate_generated_config(config)
        if issues:
            logger.warning("Configuration validation issues found:")
            for issue in issues:
                logger.warning(f"- {issue}")
        
        # Add ETL status tables
        logger.info("Adding ETL status tables...")
        etl_tables = ['etl_load_status', 'etl_transform_status']
        for table in etl_tables:
            config['source_tables'][table] = {
                'incremental_column': '_updated_at',
                'batch_size': 100,
                'is_modeled': True,
                'validation_rules': [
                    {
                        'name': 'not_null',
                        'columns': ['id', 'table_name', 'last_extracted', 'extraction_status']
                    }
                ],
                'dependencies': []
            }
            
            config['staging_tables'][table] = {
                'schema': 'staging',
                'indexes': [
                    {'columns': ['id'], 'type': 'primary'},
                    {'columns': ['table_name'], 'type': 'btree'},
                    {'columns': ['_updated_at'], 'type': 'btree'}
                ],
                'cleanup_after_load': False
            }
            
            config['target_tables'][table] = {
                'schema': 'analytics',
                'indexes': config['staging_tables'][table]['indexes'],
                'partitioning': {
                    'type': 'range',
                    'column': '_updated_at',
                    'interval': '1 month'
                }
            }
        
        # Write to YAML file
        output_path = os.path.join('etl_pipeline', 'config', 'tables.yaml')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        
        # Generate and display summary report
        generate_summary_report(progress_tracker, config, output_path)
        
    except Exception as e:
        logger.error(f"Error generating configuration: {str(e)}")
        raise

if __name__ == "__main__":
    main() 