"""
Script to generate tables.yaml configuration using schema discovery.
This script will:
1. Discover all tables in the source database
2. Analyze their schemas and characteristics
3. Find suitable incremental columns
4. Generate the appropriate configuration

Uses the SchemaDiscovery class for robust schema analysis.
"""
import os
import yaml
from typing import Dict, List, Set, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.core.connections import ConnectionFactory
import logging
from collections import defaultdict
from tqdm import tqdm
import time
from datetime import datetime
import json
from tabulate import tabulate
import networkx as nx

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

# Constants for filtering tables
EXCLUDED_PATTERNS = ['tmp_', 'temp_', 'backup_', '#', 'test_']

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

def analyze_table_relationships(schema_discovery: SchemaDiscovery, tables: List[str]) -> Dict[str, Dict]:
    """Analyze table relationships using SchemaDiscovery."""
    table_metrics = {}
    
    # Build relationship graph from foreign keys
    graph = nx.DiGraph()
    
    for table in tables:
        try:
            schema_info = schema_discovery.get_table_schema(table)
            foreign_keys = schema_info['foreign_keys']
            
            # Initialize metrics
            table_metrics[table] = {
                'referenced_by_count': 0,
                'references_count': len(foreign_keys),
                'pagerank_score': 0.0,
                'is_core_reference': False
            }
            
            # Add edges to graph
            for fk in foreign_keys:
                if fk['referenced_table'] in tables:
                    graph.add_edge(table, fk['referenced_table'])
                    
        except Exception as e:
            logger.warning(f"Could not analyze relationships for {table}: {str(e)}")
            table_metrics[table] = {
                'referenced_by_count': 0,
                'references_count': 0,
                'pagerank_score': 0.0,
                'is_core_reference': False
            }
    
    # Calculate PageRank if we have relationships
    if graph.edges():
        pagerank_scores = nx.pagerank(graph)
        for table in tables:
            if table in graph:
                table_metrics[table].update({
                    'referenced_by_count': graph.in_degree(table),
                    'pagerank_score': pagerank_scores.get(table, 0.0)
                })
    
    # Identify core reference tables
    if table_metrics:
        reference_counts = [m['referenced_by_count'] for m in table_metrics.values()]
        if reference_counts:
            threshold = max(reference_counts) * 0.3  # Top 30% by references
            for table, metrics in table_metrics.items():
                metrics['is_core_reference'] = metrics['referenced_by_count'] >= threshold
    
    return table_metrics

def analyze_table_usage(schema_discovery: SchemaDiscovery, tables: List[str]) -> Dict[str, Dict]:
    """Analyze table usage patterns using SchemaDiscovery."""
    usage_metrics = {}
    
    for table in tables:
        try:
            # Get size information
            size_info = schema_discovery.get_table_size_info(table)
            
            # Get schema information for column analysis
            schema_info = schema_discovery.get_table_schema(table)
            columns = schema_info['columns']
            
            # Analyze column patterns
            has_audit_columns = any(
                col['name'].lower() in ['datetestamp', 'datemodified', 'secdatetsedit'] 
                for col in columns
            )
            
            has_timestamp_columns = any(
                'timestamp' in col['type'].lower() or 'datetime' in col['type'].lower()
                for col in columns
            )
            
            has_soft_delete = any(
                col['name'].lower() in ['isdeleted', 'ishidden', 'isarchived']
                for col in columns
            )
            
            # Determine update frequency based on auto_increment gap
            auto_increment_col = next((col for col in columns if 'auto_increment' in (col['extra'] or '').lower()), None)
            update_frequency = 'low'
            
            if auto_increment_col and schema_info['metadata']['auto_increment'] and size_info['row_count']:
                auto_inc_value = schema_info['metadata']['auto_increment']
                row_count = size_info['row_count']
                if auto_inc_value > row_count:
                    churn_rate = (auto_inc_value - row_count) / row_count * 100
                    if churn_rate > 50:
                        update_frequency = 'high'
                    elif churn_rate > 10:
                        update_frequency = 'medium'
            
            usage_metrics[table] = {
                'row_count': size_info['row_count'],
                'size_mb': size_info['total_size_mb'],
                'has_audit_columns': has_audit_columns,
                'has_timestamp_columns': has_timestamp_columns,
                'has_soft_delete': has_soft_delete,
                'update_frequency': update_frequency
            }
            
        except Exception as e:
            logger.warning(f"Could not analyze usage for {table}: {str(e)}")
            usage_metrics[table] = {
                'row_count': 0,
                'size_mb': 0.0,
                'has_audit_columns': False,
                'has_timestamp_columns': False,
                'has_soft_delete': False,
                'update_frequency': 'low'
            }
    
    return usage_metrics

def determine_table_importance(relationships: Dict[str, Dict], usage: Dict[str, Dict]) -> Dict[str, str]:
    """Determine table importance based purely on discovered data patterns."""
    importance = {}
    
    # Calculate scores for all tables first
    table_scores = {}
    for table in relationships.keys():
        rel_metrics = relationships[table]
        usage_metrics = usage[table]
        
        # Calculate importance score based on discovered characteristics
        score = 0
        
        # Relationship-based scoring (tables that are heavily referenced are important)
        score += rel_metrics['referenced_by_count'] * 3  # High weight for being referenced
        score += rel_metrics['pagerank_score'] * 100     # PageRank indicates centrality
        if rel_metrics['is_core_reference']:
            score += 50  # Core reference tables are important
        
        # Usage-based scoring (tables with audit/tracking features are important)
        if usage_metrics['has_audit_columns']:
            score += 40  # Audit columns indicate important business data
        if usage_metrics['has_timestamp_columns']:
            score += 25  # Timestamp columns indicate activity tracking
        if usage_metrics['update_frequency'] == 'high':
            score += 30  # Frequently updated tables are active/important
        
        # Size-based scoring (larger tables likely contain more business data)
        if usage_metrics['size_mb'] > 500:
            score += 25  # Very large tables
        elif usage_metrics['size_mb'] > 100:
            score += 15  # Large tables
        elif usage_metrics['size_mb'] > 10:
            score += 5   # Medium tables
        
        # Row count scoring (tables with many rows likely important)
        if usage_metrics['row_count'] > 100000:
            score += 20  # Very high row count
        elif usage_metrics['row_count'] > 10000:
            score += 10  # High row count
        elif usage_metrics['row_count'] > 1000:
            score += 5   # Medium row count
        
        table_scores[table] = score
    
    # Determine importance levels based on score distribution
    scores = list(table_scores.values())
    if scores:
        scores.sort(reverse=True)
        
        # Use percentiles to determine thresholds dynamically
        total_tables = len(scores)
        critical_threshold = scores[min(int(total_tables * 0.05), total_tables-1)] if total_tables > 0 else 100  # Top 5%
        important_threshold = scores[min(int(total_tables * 0.20), total_tables-1)] if total_tables > 0 else 50   # Top 20%
        audit_threshold = scores[min(int(total_tables * 0.40), total_tables-1)] if total_tables > 0 else 25      # Top 40%
        
        # Minimum thresholds to ensure quality
        critical_threshold = max(critical_threshold, 80)   # Minimum score for critical
        important_threshold = max(important_threshold, 40)  # Minimum score for important
        audit_threshold = max(audit_threshold, 15)          # Minimum score for audit
    else:
        critical_threshold = 80
        important_threshold = 40
        audit_threshold = 15
    
    # Assign importance levels
    for table, score in table_scores.items():
        rel_metrics = relationships[table]
        usage_metrics = usage[table]
        
        if score >= critical_threshold:
            importance[table] = 'critical'
        elif score >= important_threshold:
            importance[table] = 'important'
        elif (score >= audit_threshold or 
              usage_metrics['has_audit_columns'] or 
              usage_metrics['has_timestamp_columns']):
            importance[table] = 'audit'
        elif rel_metrics['is_core_reference']:
            importance[table] = 'reference'
        else:
            importance[table] = 'standard'
    
    return importance

def get_recommended_batch_size(usage_metrics: Dict) -> int:
    """Determine optimal batch size based on table characteristics."""
    size_mb = usage_metrics['size_mb']
    update_freq = usage_metrics['update_frequency']
    
    if size_mb < 1:
        return 5000
    elif size_mb < 100:
        return 2000 if update_freq == 'high' else 3000
    elif size_mb < 1000:
        return 1000 if update_freq == 'high' else 2000
    else:
        return 500 if update_freq == 'high' else 1000

def get_extraction_strategy(usage_metrics: Dict, importance: str) -> str:
    """Determine optimal extraction strategy."""
    size_mb = usage_metrics['size_mb']
    update_freq = usage_metrics['update_frequency']
    
    if size_mb < 1:
        return 'full_table'
    elif size_mb < 100:
        return 'incremental'
    elif size_mb < 1000:
        return 'chunked_incremental' if update_freq == 'high' else 'incremental'
    else:
        return 'streaming_incremental'

def generate_smart_table_config(
    table_name: str, 
    schema_discovery: SchemaDiscovery,
    relationships: Dict[str, Dict],
    usage: Dict[str, Dict],
    importance: Dict[str, str]
) -> Dict:
    """Generate configuration using SchemaDiscovery and analysis."""
    try:
        # Get schema information using SchemaDiscovery
        schema_info = schema_discovery.get_table_schema(table_name)
        incremental_columns = schema_discovery.get_incremental_columns(table_name)
        size_info = schema_discovery.get_table_size_info(table_name)
        
        # Get analysis data
        rel_metrics = relationships.get(table_name, {})
        usage_metrics = usage.get(table_name, {})
        table_importance = importance.get(table_name, 'standard')
        
        # Find the best incremental column
        best_incremental = None
        if incremental_columns:
            best_incremental = incremental_columns[0]['column_name']
        
        # Get dependencies from foreign keys
        dependencies = [fk['referenced_table'] for fk in schema_info['foreign_keys']]
        
        # Generate source table config
        source_config = {
            'incremental_column': best_incremental,
            'batch_size': get_recommended_batch_size(usage_metrics),
            'extraction_strategy': get_extraction_strategy(usage_metrics, table_importance),
            'is_modeled': table_importance in ['critical', 'important'],
            'table_importance': table_importance,
            'estimated_size_mb': size_info['total_size_mb'],
            'estimated_rows': size_info['row_count'],
            'dependencies': dependencies,
            'monitoring': {
                'alert_on_failure': table_importance in ['critical', 'important'],
                'max_extraction_time_minutes': min(180, max(5, int(size_info['total_size_mb'] / 10))),
                'data_quality_threshold': 0.99 if table_importance == 'critical' else 0.95
            }
        }
        
        # Generate staging config
        staging_config = {
            'schema': 'staging',
            'indexes': [
                {
                    'columns': idx['columns'],
                    'type': 'primary' if idx['is_unique'] and idx['name'] == 'PRIMARY' else 'btree'
                }
                for idx in schema_info['indexes']
            ],
            'cleanup_after_load': not usage_metrics.get('has_audit_columns', False)
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
        logger.error(f"Error generating config for {table_name}: {str(e)}")
        return None

def save_comprehensive_json_analysis(
    relationships: Dict[str, Dict], 
    usage: Dict[str, Dict], 
    importance: Dict[str, str],
    schema_discovery: SchemaDiscovery,
    tables: List[str]
) -> str:
    """Save comprehensive analysis data to JSON file for all tables."""
    from datetime import datetime
    import json
    
    logger.info("Generating comprehensive JSON analysis...")
    
    # Build comprehensive analysis data
    analysis_data = {
        'metadata': {
            'timestamp': datetime.now().isoformat(),
            'total_tables': len(tables),
            'analysis_version': '2.0',
            'source_database': os.getenv('SOURCE_MYSQL_DB'),
            'generated_by': 'generate_table_config.py'
        },
        'relationships': relationships,
        'usage': usage,
        'importance': importance,
        'detailed_schemas': {},
        'summary_statistics': {}
    }
    
    # Add detailed schema information for a sample of important tables
    important_tables = [table for table, imp in importance.items() 
                       if imp in ['critical', 'important']][:10]  # Limit to 10 for file size
    
    for table in important_tables:
        try:
            schema_info = schema_discovery.get_table_schema(table)
            incremental_cols = schema_discovery.get_incremental_columns(table)
            
            analysis_data['detailed_schemas'][table] = {
                'schema_info': schema_info,
                'incremental_columns': incremental_cols
            }
        except Exception as e:
            logger.warning(f"Could not get detailed schema for {table}: {str(e)}")
    
    # Calculate summary statistics
    rel_stats = list(relationships.values())
    usage_stats = list(usage.values())
    
    analysis_data['summary_statistics'] = {
        'table_classifications': {
            'critical': sum(1 for imp in importance.values() if imp == 'critical'),
            'important': sum(1 for imp in importance.values() if imp == 'important'),
            'audit': sum(1 for imp in importance.values() if imp == 'audit'),
            'reference': sum(1 for imp in importance.values() if imp == 'reference'),
            'standard': sum(1 for imp in importance.values() if imp == 'standard'),
        },
        'relationship_metrics': {
            'max_referenced_by': max((r['referenced_by_count'] for r in rel_stats), default=0),
            'max_references': max((r['references_count'] for r in rel_stats), default=0),
            'total_core_references': sum(1 for r in rel_stats if r['is_core_reference']),
            'avg_pagerank': sum(r['pagerank_score'] for r in rel_stats) / len(rel_stats) if rel_stats else 0
        },
        'usage_metrics': {
            'total_size_mb': sum(u['size_mb'] for u in usage_stats),
            'total_rows': sum(u['row_count'] for u in usage_stats),
            'tables_with_audit_columns': sum(1 for u in usage_stats if u['has_audit_columns']),
            'tables_with_timestamps': sum(1 for u in usage_stats if u['has_timestamp_columns']),
            'tables_with_soft_delete': sum(1 for u in usage_stats if u['has_soft_delete']),
            'high_frequency_tables': sum(1 for u in usage_stats if u['update_frequency'] == 'high')
        },
        'top_tables_by_size': sorted(
            [(table, usage[table]['size_mb']) for table in tables],
            key=lambda x: x[1], reverse=True
        )[:20],
        'top_tables_by_rows': sorted(
            [(table, usage[table]['row_count']) for table in tables],
            key=lambda x: x[1], reverse=True
        )[:20]
    }
    
    # Save to JSON file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = f'etl_pipeline/logs/comprehensive_analysis_{timestamp}.json'
    
    with open(json_path, 'w') as f:
        json.dump(analysis_data, f, indent=2, default=str)
    
    logger.info(f"📊 Comprehensive analysis saved to: {json_path}")
    return json_path

def generate_summary_report(progress_tracker: ProgressTracker, config: Dict, output_path: str, json_path: str = None) -> None:
    """Generate a summary report of the configuration generation process."""
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
{f"Comprehensive analysis: {json_path}" if json_path else ""}
"""
    
    # Write report to file
    report_path = output_path.replace('.yaml', '_report.txt')
    with open(report_path, 'w') as f:
        f.write(report)
    
    # Print report to console
    logger.info("\n" + report)

def main():
    """Main function to generate tables.yaml configuration using SchemaDiscovery."""
    try:
        # Create logs directory if it doesn't exist
        os.makedirs('etl_pipeline/logs', exist_ok=True)
        
        # Get database connections
        source_engine = ConnectionFactory.get_source_connection()
        replication_engine = ConnectionFactory.get_replication_connection()
        
        # Initialize SchemaDiscovery
        source_db = os.getenv('SOURCE_MYSQL_DB')
        replication_db = os.getenv('REPLICATION_MYSQL_DB')
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            target_engine=replication_engine,
            source_db=source_db,
            target_db=replication_db
        )
        
        # Discover all tables
        logger.info("Discovering tables...")
        all_tables = schema_discovery.discover_all_tables()
        
        # Filter out excluded tables
        tables = [t for t in all_tables if not any(pattern in t.lower() for pattern in EXCLUDED_PATTERNS)]
        total_tables = len(tables)
        logger.info(f"Found {total_tables} tables to process (filtered from {len(all_tables)} total)")
        
        # Initialize progress tracker
        progress_tracker = ProgressTracker(total_tables)
        
        # Analyze table characteristics
        logger.info("Analyzing table relationships...")
        relationships = analyze_table_relationships(schema_discovery, tables)
        
        logger.info("Analyzing table usage patterns...")
        usage = analyze_table_usage(schema_discovery, tables)
        
        logger.info("Determining table importance...")
        importance = determine_table_importance(relationships, usage)
        
        # Generate configurations
        config = {
            'source_tables': {},
            'staging_tables': {},
            'target_tables': {}
        }
        
        # Process tables with progress tracking
        logger.info("Generating table configurations...")
        with tqdm(total=total_tables, desc="Generating configs") as pbar:
            for table_name in tables:
                try:
                    table_config = generate_smart_table_config(
                        table_name, schema_discovery, relationships, usage, importance
                    )
                    
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
                
                # Log progress every 50 tables
                if progress_tracker.processed_tables % 50 == 0:
                    progress = progress_tracker.get_progress()
                    logger.info(
                        f"Progress: {progress['processed']}/{progress['total']} tables "
                        f"({progress['success_rate']:.1f}% success)"
                    )
        
        # Save comprehensive JSON analysis
        json_path = save_comprehensive_json_analysis(
            relationships, usage, importance, schema_discovery, tables
        )
        
        # Write to YAML file
        output_path = os.path.join('etl_pipeline', 'config', 'tables.yaml')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        
        # Generate and display summary report
        generate_summary_report(progress_tracker, config, output_path, json_path)
        
        logger.info("✅ Configuration generation completed successfully!")
        
    except Exception as e:
        logger.error(f"Error generating configuration: {str(e)}")
        raise

if __name__ == "__main__":
    main() 