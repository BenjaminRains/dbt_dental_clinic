#!/usr/bin/env python3
"""
Connection Usage Analysis Script

This script analyzes connection usage patterns in the ETL pipeline to determine
how many database connections are being created when processing 400+ tables
across 3 different databases.

Features:
- Tracks connection creation and disposal
- Monitors connection pool usage
- Analyzes connection patterns by database type
- Provides recommendations for optimization
- Generates connection usage reports

Usage:
    python etl_pipeline/scripts/analyze_connection_usage.py
"""

import os
import sys
import time
import logging
import threading
from datetime import datetime
from typing import Dict, List, Optional, Set
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from pathlib import Path

# Add etl_pipeline to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from etl_pipeline.config import get_settings, DatabaseType, PostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
# Removed find_latest_tables_config import - we only use tables.yml with metadata versioning

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ConnectionEvent:
    """Represents a connection creation or disposal event."""
    timestamp: float
    event_type: str  # 'created' or 'disposed'
    database_type: str
    database_name: str
    schema: Optional[str] = None
    thread_id: int = field(default_factory=lambda: threading.get_ident())
    connection_id: Optional[str] = None

@dataclass
class ConnectionStats:
    """Statistics for connection usage."""
    total_connections_created: int = 0
    total_connections_disposed: int = 0
    active_connections: int = 0
    max_concurrent_connections: int = 0
    connection_duration_avg: float = 0.0
    connection_duration_total: float = 0.0
    database_breakdown: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    schema_breakdown: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

class ConnectionTracker:
    """Tracks database connection usage throughout the ETL pipeline."""
    
    def __init__(self):
        self.events: List[ConnectionEvent] = []
        self.active_connections: Set[str] = set()
        self.connection_start_times: Dict[str, float] = {}
        self.stats = ConnectionStats()
        self.lock = threading.Lock()
        
    def track_connection_created(self, database_type: str, database_name: str, 
                               schema: Optional[str] = None, connection_id: Optional[str] = None):
        """Track when a connection is created."""
        with self.lock:
            event = ConnectionEvent(
                timestamp=time.time(),
                event_type='created',
                database_type=database_type,
                database_name=database_name,
                schema=schema,
                connection_id=connection_id or f"{database_type}_{database_name}_{len(self.events)}"
            )
            self.events.append(event)
            if event.connection_id:
                self.active_connections.add(event.connection_id)
                self.connection_start_times[event.connection_id] = event.timestamp
            
            # Update stats
            self.stats.total_connections_created += 1
            self.stats.active_connections = len(self.active_connections)
            self.stats.max_concurrent_connections = max(
                self.stats.max_concurrent_connections, 
                self.stats.active_connections
            )
            
            # Update breakdowns
            db_key = f"{database_type}_{database_name}"
            self.stats.database_breakdown[db_key] += 1
            if schema:
                self.stats.schema_breakdown[schema] += 1
            
            logger.debug(f"Connection created: {event.connection_id} ({database_type}/{database_name})")
    
    def track_connection_disposed(self, database_type: str, database_name: str,
                                schema: Optional[str] = None, connection_id: Optional[str] = None):
        """Track when a connection is disposed."""
        with self.lock:
            event = ConnectionEvent(
                timestamp=time.time(),
                event_type='disposed',
                database_type=database_type,
                database_name=database_name,
                schema=schema,
                connection_id=connection_id or f"{database_type}_{database_name}_{len(self.events)}"
            )
            self.events.append(event)
            
            # Calculate duration if we have start time
            if event.connection_id in self.connection_start_times:
                duration = event.timestamp - self.connection_start_times[event.connection_id]
                self.stats.connection_duration_total += duration
                self.stats.connection_duration_avg = (
                    self.stats.connection_duration_total / 
                    (self.stats.total_connections_disposed + 1)
                )
                del self.connection_start_times[event.connection_id]
            
            if event.connection_id in self.active_connections:
                self.active_connections.remove(event.connection_id)
            
            # Update stats
            self.stats.total_connections_disposed += 1
            self.stats.active_connections = len(self.active_connections)
            
            logger.debug(f"Connection disposed: {event.connection_id} ({database_type}/{database_name})")
    
    def get_connection_summary(self) -> Dict:
        """Get a summary of connection usage."""
        with self.lock:
            # Calculate time-based stats
            if self.events:
                start_time = min(event.timestamp for event in self.events)
                end_time = max(event.timestamp for event in self.events)
                total_duration = end_time - start_time
            else:
                total_duration = 0
            
            # Connection rate
            connections_per_minute = (
                self.stats.total_connections_created / (total_duration / 60)
                if total_duration > 0 else 0
            )
            
            return {
                'total_connections_created': self.stats.total_connections_created,
                'total_connections_disposed': self.stats.total_connections_disposed,
                'active_connections': self.stats.active_connections,
                'max_concurrent_connections': self.stats.max_concurrent_connections,
                'average_connection_duration': self.stats.connection_duration_avg,
                'connections_per_minute': connections_per_minute,
                'total_duration_minutes': total_duration / 60,
                'database_breakdown': dict(self.stats.database_breakdown),
                'schema_breakdown': dict(self.stats.schema_breakdown),
                'connection_efficiency': (
                    self.stats.total_connections_disposed / 
                    max(self.stats.total_connections_created, 1)
                )
            }

class ConnectionAnalyzer:
    """Analyzes connection usage patterns and provides optimization recommendations."""
    
    def __init__(self):
        self.tracker = ConnectionTracker()
        self.settings = get_settings()
        
    def analyze_simple_mysql_replicator(self, table_names: Optional[List[str]] = None) -> Dict:
        """Analyze connection usage in SimpleMySQLReplicator."""
        logger.info("Analyzing SimpleMySQLReplicator connection usage...")
        
        # Get table configurations
        config_dir = os.path.join(os.path.dirname(__file__), '..', 'config')
        tables_config_path = os.path.join(config_dir, 'tables.yml')
        if not os.path.exists(tables_config_path):
            logger.error("No tables configuration found")
            return {}
        
        # Load table configurations
        import yaml
        with open(tables_config_path, 'r') as f:
            tables_config = yaml.safe_load(f)
        
        # Use provided table names or all tables
        if table_names is None:
            table_names = list(tables_config.get('tables', {}).keys())
        
        logger.info(f"Analyzing connection usage for {len(table_names)} tables")
        
        # Create replicator with connection tracking
        replicator = self._create_tracked_replicator()
        
        # Process each table and track connections
        results = {
            'tables_processed': 0,
            'tables_successful': 0,
            'tables_failed': 0,
            'connection_summary': None
        }
        
        for table_name in table_names:
            try:
                logger.info(f"Processing table: {table_name}")
                
                # Track connection creation for this table
                self._track_replicator_connections(replicator, table_name)
                
                # Attempt to copy table (this will create connections)
                success = replicator.copy_table(table_name, force_full=False)
                
                if success:
                    results['tables_successful'] += 1
                else:
                    results['tables_failed'] += 1
                
                results['tables_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing table {table_name}: {str(e)}")
                results['tables_failed'] += 1
                results['tables_processed'] += 1
        
        # Get connection summary
        results['connection_summary'] = self.tracker.get_connection_summary()
        
        return results
    
    def _create_tracked_replicator(self) -> SimpleMySQLReplicator:
        """Create a SimpleMySQLReplicator with connection tracking."""
        # Override connection creation methods to track usage
        original_get_source_connection = ConnectionFactory.get_source_connection
        original_get_replication_connection = ConnectionFactory.get_replication_connection
        
        def tracked_get_source_connection(settings):
            engine = original_get_source_connection(settings)
            source_config = settings.get_source_connection_config()
            self.tracker.track_connection_created(
                'mysql', 
                source_config.get('database', 'opendental'),
                connection_id=f"source_{len(self.tracker.events)}"
            )
            return engine
        
        def tracked_get_replication_connection(settings):
            engine = original_get_replication_connection(settings)
            repl_config = settings.get_replication_connection_config()
            self.tracker.track_connection_created(
                'mysql',
                repl_config.get('database', 'opendental_replication'),
                connection_id=f"replication_{len(self.tracker.events)}"
            )
            return engine
        
        # Temporarily replace methods
        ConnectionFactory.get_source_connection = tracked_get_source_connection
        ConnectionFactory.get_replication_connection = tracked_get_replication_connection
        
        try:
            return SimpleMySQLReplicator(settings=self.settings)
        finally:
            # Restore original methods
            ConnectionFactory.get_source_connection = original_get_source_connection
            ConnectionFactory.get_replication_connection = original_get_replication_connection
    
    def _track_replicator_connections(self, replicator: SimpleMySQLReplicator, table_name: str):
        """Track connections used by the replicator for a specific table."""
        # This method would track the actual connection usage during table processing
        # For now, we're tracking at the engine creation level
        pass
    
    def analyze_connection_patterns(self) -> Dict:
        """Analyze connection patterns and provide recommendations."""
        summary = self.tracker.get_connection_summary()
        
        analysis = {
            'summary': summary,
            'recommendations': [],
            'potential_issues': [],
            'optimization_opportunities': []
        }
        
        # Analyze connection efficiency
        efficiency = summary.get('connection_efficiency', 0)
        if efficiency < 0.9:
            analysis['potential_issues'].append(
                f"Low connection disposal rate ({efficiency:.2%}). "
                "Connections may not be properly closed."
            )
        
        # Analyze connection rate
        connections_per_minute = summary.get('connections_per_minute', 0)
        if connections_per_minute > 10:
            analysis['potential_issues'].append(
                f"High connection creation rate ({connections_per_minute:.1f}/min). "
                "Consider connection pooling or reuse."
            )
        
        # Analyze concurrent connections
        max_concurrent = summary.get('max_concurrent_connections', 0)
        if max_concurrent > 20:
            analysis['potential_issues'].append(
                f"High concurrent connections ({max_concurrent}). "
                "May overwhelm database servers."
            )
        
        # Provide optimization recommendations
        if summary.get('total_connections_created', 0) > 100:
            analysis['recommendations'].extend([
                "Consider implementing connection pooling at the application level",
                "Use ConnectionManager for batch operations to reuse connections",
                "Implement connection caching for frequently accessed databases",
                "Consider using connection factories with connection reuse"
            ])
        
        # Database-specific recommendations
        db_breakdown = summary.get('database_breakdown', {})
        for db_key, count in db_breakdown.items():
            if count > 50:
                analysis['optimization_opportunities'].append(
                    f"High connection count for {db_key} ({count} connections). "
                    "Consider dedicated connection pools."
                )
        
        return analysis
    
    def generate_report(self, analysis_results: Dict) -> str:
        """Generate a comprehensive connection usage report."""
        report = []
        report.append("=" * 80)
        report.append("ETL PIPELINE CONNECTION USAGE ANALYSIS")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Summary statistics
        summary = analysis_results.get('connection_summary', {})
        report.append("CONNECTION SUMMARY:")
        report.append(f"  Total connections created: {summary.get('total_connections_created', 0)}")
        report.append(f"  Total connections disposed: {summary.get('total_connections_disposed', 0)}")
        report.append(f"  Active connections: {summary.get('active_connections', 0)}")
        report.append(f"  Max concurrent connections: {summary.get('max_concurrent_connections', 0)}")
        report.append(f"  Average connection duration: {summary.get('average_connection_duration', 0):.2f}s")
        report.append(f"  Connections per minute: {summary.get('connections_per_minute', 0):.1f}")
        report.append(f"  Connection efficiency: {summary.get('connection_efficiency', 0):.2%}")
        report.append("")
        
        # Database breakdown
        db_breakdown = summary.get('database_breakdown', {})
        if db_breakdown:
            report.append("CONNECTION BREAKDOWN BY DATABASE:")
            for db_key, count in sorted(db_breakdown.items(), key=lambda x: x[1], reverse=True):
                report.append(f"  {db_key}: {count} connections")
            report.append("")
        
        # Schema breakdown
        schema_breakdown = summary.get('schema_breakdown', {})
        if schema_breakdown:
            report.append("CONNECTION BREAKDOWN BY SCHEMA:")
            for schema, count in sorted(schema_breakdown.items(), key=lambda x: x[1], reverse=True):
                report.append(f"  {schema}: {count} connections")
            report.append("")
        
        # Processing results
        report.append("PROCESSING RESULTS:")
        report.append(f"  Tables processed: {analysis_results.get('tables_processed', 0)}")
        report.append(f"  Tables successful: {analysis_results.get('tables_successful', 0)}")
        report.append(f"  Tables failed: {analysis_results.get('tables_failed', 0)}")
        report.append("")
        
        # Recommendations
        recommendations = analysis_results.get('recommendations', [])
        if recommendations:
            report.append("RECOMMENDATIONS:")
            for i, rec in enumerate(recommendations, 1):
                report.append(f"  {i}. {rec}")
            report.append("")
        
        # Potential issues
        issues = analysis_results.get('potential_issues', [])
        if issues:
            report.append("POTENTIAL ISSUES:")
            for i, issue in enumerate(issues, 1):
                report.append(f"  {i}. {issue}")
            report.append("")
        
        # Optimization opportunities
        opportunities = analysis_results.get('optimization_opportunities', [])
        if opportunities:
            report.append("OPTIMIZATION OPPORTUNITIES:")
            for i, opp in enumerate(opportunities, 1):
                report.append(f"  {i}. {opp}")
            report.append("")
        
        report.append("=" * 80)
        
        return "\n".join(report)

def main():
    """Main function to run connection analysis."""
    logger.info("Starting ETL pipeline connection usage analysis...")
    
    # Create analyzer
    analyzer = ConnectionAnalyzer()
    
    # Analyze connection patterns
    logger.info("Analyzing connection patterns...")
    analysis = analyzer.analyze_connection_patterns()
    
    # Generate report
    report = analyzer.generate_report(analysis)
    
    # Print report
    print(report)
    
    # Save report to file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"etl_pipeline/logs/connection_analysis_{timestamp}.txt"
    
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, 'w') as f:
        f.write(report)
    
    logger.info(f"Connection analysis report saved to: {report_file}")
    
    return analysis

if __name__ == "__main__":
    main() 