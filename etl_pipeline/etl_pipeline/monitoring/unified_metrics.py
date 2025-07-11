"""
Unified Metrics Collection for ETL Pipeline

This module consolidates all metrics functionality from the three fragmented
implementations into a single, comprehensive metrics system.

CONSOLIDATED FROM:
1. core/metrics.py - Basic in-memory metrics collection (ACTIVELY USED)
2. monitoring/metrics.py - REMOVED (consolidated into unified_metrics.py)
3. monitoring/metrics_collector.py - Advanced implementation with database persistence (UNUSED)

FEATURES:
- Real-time metrics collection during pipeline execution
- Database persistence for historical analysis
- Pipeline status reporting for CLI and monitoring
- Comprehensive error tracking and success rates
- Table-level and pipeline-level metrics
- Configurable retention and cleanup policies

USAGE:
    # Production usage with Settings injection (unified interface)
    from etl_pipeline.config import get_settings
    settings = get_settings()
    metrics = create_metrics_collector(settings)
    metrics.start_pipeline()
    metrics.record_table_processed('patient', 1000, 30.5, True)
    metrics.end_pipeline()
    
    # Test usage with Settings injection (unified interface)
    from etl_pipeline.config import create_test_settings
    test_settings = create_test_settings()
    metrics = create_metrics_collector(test_settings)
    metrics.start_pipeline()
    metrics.record_table_processed('patient', 100, 5.2, True)
    metrics.end_pipeline()
    
    # Direct usage with Settings injection (unified interface)
    settings = get_settings()
    metrics = UnifiedMetricsCollector(settings=settings)  # Environment determined by settings
    
    # Status reporting
    status = metrics.get_pipeline_status()
    table_status = metrics.get_table_status('patient')
    
    # Persistence
    metrics.save_metrics()
    metrics.cleanup_old_metrics(retention_days=30)
"""

import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine


from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.config.settings import Settings

logger = logging.getLogger(__name__)

class UnifiedMetricsCollector:
    """
    Unified metrics collector that combines real-time collection,
    database persistence, and status reporting capabilities.
    
    Follows connection architecture guidelines with explicit environment separation.
    """
    
    def __init__(self, settings: Settings, 
                 enable_persistence: bool = True):
        """
        Initialize unified metrics collector.
        
        Args:
            settings: Settings instance for connection configuration (required)
            enable_persistence: Whether to enable database persistence
        """
        self.settings = settings
        
        # Get analytics engine using ConnectionFactory (following architecture guidelines)
        try:
            self.analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
            logger.info("Using unified analytics connection for metrics persistence")
        except Exception as e:
            logger.error(f"Failed to create analytics connection: {str(e)}")
            self.analytics_engine = None
        
        self.enable_persistence = enable_persistence and self.analytics_engine is not None
        
        # Initialize metrics storage
        self.reset_metrics()
        
        # Initialize database if persistence is enabled
        if self.enable_persistence:
            self._initialize_metrics_table()
    
    def _get_analytics_connection(self) -> Engine:
        """
        Get the appropriate analytics connection based on environment.
        
        Returns:
            SQLAlchemy engine for analytics database
            
        Raises:
            ValueError: If unable to create analytics connection
        """
        try:
            # Use unified interface with settings injection
            return ConnectionFactory.get_analytics_raw_connection(self.settings)
        except Exception as e:
            logger.error(f"Failed to create analytics connection: {str(e)}")
            raise ValueError(f"Unable to create analytics connection: {str(e)}") from e
    
    def reset_metrics(self):
        """Reset metrics to initial state."""
        self.metrics = {
            'pipeline_id': f"pipeline_{int(time.time())}",
            'start_time': None,
            'end_time': None,
            'total_time': None,
            'tables_processed': 0,
            'total_rows_processed': 0,
            'errors': [],
            'table_metrics': {},
            'status': 'idle'
        }
        self.start_time = None
    
    def start_pipeline(self):
        """Start pipeline timing and metrics collection."""
        self.start_time = time.time()
        self.metrics['start_time'] = datetime.now()
        self.metrics['status'] = 'running'
        logger.info("Pipeline metrics collection started")
    
    def end_pipeline(self) -> Dict[str, Any]:
        """
        End pipeline timing and return final metrics.
        
        Returns:
            Dict containing final pipeline metrics
        """
        if self.start_time:
            self.metrics['end_time'] = datetime.now()
            self.metrics['total_time'] = time.time() - self.start_time
            self.metrics['status'] = 'completed' if len(self.metrics['errors']) == 0 else 'failed'
        
        logger.info(f"Pipeline completed: {self.metrics['tables_processed']} tables, "
                   f"{self.metrics['total_rows_processed']} rows, "
                   f"{len(self.metrics['errors'])} errors")
        
        return self.metrics
    
    def record_table_processed(self, table_name: str, rows_processed: int, 
                              processing_time: float, success: bool, 
                              error: Optional[str] = None):
        """
        Record metrics for a single table.
        
        Args:
            table_name: Name of the table
            rows_processed: Number of rows processed
            processing_time: Time taken to process (seconds)
            success: Whether processing was successful
            error: Error message if any
        """
        self.metrics['tables_processed'] += 1
        self.metrics['total_rows_processed'] += rows_processed
        
        table_metric = {
            'table_name': table_name,
            'rows_processed': rows_processed,
            'processing_time': processing_time,
            'success': success,
            'error': error,
            'timestamp': datetime.now(),
            'status': 'completed' if success else 'failed'
        }
        
        self.metrics['table_metrics'][table_name] = table_metric
        
        if not success and error:
            self.metrics['errors'].append({
                'table': table_name,
                'error': error,
                'timestamp': datetime.now()
            })
        
        logger.info(f"Table {table_name}: {rows_processed} rows in {processing_time:.2f}s "
                   f"({'success' if success else 'failed'})")
    
    def record_error(self, error: str, table_name: Optional[str] = None):
        """
        Record a general pipeline error.
        
        Args:
            error: Error message
            table_name: Optional table name associated with the error
        """
        error_record = {
            'timestamp': datetime.now(),
            'message': error,
            'table': table_name
        }
        self.metrics['errors'].append(error_record)
        logger.error(f"Pipeline error: {error}")
    
    def get_pipeline_status(self, table: Optional[str] = None) -> Dict[str, Any]:
        """
        Get current pipeline status for CLI and monitoring.
        
        Args:
            table: Optional table name to filter status for
            
        Returns:
            Dict containing pipeline status information
        """
        # Get current status
        status = {
            'last_update': datetime.now().isoformat(),
            'status': self.metrics['status'],
            'pipeline_id': self.metrics['pipeline_id'],
            'tables_processed': self.metrics['tables_processed'],
            'total_rows_processed': self.metrics['total_rows_processed'],
            'error_count': len(self.metrics['errors']),
            'tables': []
        }
        
        # Add table-specific status
        for table_name, table_metric in self.metrics['table_metrics'].items():
            if table is None or table_name == table:
                status['tables'].append({
                    'name': table_name,
                    'status': table_metric['status'],
                    'last_sync': table_metric['timestamp'].isoformat(),
                    'records_processed': table_metric['rows_processed'],
                    'processing_time': table_metric['processing_time'],
                    'error': table_metric['error']
                })
        
        # Filter by table if specified
        if table:
            status['tables'] = [t for t in status['tables'] if t['name'] == table]
        
        return status
    
    def get_table_status(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get status for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict containing table status or None if not found
        """
        return self.metrics['table_metrics'].get(table_name)
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """
        Get current pipeline statistics.
        
        Returns:
            Dict containing current pipeline statistics
        """
        total_tables = self.metrics['tables_processed']
        error_count = len(self.metrics['errors'])
        success_count = total_tables - error_count
        
        return {
            'tables_processed': total_tables,
            'total_rows_processed': self.metrics['total_rows_processed'],
            'error_count': error_count,
            'success_count': success_count,
            'success_rate': (success_count / max(1, total_tables)) * 100,
            'total_time': self.metrics.get('total_time'),
            'status': self.metrics['status']
        }
    
    def save_metrics(self) -> bool:
        """
        Save metrics to the analytics database.
        
        Returns:
            True if successful, False otherwise
        """
        if not self.enable_persistence or self.analytics_engine is None:
            logger.warning("Database persistence not enabled or analytics engine not available")
            return False
        
        try:
            with self.analytics_engine.connect() as conn:
                # Insert pipeline metrics
                conn.execute(text("""
                    INSERT INTO etl_pipeline_metrics (
                        pipeline_id,
                        start_time,
                        end_time,
                        total_time,
                        tables_processed,
                        total_rows_processed,
                        success,
                        error_count,
                        status,
                        metrics_json
                    ) VALUES (
                        :pipeline_id,
                        :start_time,
                        :end_time,
                        :total_time,
                        :tables_processed,
                        :total_rows_processed,
                        :success,
                        :error_count,
                        :status,
                        :metrics_json
                    )
                """), {
                    'pipeline_id': self.metrics['pipeline_id'],
                    'start_time': self.metrics['start_time'],
                    'end_time': self.metrics.get('end_time'),
                    'total_time': self.metrics.get('total_time'),
                    'tables_processed': self.metrics['tables_processed'],
                    'total_rows_processed': self.metrics['total_rows_processed'],
                    'success': len(self.metrics['errors']) == 0,
                    'error_count': len(self.metrics['errors']),
                    'status': self.metrics['status'],
                    'metrics_json': json.dumps(self.metrics, default=str)
                })
                
                # Insert table-level metrics
                for table_name, table_metric in self.metrics['table_metrics'].items():
                    conn.execute(text("""
                        INSERT INTO etl_table_metrics (
                            pipeline_id,
                            table_name,
                            rows_processed,
                            processing_time,
                            success,
                            error,
                            timestamp
                        ) VALUES (
                            :pipeline_id,
                            :table_name,
                            :rows_processed,
                            :processing_time,
                            :success,
                            :error,
                            :timestamp
                        )
                    """), {
                        'pipeline_id': self.metrics['pipeline_id'],
                        'table_name': table_name,
                        'rows_processed': table_metric['rows_processed'],
                        'processing_time': table_metric['processing_time'],
                        'success': table_metric['success'],
                        'error': table_metric['error'],
                        'timestamp': table_metric['timestamp']
                    })
                
                conn.commit()
                logger.info("Metrics saved successfully to database")
                return True
                
        except Exception as e:
            logger.error(f"Failed to save metrics: {str(e)}")
            return False
    
    def cleanup_old_metrics(self, retention_days: int = 30) -> bool:
        """
        Clean up old metrics data.
        
        Args:
            retention_days: Number of days to retain metrics data
            
        Returns:
            True if successful, False otherwise
        """
        if not self.enable_persistence or self.analytics_engine is None:
            return False
        
        try:
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            with self.analytics_engine.connect() as conn:
                # Delete old pipeline metrics
                result = conn.execute(text("""
                    DELETE FROM etl_pipeline_metrics 
                    WHERE start_time < :cutoff_date
                """), {'cutoff_date': cutoff_date})
                
                # Delete old table metrics
                conn.execute(text("""
                    DELETE FROM etl_table_metrics 
                    WHERE timestamp < :cutoff_date
                """), {'cutoff_date': cutoff_date})
                
                conn.commit()
                logger.info(f"Cleaned up metrics older than {retention_days} days")
                return True
                
        except Exception as e:
            logger.error(f"Failed to cleanup old metrics: {str(e)}")
            return False
    
    def _initialize_metrics_table(self):
        """Initialize metrics tables in the analytics database."""
        if self.analytics_engine is None:
            logger.error("Analytics engine not available for table initialization")
            return
            
        try:
            with self.analytics_engine.connect() as conn:
                # Create pipeline metrics table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS etl_pipeline_metrics (
                        id SERIAL PRIMARY KEY,
                        pipeline_id VARCHAR(50) NOT NULL,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        total_time FLOAT,
                        tables_processed INTEGER DEFAULT 0,
                        total_rows_processed INTEGER DEFAULT 0,
                        success BOOLEAN DEFAULT true,
                        error_count INTEGER DEFAULT 0,
                        status VARCHAR(20) DEFAULT 'idle',
                        metrics_json JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                # Create table metrics table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS etl_table_metrics (
                        id SERIAL PRIMARY KEY,
                        pipeline_id VARCHAR(50) NOT NULL,
                        table_name VARCHAR(100) NOT NULL,
                        rows_processed INTEGER DEFAULT 0,
                        processing_time FLOAT,
                        success BOOLEAN DEFAULT true,
                        error TEXT,
                        timestamp TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                # Create indexes for better performance
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_pipeline_id 
                    ON etl_pipeline_metrics(pipeline_id)
                """))
                
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_start_time 
                    ON etl_pipeline_metrics(start_time)
                """))
                
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_table_metrics_pipeline_id 
                    ON etl_table_metrics(pipeline_id)
                """))
                
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_table_metrics_table_name 
                    ON etl_table_metrics(table_name)
                """))
                
                conn.commit()
                logger.info("Metrics tables initialized successfully")
                
        except Exception as e:
            logger.error(f"Failed to initialize metrics tables: {str(e)}")
            self.enable_persistence = False

# Factory functions following connection architecture guidelines
def create_metrics_collector(settings: Settings, enable_persistence: bool = True) -> UnifiedMetricsCollector:
    """
    Create a metrics collector with appropriate connection following architecture guidelines.
    
    Args:
        settings: Settings instance for connection configuration (required)
        enable_persistence: Whether to enable database persistence
        
    Returns:
        UnifiedMetricsCollector instance with appropriate connection
    """
    return UnifiedMetricsCollector(
        settings=settings,
        enable_persistence=enable_persistence
    ) 