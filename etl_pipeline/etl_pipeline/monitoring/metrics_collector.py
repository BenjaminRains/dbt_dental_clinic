"""
Metrics Collector

Handles collection and reporting of ETL pipeline metrics,
including processing times, row counts, and error rates.

STATUS: REDUNDANT - Multiple Metrics Implementations
==================================================

This module is one of THREE different metrics implementations in the ETL pipeline,
creating confusion and redundancy. The metrics collection is fragmented across:

1. monitoring/metrics_collector.py (THIS FILE) - Advanced implementation with database persistence
2. core/metrics.py - Basic in-memory implementation (ACTIVELY USED)
3. monitoring/metrics.py - Mock implementation with PipelineMetrics class

CURRENT STATE:
- ✅ Advanced implementation with database persistence
- ✅ Comprehensive metrics collection (timing, rows, errors, table-level)
- ✅ SQLAlchemy integration for analytics database storage
- ✅ Proper error handling and logging
- ❌ REDUNDANT: Not imported or used anywhere in the codebase
- ❌ CONFLICTING: Three different metrics implementations exist
- ❌ UNTESTED: No tests or validation of this implementation
- ❌ ISOLATED: No integration with existing pipeline components

ACTIVE USAGE ANALYSIS:
- core/metrics.py: Used by orchestration modules (pipeline_orchestrator.py, table_processor.py)
- monitoring/metrics.py: Used by main.py and CLI commands
- monitoring/metrics_collector.py: NOT USED ANYWHERE

DEPENDENCIES:
- sqlalchemy (for database persistence)
- logging (for error handling)
- datetime, time (for timing metrics)

INTEGRATION POINTS:
- Analytics database: Creates and writes to etl_metrics table
- ETL Pipeline: Designed to integrate with pipeline processing
- Monitoring: Could integrate with health checks and alerts

DEVELOPMENT RECOMMENDATIONS:
1. CONSOLIDATE: Choose one metrics implementation and remove others
2. INTEGRATE: Connect chosen implementation to active pipeline components
3. ENHANCE: Add features from this implementation to the chosen one
4. TEST: Add comprehensive testing for metrics functionality
5. DOCUMENT: Clear documentation of metrics collection and storage

POTENTIAL ACTIONS:
- MERGE: Combine best features from all three implementations
- REPLACE: Replace core/metrics.py with this more advanced implementation
- REMOVE: Delete redundant implementations after consolidation
- ENHANCE: Add configuration-driven metrics collection

This file represents the most advanced metrics implementation but is currently
unused due to the fragmented state of metrics collection in the pipeline.
"""

import logging
import time
from datetime import datetime
from typing import Dict, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

class MetricsCollector:
    def __init__(self, analytics_engine: Engine):
        """
        Initialize metrics collector.
        
        Args:
            analytics_engine: SQLAlchemy engine for analytics database
        """
        self.analytics_engine = analytics_engine
        self.metrics = {}
        self.start_time = None
        
    def start_pipeline(self):
        """Start timing the pipeline."""
        self.start_time = time.time()
        self.metrics = {
            'start_time': datetime.now(),
            'tables_processed': 0,
            'total_rows_processed': 0,
            'errors': [],
            'table_metrics': {}
        }
        
    def record_table_metrics(self, table_name: str, 
                           rows_processed: int,
                           processing_time: float,
                           success: bool,
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
            'rows_processed': rows_processed,
            'processing_time': processing_time,
            'success': success,
            'error': error,
            'timestamp': datetime.now()
        }
        
        self.metrics['table_metrics'][table_name] = table_metric
        
        if not success:
            self.metrics['errors'].append({
                'table': table_name,
                'error': error,
                'timestamp': datetime.now()
            })
            
    def end_pipeline(self) -> Dict:
        """
        End pipeline timing and return final metrics.
        
        Returns:
            Dict containing final metrics
        """
        if self.start_time:
            self.metrics['end_time'] = datetime.now()
            self.metrics['total_time'] = time.time() - self.start_time
            
        return self.metrics
        
    def save_metrics(self):
        """
        Save metrics to the analytics database.
        
        ADVANCED FEATURE: This method provides database persistence for metrics,
        which is not available in the other metrics implementations. It creates
        an etl_metrics table and stores comprehensive pipeline metrics including:
        - Timing information (start/end/total time)
        - Processing statistics (tables/rows processed)
        - Error tracking and success status
        - Full metrics JSON for detailed analysis
        
        This feature makes this implementation more advanced than core/metrics.py
        which only stores metrics in memory and loses them on pipeline restart.
        """
        try:
            with self.analytics_engine.connect() as conn:
                # Create metrics table if it doesn't exist
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS etl_metrics (
                        id SERIAL PRIMARY KEY,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        total_time FLOAT,
                        tables_processed INTEGER,
                        total_rows_processed INTEGER,
                        success BOOLEAN,
                        error_count INTEGER,
                        metrics JSONB
                    )
                """))
                
                # Insert metrics
                conn.execute(text("""
                    INSERT INTO etl_metrics (
                        start_time,
                        end_time,
                        total_time,
                        tables_processed,
                        total_rows_processed,
                        success,
                        error_count,
                        metrics
                    ) VALUES (
                        :start_time,
                        :end_time,
                        :total_time,
                        :tables_processed,
                        :total_rows_processed,
                        :success,
                        :error_count,
                        :metrics
                    )
                """), {
                    'start_time': self.metrics['start_time'],
                    'end_time': self.metrics.get('end_time'),
                    'total_time': self.metrics.get('total_time'),
                    'tables_processed': self.metrics['tables_processed'],
                    'total_rows_processed': self.metrics['total_rows_processed'],
                    'success': len(self.metrics['errors']) == 0,
                    'error_count': len(self.metrics['errors']),
                    'metrics': self.metrics
                })
                
                conn.commit()
                logger.info("Metrics saved successfully")
                
        except Exception as e:
            logger.error(f"Failed to save metrics: {str(e)}")
            
    def get_pipeline_stats(self) -> Dict:
        """
        Get current pipeline statistics.
        
        Returns:
            Dict containing current pipeline stats
        """
        return {
            'tables_processed': self.metrics['tables_processed'],
            'total_rows_processed': self.metrics['total_rows_processed'],
            'error_count': len(self.metrics['errors']),
            'success_rate': (self.metrics['tables_processed'] - len(self.metrics['errors'])) / 
                           max(1, self.metrics['tables_processed']) * 100
        } 