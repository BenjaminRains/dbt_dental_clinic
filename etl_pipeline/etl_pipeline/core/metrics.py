"""
DEPRECATED: Basic metrics collection module.

STATUS: DEPRECATED - Replaced by unified_metrics.py
==================================================

This module has been deprecated and replaced by the unified metrics system.
All functionality has been consolidated into monitoring/unified_metrics.py.

MIGRATION:
- Use UnifiedMetricsCollector from monitoring/unified_metrics.py instead
- This file will be removed in a future release
- All imports have been updated to use the new unified system

DEPRECATION TIMELINE:
- Phase 1: Mark as deprecated (current)
- Phase 2: Remove after validation period
- Phase 3: Clean up any remaining references

DO NOT USE: This file is maintained for backward compatibility only.
Use etl_pipeline.monitoring.unified_metrics.UnifiedMetricsCollector instead.
"""

import logging
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class MetricsCollector:
    """
    DEPRECATED: Basic metrics collector.
    
    This class has been deprecated. Use UnifiedMetricsCollector from
    monitoring/unified_metrics.py instead.
    
    The unified system provides:
    - Database persistence
    - Real-time metrics collection
    - Pipeline status reporting
    - Error tracking and success rates
    - Configurable retention policies
    """
    
    def __init__(self):
        """
        DEPRECATED: Initialize metrics collector.
        
        Use UnifiedMetricsCollector(analytics_engine) instead.
        """
        logger.warning("MetricsCollector is deprecated. Use UnifiedMetricsCollector from monitoring/unified_metrics.py")
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'tables_processed': 0,
            'rows_processed': 0,
            'errors': []
        }
    
    def start_pipeline(self):
        """
        DEPRECATED: Start pipeline timing.
        
        Use UnifiedMetricsCollector.start_pipeline() instead.
        """
        logger.warning("MetricsCollector.start_pipeline() is deprecated")
        self.metrics['start_time'] = datetime.now()
    
    def end_pipeline(self):
        """
        DEPRECATED: End pipeline timing.
        
        Use UnifiedMetricsCollector.end_pipeline() instead.
        """
        logger.warning("MetricsCollector.end_pipeline() is deprecated")
        self.metrics['end_time'] = datetime.now()
    
    def record_table_processed(self, table_name: str, rows_processed: int):
        """
        DEPRECATED: Record table processing.
        
        Use UnifiedMetricsCollector.record_table_processed() instead.
        """
        logger.warning("MetricsCollector.record_table_processed() is deprecated")
        self.metrics['tables_processed'] += 1
        self.metrics['rows_processed'] += rows_processed
    
    def record_error(self, error_message: str):
        """
        DEPRECATED: Record error.
        
        Use UnifiedMetricsCollector.record_error() instead.
        """
        logger.warning("MetricsCollector.record_error() is deprecated")
        error = {
            'timestamp': datetime.now(),
            'message': error_message
        }
        self.metrics['errors'].append(error)
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        DEPRECATED: Get metrics.
        
        Use UnifiedMetricsCollector.get_pipeline_status() or 
        UnifiedMetricsCollector.get_pipeline_stats() instead.
        """
        logger.warning("MetricsCollector.get_metrics() is deprecated")
        return self.metrics.copy()