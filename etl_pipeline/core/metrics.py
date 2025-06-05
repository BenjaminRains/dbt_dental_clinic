"""
Metrics collection for the ETL pipeline.
"""
from datetime import datetime
from typing import Dict, Any

class MetricsCollector:
    def __init__(self):
        """Initialize metrics collector."""
        self.metrics: Dict[str, Any] = {
            'start_time': None,
            'end_time': None,
            'tables_processed': 0,
            'rows_processed': 0,
            'errors': []
        }
    
    def start_pipeline(self):
        """Record pipeline start time."""
        self.metrics['start_time'] = datetime.now()
    
    def end_pipeline(self):
        """Record pipeline end time."""
        self.metrics['end_time'] = datetime.now()
    
    def record_table_processed(self, table_name: str, rows: int):
        """Record a processed table."""
        self.metrics['tables_processed'] += 1
        self.metrics['rows_processed'] += rows
    
    def record_error(self, error: str):
        """Record an error."""
        self.metrics['errors'].append({
            'timestamp': datetime.now(),
            'message': error
        })
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        return self.metrics