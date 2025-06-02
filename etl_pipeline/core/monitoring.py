"""
Monitoring module for the ELT pipeline.
Handles logging, metrics collection, and alerting.
"""
import logging
import time
from datetime import datetime
from typing import Dict, Optional, Any
import json
import os
from pathlib import Path
import requests
from functools import wraps
from etl_pipeline.config.database import DatabaseConfig
from etl_pipeline.core.connections import ConnectionFactory

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / "elt_pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class PipelineMetrics:
    """Collects and stores pipeline metrics."""
    
    def __init__(self, metrics_dir: Optional[str] = None):
        self.metrics_dir = Path(metrics_dir) if metrics_dir else PROJECT_ROOT / "metrics"
        self.metrics_dir.mkdir(exist_ok=True)
        self.current_run = {
            'start_time': datetime.now().isoformat(),
            'tables': {},
            'errors': [],
            'warnings': [],
            'database_health': {}
        }
    
    def record_table_metrics(self, table_name: str, metrics: Dict[str, Any]):
        """Record metrics for a specific table."""
        self.current_run['tables'][table_name] = {
            'timestamp': datetime.now().isoformat(),
            **metrics
        }
    
    def record_error(self, error: str, context: Optional[Dict] = None):
        """Record an error with optional context."""
        self.current_run['errors'].append({
            'timestamp': datetime.now().isoformat(),
            'error': error,
            'context': context or {}
        })
    
    def record_warning(self, warning: str, context: Optional[Dict] = None):
        """Record a warning with optional context."""
        self.current_run['warnings'].append({
            'timestamp': datetime.now().isoformat(),
            'warning': warning,
            'context': context or {}
        })
    
    def record_database_health(self, db_type: str, is_healthy: bool, details: Optional[Dict] = None):
        """Record database health status."""
        self.current_run['database_health'][db_type] = {
            'timestamp': datetime.now().isoformat(),
            'is_healthy': is_healthy,
            'details': details or {}
        }
    
    def save_metrics(self):
        """Save the current run metrics to a file."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        metrics_file = self.metrics_dir / f"pipeline_metrics_{timestamp}.json"
        
        with open(metrics_file, 'w') as f:
            json.dump(self.current_run, f, indent=2)
        
        logger.info(f"Saved metrics to {metrics_file}")
    
    def get_summary(self) -> Dict:
        """Get a summary of the current run."""
        return {
            'start_time': self.current_run['start_time'],
            'end_time': datetime.now().isoformat(),
            'tables_processed': len(self.current_run['tables']),
            'error_count': len(self.current_run['errors']),
            'warning_count': len(self.current_run['warnings']),
            'success_rate': self._calculate_success_rate(),
            'database_health': self.current_run['database_health']
        }
    
    def _calculate_success_rate(self) -> float:
        """Calculate the success rate of the pipeline run."""
        if not self.current_run['tables']:
            return 0.0
        
        successful_tables = sum(
            1 for metrics in self.current_run['tables'].values()
            if metrics.get('status') == 'success'
        )
        
        return (successful_tables / len(self.current_run['tables'])) * 100


class PipelineMonitor:
    """Monitors pipeline execution and handles alerts."""
    
    def __init__(self, slack_webhook: Optional[str] = None, email_notifications: bool = False):
        self.metrics = PipelineMetrics()
        self.slack_webhook = slack_webhook or os.getenv('SLACK_WEBHOOK_URL')
        self.email_notifications = email_notifications or os.getenv('ENABLE_EMAIL_NOTIFICATIONS', 'false').lower() == 'true'
    
    def monitor_operation(self, operation_name: str):
        """Decorator to monitor pipeline operations."""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                table_name = kwargs.get('table_name', 'unknown')
                
                try:
                    # Check database health before operation
                    self._check_database_health()
                    
                    result = func(*args, **kwargs)
                    
                    # Record metrics
                    self.metrics.record_table_metrics(table_name, {
                        'operation': operation_name,
                        'status': 'success' if result else 'failed',
                        'duration': time.time() - start_time
                    })
                    
                    return result
                    
                except Exception as e:
                    # Record error
                    self.metrics.record_error(
                        str(e),
                        {
                            'operation': operation_name,
                            'table': table_name,
                            'args': str(args),
                            'kwargs': str(kwargs)
                        }
                    )
                    
                    # Send alert
                    self.send_alert(
                        f"Error in {operation_name} for table {table_name}",
                        str(e)
                    )
                    
                    raise
                
            return wrapper
        return decorator
    
    def _check_database_health(self):
        """Check health of all database connections."""
        try:
            # Test all connections
            results = ConnectionFactory.test_connections()
            
            # Record health status
            for db_type, is_healthy in results.items():
                self.metrics.record_database_health(
                    db_type,
                    is_healthy,
                    {'connection_test': 'success' if is_healthy else 'failed'}
                )
                
                if not is_healthy:
                    self.send_alert(
                        f"Database Health Check Failed",
                        f"Connection to {db_type} database failed",
                        level='warning'
                    )
        except Exception as e:
            logger.error(f"Database health check failed: {str(e)}")
            self.metrics.record_error(
                "Database health check failed",
                {'error': str(e)}
            )
    
    def send_alert(self, title: str, message: str, level: str = 'error'):
        """Send an alert through configured channels."""
        if self.slack_webhook:
            self._send_slack_alert(title, message, level)
        
        if self.email_notifications:
            self._send_email_alert(title, message, level)
    
    def _send_slack_alert(self, title: str, message: str, level: str):
        """Send an alert to Slack."""
        if not self.slack_webhook:
            return
        
        color = {
            'error': '#FF0000',
            'warning': '#FFA500',
            'info': '#00FF00'
        }.get(level, '#808080')
        
        payload = {
            'attachments': [{
                'color': color,
                'title': title,
                'text': message,
                'ts': int(time.time())
            }]
        }
        
        try:
            response = requests.post(
                self.slack_webhook,
                json=payload,
                timeout=5
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {str(e)}")
    
    def _send_email_alert(self, title: str, message: str, level: str):
        """Send an alert via email."""
        # TODO: Implement email notifications
        logger.info(f"Email alert would be sent: {title} - {message}")
    
    def get_pipeline_status(self) -> Dict:
        """Get the current status of the pipeline."""
        return {
            'metrics': self.metrics.get_summary(),
            'errors': self.metrics.current_run['errors'],
            'warnings': self.metrics.current_run['warnings']
        }


def setup_monitoring(slack_webhook: Optional[str] = None, email_notifications: bool = False) -> PipelineMonitor:
    """Set up pipeline monitoring with the specified configuration."""
    return PipelineMonitor(slack_webhook, email_notifications) 