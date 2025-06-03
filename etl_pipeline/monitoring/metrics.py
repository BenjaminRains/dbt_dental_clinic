"""
Metrics collection for the ETL pipeline.
Tracks performance and operational metrics.
"""
import logging
import json
from typing import Dict, Optional, List, Any
from datetime import datetime
from pathlib import Path
from etl_pipeline.core.logger import get_logger
# Can also import from config if needed

# Use standard logging instead of importing from core
logger = logging.getLogger(__name__)

class PipelineMetrics:
    """Collects and manages ETL pipeline metrics."""
    
    def __init__(self, metrics_dir: Optional[str] = None):
        """Initialize metrics collection."""
        if metrics_dir is None:
            # Default to metrics directory in project root
            project_root = Path(__file__).parent.parent.parent
            metrics_dir = project_root / "metrics"
        
        self.metrics_dir = Path(metrics_dir)
        self.metrics_dir.mkdir(exist_ok=True)
        
        self.metrics: Dict[str, Dict] = {}
        self.start_time: Optional[datetime] = None
        self.current_pipeline_id: Optional[str] = None
    
    def start_pipeline(self, pipeline_id: Optional[str] = None) -> str:
        """Start tracking metrics for a pipeline run."""
        if pipeline_id is None:
            pipeline_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        self.current_pipeline_id = pipeline_id
        self.start_time = datetime.now()
        self.metrics[pipeline_id] = {
            'pipeline_id': pipeline_id,
            'start_time': self.start_time.isoformat(),
            'end_time': None,
            'duration_seconds': None,
            'records_processed': 0,
            'errors': 0,
            'warnings': 0,
            'tables_processed': [],
            'status': 'running',
            'error_details': [],
            'warning_details': []
        }
        logger.info(f"Started metrics collection for pipeline {pipeline_id}")
        return pipeline_id
    
    def end_pipeline(self, pipeline_id: Optional[str] = None, status: str = 'completed') -> None:
        """End tracking metrics for a pipeline run."""
        if pipeline_id is None:
            pipeline_id = self.current_pipeline_id
        
        if pipeline_id is None or pipeline_id not in self.metrics:
            logger.warning(f"No metrics found for pipeline {pipeline_id}")
            return
        
        end_time = datetime.now()
        start_time_str = self.metrics[pipeline_id]['start_time']
        start_time = datetime.fromisoformat(start_time_str)
        duration = (end_time - start_time).total_seconds()
        
        self.metrics[pipeline_id].update({
            'end_time': end_time.isoformat(),
            'duration_seconds': duration,
            'status': status
        })
        
        # Save metrics to file
        self._save_metrics_to_file(pipeline_id)
        
        logger.info(f"Completed metrics collection for pipeline {pipeline_id}")
        logger.info(f"  Duration: {duration:.2f} seconds")
        logger.info(f"  Status: {status}")
        logger.info(f"  Records processed: {self.metrics[pipeline_id]['records_processed']:,}")
        logger.info(f"  Tables processed: {len(self.metrics[pipeline_id]['tables_processed'])}")
        logger.info(f"  Errors: {self.metrics[pipeline_id]['errors']}")
        logger.info(f"  Warnings: {self.metrics[pipeline_id]['warnings']}")
    
    def record_table_processed(self, table_name: str, record_count: int, 
                             pipeline_id: Optional[str] = None) -> None:
        """Record metrics for a processed table."""
        if pipeline_id is None:
            pipeline_id = self.current_pipeline_id
        
        if pipeline_id is None or pipeline_id not in self.metrics:
            logger.warning(f"No metrics found for pipeline {pipeline_id}")
            return
        
        table_metrics = {
            'table': table_name,
            'records': record_count,
            'timestamp': datetime.now().isoformat(),
            'status': 'completed'
        }
        
        self.metrics[pipeline_id]['tables_processed'].append(table_metrics)
        self.metrics[pipeline_id]['records_processed'] += record_count
        logger.info(f"Processed {record_count:,} records from table {table_name}")
    
    def record_table_error(self, table_name: str, error_message: str,
                          pipeline_id: Optional[str] = None) -> None:
        """Record an error for a specific table."""
        if pipeline_id is None:
            pipeline_id = self.current_pipeline_id
        
        if pipeline_id is None or pipeline_id not in self.metrics:
            logger.warning(f"No metrics found for pipeline {pipeline_id}")
            return
        
        table_metrics = {
            'table': table_name,
            'records': 0,
            'timestamp': datetime.now().isoformat(),
            'status': 'error',
            'error_message': error_message
        }
        
        self.metrics[pipeline_id]['tables_processed'].append(table_metrics)
        self.record_error(pipeline_id, f"Table {table_name}: {error_message}")
    
    def record_error(self, pipeline_id: str, error_message: str) -> None:
        """Record an error in the pipeline."""
        if pipeline_id not in self.metrics:
            logger.warning(f"No metrics found for pipeline {pipeline_id}")
            return
        
        self.metrics[pipeline_id]['errors'] += 1
        error_detail = {
            'message': error_message,
            'timestamp': datetime.now().isoformat()
        }
        self.metrics[pipeline_id]['error_details'].append(error_detail)
        logger.error(f"Pipeline {pipeline_id} error: {error_message}")
    
    def record_warning(self, pipeline_id: str, warning_message: str) -> None:
        """Record a warning in the pipeline."""
        if pipeline_id not in self.metrics:
            logger.warning(f"No metrics found for pipeline {pipeline_id}")
            return
        
        self.metrics[pipeline_id]['warnings'] += 1
        warning_detail = {
            'message': warning_message,
            'timestamp': datetime.now().isoformat()
        }
        self.metrics[pipeline_id]['warning_details'].append(warning_detail)
        logger.warning(f"Pipeline {pipeline_id} warning: {warning_message}")
    
    def get_metrics(self, pipeline_id: str) -> Dict:
        """Get metrics for a specific pipeline run."""
        return self.metrics.get(pipeline_id, {})
    
    def get_current_metrics(self) -> Dict:
        """Get metrics for the current pipeline run."""
        if self.current_pipeline_id:
            return self.get_metrics(self.current_pipeline_id)
        return {}
    
    def get_all_metrics(self) -> Dict[str, Dict]:
        """Get metrics for all pipeline runs."""
        return self.metrics
    
    def get_summary(self, pipeline_id: Optional[str] = None) -> Dict[str, Any]:
        """Get a summary of metrics for a pipeline run."""
        if pipeline_id is None:
            pipeline_id = self.current_pipeline_id
        
        if pipeline_id is None or pipeline_id not in self.metrics:
            return {'error': 'No metrics found'}
        
        metrics = self.metrics[pipeline_id]
        
        # Calculate success rate
        total_tables = len(metrics['tables_processed'])
        successful_tables = sum(1 for table in metrics['tables_processed'] 
                              if table.get('status') == 'completed')
        success_rate = (successful_tables / total_tables * 100) if total_tables > 0 else 0
        
        return {
            'pipeline_id': pipeline_id,
            'status': metrics['status'],
            'start_time': metrics['start_time'],
            'end_time': metrics['end_time'],
            'duration_seconds': metrics['duration_seconds'],
            'total_tables': total_tables,
            'successful_tables': successful_tables,
            'failed_tables': total_tables - successful_tables,
            'success_rate_percent': round(success_rate, 2),
            'records_processed': metrics['records_processed'],
            'errors': metrics['errors'],
            'warnings': metrics['warnings']
        }
    
    def _save_metrics_to_file(self, pipeline_id: str):
        """Save metrics to a JSON file."""
        try:
            filename = f"pipeline_metrics_{pipeline_id}.json"
            filepath = self.metrics_dir / filename
            
            metrics_data = {
                'summary': self.get_summary(pipeline_id),
                'detailed_metrics': self.metrics[pipeline_id]
            }
            
            with open(filepath, 'w') as f:
                json.dump(metrics_data, f, indent=2, default=str)
            
            logger.info(f"Metrics saved to {filepath}")
            
        except Exception as e:
            logger.error(f"Failed to save metrics to file: {str(e)}")
    
    def load_metrics_from_file(self, pipeline_id: str) -> Optional[Dict]:
        """Load metrics from a file."""
        try:
            filename = f"pipeline_metrics_{pipeline_id}.json"
            filepath = self.metrics_dir / filename
            
            if not filepath.exists():
                logger.warning(f"Metrics file not found: {filepath}")
                return None
            
            with open(filepath, 'r') as f:
                return json.load(f)
                
        except Exception as e:
            logger.error(f"Failed to load metrics from file: {str(e)}")
            return None
    
    def list_recent_runs(self, limit: int = 10) -> List[str]:
        """List recent pipeline run IDs."""
        try:
            # Get all metrics files
            metrics_files = list(self.metrics_dir.glob("pipeline_metrics_*.json"))
            
            # Sort by modification time (newest first)
            metrics_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            # Extract pipeline IDs from filenames
            pipeline_ids = []
            for file_path in metrics_files[:limit]:
                filename = file_path.stem
                pipeline_id = filename.replace("pipeline_metrics_", "")
                pipeline_ids.append(pipeline_id)
            
            return pipeline_ids
            
        except Exception as e:
            logger.error(f"Failed to list recent runs: {str(e)}")
            return []
    
    def cleanup_old_metrics(self, retention_days: int = 30):
        """Clean up old metrics files."""
        try:
            import time
            cutoff_time = time.time() - (retention_days * 24 * 60 * 60)
            
            deleted_count = 0
            for file_path in self.metrics_dir.glob("pipeline_metrics_*.json"):
                if file_path.stat().st_mtime < cutoff_time:
                    file_path.unlink()
                    deleted_count += 1
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old metrics files")
                
        except Exception as e:
            logger.error(f"Failed to cleanup old metrics: {str(e)}")

# Create global metrics instance
metrics = PipelineMetrics()

def get_metrics() -> PipelineMetrics:
    """Get the global metrics instance."""
    return metrics