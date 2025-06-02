"""
Performance monitoring utilities for the ETL pipeline.
"""
import logging
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import psutil
import threading
from collections import deque

logger = logging.getLogger(__name__)

@dataclass
class PerformanceMetrics:
    """Performance metrics for a pipeline component."""
    component_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    records_processed: int = 0
    memory_usage: float = 0.0
    cpu_usage: float = 0.0
    error_count: int = 0
    warning_count: int = 0

class PerformanceMonitor:
    """Monitors and tracks performance metrics for the ETL pipeline."""
    
    def __init__(self, history_size: int = 1000):
        self.metrics_history: Dict[str, deque] = {}
        self.active_metrics: Dict[str, PerformanceMetrics] = {}
        self.history_size = history_size
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_monitoring = threading.Event()
    
    def start_monitoring(self, component_name: str) -> None:
        """
        Start monitoring a component.
        
        Args:
            component_name: Name of the component to monitor
        """
        if component_name in self.active_metrics:
            logger.warning(f"Component {component_name} is already being monitored")
            return
        
        self.active_metrics[component_name] = PerformanceMetrics(
            component_name=component_name,
            start_time=datetime.now()
        )
        
        if component_name not in self.metrics_history:
            self.metrics_history[component_name] = deque(maxlen=self.history_size)
        
        logger.info(f"Started monitoring component {component_name}")
    
    def stop_monitoring(self, component_name: str) -> Optional[PerformanceMetrics]:
        """
        Stop monitoring a component and return its metrics.
        
        Args:
            component_name: Name of the component to stop monitoring
            
        Returns:
            Optional[PerformanceMetrics]: Final metrics for the component
        """
        if component_name not in self.active_metrics:
            logger.warning(f"Component {component_name} is not being monitored")
            return None
        
        metrics = self.active_metrics.pop(component_name)
        metrics.end_time = datetime.now()
        metrics.duration = (metrics.end_time - metrics.start_time).total_seconds()
        
        self.metrics_history[component_name].append(metrics)
        logger.info(f"Stopped monitoring component {component_name}")
        
        return metrics
    
    def update_metrics(self, component_name: str,
                      records_processed: Optional[int] = None,
                      error_count: Optional[int] = None,
                      warning_count: Optional[int] = None) -> None:
        """
        Update metrics for a component.
        
        Args:
            component_name: Name of the component to update
            records_processed: Number of records processed
            error_count: Number of errors encountered
            warning_count: Number of warnings encountered
        """
        if component_name not in self.active_metrics:
            logger.warning(f"Component {component_name} is not being monitored")
            return
        
        metrics = self.active_metrics[component_name]
        if records_processed is not None:
            metrics.records_processed = records_processed
        if error_count is not None:
            metrics.error_count = error_count
        if warning_count is not None:
            metrics.warning_count = warning_count
    
    def start_resource_monitoring(self, interval: float = 1.0) -> None:
        """
        Start monitoring system resources in a background thread.
        
        Args:
            interval: Monitoring interval in seconds
        """
        if self._monitor_thread is not None:
            logger.warning("Resource monitoring is already running")
            return
        
        self._stop_monitoring.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_resources,
            args=(interval,),
            daemon=True
        )
        self._monitor_thread.start()
        logger.info("Started resource monitoring")
    
    def stop_resource_monitoring(self) -> None:
        """Stop monitoring system resources."""
        if self._monitor_thread is None:
            logger.warning("Resource monitoring is not running")
            return
        
        self._stop_monitoring.set()
        self._monitor_thread.join()
        self._monitor_thread = None
        logger.info("Stopped resource monitoring")
    
    def _monitor_resources(self, interval: float) -> None:
        """Monitor system resources in a background thread."""
        process = psutil.Process()
        
        while not self._stop_monitoring.is_set():
            try:
                for component_name, metrics in self.active_metrics.items():
                    metrics.memory_usage = process.memory_info().rss / 1024 / 1024  # MB
                    metrics.cpu_usage = process.cpu_percent()
                
                time.sleep(interval)
            except Exception as e:
                logger.error(f"Error monitoring resources: {str(e)}")
    
    def get_metrics(self, component_name: str,
                   limit: Optional[int] = None) -> List[PerformanceMetrics]:
        """
        Get metrics history for a component.
        
        Args:
            component_name: Name of the component
            limit: Optional limit on number of metrics to return
            
        Returns:
            List[PerformanceMetrics]: Metrics history
        """
        if component_name not in self.metrics_history:
            return []
        
        metrics = list(self.metrics_history[component_name])
        if limit:
            return metrics[-limit:]
        return metrics
    
    def get_active_metrics(self) -> Dict[str, PerformanceMetrics]:
        """
        Get metrics for all currently monitored components.
        
        Returns:
            Dict[str, PerformanceMetrics]: Active metrics
        """
        return self.active_metrics.copy()
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Get a summary of performance metrics.
        
        Returns:
            Dict[str, Any]: Performance summary
        """
        summary = {
            'active_components': len(self.active_metrics),
            'monitored_components': list(self.metrics_history.keys()),
            'total_metrics': sum(len(metrics) for metrics in self.metrics_history.values()),
            'component_summaries': {}
        }
        
        for component_name, metrics in self.metrics_history.items():
            if not metrics:
                continue
            
            durations = [m.duration for m in metrics if m.duration is not None]
            records = [m.records_processed for m in metrics]
            errors = [m.error_count for m in metrics]
            warnings = [m.warning_count for m in metrics]
            
            summary['component_summaries'][component_name] = {
                'total_runs': len(metrics),
                'avg_duration': sum(durations) / len(durations) if durations else 0,
                'total_records': sum(records),
                'total_errors': sum(errors),
                'total_warnings': sum(warnings),
                'avg_records_per_run': sum(records) / len(metrics) if metrics else 0
            }
        
        return summary
    
    def clear_history(self, component_name: Optional[str] = None) -> None:
        """
        Clear metrics history.
        
        Args:
            component_name: Optional component name to clear history for
        """
        if component_name:
            if component_name in self.metrics_history:
                self.metrics_history[component_name].clear()
        else:
            for metrics in self.metrics_history.values():
                metrics.clear() 