"""
Main pipeline orchestrator for ETL pipeline execution.
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from sqlalchemy.engine import Engine

from etl_pipeline.orchestration.dependency_graph import DependencyGraph
from etl_pipeline.orchestration.scheduler import PipelineScheduler, ScheduleConfig, RetryConfig
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.monitoring import PipelineMetrics

logger = logging.getLogger(__name__)

class PipelineRunner:
    """Main orchestrator for ETL pipeline execution."""
    
    def __init__(self, config: Dict[str, Any] = None, 
                 connection_factory: Optional[ConnectionFactory] = None,
                 monitor: Optional[PipelineMetrics] = None,
                 performance_monitor: Optional[Any] = None,
                 notification_manager: Optional[Any] = None):
        self.config = config or {}
        self.dependency_graph = DependencyGraph()
        self.scheduler = PipelineScheduler()
        self.monitor = monitor or PipelineMetrics()
        self.connection_factory = connection_factory or ConnectionFactory()
        self.performance_monitor = performance_monitor
        self.notification_manager = notification_manager
        
        # Initialize connections if not provided
        if not connection_factory:
            self.source_engine = self.connection_factory.get_opendental_source_connection()
            self.staging_engine = self.connection_factory.get_mysql_replication_connection()
            self.target_engine = self.connection_factory.get_postgres_analytics_connection()
    
    def register_task(self, task_id: str, 
                     task_func: Callable,
                     dependencies: List[str] = None,
                     schedule_config: Optional[ScheduleConfig] = None,
                     retry_config: Optional[RetryConfig] = None) -> bool:
        """
        Register a task with the pipeline.
        
        Args:
            task_id: Unique identifier for the task
            task_func: Function to execute
            dependencies: List of task dependencies
            schedule_config: Optional schedule configuration
            retry_config: Optional retry configuration
            
        Returns:
            bool: True if task was registered successfully
        """
        try:
            # Add task to dependency graph
            metadata = {
                'dependencies': dependencies or [],
                'schedule_config': schedule_config,
                'retry_config': retry_config
            }
            self.dependency_graph.add_task(task_id, metadata)
            
            # Schedule task if schedule config provided
            if schedule_config:
                self.scheduler.schedule_task(
                    task_id,
                    task_func,
                    schedule_config,
                    retry_config
                )
            
            logger.info(f"Registered task {task_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error registering task {task_id}: {str(e)}")
            return False
    
    def run_pipeline(self, task_id: Optional[str] = None) -> bool:
        """
        Run the pipeline or a specific task.
        
        Args:
            task_id: Optional task ID to run
            
        Returns:
            bool: True if pipeline/task executed successfully
        """
        try:
            if task_id:
                return self._run_task(task_id)
            
            # Validate dependencies
            if not self.dependency_graph.validate_dependencies():
                logger.error("Invalid dependency graph")
                return False
            
            # Get execution order
            execution_order = self.dependency_graph.get_execution_order()
            
            # Execute tasks in order
            for task_id in execution_order:
                if not self._run_task(task_id):
                    logger.error(f"Pipeline failed at task {task_id}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error running pipeline: {str(e)}")
            return False
    
    def _run_task(self, task_id: str) -> bool:
        """
        Run a specific task.
        
        Args:
            task_id: ID of the task to run
            
        Returns:
            bool: True if task executed successfully
        """
        try:
            # Check dependencies
            deps = self.dependency_graph.get_dependencies(task_id)
            for dep in deps:
                dep_status = self.dependency_graph.get_task_status(dep)
                if dep_status['status'] != 'success':
                    logger.error(
                        f"Task {task_id} dependencies not met: "
                        f"{dep} status is {dep_status['status']}"
                    )
                    return False
            
            # Get task metadata
            metadata = self.dependency_graph.get_task_metadata(task_id)
            task_func = metadata.get('task_func')
            
            if not task_func:
                logger.error(f"No task function found for {task_id}")
                return False
            
            # Execute task
            start_time = datetime.now()
            success = self.scheduler.execute_task(task_id)
            end_time = datetime.now()
            
            # Record execution
            self.dependency_graph.record_execution(
                task_id,
                'success' if success else 'failure',
                start_time,
                end_time,
                {'error': str(e) if not success else None}
            )
            
            return success
            
        except Exception as e:
            logger.error(f"Error running task {task_id}: {str(e)}")
            return False
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current pipeline status.
        
        Returns:
            Dict[str, Any]: Pipeline status information
        """
        try:
            return {
                'graph_summary': self.dependency_graph.get_graph_summary(),
                'scheduled_tasks': self.scheduler.get_scheduled_tasks(),
                'monitoring_metrics': self.monitor.current_run if hasattr(self.monitor, 'current_run') else {}
            }
            
        except Exception as e:
            logger.error(f"Error getting pipeline status: {str(e)}")
            return {'error': str(e)}
    
    def start_scheduler(self) -> None:
        """Start the pipeline scheduler."""
        self.scheduler.start()
    
    def stop_scheduler(self) -> None:
        """Stop the pipeline scheduler."""
        self.scheduler.stop()
    
    def cleanup(self) -> None:
        """Clean up resources."""
        try:
            self.stop_scheduler()
            self.connection_factory.dispose_all()
            logger.info("Cleaned up pipeline resources")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup() 