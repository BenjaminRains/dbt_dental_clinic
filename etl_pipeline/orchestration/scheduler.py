"""
Pipeline scheduler for managing execution schedules and retry logic.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
import time
import threading
from queue import Queue
import schedule

logger = logging.getLogger(__name__)

@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: int = 60  # seconds
    max_delay: int = 3600  # seconds
    backoff_factor: float = 2.0

@dataclass
class ScheduleConfig:
    """Configuration for task scheduling."""
    cron_expression: Optional[str] = None
    interval_minutes: Optional[int] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    timezone: str = 'UTC'

class PipelineScheduler:
    """Manages pipeline execution schedules and retry logic."""
    
    def __init__(self):
        self.scheduled_tasks: Dict[str, Any] = {}
        self.retry_configs: Dict[str, RetryConfig] = {}
        self.execution_queue = Queue()
        self.running = False
        self.worker_thread = None
    
    def schedule_task(self, task_id: str, 
                     task_func: Callable,
                     schedule_config: ScheduleConfig,
                     retry_config: Optional[RetryConfig] = None) -> bool:
        """
        Schedule a task for execution.
        
        Args:
            task_id: Unique identifier for the task
            task_func: Function to execute
            schedule_config: Schedule configuration
            retry_config: Optional retry configuration
            
        Returns:
            bool: True if task was scheduled successfully
        """
        try:
            if task_id in self.scheduled_tasks:
                logger.warning(f"Task {task_id} is already scheduled")
                return False
            
            # Store retry configuration
            if retry_config:
                self.retry_configs[task_id] = retry_config
            
            # Create scheduled job
            job = schedule.every()
            
            if schedule_config.cron_expression:
                job.cron(schedule_config.cron_expression)
            elif schedule_config.interval_minutes:
                job.minutes(schedule_config.interval_minutes)
            else:
                logger.error(f"Invalid schedule configuration for task {task_id}")
                return False
            
            # Add time constraints if specified
            if schedule_config.start_time:
                job.at(schedule_config.start_time.strftime('%H:%M'))
            if schedule_config.end_time:
                job.until(schedule_config.end_time.strftime('%H:%M'))
            
            # Store the job
            self.scheduled_tasks[task_id] = {
                'job': job,
                'func': task_func,
                'config': schedule_config
            }
            
            logger.info(f"Scheduled task {task_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error scheduling task {task_id}: {str(e)}")
            return False
    
    def unschedule_task(self, task_id: str) -> bool:
        """
        Remove a task from the schedule.
        
        Args:
            task_id: ID of the task to unschedule
            
        Returns:
            bool: True if task was unscheduled successfully
        """
        try:
            if task_id not in self.scheduled_tasks:
                logger.warning(f"Task {task_id} is not scheduled")
                return False
            
            schedule.clear(task_id)
            self.scheduled_tasks.pop(task_id)
            self.retry_configs.pop(task_id, None)
            
            logger.info(f"Unscheduled task {task_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error unscheduling task {task_id}: {str(e)}")
            return False
    
    def start(self) -> None:
        """Start the scheduler."""
        if self.running:
            logger.warning("Scheduler is already running")
            return
        
        self.running = True
        self.worker_thread = threading.Thread(target=self._run_scheduler)
        self.worker_thread.daemon = True
        self.worker_thread.start()
        
        logger.info("Started pipeline scheduler")
    
    def stop(self) -> None:
        """Stop the scheduler."""
        if not self.running:
            logger.warning("Scheduler is not running")
            return
        
        self.running = False
        if self.worker_thread:
            self.worker_thread.join()
        
        logger.info("Stopped pipeline scheduler")
    
    def _run_scheduler(self) -> None:
        """Run the scheduler loop."""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {str(e)}")
    
    def execute_task(self, task_id: str, *args, **kwargs) -> bool:
        """
        Execute a task with retry logic.
        
        Args:
            task_id: ID of the task to execute
            *args: Positional arguments for the task function
            **kwargs: Keyword arguments for the task function
            
        Returns:
            bool: True if task executed successfully
        """
        try:
            if task_id not in self.scheduled_tasks:
                logger.error(f"Task {task_id} not found")
                return False
            
            task_info = self.scheduled_tasks[task_id]
            retry_config = self.retry_configs.get(task_id)
            
            if not retry_config:
                return self._execute_without_retry(task_info['func'], *args, **kwargs)
            
            return self._execute_with_retry(
                task_info['func'],
                retry_config,
                *args,
                **kwargs
            )
            
        except Exception as e:
            logger.error(f"Error executing task {task_id}: {str(e)}")
            return False
    
    def _execute_without_retry(self, task_func: Callable, 
                             *args, **kwargs) -> bool:
        """Execute a task without retry logic."""
        try:
            task_func(*args, **kwargs)
            return True
        except Exception as e:
            logger.error(f"Task execution failed: {str(e)}")
            return False
    
    def _execute_with_retry(self, task_func: Callable,
                          retry_config: RetryConfig,
                          *args, **kwargs) -> bool:
        """Execute a task with retry logic."""
        attempt = 0
        delay = retry_config.initial_delay
        
        while attempt < retry_config.max_attempts:
            try:
                task_func(*args, **kwargs)
                return True
                
            except Exception as e:
                attempt += 1
                if attempt == retry_config.max_attempts:
                    logger.error(
                        f"Task failed after {attempt} attempts: {str(e)}"
                    )
                    return False
                
                logger.warning(
                    f"Task failed (attempt {attempt}/{retry_config.max_attempts}): "
                    f"{str(e)}. Retrying in {delay} seconds..."
                )
                
                time.sleep(delay)
                delay = min(
                    delay * retry_config.backoff_factor,
                    retry_config.max_delay
                )
        
        return False
    
    def get_scheduled_tasks(self) -> List[Dict[str, Any]]:
        """
        Get information about scheduled tasks.
        
        Returns:
            List[Dict[str, Any]]: List of scheduled task information
        """
        tasks = []
        for task_id, task_info in self.scheduled_tasks.items():
            tasks.append({
                'task_id': task_id,
                'schedule': task_info['config'].__dict__,
                'retry_config': self.retry_configs.get(task_id).__dict__ 
                    if task_id in self.retry_configs else None
            })
        return tasks
    
    def clear_schedule(self) -> None:
        """Clear all scheduled tasks."""
        schedule.clear()
        self.scheduled_tasks.clear()
        self.retry_configs.clear()
        logger.info("Cleared all scheduled tasks") 