"""
Dependency graph module for managing ETL pipeline task dependencies.
"""
import logging
from typing import Dict, List, Set, Optional, Any
from collections import defaultdict
import networkx as nx
from datetime import datetime

logger = logging.getLogger(__name__)

class DependencyGraph:
    """Manages task dependencies in the ETL pipeline."""
    
    def __init__(self):
        self.graph = nx.DiGraph()
        self.task_metadata = {}
        self.execution_history = {}
    
    def add_task(self, task_id: str, metadata: Dict[str, Any]) -> None:
        """
        Add a task to the dependency graph.
        
        Args:
            task_id: Unique identifier for the task
            metadata: Task metadata including dependencies, schedule, etc.
        """
        try:
            self.graph.add_node(task_id, **metadata)
            self.task_metadata[task_id] = metadata
            
            # Add dependencies
            for dep in metadata.get('dependencies', []):
                self.graph.add_edge(dep, task_id)
                
            logger.info(f"Added task {task_id} to dependency graph")
            
        except Exception as e:
            logger.error(f"Error adding task {task_id}: {str(e)}")
            raise
    
    def remove_task(self, task_id: str) -> None:
        """
        Remove a task from the dependency graph.
        
        Args:
            task_id: ID of the task to remove
        """
        try:
            if task_id in self.graph:
                self.graph.remove_node(task_id)
                self.task_metadata.pop(task_id, None)
                logger.info(f"Removed task {task_id} from dependency graph")
            else:
                logger.warning(f"Task {task_id} not found in dependency graph")
                
        except Exception as e:
            logger.error(f"Error removing task {task_id}: {str(e)}")
            raise
    
    def get_dependencies(self, task_id: str) -> List[str]:
        """
        Get all dependencies for a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            List[str]: List of dependency task IDs
        """
        try:
            if task_id not in self.graph:
                logger.warning(f"Task {task_id} not found in dependency graph")
                return []
                
            return list(self.graph.predecessors(task_id))
            
        except Exception as e:
            logger.error(f"Error getting dependencies for task {task_id}: {str(e)}")
            return []
    
    def get_dependents(self, task_id: str) -> List[str]:
        """
        Get all tasks that depend on the given task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            List[str]: List of dependent task IDs
        """
        try:
            if task_id not in self.graph:
                logger.warning(f"Task {task_id} not found in dependency graph")
                return []
                
            return list(self.graph.successors(task_id))
            
        except Exception as e:
            logger.error(f"Error getting dependents for task {task_id}: {str(e)}")
            return []
    
    def get_execution_order(self) -> List[str]:
        """
        Get the topological order of tasks for execution.
        
        Returns:
            List[str]: List of task IDs in execution order
        """
        try:
            if not nx.is_directed_acyclic_graph(self.graph):
                raise ValueError("Dependency graph contains cycles")
                
            return list(nx.topological_sort(self.graph))
            
        except Exception as e:
            logger.error(f"Error getting execution order: {str(e)}")
            raise
    
    def validate_dependencies(self) -> bool:
        """
        Validate the dependency graph for cycles and missing dependencies.
        
        Returns:
            bool: True if graph is valid
        """
        try:
            # Check for cycles
            if not nx.is_directed_acyclic_graph(self.graph):
                logger.error("Dependency graph contains cycles")
                return False
            
            # Check for missing dependencies
            for task_id in self.graph.nodes():
                deps = self.get_dependencies(task_id)
                for dep in deps:
                    if dep not in self.graph:
                        logger.error(f"Missing dependency {dep} for task {task_id}")
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating dependencies: {str(e)}")
            return False
    
    def record_execution(self, task_id: str, status: str, 
                        start_time: datetime, end_time: datetime,
                        metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record task execution details.
        
        Args:
            task_id: ID of the executed task
            status: Execution status (success/failure)
            start_time: Task start time
            end_time: Task end time
            metadata: Additional execution metadata
        """
        try:
            if task_id not in self.execution_history:
                self.execution_history[task_id] = []
            
            execution_record = {
                'timestamp': datetime.now(),
                'status': status,
                'start_time': start_time,
                'end_time': end_time,
                'duration': (end_time - start_time).total_seconds(),
                'metadata': metadata or {}
            }
            
            self.execution_history[task_id].append(execution_record)
            logger.info(f"Recorded execution for task {task_id}: {status}")
            
        except Exception as e:
            logger.error(f"Error recording execution for task {task_id}: {str(e)}")
    
    def get_execution_history(self, task_id: str) -> List[Dict[str, Any]]:
        """
        Get execution history for a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            List[Dict[str, Any]]: List of execution records
        """
        return self.execution_history.get(task_id, [])
    
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Get current status of a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            Dict[str, Any]: Task status information
        """
        try:
            if task_id not in self.graph:
                return {'status': 'unknown', 'error': 'Task not found'}
            
            history = self.get_execution_history(task_id)
            if not history:
                return {'status': 'pending'}
            
            latest = history[-1]
            deps = self.get_dependencies(task_id)
            dep_statuses = {
                dep: self.get_task_status(dep)['status']
                for dep in deps
            }
            
            return {
                'status': latest['status'],
                'last_execution': latest['timestamp'],
                'duration': latest['duration'],
                'dependencies': dep_statuses,
                'metadata': latest['metadata']
            }
            
        except Exception as e:
            logger.error(f"Error getting status for task {task_id}: {str(e)}")
            return {'status': 'error', 'error': str(e)}
    
    def get_failed_tasks(self) -> List[str]:
        """
        Get list of failed tasks.
        
        Returns:
            List[str]: List of failed task IDs
        """
        failed_tasks = []
        for task_id in self.graph.nodes():
            status = self.get_task_status(task_id)
            if status['status'] == 'failure':
                failed_tasks.append(task_id)
        return failed_tasks
    
    def get_ready_tasks(self) -> List[str]:
        """
        Get list of tasks ready for execution.
        
        Returns:
            List[str]: List of task IDs ready to run
        """
        ready_tasks = []
        for task_id in self.graph.nodes():
            status = self.get_task_status(task_id)
            if status['status'] == 'pending':
                deps = self.get_dependencies(task_id)
                if all(self.get_task_status(dep)['status'] == 'success' 
                      for dep in deps):
                    ready_tasks.append(task_id)
        return ready_tasks
    
    def get_task_metadata(self, task_id: str) -> Dict[str, Any]:
        """
        Get metadata for a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            Dict[str, Any]: Task metadata
        """
        return self.task_metadata.get(task_id, {})
    
    def update_task_metadata(self, task_id: str, 
                           metadata: Dict[str, Any]) -> None:
        """
        Update metadata for a task.
        
        Args:
            task_id: ID of the task
            metadata: New metadata
        """
        try:
            if task_id in self.graph:
                self.graph.nodes[task_id].update(metadata)
                self.task_metadata[task_id].update(metadata)
                logger.info(f"Updated metadata for task {task_id}")
            else:
                logger.warning(f"Task {task_id} not found in dependency graph")
                
        except Exception as e:
            logger.error(f"Error updating metadata for task {task_id}: {str(e)}")
            raise
    
    def get_graph_summary(self) -> Dict[str, Any]:
        """
        Get summary of the dependency graph.
        
        Returns:
            Dict[str, Any]: Graph summary information
        """
        try:
            return {
                'total_tasks': len(self.graph.nodes()),
                'total_dependencies': len(self.graph.edges()),
                'task_statuses': {
                    task_id: self.get_task_status(task_id)['status']
                    for task_id in self.graph.nodes()
                },
                'failed_tasks': self.get_failed_tasks(),
                'ready_tasks': self.get_ready_tasks()
            }
            
        except Exception as e:
            logger.error(f"Error getting graph summary: {str(e)}")
            return {'error': str(e)} 