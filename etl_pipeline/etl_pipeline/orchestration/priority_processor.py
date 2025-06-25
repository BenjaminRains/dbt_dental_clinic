"""
Priority Processor

Handles table processing based on priority levels with intelligent parallelization.

STATUS: ACTIVE - Core Orchestration Component
============================================

This module is an active component of the orchestration framework that manages
table processing based on priority levels. It's actively used by the PipelineOrchestrator
and provides intelligent parallelization for critical tables.

CURRENT STATE:
- ✅ ACTIVE IMPLEMENTATION: Used by PipelineOrchestrator for batch processing
- ✅ INTELLIGENT PARALLELIZATION: Parallel processing for critical tables
- ✅ SEQUENTIAL PROCESSING: Sequential processing for non-critical tables
- ✅ ERROR HANDLING: Proper error handling and failure propagation
- ✅ RESOURCE MANAGEMENT: ThreadPoolExecutor for parallel processing
- ✅ MODERN CONFIG: Uses Settings class for table priority lookup
- ❌ UNTESTED: No comprehensive testing of parallel processing

ACTIVE USAGE:
- PipelineOrchestrator: Calls process_by_priority for batch table processing
- CLI Commands: Indirectly used through orchestrator
- Orchestration Framework: Core component of the pipeline architecture

DEPENDENCIES:
- TableProcessor: Processes individual tables
- ThreadPoolExecutor: Manages parallel processing
- Settings: Uses Settings.get_tables_by_priority() for table priority lookup

PROCESSING LOGIC:
1. Priority Levels: critical, important, audit, reference
2. Critical Tables: Processed in parallel for speed
3. Other Tables: Processed sequentially to manage resources
4. Failure Handling: Stops processing if critical tables fail
5. Resource Management: Configurable max_workers for parallel processing

INTEGRATION POINTS:
- PipelineOrchestrator: Main integration point for batch processing
- TableProcessor: Delegates individual table processing
- Settings: Uses modern Settings system for table priority lookup
- Logging: Comprehensive logging for monitoring and debugging

CRITICAL ISSUES:
1. UNTESTED PARALLEL PROCESSING: No validation of parallel processing logic
2. RESOURCE MANAGEMENT: No validation of thread pool management
3. ERROR PROPAGATION: Limited testing of failure scenarios

DEVELOPMENT NEEDS:
1. COMPREHENSIVE TESTING: Add tests for parallel and sequential processing
2. PERFORMANCE TESTING: Validate parallel processing performance
3. ERROR HANDLING: Test failure scenarios and error propagation
4. RESOURCE OPTIMIZATION: Optimize thread pool management
5. MONITORING: Add performance metrics for parallel processing

TESTING REQUIREMENTS:
1. Parallel Processing: Test with various worker counts and table sets
2. Sequential Processing: Test sequential processing logic
3. Error Scenarios: Test failure handling and propagation
4. Resource Management: Test thread pool cleanup and resource usage
5. Performance: Test processing time with different configurations
6. Integration: Test integration with TableProcessor and PipelineOrchestrator

This component is critical for efficient batch processing and needs
comprehensive testing for production readiness.
"""

import logging
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from .table_processor import TableProcessor
from ..config.settings import Settings

logger = logging.getLogger(__name__)

class PriorityProcessor:
    def __init__(self, settings: Settings = None):
        """
        Initialize the priority processor.
        
        Args:
            settings: Settings instance for table configuration (defaults to global settings)
        """
        self.settings = settings or Settings()
    
    def process_by_priority(self, table_processor: TableProcessor,
                          importance_levels: List[str] = None,
                          max_workers: int = 5,
                          force_full: bool = False) -> Dict[str, List[str]]:
        """
        Process tables by priority with intelligent parallelization.
        
        PROCESSING STRATEGY:
        - Critical tables: Processed in parallel for speed
        - Other tables: Processed sequentially to manage resources
        - Failure handling: Stops processing if critical tables fail
        
        Args:
            table_processor: Table processor instance
            importance_levels: List of importance levels to process
            max_workers: Maximum number of parallel workers
            force_full: Whether to force full extraction
            
        Returns:
            Dict with success/failure lists for each importance level
        """
        if importance_levels is None:
            importance_levels = ['critical', 'important', 'audit', 'reference']
            
        results = {}
        
        for importance in importance_levels:
            # Use Settings class for table priority lookup
            tables = self.settings.get_tables_by_priority(importance)
            if not tables:
                logger.info(f"No tables found for importance level: {importance}")
                continue
                
            logger.info(f"Processing {len(tables)} {importance} tables")
            
            if importance == 'critical' and len(tables) > 1:
                # Process critical tables in parallel for speed
                success_tables, failed_tables = self._process_parallel(
                    table_processor,
                    tables,
                    max_workers,
                    force_full
                )
            else:
                # Process other tables sequentially to manage resources
                success_tables, failed_tables = self._process_sequential(
                    table_processor,
                    tables,
                    force_full
                )
            
            results[importance] = {
                'success': success_tables,
                'failed': failed_tables,
                'total': len(tables)
            }
            
            logger.info(f"{importance.capitalize()} tables: {len(success_tables)}/{len(tables)} successful")
            
            # Stop processing if critical tables failed
            if importance == 'critical' and failed_tables:
                logger.error("Critical table failures detected. Stopping pipeline.")
                break
        
        return results
        
    def _process_parallel(self, table_processor: TableProcessor,
                         tables: List[str],
                         max_workers: int,
                         force_full: bool) -> Tuple[List[str], List[str]]:
        """Process tables in parallel using ThreadPoolExecutor."""
        success_tables = []
        failed_tables = []
        
        logger.info(f"Processing {len(tables)} tables in parallel (max workers: {max_workers})")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_table = {
                executor.submit(table_processor.process_table, table, force_full): table 
                for table in tables
            }
            
            # Process completed tasks
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    success = future.result()
                    if success:
                        success_tables.append(table)
                    else:
                        failed_tables.append(table)
                except Exception as e:
                    logger.error(f"Exception in parallel processing for {table}: {str(e)}")
                    failed_tables.append(table)
        
        return success_tables, failed_tables
        
    def _process_sequential(self, table_processor: TableProcessor,
                          tables: List[str],
                          force_full: bool) -> Tuple[List[str], List[str]]:
        """Process tables sequentially."""
        success_tables = []
        failed_tables = []
        
        logger.info(f"Processing {len(tables)} tables sequentially")
        
        for table in tables:
            try:
                success = table_processor.process_table(table, force_full)
                if success:
                    success_tables.append(table)
                else:
                    failed_tables.append(table)
            except Exception as e:
                logger.error(f"Exception in sequential processing for {table}: {str(e)}")
                failed_tables.append(table)
        
        return success_tables, failed_tables 