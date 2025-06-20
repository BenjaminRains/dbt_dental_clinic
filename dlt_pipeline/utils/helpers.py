"""
Minimal utility functions for dlt pipeline.
Replaces your extensive utils/ directory with just the essentials.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import dlt
import os

logger = logging.getLogger(__name__)

def get_log_file_path(filename: str) -> str:
    """
    Get the full path for a log file in the logs directory.
    Creates the logs directory if it doesn't exist.
    
    Args:
        filename: Name of the log file
        
    Returns:
        Full path to the log file
    """
    # Get the directory of the current file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up one level to the elt_pipeline_dlt directory
    base_dir = os.path.dirname(current_dir)
    # Create logs directory path
    logs_dir = os.path.join(base_dir, 'logs')
    
    # Create logs directory if it doesn't exist
    os.makedirs(logs_dir, exist_ok=True)
    
    return os.path.join(logs_dir, filename)

def log_pipeline_results(load_info, context: str = "Pipeline"):
    """
    Log pipeline results in a readable format.
    Replaces your custom MetricsCollector functionality.
    
    Args:
        load_info: dlt load information
        context: Context description for logging
    """
    
    logger.info("="*60)
    logger.info(f"{context.upper()} RESULTS")
    logger.info("="*60)
    
    # Basic information
    logger.info(f"Load ID: {load_info.load_id}")
    logger.info(f"Pipeline: {load_info.pipeline.pipeline_name}")
    logger.info(f"Destination: {load_info.pipeline.destination}")
    logger.info(f"Dataset: {load_info.pipeline.dataset_name}")
    
    # Process each load package
    total_jobs = 0
    successful_jobs = 0
    failed_jobs = 0
    
    for load_package in load_info.load_packages:
        logger.info(f"\nLoad Package: {load_package.load_id}")
        logger.info(f"Started: {load_package.created_at}")
        
        # Job details
        for job in load_package.jobs.values():
            total_jobs += 1
            
            if job.status == "completed":
                successful_jobs += 1
                status_icon = "[SUCCESS]"
            else:
                failed_jobs += 1
                status_icon = "[FAILED]"
            
            logger.info(f"  {status_icon} {job.table_name}: {job.status}")
            
            # Show row count if available
            if hasattr(job, 'rows_count') and job.rows_count:
                logger.info(f"    Rows: {job.rows_count:,}")
    
    # Summary
    logger.info(f"\nSUMMARY:")
    logger.info(f"Total jobs: {total_jobs}")
    logger.info(f"Successful: {successful_jobs}")
    logger.info(f"Failed: {failed_jobs}")
    logger.info(f"Success rate: {(successful_jobs/total_jobs)*100:.1f}%" if total_jobs > 0 else "N/A")
    
    # Overall metrics if available
    if hasattr(load_info, 'metrics') and load_info.metrics:
        logger.info(f"Total rows processed: {load_info.metrics.get('rows_count', 'N/A')}")
    
    logger.info("="*60)

def validate_data_quality(pipeline) -> bool:
    """
    Basic data quality validation.
    Replaces your complex data quality monitoring.
    
    Args:
        pipeline: dlt pipeline instance
        
    Returns:
        bool: True if validation passes
    """
    
    logger.info("Starting data quality validation...")
    
    try:
        # Get pipeline state
        state = pipeline.state
        
        # Check if we have any tables
        sources = state.get('sources', {})
        if not sources:
            logger.warning("No sources found in pipeline state")
            return False
        
        # Basic validation: check that tables have data
        dataset = pipeline.dataset()
        
        if not dataset:
            logger.error("Could not access pipeline dataset")
            return False
        
        tables_checked = 0
        tables_with_data = 0
        
        # Check each table
        for source_name, source_state in sources.items():
            resources = source_state.get('resources', {})
            
            for table_name in resources.keys():
                try:
                    # Try to get table info
                    table = getattr(dataset, table_name, None)
                    if table is not None:
                        # Basic check: does table have any rows?
                        df = table.df()
                        row_count = len(df)
                        
                        tables_checked += 1
                        if row_count > 0:
                            tables_with_data += 1
                            logger.info(f"[SUCCESS] {table_name}: {row_count:,} rows")
                        else:
                            logger.warning(f"[WARNING] {table_name}: empty table")
                            
                except Exception as e:
                    logger.warning(f"Could not validate table {table_name}: {str(e)}")
        
        # Validation summary
        if tables_checked == 0:
            logger.warning("No tables could be validated")
            return False
        
        success_rate = (tables_with_data / tables_checked) * 100
        logger.info(f"Data quality validation: {tables_with_data}/{tables_checked} tables have data ({success_rate:.1f}%)")
        
        # Consider validation successful if at least 80% of tables have data
        return success_rate >= 80.0
        
    except Exception as e:
        logger.error(f"Data quality validation failed: {str(e)}")
        return False

def get_table_info(pipeline, table_name: str) -> Optional[Dict]:
    """
    Get basic information about a table.
    
    Args:
        pipeline: dlt pipeline instance
        table_name: Name of the table
        
    Returns:
        Dict with table information or None if not found
    """
    
    try:
        dataset = pipeline.dataset()
        table = getattr(dataset, table_name, None)
        
        if table is None:
            return None
        
        df = table.df()
        
        return {
            'table_name': table_name,
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'last_updated': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting table info for {table_name}: {str(e)}")
        return None

def compare_table_counts(pipeline, expected_counts: Dict[str, int]) -> Dict[str, Dict]:
    """
    Compare actual table counts with expected counts.
    
    Args:
        pipeline: dlt pipeline instance
        expected_counts: Dict mapping table names to expected row counts
        
    Returns:
        Dict with comparison results
    """
    
    results = {}
    
    for table_name, expected_count in expected_counts.items():
        try:
            table_info = get_table_info(pipeline, table_name)
            
            if table_info is None:
                results[table_name] = {
                    'status': 'missing',
                    'expected': expected_count,
                    'actual': 0,
                    'difference': -expected_count
                }
                continue
            
            actual_count = table_info['row_count']
            difference = actual_count - expected_count
            
            # Determine status
            if actual_count == expected_count:
                status = 'exact_match'
            elif actual_count >= expected_count:
                status = 'higher_than_expected'
            else:
                status = 'lower_than_expected'
            
            results[table_name] = {
                'status': status,
                'expected': expected_count,
                'actual': actual_count,
                'difference': difference,
                'percentage_diff': (difference / expected_count * 100) if expected_count > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error comparing counts for {table_name}: {str(e)}")
            results[table_name] = {
                'status': 'error',
                'error': str(e)
            }
    
    return results

def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
    """
    
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"

def get_pipeline_summary(pipeline) -> Dict:
    """
    Get a summary of the pipeline state and recent activity.
    
    Args:
        pipeline: dlt pipeline instance
        
    Returns:
        Dict with pipeline summary
    """
    
    try:
        state = pipeline.state
        
        # Get basic pipeline info
        summary = {
            'pipeline_name': pipeline.pipeline_name,
            'destination': str(pipeline.destination),
            'dataset_name': pipeline.dataset_name,
            'created_at': state.get('created_at'),
            'last_run': state.get('last_run'),
            'version': state.get('_dlt_version')
        }
        
        # Get source information
        sources = state.get('sources', {})
        summary['sources'] = {}
        
        for source_name, source_state in sources.items():
            resources = source_state.get('resources', {})
            summary['sources'][source_name] = {
                'resource_count': len(resources),
                'resources': list(resources.keys())
            }
        
        # Get dataset information if available
        try:
            dataset = pipeline.dataset()
            if dataset:
                # Count tables (this is a rough approximation)
                table_count = 0
                for source_info in summary['sources'].values():
                    table_count += source_info['resource_count']
                
                summary['table_count'] = table_count
        except:
            summary['table_count'] = 0
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting pipeline summary: {str(e)}")
        return {'error': str(e)}

# Simple monitoring functions
def log_memory_usage():
    """Log current memory usage"""
    try:
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024
        logger.info(f"Memory usage: {memory_mb:.1f} MB")
        
    except ImportError:
        logger.debug("psutil not available for memory monitoring")
    except Exception as e:
        logger.debug(f"Could not get memory usage: {str(e)}")

def create_simple_report(pipeline, output_file: str = "pipeline_report.txt"):
    """
    Create a simple text report of pipeline status.
    
    Args:
        pipeline: dlt pipeline instance
        output_file: Output file name (will be placed in logs directory)
    """
    
    try:
        summary = get_pipeline_summary(pipeline)
        
        # Get the full path in the logs directory
        output_path = get_log_file_path(output_file)
        
        with open(output_path, 'w') as f:
            f.write("DLT PIPELINE REPORT\n")
            f.write("="*50 + "\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n\n")
            
            f.write("PIPELINE INFORMATION:\n")
            f.write(f"Name: {summary.get('pipeline_name', 'N/A')}\n")
            f.write(f"Destination: {summary.get('destination', 'N/A')}\n")
            f.write(f"Dataset: {summary.get('dataset_name', 'N/A')}\n")
            f.write(f"Last Run: {summary.get('last_run', 'N/A')}\n")
            f.write(f"Table Count: {summary.get('table_count', 'N/A')}\n\n")
            
            f.write("SOURCES:\n")
            for source_name, source_info in summary.get('sources', {}).items():
                f.write(f"- {source_name}: {source_info['resource_count']} resources\n")
                for resource in source_info['resources']:
                    f.write(f"  * {resource}\n")
            
            f.write("\nReport completed successfully.\n")
        
        logger.info(f"Pipeline report written to: {output_path}")
        
    except Exception as e:
        logger.error(f"Error creating pipeline report: {str(e)}")