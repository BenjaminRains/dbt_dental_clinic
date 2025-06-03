"""
Main entry point for the ETL pipeline.
Coordinates the execution of the pipeline components.
"""
import sys
import uuid
from datetime import datetime
from etl_pipeline.config.logging import setup_logging
from etl_pipeline.core.logger import get_logger
from etl_pipeline.config.settings import settings
from etl_pipeline.monitoring.metrics import metrics
from etl_pipeline.monitoring.alerts import alert_manager
from etl_pipeline.monitoring.health_checks import health_checker
from etl_pipeline.core.exceptions import ETLPipelineError

logger = get_logger(__name__)

def run_pipeline() -> None:
    """Run the ETL pipeline."""
    pipeline_id = str(uuid.uuid4())
    logger.info(f"Starting pipeline run {pipeline_id}")
    
    try:
        # Setup logging
        setup_logging()
        
        # Validate configuration
        if not settings.validate_configs():
            raise ETLPipelineError("Configuration validation failed")
        
        # Start metrics collection
        metrics.start_pipeline(pipeline_id)
        
        # Run health checks
        health_results = health_checker.check_all()
        if not all(result['connection'] for result in health_results.values()):
            raise ETLPipelineError("Health checks failed")
        
        # TODO: Implement pipeline execution logic
        # This will be implemented in subsequent steps
        
        # End metrics collection
        metrics.end_pipeline(pipeline_id)
        
        # Send success alert
        alert_manager.send_pipeline_alert(
            pipeline_id,
            "completed",
            f"Pipeline completed successfully at {datetime.now()}"
        )
        
        logger.info(f"Pipeline {pipeline_id} completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline {pipeline_id} failed: {e}")
        metrics.record_error(pipeline_id, str(e))
        alert_manager.send_pipeline_alert(
            pipeline_id,
            "failed",
            f"Pipeline failed at {datetime.now()}",
            str(e)
        )
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline() 