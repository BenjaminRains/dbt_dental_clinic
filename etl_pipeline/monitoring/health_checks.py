"""
Database health monitoring for the ETL pipeline.
Checks database connectivity and performance.
"""
from typing import Dict, List, Optional
import time
from sqlalchemy import create_engine, text
from etl_pipeline.config.settings import settings
from etl_pipeline.core.logger import get_logger
from etl_pipeline.monitoring.alerts import alert_manager

logger = get_logger(__name__)

class HealthChecker:
    """Monitors database health and performance."""
    
    def __init__(self):
        """Initialize health checker."""
        self.engines = {}
        self.last_check = {}
        self.check_interval = 300  # 5 minutes
    
    def _get_engine(self, db_type: str):
        """Get or create SQLAlchemy engine for database type."""
        if db_type not in self.engines:
            conn_string = settings.get_connection_string(db_type)
            self.engines[db_type] = create_engine(conn_string)
        return self.engines[db_type]
    
    def check_connection(self, db_type: str) -> bool:
        """
        Check database connection.
        
        Args:
            db_type: Type of database to check ('source', 'staging', 'target')
            
        Returns:
            bool: True if connection is healthy
        """
        try:
            engine = self._get_engine(db_type)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Database {db_type} connection check passed")
            return True
        except Exception as e:
            logger.error(f"Database {db_type} connection check failed: {e}")
            alert_manager.send_pipeline_alert(
                "health_check",
                "failed",
                f"Database {db_type} connection check failed",
                str(e)
            )
            return False
    
    def check_performance(self, db_type: str) -> Dict:
        """
        Check database performance metrics.
        
        Args:
            db_type: Type of database to check
            
        Returns:
            Dict: Performance metrics
        """
        metrics = {}
        try:
            engine = self._get_engine(db_type)
            with engine.connect() as conn:
                # Check query execution time
                start_time = time.time()
                conn.execute(text("SELECT 1"))
                metrics['query_time'] = time.time() - start_time
                
                # Get database-specific metrics
                if db_type in ['source', 'staging']:
                    # MySQL metrics
                    result = conn.execute(text("""
                        SHOW GLOBAL STATUS 
                        WHERE Variable_name IN (
                            'Threads_connected',
                            'Slow_queries',
                            'Questions'
                        )
                    """))
                    for row in result:
                        metrics[row[0]] = row[1]
                else:
                    # PostgreSQL metrics
                    result = conn.execute(text("""
                        SELECT 
                            numbackends as active_connections,
                            xact_commit as transactions_committed,
                            xact_rollback as transactions_rolled_back
                        FROM pg_stat_database 
                        WHERE datname = current_database()
                    """))
                    row = result.fetchone()
                    if row:
                        metrics.update(dict(row))
            
            logger.info(f"Database {db_type} performance check completed")
            return metrics
        except Exception as e:
            logger.error(f"Database {db_type} performance check failed: {e}")
            return {}
    
    def check_all(self) -> Dict[str, Dict]:
        """
        Check health of all databases.
        
        Returns:
            Dict: Health check results for each database
        """
        results = {}
        for db_type in ['source', 'staging', 'target']:
            results[db_type] = {
                'connection': self.check_connection(db_type),
                'performance': self.check_performance(db_type)
            }
        return results
    
    def should_check(self, db_type: str) -> bool:
        """
        Determine if health check should be performed.
        
        Args:
            db_type: Type of database to check
            
        Returns:
            bool: True if check should be performed
        """
        current_time = time.time()
        if db_type not in self.last_check:
            self.last_check[db_type] = 0
            return True
        
        return (current_time - self.last_check[db_type]) >= self.check_interval
    
    def run_health_checks(self) -> Dict[str, Dict]:
        """
        Run health checks for all databases if needed.
        
        Returns:
            Dict: Health check results
        """
        results = {}
        for db_type in ['source', 'staging', 'target']:
            if self.should_check(db_type):
                results[db_type] = {
                    'connection': self.check_connection(db_type),
                    'performance': self.check_performance(db_type)
                }
                self.last_check[db_type] = time.time()
        return results

# Create global health checker instance
health_checker = HealthChecker() 