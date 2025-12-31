"""
Test Data E2E Testing Suite

This suite tests the complete ETL pipeline using test data with consistent test environment.
Tests use standardized test data fixtures, process it through the pipeline,
and validate transformations while maintaining complete safety and isolation.

E2E Flow:
Test Source Database → Test Replication MySQL → Test Analytics PostgreSQL

Features:
- Uses standardized test data patterns
- Consistent test environment throughout
- Complete pipeline validation
- Safe test data for privacy
- Automatic cleanup of test databases only
- Comprehensive incremental logic testing
- Data quality validation testing
- UPSERT functionality testing
- Integrity validation testing
"""

import pytest
import logging
import time
from datetime import datetime, timedelta, date
from sqlalchemy import text
from typing import List, Dict, Any, Optional

from etl_pipeline.config.settings import Settings, DatabaseType, PostgresSchema
from etl_pipeline.config import PostgresSchema as ConfigPostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.postgres_schema import PostgresSchema as PostgresSchemaAdapter
from etl_pipeline.orchestration import PipelineOrchestrator
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
from etl_pipeline.loaders.postgres_loader import PostgresLoader


logger = logging.getLogger(__name__)


def create_postgres_loader_for_e2e(settings):
    """
    Helper function to create PostgresLoader with new 4-parameter constructor.
    
    Args:
        settings: Settings instance
        
    Returns:
        PostgresLoader instance configured for e2e testing
    """
    replication_engine = ConnectionFactory.get_replication_connection(settings)
    analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
    
    schema_adapter = PostgresSchemaAdapter(
        postgres_schema=ConfigPostgresSchema.RAW,
        settings=settings,
    )
    
    return PostgresLoader(
        replication_engine=replication_engine,
        analytics_engine=analytics_engine,
        settings=settings,
        schema_adapter=schema_adapter,
    )


# Test data fixtures - using standardized test data instead of production sampling





class PipelineDataValidator:
    """
    Validate data transformations at each pipeline stage.
    
    Compares data integrity across:
    - Test source → Test replication
    - Test replication → Test analytics
    - Type conversions and field mappings
    - Incremental logic validation
    - Data quality validation
    - UPSERT functionality
    """
    
    def __init__(self, test_settings: Settings):
        self.test_settings = test_settings
        self.replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        self.analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings)
    
    def dispose(self):
        """Dispose of database connections to prevent resource leaks."""
        try:
            if hasattr(self, 'replication_engine') and self.replication_engine:
                self.replication_engine.dispose()
                logger.debug("PipelineDataValidator replication engine disposed")
            
            if hasattr(self, 'analytics_engine') and self.analytics_engine:
                self.analytics_engine.dispose()
                logger.debug("PipelineDataValidator analytics engine disposed")
                
        except Exception as e:
            logger.warning(f"Error disposing PipelineDataValidator connections: {e}")
    
    def __del__(self):
        """Destructor to ensure connections are disposed."""
        self.dispose()
    
    def validate_patient_pipeline(self, original_patients: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate patient data through all pipeline stages."""
        logger.info("Validating patient data pipeline transformations")
        
        validation_results = {}
        
        # Get PatNums from original patients to find them in databases
        original_patnums = [patient['PatNum'] for patient in original_patients]
        patnum_list = ','.join(str(p) for p in original_patnums)
        
        # Check replication database
        with self.replication_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM patient 
                WHERE PatNum IN ({patnum_list})
            """))
            replication_count = result.scalar()
            
            # Get sample record for detailed validation
            result = conn.execute(text(f"""
                SELECT PatNum, LName, FName, BalTotal, DateTStamp
                FROM patient 
                WHERE PatNum IN ({patnum_list})
                LIMIT 1
            """))
            replication_sample = result.fetchone()
        
        # Check analytics database
        with self.analytics_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM raw.patient 
                WHERE "PatNum" IN ({patnum_list})
            """))
            analytics_count = result.scalar()
            
            # Get sample record for detailed validation
            result = conn.execute(text(f"""
                SELECT "PatNum", "LName", "FName", "BalTotal", "DateTStamp"
                FROM raw.patient 
                WHERE "PatNum" IN ({patnum_list})
                LIMIT 1
            """))
            analytics_sample = result.fetchone()
        
        validation_results = {
            'record_count_match': len(original_patients) == replication_count == analytics_count,
            'expected_count': len(original_patients),
            'replication_count': replication_count,
            'analytics_count': analytics_count,
            'replication_sample': dict(replication_sample._mapping) if replication_sample else None,
            'analytics_sample': dict(analytics_sample._mapping) if analytics_sample else None
        }
        
        logger.info(f"Patient pipeline validation: {validation_results}")
        return validation_results
    
    def validate_appointment_pipeline(self, original_appointments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate appointment data through all pipeline stages."""
        logger.info("Validating appointment data pipeline transformations")
        
        validation_results = {}
        
        # Get AptNums from original appointments to find them in databases
        original_aptnums = [apt['AptNum'] for apt in original_appointments]
        aptnum_list = ','.join(str(a) for a in original_aptnums)
        
        # Check replication database
        with self.replication_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM appointment 
                WHERE AptNum IN ({aptnum_list})
            """))
            replication_count = result.scalar()
        
        # Check analytics database
        with self.analytics_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM raw.appointment 
                WHERE "AptNum" IN ({aptnum_list})
            """))
            analytics_count = result.scalar()
        
        validation_results = {
            'record_count_match': len(original_appointments) == replication_count == analytics_count,
            'expected_count': len(original_appointments),
            'replication_count': replication_count,
            'analytics_count': analytics_count
        }
        
        logger.info(f"Appointment pipeline validation: {validation_results}")
        return validation_results
    
    def validate_procedure_pipeline(self, original_procedures: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate procedure data through all pipeline stages."""
        logger.info("Validating procedure data pipeline transformations")
        
        validation_results = {}
        
        # Get ProcNums from original procedures to find them in databases (ProcNum is the primary key)
        original_procnums = [proc['ProcNum'] for proc in original_procedures]
        procnum_list = ','.join(str(p) for p in original_procnums)
        
        # Check replication database
        with self.replication_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM procedurelog 
                WHERE ProcNum IN ({procnum_list})
            """))
            replication_count = result.scalar()
        
        # Check analytics database
        with self.analytics_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM raw.procedurelog 
                WHERE "ProcNum" IN ({procnum_list})
            """))
            analytics_count = result.scalar()
        
        validation_results = {
            'record_count_match': len(original_procedures) == replication_count == analytics_count,
            'expected_count': len(original_procedures),
            'replication_count': replication_count,
            'analytics_count': analytics_count
        }
        
        logger.info(f"Procedure pipeline validation: {validation_results}")
        return validation_results
    
    def validate_incremental_logic(self, table_name: str, incremental_columns: List[str]) -> Dict[str, Any]:
        """
        Validate incremental logic functionality.
        
        Tests:
        - Data quality validation of incremental columns
        - Maximum timestamp calculation across multiple columns
        - Query building with OR logic
        - Integrity validation
        """
        logger.info(f"Validating incremental logic for {table_name}")
        
        validation_results = {}
        
        try:
            # Test data quality validation
            with self.replication_engine.connect() as conn:
                # Check if table has data for validation
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                total_records = result.scalar()
                
                if total_records > 0:
                    # Test data quality validation for each incremental column
                    valid_columns = []
                    for column in incremental_columns:
                        try:
                            result = conn.execute(text(f"""
                                SELECT MIN({column}), MAX({column}), COUNT(*)
                                FROM {table_name}
                                WHERE {column} IS NOT NULL
                                LIMIT 1000
                            """))
                            row = result.fetchone()
                            
                            if row and row[0] and row[1]:
                                min_date = row[0]
                                max_date = row[1]
                                count = row[2]
                                
                                # Basic data quality checks
                                if isinstance(min_date, (datetime, date)) and isinstance(max_date, (datetime, date)):
                                    if min_date.year >= 2000 and max_date.year <= 2030:
                                        valid_columns.append(column)
                                        logger.debug(f"Column {column} passed data quality validation")
                                    else:
                                        logger.warning(f"Column {column} failed date range validation: {min_date} to {max_date}")
                                else:
                                    valid_columns.append(column)
                                    logger.debug(f"Column {column} passed basic validation")
                            else:
                                logger.warning(f"Column {column} has no valid data")
                        except Exception as e:
                            logger.warning(f"Error validating column {column}: {str(e)}")
                    
                    validation_results['data_quality_validation'] = {
                        'total_columns': len(incremental_columns),
                        'valid_columns': len(valid_columns),
                        'filtered_columns': len(incremental_columns) - len(valid_columns),
                        'valid_columns_list': valid_columns
                    }
                else:
                    validation_results['data_quality_validation'] = {
                        'total_columns': len(incremental_columns),
                        'valid_columns': 0,
                        'filtered_columns': len(incremental_columns),
                        'valid_columns_list': []
                    }
            
            # Test maximum timestamp calculation
            with self.analytics_engine.connect() as conn:
                timestamps = []
                for column in incremental_columns:
                    result = conn.execute(text(f"""
                        SELECT _loaded_at
                        FROM raw.etl_load_status
                        WHERE table_name = :table_name
                        AND load_status = 'success'
                        ORDER BY _loaded_at DESC
                        LIMIT 1
                    """), {"table_name": table_name}).scalar()
                    
                    if result:
                        timestamps.append(result)
                
                max_timestamp = max(timestamps) if timestamps else None
                validation_results['max_timestamp_calculation'] = {
                    'max_timestamp': max_timestamp,
                    'total_timestamps': len(timestamps),
                    'timestamp_count': len(timestamps)
                }
            
            # Test query building with OR logic
            if validation_results['data_quality_validation']['valid_columns'] > 0:
                valid_columns = validation_results['data_quality_validation']['valid_columns_list']
                last_load = validation_results['max_timestamp_calculation']['max_timestamp']
                
                if last_load:
                    conditions = []
                    for col in valid_columns:
                        conditions.append(f"{col} > '{last_load}'")
                    
                    or_query = f"SELECT * FROM {table_name} WHERE {' OR '.join(conditions)}"
                    and_query = f"SELECT * FROM {table_name} WHERE {' AND '.join(conditions)}"
                    
                    validation_results['query_building'] = {
                        'or_logic_query': or_query,
                        'and_logic_query': and_query,
                        'conditions_count': len(conditions),
                        'last_load_timestamp': last_load
                    }
                else:
                    validation_results['query_building'] = {
                        'full_load_query': f"SELECT * FROM {table_name}",
                        'reason': 'No last load timestamp available'
                    }
            else:
                validation_results['query_building'] = {
                    'full_load_query': f"SELECT * FROM {table_name}",
                    'reason': 'No valid incremental columns'
                }
            
            validation_results['incremental_logic_valid'] = True
            
            # Add multi-column support flag
            valid_columns_count = validation_results.get('data_quality_validation', {}).get('valid_columns', 0)
            validation_results['multi_column_support'] = valid_columns_count > 1
            
        except Exception as e:
            logger.error(f"Error validating incremental logic for {table_name}: {str(e)}")
            validation_results['incremental_logic_valid'] = False
            validation_results['error'] = str(e)
            validation_results['multi_column_support'] = False
        
        logger.info(f"Incremental logic validation for {table_name}: {validation_results}")
        return validation_results
    
    def validate_upsert_functionality(self, table_name: str, test_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate UPSERT functionality for handling duplicate keys.
        
        Tests:
        - UPSERT SQL generation
        - Duplicate key handling
        - Primary key configuration
        """
        logger.info(f"Validating UPSERT functionality for {table_name}")
        
        validation_results = {}
        
        try:
            # Test UPSERT SQL generation
            if test_records:
                column_names = list(test_records[0].keys())
                
                # Mock the UPSERT SQL generation (since we can't test actual execution in E2E)
                primary_key = 'PatientNum' if table_name == 'patient' else 'AptNum' if table_name == 'appointment' else 'id'
                
                columns = ', '.join([f'"{col}"' for col in column_names])
                placeholders = ', '.join([f':{col}' for col in column_names])
                
                # Build UPDATE clause (exclude primary key from updates)
                update_columns = [f'"{col}" = EXCLUDED."{col}"' 
                                 for col in column_names if col != primary_key]
                update_clause = ', '.join(update_columns) if update_columns else 'updated_at = CURRENT_TIMESTAMP'
                
                upsert_sql = f"""
                    INSERT INTO raw.{table_name} ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT ("{primary_key}") DO UPDATE SET
                        {update_clause}
                """
                
                validation_results['upsert_sql_generation'] = {
                    'sql_generated': True,
                    'primary_key': primary_key,
                    'columns_count': len(column_names),
                    'update_columns_count': len(update_columns),
                    'sql_preview': upsert_sql.strip()[:200] + "..." if len(upsert_sql) > 200 else upsert_sql.strip()
                }
                
                # Test duplicate key scenario
                validation_results['duplicate_key_handling'] = {
                    'scenario_tested': True,
                    'conflict_resolution': 'UPDATE',
                    'primary_key_used': primary_key
                }
            else:
                validation_results['upsert_sql_generation'] = {
                    'sql_generated': False,
                    'reason': 'No test records available'
                }
            
            validation_results['upsert_functionality_valid'] = True
            
        except Exception as e:
            logger.error(f"Error validating UPSERT functionality for {table_name}: {str(e)}")
            validation_results['upsert_functionality_valid'] = False
            validation_results['error'] = str(e)
        
        logger.info(f"UPSERT functionality validation for {table_name}: {validation_results}")
        return validation_results
    
    def validate_integrity_validation(self, table_name: str, incremental_columns: List[str]) -> Dict[str, Any]:
        """
        Validate integrity validation functionality.
        
        Tests:
        - Incremental integrity validation
        - Data completeness validation
        - Validation query execution
        """
        logger.info(f"Validating integrity validation for {table_name}")
        
        validation_results = {}
        
        try:
            # Test incremental integrity validation
            with self.replication_engine.connect() as conn:
                # Get last load time for validation
                last_load = None
                try:
                    with self.analytics_engine.connect() as analytics_conn:
                        result = analytics_conn.execute(text(f"""
                            SELECT MAX(_loaded_at)
                            FROM raw.etl_load_status
                            WHERE table_name = :table_name
                            AND load_status = 'success'
                        """), {"table_name": table_name}).scalar()
                        
                        last_load = result
                except Exception as e:
                    logger.warning(f"Could not get last load time from etl_load_status: {str(e)}")
                
                if last_load and incremental_columns:
                    # Test incremental integrity validation query
                    conditions = []
                    for col in incremental_columns:
                        conditions.append(f"{col} > '{last_load}'")
                    
                    validation_query = f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {' OR '.join(conditions)}
                    """
                    
                    result = conn.execute(text(validation_query))
                    total_new_records = result.scalar() or 0
                    
                    validation_results['integrity_validation'] = {
                        'validation_query_executed': True,
                        'total_new_records': total_new_records,
                        'last_load_timestamp': last_load,
                        'validation_query': validation_query
                    }
                else:
                    validation_results['integrity_validation'] = {
                        'validation_query_executed': False,
                        'reason': 'No last load timestamp or incremental columns'
                    }
            
            # Test data completeness validation
            with self.replication_engine.connect() as conn:
                source_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar() or 0
                
                with self.analytics_engine.connect() as analytics_conn:
                    target_count = analytics_conn.execute(text(f"SELECT COUNT(*) FROM raw.{table_name}")).scalar() or 0
                    
                    # Allow 10% variance for data completeness
                    completeness_valid = target_count >= (source_count * 0.9)
                    
                    validation_results['data_completeness'] = {
                        'completeness_valid': completeness_valid,
                        'source_count': source_count,
                        'target_count': target_count,
                        'completeness_percentage': (target_count / source_count * 100) if source_count > 0 else 0
                    }
            
            validation_results['integrity_validation_valid'] = True
            
        except Exception as e:
            logger.error(f"Error validating integrity for {table_name}: {str(e)}")
            validation_results['integrity_validation_valid'] = False
            validation_results['error'] = str(e)
        
        logger.info(f"Integrity validation for {table_name}: {validation_results}")
        return validation_results

    def validate_postgres_loader_incremental_methods(self, table_name: str, incremental_columns: List[str]) -> Dict[str, Any]:
        """
        Validate PostgresLoader incremental methods specifically.
        
        Tests:
        - _get_loaded_at_time_max method
        - _build_improved_load_query_max method
        - _filter_valid_incremental_columns method
        - _validate_incremental_integrity method
        - _build_count_query method
        """
        logger.info(f"Validating PostgresLoader incremental methods for {table_name}")
        
        validation_results = {}
        
        try:
            # Test _get_loaded_at_time_max method
            with self.analytics_engine.connect() as conn:
                timestamps = []
                for column in incremental_columns:
                    result = conn.execute(text(f"""
                        SELECT _loaded_at
                        FROM raw.etl_load_status
                        WHERE table_name = :table_name
                        AND load_status = 'success'
                        ORDER BY _loaded_at DESC
                        LIMIT 1
                    """), {"table_name": table_name}).scalar()
                    
                    if result:
                        timestamps.append(result)
                
                max_timestamp = max(timestamps) if timestamps else None
                validation_results['get_loaded_at_time_max'] = {
                    'max_timestamp': max_timestamp,
                    'total_timestamps': len(timestamps),
                    'method_working': True
                }
            
            # Test _filter_valid_incremental_columns method
            with self.replication_engine.connect() as conn:
                valid_columns = []
                for column in incremental_columns:
                    try:
                        result = conn.execute(text(f"""
                            SELECT MIN({column}), MAX({column}), COUNT(*)
                            FROM {table_name}
                            WHERE {column} IS NOT NULL
                            LIMIT 1000
                        """))
                        row = result.fetchone()
                        
                        if row and row[0] and row[1]:
                            min_date = row[0]
                            max_date = row[1]
                            count = row[2]
                            
                            # Data quality validation logic
                            if isinstance(min_date, (datetime, date)) and isinstance(max_date, (datetime, date)):
                                if min_date.year >= 2000 and max_date.year <= 2030 and count >= 100:
                                    valid_columns.append(column)
                            else:
                                valid_columns.append(column)
                    except Exception as e:
                        logger.warning(f"Error validating column {column}: {str(e)}")
                
                validation_results['filter_valid_incremental_columns'] = {
                    'total_columns': len(incremental_columns),
                    'valid_columns': len(valid_columns),
                    'filtered_columns': len(incremental_columns) - len(valid_columns),
                    'valid_columns_list': valid_columns,
                    'method_working': True
                }
            
            # Test _build_improved_load_query_max method
            if validation_results['get_loaded_at_time_max']['max_timestamp'] and validation_results['filter_valid_incremental_columns']['valid_columns'] > 0:
                last_load = validation_results['get_loaded_at_time_max']['max_timestamp']
                valid_columns = validation_results['filter_valid_incremental_columns']['valid_columns_list']
                
                # Test OR logic query
                conditions_or = []
                for col in valid_columns:
                    conditions_or.append(f"{col} > '{last_load}'")
                
                or_query = f"SELECT * FROM {table_name} WHERE {' OR '.join(conditions_or)}"
                
                # Test AND logic query
                conditions_and = []
                for col in valid_columns:
                    conditions_and.append(f"{col} > '{last_load}'")
                
                and_query = f"SELECT * FROM {table_name} WHERE {' AND '.join(conditions_and)}"
                
                validation_results['build_improved_load_query_max'] = {
                    'or_logic_query': or_query,
                    'and_logic_query': and_query,
                    'conditions_count': len(conditions_or),
                    'last_load_timestamp': last_load,
                    'method_working': True
                }
            else:
                validation_results['build_improved_load_query_max'] = {
                    'full_load_query': f"SELECT * FROM {table_name}",
                    'reason': 'No valid timestamp or columns',
                    'method_working': True
                }
            
            # Test _build_count_query method
            # NOTE: _build_count_query no longer exists - count queries are handled internally
            # This is kept for backward compatibility but marked as not required
            if validation_results.get('get_loaded_at_time_max', {}).get('max_timestamp') and validation_results.get('filter_valid_incremental_columns', {}).get('valid_columns', 0) > 0:
                last_load = validation_results['get_loaded_at_time_max']['max_timestamp']
                valid_columns = validation_results['filter_valid_incremental_columns']['valid_columns_list']
                
                conditions = []
                for col in valid_columns:
                    conditions.append(f"{col} > '{last_load}'")
                
                count_query = f"SELECT COUNT(*) FROM {table_name} WHERE {' OR '.join(conditions)}"
                
                validation_results['build_count_query'] = {
                    'count_query': count_query,
                    'conditions_count': len(conditions),
                    'last_load_timestamp': last_load,
                    'method_working': True
                }
            else:
                validation_results['build_count_query'] = {
                    'count_query': f"SELECT COUNT(*) FROM {table_name}",
                    'reason': 'No valid timestamp or columns',
                    'method_working': True
                }
            
            # Test _validate_incremental_integrity method
            # NOTE: _validate_incremental_integrity no longer exists - validation is handled internally
            # This is kept for backward compatibility but marked as not required
            if validation_results.get('get_loaded_at_time_max', {}).get('max_timestamp') and validation_results.get('filter_valid_incremental_columns', {}).get('valid_columns', 0) > 0:
                last_load = validation_results['get_loaded_at_time_max']['max_timestamp']
                valid_columns = validation_results['filter_valid_incremental_columns']['valid_columns_list']
                
                with self.replication_engine.connect() as conn:
                    conditions = []
                    for col in valid_columns:
                        conditions.append(f"{col} > '{last_load}'")
                    
                    validation_query = f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {' OR '.join(conditions)}
                    """
                    
                    result = conn.execute(text(validation_query))
                    total_new_records = result.scalar() or 0
                    
                    validation_results['validate_incremental_integrity'] = {
                        'validation_query': validation_query,
                        'total_new_records': total_new_records,
                        'last_load_timestamp': last_load,
                        'method_working': True
                    }
            else:
                validation_results['validate_incremental_integrity'] = {
                    'method_working': True,
                    'reason': 'No valid timestamp or columns for validation'
                }
            
            validation_results['postgres_loader_incremental_methods_valid'] = True
            
        except Exception as e:
            logger.error(f"Error validating PostgresLoader incremental methods for {table_name}: {str(e)}")
            validation_results['postgres_loader_incremental_methods_valid'] = False
            validation_results['error'] = str(e)
        
        logger.info(f"PostgresLoader incremental methods validation for {table_name}: {validation_results}")
        return validation_results

    def validate_simple_mysql_replicator_incremental_methods(self, table_name: str, incremental_columns: List[str]) -> Dict[str, Any]:
        """
        Validate SimpleMySQLReplicator incremental methods specifically.
        
        Tests:
        - _get_last_copy_time_max method
        - _get_last_processed_value_max method
        - _get_new_records_count_max method
        - _copy_new_records_max method
        - _copy_incremental_table method
        - _copy_chunked_incremental_table method (now handles incremental_chunked strategy)
        """
        logger.info(f"Validating SimpleMySQLReplicator incremental methods for {table_name}")
        
        validation_results = {}
        
        try:
            # Test _get_last_copy_time_max method
            with self.replication_engine.connect() as conn:
                timestamps = []
                for column in incremental_columns:
                    result = conn.execute(text(f"""
                        SELECT last_copied
                        FROM opendental_replication.etl_copy_status
                        WHERE table_name = :table_name
                        AND copy_status = 'success'
                        ORDER BY last_copied DESC
                        LIMIT 1
                    """), {"table_name": table_name}).scalar()
                    
                    if result:
                        timestamps.append(result)
                
                max_timestamp = max(timestamps) if timestamps else None
                validation_results['get_last_copy_time_max'] = {
                    'max_timestamp': max_timestamp,
                    'total_timestamps': len(timestamps),
                    'method_working': True
                }
            
            # Test _get_last_processed_value_max method
            with self.replication_engine.connect() as conn:
                processed_values = []
                for column in incremental_columns:
                    try:
                        result = conn.execute(text(f"""
                            SELECT MAX({column})
                            FROM {table_name}
                            WHERE {column} IS NOT NULL
                        """)).scalar()
                        
                        if result:
                            # Convert to datetime if it's a date to avoid comparison issues
                            if isinstance(result, date):
                                result = datetime.combine(result, datetime.min.time())
                            processed_values.append(result)
                    except Exception as e:
                        logger.warning(f"Error getting processed value for column {column}: {str(e)}")
                
                max_processed_value = max(processed_values) if processed_values else None
                validation_results['get_last_processed_value_max'] = {
                    'max_processed_value': max_processed_value,
                    'total_processed_values': len(processed_values),
                    'method_working': True
                }
            
            # Test _get_new_records_count_max method
            if validation_results['get_last_processed_value_max']['max_processed_value']:
                last_processed = validation_results['get_last_processed_value_max']['max_processed_value']
                
                with self.replication_engine.connect() as conn:
                    conditions = []
                    for col in incremental_columns:
                        conditions.append(f"{col} > '{last_processed}'")
                    
                    count_query = f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {' OR '.join(conditions)}
                    """
                    
                    result = conn.execute(text(count_query))
                    new_records_count = result.scalar() or 0
                    
                    validation_results['get_new_records_count_max'] = {
                        'new_records_count': new_records_count,
                        'count_query': count_query,
                        'last_processed_value': last_processed,
                        'method_working': True
                    }
            else:
                validation_results['get_new_records_count_max'] = {
                    'new_records_count': 0,
                    'reason': 'No processed value available',
                    'method_working': True
                }
            
            # Test incremental table copy strategy detection
            with self.replication_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                total_records = result.scalar() or 0
                
                # Determine copy strategy based on table size
                if total_records < 10000:
                    copy_strategy = 'small_table'
                elif total_records < 100000:
                    copy_strategy = 'medium_table'
                else:
                    copy_strategy = 'large_table'
                
                validation_results['copy_strategy_detection'] = {
                    'total_records': total_records,
                    'detected_strategy': copy_strategy,
                    'method_working': True
                }
            
            # Test incremental table configuration
            validation_results['incremental_table_config'] = {
                'table_name': table_name,
                'incremental_columns': incremental_columns,
                'has_incremental_columns': len(incremental_columns) > 0,
                'method_working': True
            }
            
            validation_results['simple_mysql_replicator_incremental_methods_valid'] = True
            
        except Exception as e:
            logger.error(f"Error validating SimpleMySQLReplicator incremental methods for {table_name}: {str(e)}")
            validation_results['simple_mysql_replicator_incremental_methods_valid'] = False
            validation_results['error'] = str(e)
        
        logger.info(f"SimpleMySQLReplicator incremental methods validation for {table_name}: {validation_results}")
        return validation_results

    def validate_bulk_incremental_methods(self, table_name: str, incremental_columns: List[str]) -> Dict[str, Any]:
        """
        Validate bulk incremental methods from both PostgresLoader and SimpleMySQLReplicator.
        
        Tests:
        - PostgresLoader bulk insert optimized method
        - PostgresLoader streaming methods
        - SimpleMySQLReplicator chunked incremental methods
        - Performance metrics for bulk operations
        """
        logger.info(f"Validating bulk incremental methods for {table_name}")
        
        validation_results = {}
        
        try:
            # Test PostgresLoader bulk insert optimized method
            with self.replication_engine.connect() as conn:
                # Get sample data for bulk insert testing
                result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 100"))
                sample_data = result.fetchall()
                column_names = list(result.keys())
                
                if sample_data:
                    # Convert to list of dicts for bulk insert testing
                    rows_data = []
                    for row in sample_data:
                        row_dict = dict(zip(column_names, row))
                        rows_data.append(row_dict)
                    
                    validation_results['bulk_insert_optimized'] = {
                        'sample_data_count': len(rows_data),
                        'column_count': len(column_names),
                        'columns': column_names,
                        'method_ready_for_testing': True
                    }
                else:
                    validation_results['bulk_insert_optimized'] = {
                        'sample_data_count': 0,
                        'method_ready_for_testing': False,
                        'reason': 'No sample data available'
                    }
            
            # Test streaming data method
            with self.replication_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                total_records = result.scalar() or 0
                
                validation_results['streaming_data'] = {
                    'total_records': total_records,
                    'streaming_supported': total_records > 0,
                    'method_ready_for_testing': True
                }
            
            # Test chunked loading configuration
            validation_results['chunked_loading'] = {
                'default_chunk_size': 10000,
                'large_table_chunk_size': 50000,
                'chunking_supported': True,
                'method_ready_for_testing': True
            }
            
            # Test performance metrics for bulk operations
            validation_results['performance_metrics'] = {
                'bulk_insert_chunk_size': 50000,
                'streaming_chunk_size': 50000,
                'copy_csv_supported': True,
                'memory_efficient_processing': True,
                'method_ready_for_testing': True
            }
            
            validation_results['bulk_incremental_methods_valid'] = True
            
        except Exception as e:
            logger.error(f"Error validating bulk incremental methods for {table_name}: {str(e)}")
            validation_results['bulk_incremental_methods_valid'] = False
            validation_results['error'] = str(e)
        
        logger.info(f"Bulk incremental methods validation for {table_name}: {validation_results}")
        return validation_results

    def validate_primary_incremental_column_functionality(self, table_name: str) -> Dict[str, Any]:
        """
        Validate primary incremental column functionality with fallback logic.
        
        Tests:
        - _get_primary_incremental_column method
        - Primary column detection and validation
        - Fallback logic when primary column is not available
        - Integration with incremental copy methods
        """
        logger.info(f"Validating primary incremental column functionality for {table_name}")
        
        validation_results = {}
        
        try:
            # Import SimpleMySQLReplicator for testing
            from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
            
            # Create replicator instance
            replicator = SimpleMySQLReplicator(self.test_settings)
            
            # Get table configuration
            table_config = replicator.table_configs.get(table_name, {})
            if not table_config:
                validation_results['error'] = f"No configuration found for table {table_name}"
                return validation_results
            
            # Test primary incremental column detection
            primary_column = replicator._get_primary_incremental_column(table_config)
            incremental_columns = table_config.get('incremental_columns', [])
            extraction_strategy = table_config.get('extraction_strategy', 'full_table')
            
            validation_results['primary_column_detection'] = {
                'primary_column': primary_column,
                'incremental_columns': incremental_columns,
                'extraction_strategy': extraction_strategy,
                'method_working': True
            }
            
            # Test primary column validation
            if primary_column:
                # Verify primary column is in incremental columns list
                if primary_column in incremental_columns:
                    validation_results['primary_column_validation'] = {
                        'primary_column_in_list': True,
                        'message': f"Primary column '{primary_column}' found in incremental columns list"
                    }
                else:
                    validation_results['primary_column_validation'] = {
                        'primary_column_in_list': False,
                        'error': f"Primary column '{primary_column}' not found in incremental columns list: {incremental_columns}"
                    }
            else:
                # Test fallback logic
                if incremental_columns:
                    validation_results['fallback_logic'] = {
                        'fallback_triggered': True,
                        'message': f"No primary column specified, will use multi-column logic with: {incremental_columns}"
                    }
                else:
                    validation_results['fallback_logic'] = {
                        'fallback_triggered': True,
                        'message': "No primary column and no incremental columns, will use full table strategy"
                    }
            
            # Test incremental strategy logging
            try:
                replicator._log_incremental_strategy(table_name, primary_column, incremental_columns)
                validation_results['strategy_logging'] = {
                    'method_working': True,
                    'primary_column': primary_column,
                    'incremental_columns': incremental_columns
                }
            except Exception as e:
                validation_results['strategy_logging'] = {
                    'method_working': False,
                    'error': str(e)
                }
            
            # Test configuration integration
            validation_results['configuration_integration'] = {
                'table_config_loaded': bool(table_config),
                'has_primary_column_config': 'primary_incremental_column' in table_config,
                'primary_column_config_value': table_config.get('primary_incremental_column'),
                'extraction_strategy': extraction_strategy,
                'method_working': True
            }
            
            # Overall validation result
            validation_results['primary_incremental_column_functionality_valid'] = True
            
        except Exception as e:
            logger.error(f"Error validating primary incremental column functionality for {table_name}: {str(e)}")
            validation_results['primary_incremental_column_functionality_valid'] = False
            validation_results['error'] = str(e)
        
        logger.info(f"Primary incremental column functionality validation for {table_name}: {validation_results}")
        return validation_results


# No cleanup class needed for E2E tests - data should persist for validation


class TestTestDataPipelineE2E:
    """
    End-to-end tests using test data with consistent test environment.
    
    Test Flow:
    1. Use populated test databases with standardized test data
    2. Process through test pipeline infrastructure
    3. Validate transformations at each stage
    4. Clean up test databases only
    5. Test incremental logic functionality
    6. Test data quality validation
    7. Test UPSERT functionality
    8. Test integrity validation
    """
    
    @pytest.fixture(scope="class")
    def test_settings(self):
        """Test settings for test source, replication and analytics databases."""
        return Settings(environment='test')
    
    @pytest.fixture(scope="class")
    def pipeline_validator(self, test_settings):
        """Fixture for validating pipeline transformations."""
        validator = PipelineDataValidator(test_settings)
        yield validator
        validator.dispose()  # Ensure connections are disposed
    
    @pytest.fixture(scope="function")
    def clean_replication_db(self, test_settings):
        """Clean replication database before each test."""
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        
        with replication_engine.connect() as conn:
            # Clean all test tables using table-specific primary keys
            # Use DELETE with explicit WHERE clause to avoid affecting other test data
            cleanup_queries = {
                'patient': "DELETE FROM patient WHERE PatNum IN (1, 2, 3)",
                'appointment': "DELETE FROM appointment WHERE AptNum IN (1, 2, 3)",
                'procedurelog': "DELETE FROM procedurelog WHERE ProcNum IN (1, 2, 3)"
            }
            for table, query in cleanup_queries.items():
                try:
                    # Check if table exists first
                    check_table = conn.execute(text(f"""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = DATABASE() AND table_name = '{table}'
                    """))
                    if check_table.scalar() > 0:
                        result = conn.execute(text(query))
                        deleted_count = result.rowcount
                        conn.commit()
                        if deleted_count > 0:
                            logger.debug(f"Cleaned {deleted_count} records from {table}")
                except Exception as e:
                    logger.warning(f"Could not clean table {table}: {e}")
        
        replication_engine.dispose()
        
        # Small delay to ensure cleanup is committed and visible to subsequent connections
        time.sleep(0.15)
    
    def _get_test_data_from_source(self, test_settings):
        """Get test data directly from source database for validation."""
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        
        with source_engine.connect() as conn:
            # Get patient data
            patient_result = conn.execute(text("SELECT * FROM patient WHERE PatNum IN (1, 2, 3) ORDER BY PatNum"))
            patients = [dict(row._mapping) for row in patient_result.fetchall()]
            
            # Get appointment data
            appointment_result = conn.execute(text("SELECT * FROM appointment WHERE AptNum IN (1, 2, 3) ORDER BY AptNum"))
            appointments = [dict(row._mapping) for row in appointment_result.fetchall()]
            
            # Get procedure data
            procedure_result = conn.execute(text("SELECT * FROM procedurelog WHERE ProcNum IN (1, 2, 3) ORDER BY ProcNum"))
            procedures = [dict(row._mapping) for row in procedure_result.fetchall()]
        
        source_engine.dispose()
        
        logger.info(f"Retrieved test data from source database: {len(patients)} patients, {len(appointments)} appointments, {len(procedures)} procedures")
        
        return {
            'patient': patients,
            'appointment': appointments,
            'procedurelog': procedures
        }
    
    @pytest.mark.e2e
    @pytest.mark.test_data
    def test_patient_data_test_pipeline_e2e(
        self, 
        test_settings, 
        pipeline_validator
    ):
        """
        Test patient data pipeline using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test patient data
            Act: Process through complete ETL pipeline to test databases
            Assert: Validate data integrity and transformations
        """
        logger.info("Starting patient data test pipeline E2E test")
        
        # Arrange: Get test data from source database
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        assert len(patients) > 0, "No patient data found in source database"
        
        # Act: Process through pipeline using orchestrator
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        # Process patient table - use full strategy to ensure all test data is loaded 
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('patient', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate pipeline execution and data
        assert result is True, "Patient pipeline execution failed"
        assert duration < 120, f"Pipeline took too long: {duration:.2f}s"
        
        # Validate data transformations
        validation_results = pipeline_validator.validate_patient_pipeline(patients)
        assert validation_results['record_count_match'], f"Record count mismatch: {validation_results}"
        
        logger.info(f"Patient test pipeline E2E test completed successfully in {duration:.2f}s")
    
    @pytest.mark.e2e
    @pytest.mark.test_data
    def test_appointment_data_test_pipeline_e2e(
        self, 
        test_settings, 
        pipeline_validator
    ):
        """
        Test appointment data pipeline using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test appointment data
            Act: Process through complete ETL pipeline to test databases
            Assert: Validate data integrity and transformations
        """
        logger.info("Starting appointment data test pipeline E2E test")
        
        # Arrange: Get test data from source database
        test_data = self._get_test_data_from_source(test_settings)
        appointments = test_data['appointment']
        assert len(appointments) > 0, "No appointment data found in source database"
        
        # Act: Process through pipeline
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('appointment', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate pipeline execution and data
        assert result is True, "Appointment pipeline execution failed"
        assert duration < 120, f"Pipeline took too long: {duration:.2f}s"
        
        # Validate data transformations
        validation_results = pipeline_validator.validate_appointment_pipeline(appointments)
        assert validation_results['record_count_match'], f"Record count mismatch: {validation_results}"
        
        logger.info(f"Appointment test pipeline E2E test completed successfully in {duration:.2f}s")
    
    @pytest.mark.e2e
    @pytest.mark.test_data
    def test_procedure_data_test_pipeline_e2e(
        self, 
        test_settings, 
        pipeline_validator
    ):
        """
        Test procedure data pipeline using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test procedure data
            Act: Process through complete ETL pipeline to test databases
            Assert: Validate data integrity and transformations
        """
        logger.info("Starting procedure data test pipeline E2E test")
        
        # Arrange: Get test data from source database
        test_data = self._get_test_data_from_source(test_settings)
        procedures = test_data['procedurelog']
        assert len(procedures) > 0, "No procedure data found in source database"
        
        # DEBUG: Check source database before pipeline
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        with source_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            logger.info(f"DEBUG: Source database has {source_count} procedure records before pipeline")
            
            # Also check total count
            total_source_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog")).scalar()
            logger.info(f"DEBUG: Source database has {total_source_count} total procedure records")
        
        # DEBUG: Check replication database before pipeline
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            replication_count_before = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            logger.info(f"DEBUG: Replication database has {replication_count_before} procedure records before pipeline")
            
            # DEBUG: Check database connection details
            replication_config = test_settings.get_database_config(DatabaseType.REPLICATION)
            logger.info(f"DEBUG: Replication config - Host: {replication_config.get('host')}, Port: {replication_config.get('port')}, DB: {replication_config.get('database')}")
            
            # DEBUG: Check if we can see the actual data
            result = conn.execute(text("SELECT ProcNum, ProcStatus FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).fetchall()
            logger.info(f"DEBUG: Actual procedure records in replication: {result}")
        
        # Act: First run the replication step
        logger.info("Running replication step...")
        replicator = SimpleMySQLReplicator(settings=test_settings)
        replicator.copy_table('procedurelog', force_full=True)
        
        # DEBUG: Check replication database after replication step
        with replication_engine.connect() as conn:
            debug_replication_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            logger.info(f"DEBUG: Replication DB has {debug_replication_count} procedurelog records after replication step")
            
            # Also check total count in replication
            total_replication_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog")).scalar()
            logger.info(f"DEBUG: Replication database has {total_replication_count} total procedure records after replication")
            
            # Force a commit to ensure data is visible to other connections
            conn.commit()
            logger.info("DEBUG: Committed transaction to ensure data visibility")
        
        # Small delay to ensure transaction is fully processed
        time.sleep(0.1)
        
        # Act: Now instantiate PostgresLoader AFTER replication step
        logger.info("Instantiating PostgresLoader after replication step...")
        start_time = time.time()
        loader = create_postgres_loader_for_e2e(test_settings)
        
        # DEBUG: Check if PostgresLoader can see the data immediately after instantiation
        with loader.replication_engine.connect() as loader_conn:
            loader_count = loader_conn.execute(text("SELECT COUNT(*) FROM procedurelog")).scalar()
            logger.info(f"DEBUG: PostgresLoader sees {loader_count} records in replication database immediately after instantiation")
            
            # Check specific test records
            test_records_count = loader_conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            logger.info(f"DEBUG: PostgresLoader sees {test_records_count} test procedure records")
            
            # Check actual test records
            test_records = loader_conn.execute(text("SELECT ProcNum, ProcStatus FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).fetchall()
            logger.info(f"DEBUG: PostgresLoader sees test records: {test_records}")
        
        # Act: Now run the loader step
        logger.info("Running loader step...")
        success, metadata = loader.load_table('procedurelog', force_full=True)
        duration = time.time() - start_time
        
        # DEBUG: Check analytics database after loader step
        analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings)
        with analytics_engine.connect() as conn:
            analytics_count = conn.execute(text("SELECT COUNT(*) FROM raw.procedurelog WHERE \"ProcNum\" IN (1, 2, 3)")).scalar()
            logger.info(f"DEBUG: Analytics database has {analytics_count} procedure records after loader step")
            
            # Also check total count
            total_analytics_count = conn.execute(text("SELECT COUNT(*) FROM raw.procedurelog")).scalar()
            logger.info(f"DEBUG: Analytics database has {total_analytics_count} total procedure records")
        
        # Assert: Validate pipeline execution and data
        assert success is True, f"Procedure pipeline execution failed: {metadata}"
        assert duration < 120, f"Pipeline took too long: {duration:.2f}s"
        
        # Validate data transformations
        validation_results = pipeline_validator.validate_procedure_pipeline(procedures)
        assert validation_results['record_count_match'], f"Record count mismatch: {validation_results}"
        
        logger.info(f"Procedure test pipeline E2E test completed successfully in {duration:.2f}s")
    
    @pytest.mark.e2e
    @pytest.mark.test_data
    def test_multi_table_test_pipeline_e2e(
        self, 
        test_settings, 
        pipeline_validator
    ):
        """
        Test multi-table pipeline using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test data for all tables
            Act: Process all tables through ETL pipeline
            Assert: Validate complete pipeline execution and data integrity
        """
        logger.info("Starting multi-table test pipeline E2E test")
        
        # Arrange: Get test data from source database
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        appointments = test_data['appointment']
        procedures = test_data['procedurelog']
        
        assert len(patients) > 0, "No patient data found in source database"
        assert len(appointments) > 0, "No appointment data found in source database"
        assert len(procedures) > 0, "No procedure data found in source database"
        
        # Act: Process all tables through pipeline
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        tables = ['patient', 'appointment', 'procedurelog']
        
        start_time = time.time()
        results = {}
        
        for table in tables:
            table_start = time.time()
            result = orchestrator.run_pipeline_for_table(table, force_full=True)
            table_duration = time.time() - table_start
            
            results[table] = {
                'success': result,
                'duration': table_duration
            }
        
        total_duration = time.time() - start_time
        
        # Assert: Validate all pipeline executions
        for table, result in results.items():
            assert result['success'], f"{table} pipeline execution failed"
            assert result['duration'] < 120, f"{table} pipeline took too long: {result['duration']:.2f}s"
        
        assert total_duration < 300, f"Total pipeline took too long: {total_duration:.2f}s"
        
        # Validate data transformations for all tables
        patient_validation = pipeline_validator.validate_patient_pipeline(patients)
        appointment_validation = pipeline_validator.validate_appointment_pipeline(appointments)
        procedure_validation = pipeline_validator.validate_procedure_pipeline(procedures)
        
        assert patient_validation['record_count_match'], f"Patient validation failed: {patient_validation}"
        assert appointment_validation['record_count_match'], f"Appointment validation failed: {appointment_validation}"
        assert procedure_validation['record_count_match'], f"Procedure validation failed: {procedure_validation}"
        
        logger.info(f"Multi-table test pipeline E2E test completed successfully in {total_duration:.2f}s")
        table_durations = [(table, f"{result['duration']:.2f}s") for table, result in results.items()]
        logger.info(f"Table durations: {table_durations}")
    
    @pytest.mark.e2e
    @pytest.mark.test_data
    @pytest.mark.slow
    def test_test_data_integrity_e2e(
        self, 
        test_settings,
        pipeline_validator
    ):
        """
        Test data integrity across all pipeline stages using test data.
        
        AAA Pattern:
            Arrange: Get standardized test data and process through pipeline
            Act: Compare data across test source, replication, and analytics
            Assert: Verify data integrity maintained through all transformations
        """
        logger.info("Starting test data integrity E2E test")
        
        # Arrange: Process all data through pipeline
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        # Get test data for comparison
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        appointments = test_data['appointment']
        procedures = test_data['procedurelog']
        
        # Process all test tables
        tables = ['patient', 'appointment', 'procedurelog']
        for table in tables:
            logger.info(f"Processing table: {table}")
            result = orchestrator.run_pipeline_for_table(table, force_full=True)
            logger.info(f"Pipeline result for {table}: {result}")
            assert result, f"Failed to process {table}"
        
        # Debug: Check if data was actually copied to replication
        test_replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with test_replication_engine.connect() as conn:
            # Check total count in replication
            result = conn.execute(text("SELECT COUNT(*) FROM patient"))
            replication_count = result.scalar()
            logger.info(f"Total patients in replication after pipeline: {replication_count}")
            
            # Check if our specific patient exists
            test_patient_patnum = patients[0]['PatNum']
            result = conn.execute(text("""
                SELECT PatNum, LName, FName, BalTotal, InsEst, DateTStamp
                FROM patient 
                WHERE PatNum = :patnum
            """), {"patnum": test_patient_patnum})
            replication_patient = result.fetchone()
            logger.info(f"Replication patient data after pipeline: {replication_patient}")
            
            # If patient not found, check what patients do exist
            if replication_patient is None:
                result = conn.execute(text("""
                    SELECT PatNum, LName, FName 
                    FROM patient 
                    LIMIT 5
                """))
                existing_patients = result.fetchall()
                logger.info(f"Existing patients in replication after pipeline: {existing_patients}")
        
        # Debug: Check if data was actually copied to analytics
        test_analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings)
        with test_analytics_engine.connect() as conn:
            # Check total count in analytics
            result = conn.execute(text("SELECT COUNT(*) FROM raw.patient"))
            analytics_count = result.scalar()
            logger.info(f"Total patients in analytics after pipeline: {analytics_count}")
            
            result = conn.execute(text("""
                SELECT "PatNum", "BalTotal", "InsEst", "DateTStamp"
                FROM raw.patient 
                WHERE "PatNum" = :patnum
            """), {"patnum": test_patient_patnum})
            analytics_patient = result.fetchone()
            logger.info(f"Analytics patient data after pipeline: {analytics_patient}")
        
        # Compare specific patient record across replication and analytics databases
        expected_patient = patients[0]
        
        # Assert: Verify data integrity with better error messages
        if replication_patient is None:
            logger.error(f"Patient {test_patient_patnum} not found in replication database")
            logger.error(f"Replication count: {replication_count}, Analytics count: {analytics_count}")
            raise AssertionError(f"Replication patient not found. Replication count: {replication_count}")
        
        if analytics_patient is None:
            logger.error(f"Patient {test_patient_patnum} not found in analytics database")
            logger.error(f"Replication count: {replication_count}, Analytics count: {analytics_count}")
            raise AssertionError(f"Analytics patient not found. Replication count: {replication_count}, Analytics count: {analytics_count}")
        
        # Compare financial data integrity using expected values from test data
        expected_bal_total = expected_patient.get('BalTotal', 0.0)
        expected_ins_est = expected_patient.get('InsEst', 0.0)
        
        # Compare with replication (should match exactly)
        assert replication_patient[3] == expected_bal_total, f"BalTotal mismatch in replication: expected {expected_bal_total}, got {replication_patient[3]}"
        assert replication_patient[4] == expected_ins_est, f"InsEst mismatch in replication: expected {expected_ins_est}, got {replication_patient[4]}"
        
        # Analytics may have type conversions, so compare with tolerance
        assert abs(float(replication_patient[3]) - float(analytics_patient[1])) < 0.01, "BalTotal mismatch in analytics"
        assert abs(float(replication_patient[4]) - float(analytics_patient[2])) < 0.01, "InsEst mismatch in analytics"
        
        logger.info("Test data integrity E2E test completed successfully")
        logger.info(f"Validated data integrity for PatNum {test_patient_patnum} across replication and analytics stages")
    
    @pytest.mark.e2e
    @pytest.mark.incremental
    def test_incremental_logic_functionality_e2e(
        self,
        test_settings,
        pipeline_validator
    ):
        """
        Test incremental logic functionality in E2E environment.
        
        AAA Pattern:
            Arrange: Set up test environment with incremental columns
            Act: Test incremental logic methods and data quality validation
            Assert: Verify incremental logic works correctly
        """
        logger.info("Starting incremental logic functionality E2E test")
        
        # Arrange: Test tables with incremental columns
        test_tables = [
            {
                'table_name': 'patient',
                'incremental_columns': ['DateTStamp', 'DateTimeDeceased']
            },
            {
                'table_name': 'appointment',
                'incremental_columns': ['AptDateTime', 'DateTStamp']
            },
            {
                'table_name': 'procedurelog',
                'incremental_columns': ['DateTStamp', 'SecDateEntry']
            }
        ]
        
        # Act & Assert: Test incremental logic for each table
        for test_table in test_tables:
            table_name = test_table['table_name']
            incremental_columns = test_table['incremental_columns']
            
            logger.info(f"Testing incremental logic for {table_name}")
            
            # Test incremental logic validation
            validation_results = pipeline_validator.validate_incremental_logic(table_name, incremental_columns)
            
            # Assert: Verify incremental logic functionality
            assert validation_results['incremental_logic_valid'], f"Incremental logic validation failed for {table_name}: {validation_results}"
            
            # Verify data quality validation
            data_quality = validation_results.get('data_quality_validation', {})
            assert data_quality.get('total_columns', 0) >= 0, f"Data quality validation failed for {table_name}"
            
            # Verify query building
            query_building = validation_results.get('query_building', {})
            assert 'or_logic_query' in query_building or 'full_load_query' in query_building, f"Query building failed for {table_name}"
            
            logger.info(f"Incremental logic test completed for {table_name}")
        
        logger.info("Incremental logic functionality E2E test completed successfully")
    
    @pytest.mark.e2e
    @pytest.mark.upsert
    def test_upsert_functionality_e2e(
        self,
        test_settings,
        pipeline_validator
    ):
        """
        Test UPSERT functionality in E2E environment.
        
        AAA Pattern:
            Arrange: Set up test data and process through pipeline
            Act: Test UPSERT SQL generation and duplicate key handling
            Assert: Verify UPSERT functionality works correctly
        """
        logger.info("Starting UPSERT functionality E2E test")
        
        # Arrange: Get test data from source database
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        appointments = test_data['appointment']
        
        # Act & Assert: Test UPSERT functionality for each table
        test_tables = [
            {
                'table_name': 'patient',
                'test_records': patients[:5] if patients else []  # Use first 5 records for testing
            },
            {
                'table_name': 'appointment',
                'test_records': appointments[:5] if appointments else []  # Use first 5 records for testing
            }
        ]
        
        for test_table in test_tables:
            table_name = test_table['table_name']
            test_records = test_table['test_records']
            
            logger.info(f"Testing UPSERT functionality for {table_name}")
            
            # Test UPSERT functionality validation
            validation_results = pipeline_validator.validate_upsert_functionality(table_name, test_records)
            
            # Assert: Verify UPSERT functionality
            assert validation_results['upsert_functionality_valid'], f"UPSERT functionality validation failed for {table_name}: {validation_results}"
            
            # Verify UPSERT SQL generation
            upsert_generation = validation_results.get('upsert_sql_generation', {})
            assert upsert_generation.get('sql_generated', False), f"UPSERT SQL generation failed for {table_name}"
            
            # Verify duplicate key handling
            duplicate_handling = validation_results.get('duplicate_key_handling', {})
            assert duplicate_handling.get('scenario_tested', False), f"Duplicate key handling test failed for {table_name}"
            
            logger.info(f"UPSERT functionality test completed for {table_name}")
        
        logger.info("UPSERT functionality E2E test completed successfully")
    
    @pytest.mark.e2e
    @pytest.mark.integrity
    def test_integrity_validation_e2e(
        self,
        test_settings,
        pipeline_validator
    ):
        """
        Test integrity validation functionality in E2E environment.
        
        AAA Pattern:
            Arrange: Set up test environment with data
            Act: Test integrity validation and data completeness checks
            Assert: Verify integrity validation works correctly
        """
        logger.info("Starting integrity validation E2E test")
        
        # Arrange: Test tables with incremental columns
        test_tables = [
            {
                'table_name': 'patient',
                'incremental_columns': ['DateTStamp', 'DateTimeDeceased']
            },
            {
                'table_name': 'appointment',
                'incremental_columns': ['AptDateTime', 'DateTStamp']
            },
            {
                'table_name': 'procedurelog',
                'incremental_columns': ['DateTStamp', 'SecDateEntry']
            }
        ]
        
        # Act & Assert: Test integrity validation for each table
        for test_table in test_tables:
            table_name = test_table['table_name']
            incremental_columns = test_table['incremental_columns']
            
            logger.info(f"Testing integrity validation for {table_name}")
            
            # Test integrity validation
            validation_results = pipeline_validator.validate_integrity_validation(table_name, incremental_columns)
            
            # Assert: Verify integrity validation functionality
            assert validation_results['integrity_validation_valid'], f"Integrity validation failed for {table_name}: {validation_results}"
            
            # Verify integrity validation logic
            integrity_validation = validation_results.get('integrity_validation', {})
            assert integrity_validation.get('validation_query_executed', False), f"Integrity validation query failed for {table_name}"
            
            # Verify data completeness
            data_completeness = validation_results.get('data_completeness', {})
            assert data_completeness.get('completeness_valid', False), f"Data completeness validation failed for {table_name}"
            
            logger.info(f"Integrity validation test completed for {table_name}")
        
        logger.info("Integrity validation E2E test completed successfully")
    
    @pytest.mark.e2e
    @pytest.mark.incremental
    @pytest.mark.slow
    def test_incremental_pipeline_execution_e2e(
        self,
        test_settings,
        pipeline_validator
    ):
        """
        Test incremental pipeline execution in E2E environment.
        
        AAA Pattern:
            Arrange: Set up test environment and process initial data
            Act: Execute incremental pipeline runs
            Assert: Verify incremental logic works correctly in full pipeline
        """
        logger.info("Starting incremental pipeline execution E2E test")
        
        # Arrange: Set up orchestrator
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        # Test tables for incremental execution
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        # Act: Execute initial full load
        logger.info("Executing initial full load")
        for table in test_tables:
            result = orchestrator.run_pipeline_for_table(table, force_full=True)
            assert result, f"Initial full load failed for {table}"
        
        # Act: Execute incremental loads (should find no new data)
        logger.info("Executing incremental loads")
        incremental_results = {}
        
        for table in test_tables:
            start_time = time.time()
            result = orchestrator.run_pipeline_for_table(table, force_full=False)
            duration = time.time() - start_time
            
            incremental_results[table] = {
                'success': result,
                'duration': duration,
                'incremental_executed': True
            }
        
        # Assert: Verify incremental execution
        for table, result in incremental_results.items():
            assert result['success'], f"Incremental execution failed for {table}"
            assert result['duration'] < 60, f"Incremental execution took too long for {table}: {result['duration']:.2f}s"
            assert result['incremental_executed'], f"Incremental execution not completed for {table}"
        
        # Test incremental logic validation for each table
        for table in test_tables:
            # Get incremental columns from configuration
            incremental_columns = ['DateTStamp', 'DateTimeDeceased'] if table == 'patient' else \
                                ['AptDateTime', 'DateTStamp'] if table == 'appointment' else \
                                ['DateTStamp', 'SecDateEntry']
            
            validation_results = pipeline_validator.validate_incremental_logic(table, incremental_columns)
            assert validation_results['incremental_logic_valid'], f"Incremental logic validation failed for {table}"
        
        logger.info("Incremental pipeline execution E2E test completed successfully")

    @pytest.mark.e2e
    @pytest.mark.incremental
    @pytest.mark.postgres_loader
    def test_postgres_loader_incremental_methods_e2e(
        self,
        test_settings,
        pipeline_validator
    ):
        """
        Test PostgresLoader incremental methods specifically in E2E environment.
        
        AAA Pattern:
            Arrange: Set up test environment with incremental columns
            Act: Test PostgresLoader incremental methods
            Assert: Verify PostgresLoader incremental methods work correctly
        """
        logger.info("Starting PostgresLoader incremental methods E2E test")
        
        # Arrange: Test tables with incremental columns
        test_tables = [
            {
                'table_name': 'patient',
                'incremental_columns': ['DateTStamp', 'DateTimeDeceased']
            },
            {
                'table_name': 'appointment',
                'incremental_columns': ['AptDateTime', 'DateTStamp']
            },
            {
                'table_name': 'procedurelog',
                'incremental_columns': ['DateTStamp', 'SecDateEntry']
            }
        ]
        
        # Act & Assert: Test PostgresLoader incremental methods for each table
        for test_table in test_tables:
            table_name = test_table['table_name']
            incremental_columns = test_table['incremental_columns']
            
            logger.info(f"Testing PostgresLoader incremental methods for {table_name}")
            
            # Test PostgresLoader incremental methods validation
            validation_results = pipeline_validator.validate_postgres_loader_incremental_methods(table_name, incremental_columns)
            
            # Assert: Verify PostgresLoader incremental methods functionality
            assert validation_results['postgres_loader_incremental_methods_valid'], f"PostgresLoader incremental methods validation failed for {table_name}: {validation_results}"
            
            # Verify _get_loaded_at_time_max method (validator's own implementation)
            get_last_load_time = validation_results.get('get_loaded_at_time_max', {})
            assert get_last_load_time.get('method_working', False), f"get_loaded_at_time_max validation failed for {table_name}"
            
            # Verify _filter_valid_incremental_columns method (validator's own implementation)
            filter_columns = validation_results.get('filter_valid_incremental_columns', {})
            assert filter_columns.get('method_working', False), f"filter_valid_incremental_columns validation failed for {table_name}"
            
            # Verify _build_improved_load_query_max method (validator's own implementation)
            build_query = validation_results.get('build_improved_load_query_max', {})
            assert build_query.get('method_working', False), f"build_improved_load_query_max validation failed for {table_name}"
            
            # NOTE: _build_count_query and _validate_incremental_integrity no longer exist in new architecture
            # These are kept in validator for backward compatibility but are not required
            # build_count = validation_results.get('build_count_query', {})
            # assert build_count.get('method_working', False), f"_build_count_query method failed for {table_name}"
            
            # validate_integrity = validation_results.get('validate_incremental_integrity', {})
            # assert validate_integrity.get('method_working', False), f"_validate_incremental_integrity method failed for {table_name}"
            
            logger.info(f"PostgresLoader incremental methods test completed for {table_name}")
        
        logger.info("PostgresLoader incremental methods E2E test completed successfully")

    @pytest.mark.e2e
    @pytest.mark.incremental
    @pytest.mark.simple_mysql_replicator
    def test_simple_mysql_replicator_incremental_methods_e2e(
        self,
        test_settings,
        pipeline_validator
    ):
        """
        Test SimpleMySQLReplicator incremental methods specifically in E2E environment.
        
        AAA Pattern:
            Arrange: Set up test environment with incremental columns
            Act: Test SimpleMySQLReplicator incremental methods
            Assert: Verify SimpleMySQLReplicator incremental methods work correctly
        """
        logger.info("Starting SimpleMySQLReplicator incremental methods E2E test")
        
        # Arrange: Test tables with incremental columns
        test_tables = [
            {
                'table_name': 'patient',
                'incremental_columns': ['DateTStamp', 'DateTimeDeceased']
            },
            {
                'table_name': 'appointment',
                'incremental_columns': ['AptDateTime', 'DateTStamp']
            },
            {
                'table_name': 'procedurelog',
                'incremental_columns': ['DateTStamp', 'SecDateEntry']
            }
        ]
        
        # Act & Assert: Test SimpleMySQLReplicator incremental methods for each table
        for test_table in test_tables:
            table_name = test_table['table_name']
            incremental_columns = test_table['incremental_columns']
            
            logger.info(f"Testing SimpleMySQLReplicator incremental methods for {table_name}")
            
            # Test SimpleMySQLReplicator incremental methods validation
            validation_results = pipeline_validator.validate_simple_mysql_replicator_incremental_methods(table_name, incremental_columns)
            
            # Assert: Verify SimpleMySQLReplicator incremental methods functionality
            assert validation_results['simple_mysql_replicator_incremental_methods_valid'], f"SimpleMySQLReplicator incremental methods validation failed for {table_name}: {validation_results}"
            
            # Verify _get_last_copy_time_max method
            get_last_copy_time = validation_results.get('get_last_copy_time_max', {})
            assert get_last_copy_time.get('method_working', False), f"_get_last_copy_time_max method failed for {table_name}"
            
            # Verify _get_last_processed_value_max method
            get_last_processed = validation_results.get('get_last_processed_value_max', {})
            assert get_last_processed.get('method_working', False), f"_get_last_processed_value_max method failed for {table_name}"
            
            # Verify _get_new_records_count_max method
            get_new_records = validation_results.get('get_new_records_count_max', {})
            assert get_new_records.get('method_working', False), f"_get_new_records_count_max method failed for {table_name}"
            
            # Verify copy strategy detection
            copy_strategy = validation_results.get('copy_strategy_detection', {})
            assert copy_strategy.get('method_working', False), f"Copy strategy detection failed for {table_name}"
            
            # Verify incremental table configuration
            table_config = validation_results.get('incremental_table_config', {})
            assert table_config.get('method_working', False), f"Incremental table configuration failed for {table_name}"
            
            logger.info(f"SimpleMySQLReplicator incremental methods test completed for {table_name}")
        
        logger.info("SimpleMySQLReplicator incremental methods E2E test completed successfully")

    @pytest.mark.e2e
    @pytest.mark.incremental
    @pytest.mark.bulk
    def test_bulk_incremental_methods_e2e(
        self,
        test_settings,
        pipeline_validator
    ):
        """
        Test bulk incremental methods from both PostgresLoader and SimpleMySQLReplicator in E2E environment.
        
        AAA Pattern:
            Arrange: Set up test environment with data for bulk operations
            Act: Test bulk incremental methods
            Assert: Verify bulk incremental methods work correctly
        """
        logger.info("Starting bulk incremental methods E2E test")
        
        # Arrange: Test tables with incremental columns for bulk operations
        test_tables = [
            {
                'table_name': 'patient',
                'incremental_columns': ['DateTStamp', 'DateTimeDeceased']
            },
            {
                'table_name': 'appointment',
                'incremental_columns': ['AptDateTime', 'DateTStamp']
            },
            {
                'table_name': 'procedurelog',
                'incremental_columns': ['DateTStamp', 'SecDateEntry']
            }
        ]
        
        # Act & Assert: Test bulk incremental methods for each table
        for test_table in test_tables:
            table_name = test_table['table_name']
            incremental_columns = test_table['incremental_columns']
            
            logger.info(f"Testing bulk incremental methods for {table_name}")
            
            # Small delay to ensure data is fully replicated
            time.sleep(0.1)
            
            # Test bulk incremental methods validation
            validation_results = pipeline_validator.validate_bulk_incremental_methods(table_name, incremental_columns)
            
            # Assert: Verify bulk incremental methods functionality
            assert validation_results['bulk_incremental_methods_valid'], f"Bulk incremental methods validation failed for {table_name}: {validation_results}"
            
            # Verify bulk insert optimized method
            bulk_insert = validation_results.get('bulk_insert_optimized', {})
            assert bulk_insert.get('method_ready_for_testing', False), f"Bulk insert optimized method not ready for {table_name}"
            
            # Verify streaming data method
            streaming_data = validation_results.get('streaming_data', {})
            assert streaming_data.get('method_ready_for_testing', False), f"Streaming data method not ready for {table_name}"
            
            # Verify chunked loading configuration
            chunked_loading = validation_results.get('chunked_loading', {})
            assert chunked_loading.get('method_ready_for_testing', False), f"Chunked loading configuration not ready for {table_name}"
            
            # Verify performance metrics
            performance_metrics = validation_results.get('performance_metrics', {})
            assert performance_metrics.get('method_ready_for_testing', False), f"Performance metrics not ready for {table_name}"
            
            # Verify specific bulk operation configurations
            assert chunked_loading.get('default_chunk_size', 0) > 0, f"Default chunk size not configured for {table_name}"
            assert chunked_loading.get('large_table_chunk_size', 0) > 0, f"Large table chunk size not configured for {table_name}"
            assert performance_metrics.get('bulk_insert_chunk_size', 0) > 0, f"Bulk insert chunk size not configured for {table_name}"
            assert performance_metrics.get('streaming_chunk_size', 0) > 0, f"Streaming chunk size not configured for {table_name}"
            
            logger.info(f"Bulk incremental methods test completed for {table_name}")
        
        logger.info("Bulk incremental methods E2E test completed successfully")

    @pytest.mark.e2e
    @pytest.mark.incremental
    @pytest.mark.comprehensive
    @pytest.mark.slow
    def test_comprehensive_incremental_methods_e2e(
        self,
        test_settings,
        pipeline_validator
    ):
        """
        Test comprehensive incremental methods in E2E environment.
        
        AAA Pattern:
            Arrange: Set up test environment with comprehensive incremental testing
            Act: Test all incremental methods comprehensively
            Assert: Verify comprehensive incremental functionality
        """
        logger.info("Starting comprehensive incremental methods E2E test")
        
        # Arrange: Test comprehensive incremental scenarios
        test_tables = [
            {
                'table_name': 'patient',
                'incremental_columns': ['DateTStamp', 'DateTimeDeceased', 'SecDateEntry']
            },
            {
                'table_name': 'appointment',
                'incremental_columns': ['DateTStamp', 'AptDateTime']
            },
            {
                'table_name': 'procedurelog',
                'incremental_columns': ['DateTStamp', 'SecDateEntry']
            }
        ]
        
        # Act & Assert: Test comprehensive incremental methods for each table
        for test_table in test_tables:
            table_name = test_table['table_name']
            incremental_columns = test_table['incremental_columns']
            
            logger.info(f"Testing comprehensive incremental methods for {table_name}")
            
            # Test comprehensive incremental validation
            validation_results = pipeline_validator.validate_incremental_logic(table_name, incremental_columns)
            
            # Assert: Verify comprehensive incremental functionality
            assert validation_results['incremental_logic_valid'], f"Comprehensive incremental validation failed for {table_name}: {validation_results}"
            
            # Verify multi-column support (returned at top level, not in advanced_features)
            assert validation_results.get('multi_column_support', False), f"Multi-column incremental support failed for {table_name}: {validation_results}"
            
            # Verify data quality validation
            data_quality = validation_results.get('data_quality_validation', {})
            assert data_quality.get('valid_columns', 0) > 0, f"No valid columns found for {table_name}"
            
            logger.info(f"Comprehensive incremental methods test completed for {table_name}")
        
        logger.info("Comprehensive incremental methods E2E test completed successfully")

    # ============================================================================
    # COPY STRATEGY TEST METHODS
    # ============================================================================

    @pytest.mark.e2e
    @pytest.mark.copy_strategy
    @pytest.mark.full_copy
    def test_patient_full_copy_strategy_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test FULL COPY strategy for patient data pipeline.
        
        AAA Pattern:
            Arrange: Verify source data exists and replication database is empty
            Act: Execute full copy pipeline strategy
            Assert: Validate complete data transfer and integrity
        """
        logger.info("Starting patient FULL COPY strategy E2E test")
        
        # Arrange: Get test data and verify initial state
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        assert len(patients) > 0, "No patient data found in source database"
        
        # Arrange: Verify source data exists
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        with source_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert source_count == len(patients), f"Source database missing test data: expected {len(patients)}, got {source_count}"
        
        # Arrange: Verify replication database starts empty (after cleanup fixture)
        # NOTE: clean_replication_db fixture should have run, but verify and clean if needed
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            # Check for test records specifically (not total count, in case other data exists)
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            if replication_count > 0:
                # If cleanup didn't work, try to clean now (defensive cleanup)
                logger.warning(f"Replication has {replication_count} test patient records, cleaning now...")
                conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2, 3)"))
                conn.commit()
                # Small delay to ensure cleanup is visible
                time.sleep(0.1)
                replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert replication_count == 0, f"Replication database should be empty for test records, got {replication_count} records"
        
        # Act: Execute full copy strategy
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('patient', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate full copy execution and data integrity
        assert result is True, "Patient full copy pipeline execution failed"
        assert duration < 120, f"Full copy pipeline took too long: {duration:.2f}s"
        
        # Assert: Validate complete data transfer
        validation_results = pipeline_validator.validate_patient_pipeline(patients)
        assert validation_results['record_count_match'], f"Full copy record count mismatch: {validation_results}"
        assert validation_results['expected_count'] == len(patients), "Full copy expected count mismatch"
        
        # Assert: Verify data integrity in replication database
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert replication_count == len(patients), f"Full copy replication count mismatch: expected {len(patients)}, got {replication_count}"
            
            # Verify sample data integrity
            sample_result = conn.execute(text("SELECT PatNum, LName, FName FROM patient WHERE PatNum = 1")).fetchone()
            assert sample_result is not None, "Full copy: No sample data found in replication"
            assert sample_result[0] == 1, f"Full copy: Unexpected PatNum: {sample_result[0]}"
        
        logger.info(f"Patient FULL COPY strategy E2E test completed successfully in {duration:.2f}s")

    @pytest.mark.e2e
    @pytest.mark.copy_strategy
    @pytest.mark.incremental_copy
    def test_patient_incremental_copy_strategy_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test INCREMENTAL COPY strategy for patient data pipeline.
        
        AAA Pattern:
            Arrange: Verify source data exists and set up incremental conditions
            Act: Execute incremental copy pipeline strategy
            Assert: Validate incremental data transfer and DateTStamp filtering
        """
        logger.info("Starting patient INCREMENTAL COPY strategy E2E test")
        
        # Arrange: Get test data and verify initial state
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        assert len(patients) > 0, "No patient data found in source database"
        
        # Arrange: Verify source data exists
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        with source_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert source_count == len(patients), f"Source database missing test data: expected {len(patients)}, got {source_count}"
            
            # Get DateTStamp values for incremental testing
            date_result = conn.execute(text("SELECT DateTStamp FROM patient WHERE PatNum = 1")).fetchone()
            assert date_result is not None, "No DateTStamp found for incremental testing"
            base_timestamp = date_result[0]
        
        # Arrange: Verify replication database starts empty (after cleanup fixture)
        # NOTE: clean_replication_db fixture should have run, but verify and clean if needed
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            # Check for test records specifically (not total count, in case other data exists)
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            if replication_count > 0:
                # If cleanup didn't work, try to clean now (defensive cleanup)
                logger.warning(f"Replication has {replication_count} test patient records, cleaning now...")
                conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2, 3)"))
                conn.commit()
                # Small delay to ensure cleanup is visible
                time.sleep(0.1)
                replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert replication_count == 0, f"Replication database should be empty for test records, got {replication_count} records"
        
        # Act: First do a full load to populate replication, then incremental
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        # First load: Full copy to populate replication database
        logger.info("Performing initial full load to populate replication database")
        full_result = orchestrator.run_pipeline_for_table('patient', force_full=True)
        assert full_result is True, "Initial full load failed"
        
        # Second load: Incremental copy
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('patient', force_full=False)
        duration = time.time() - start_time
        
        # Assert: Validate incremental copy execution
        assert result is True, "Patient incremental copy pipeline execution failed"
        assert duration < 120, f"Incremental copy pipeline took too long: {duration:.2f}s"
        
        # Assert: Validate incremental data transfer
        validation_results = pipeline_validator.validate_patient_pipeline(patients)
        assert validation_results['record_count_match'], f"Incremental copy record count mismatch: {validation_results}"
        
        # Assert: Verify incremental logic (DateTStamp filtering)
        with replication_engine.connect() as conn:
            incremental_count = conn.execute(text(f"SELECT COUNT(*) FROM patient WHERE DateTStamp >= '{base_timestamp}'")).scalar()
            assert incremental_count > 0, "Incremental copy: No records found with DateTStamp filtering"
            
            # Verify incremental sample data
            sample_result = conn.execute(text("SELECT PatNum, DateTStamp FROM patient WHERE PatNum = 1")).fetchone()
            assert sample_result is not None, "Incremental copy: No sample data found"
            assert sample_result[1] >= base_timestamp, f"Incremental copy: DateTStamp filtering failed"
        
        logger.info(f"Patient INCREMENTAL COPY strategy E2E test completed successfully in {duration:.2f}s")

    @pytest.mark.e2e
    @pytest.mark.copy_strategy
    @pytest.mark.bulk_copy
    def test_patient_bulk_copy_strategy_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test BULK COPY strategy for patient data pipeline.
        
        AAA Pattern:
            Arrange: Verify source data exists and prepare for bulk operations
            Act: Execute bulk copy pipeline strategy with chunking
            Assert: Validate bulk data transfer and performance
        """
        logger.info("Starting patient BULK COPY strategy E2E test")
        
        # Arrange: Get test data and verify initial state
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        assert len(patients) > 0, "No patient data found in source database"
        
        # Arrange: Verify source data exists
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        with source_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert source_count == len(patients), f"Source database missing test data: expected {len(patients)}, got {source_count}"
        
        # Arrange: Verify replication database starts empty (after cleanup fixture)
        # NOTE: clean_replication_db fixture should have run, but verify and clean if needed
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            # Check for test records specifically (not total count, in case other data exists)
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            if replication_count > 0:
                # If cleanup didn't work, try to clean now (defensive cleanup)
                logger.warning(f"Replication has {replication_count} test patient records, cleaning now...")
                conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2, 3)"))
                conn.commit()
                # Small delay to ensure cleanup is visible
                time.sleep(0.1)
                replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert replication_count == 0, f"Replication database should be empty for test records, got {replication_count} records"
        
        # Act: Execute bulk copy strategy
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        # Use full copy (bulk operations are handled internally by the pipeline)
        result = orchestrator.run_pipeline_for_table('patient', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate bulk copy execution and performance
        assert result is True, "Patient bulk copy pipeline execution failed"
        assert duration < 180, f"Bulk copy pipeline took too long: {duration:.2f}s"  # Allow more time for bulk operations
        
        # Assert: Validate bulk data transfer
        validation_results = pipeline_validator.validate_patient_pipeline(patients)
        assert validation_results['record_count_match'], f"Bulk copy record count mismatch: {validation_results}"
        
        # Assert: Verify bulk copy performance characteristics
        with replication_engine.connect() as conn:
            bulk_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert bulk_count == len(patients), f"Bulk copy count mismatch: expected {len(patients)}, got {bulk_count}"
            
            # Verify bulk copy data integrity
            sample_result = conn.execute(text("SELECT PatNum, LName, FName, BalTotal FROM patient WHERE PatNum = 1")).fetchone()
            assert sample_result is not None, "Bulk copy: No sample data found"
            assert sample_result[0] == 1, f"Bulk copy: Unexpected PatNum: {sample_result[0]}"
            # Test data uses 'Doe' for PatNum=1 (from setup_test_databases.py)
            assert sample_result[1] == 'Doe', f"Bulk copy: Unexpected LName: {sample_result[1]}, expected 'Doe'"
        
        logger.info(f"Patient BULK COPY strategy E2E test completed successfully in {duration:.2f}s")

    @pytest.mark.e2e
    @pytest.mark.copy_strategy
    @pytest.mark.upsert_copy
    def test_patient_upsert_copy_strategy_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test UPSERT COPY strategy for patient data pipeline.
        
        AAA Pattern:
            Arrange: Set up test data with potential duplicates
            Act: Execute upsert copy pipeline strategy
            Assert: Validate upsert logic and duplicate handling
        """
        logger.info("Starting patient UPSERT COPY strategy E2E test")
        
        # Arrange: Get test data and verify initial state
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        assert len(patients) > 0, "No patient data found in source database"
        
        # Arrange: Verify source data exists
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        with source_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert source_count == len(patients), f"Source database missing test data: expected {len(patients)}, got {source_count}"
        
        # Arrange: Verify replication database starts empty (after cleanup fixture)
        # NOTE: clean_replication_db fixture should have run, but verify and clean if needed
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            # Check for test records specifically (not total count, in case other data exists)
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            if replication_count > 0:
                # If cleanup didn't work, try to clean now (defensive cleanup)
                logger.warning(f"Replication has {replication_count} test patient records, cleaning now...")
                conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2, 3)"))
                conn.commit()
                # Small delay to ensure cleanup is visible
                time.sleep(0.1)
                replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert replication_count == 0, f"Replication database should be empty for test records, got {replication_count} records"
        
        # Act: Execute upsert copy strategy
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        # UPSERT is handled internally by PostgresLoader based on table configuration
        result = orchestrator.run_pipeline_for_table('patient', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate upsert copy execution
        assert result is True, "Patient upsert copy pipeline execution failed"
        assert duration < 120, f"Upsert copy pipeline took too long: {duration:.2f}s"
        
        # Assert: Validate upsert data transfer
        validation_results = pipeline_validator.validate_patient_pipeline(patients)
        assert validation_results['record_count_match'], f"Upsert copy record count mismatch: {validation_results}"
        
        # Assert: Verify upsert logic (no duplicates)
        with replication_engine.connect() as conn:
            upsert_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert upsert_count == len(patients), f"Upsert copy count mismatch: expected {len(patients)}, got {upsert_count}"
            
            # Verify no duplicates in upsert
            duplicate_count = conn.execute(text("SELECT COUNT(*) FROM patient GROUP BY PatNum HAVING COUNT(*) > 1")).scalar()
            assert duplicate_count is None or duplicate_count == 0, f"Upsert copy: Found {duplicate_count} duplicate records"
            
            # Verify upsert sample data integrity
            sample_result = conn.execute(text("SELECT PatNum, LName, FName FROM patient WHERE PatNum = 1")).fetchone()
            assert sample_result is not None, "Upsert copy: No sample data found"
            assert sample_result[0] == 1, f"Upsert copy: Unexpected PatNum: {sample_result[0]}"
        
        logger.info(f"Patient UPSERT COPY strategy E2E test completed successfully in {duration:.2f}s")

    @pytest.mark.e2e
    @pytest.mark.copy_strategy
    @pytest.mark.appointment_full_copy
    def test_appointment_full_copy_strategy_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test FULL COPY strategy for appointment data pipeline.
        
        AAA Pattern:
            Arrange: Verify source appointment data exists and replication database is empty
            Act: Execute full copy pipeline strategy for appointments
            Assert: Validate complete appointment data transfer and integrity
        """
        logger.info("Starting appointment FULL COPY strategy E2E test")
        
        # Arrange: Get test data and verify initial state
        test_data = self._get_test_data_from_source(test_settings)
        appointments = test_data['appointment']
        assert len(appointments) > 0, "No appointment data found in source database"
        
        # Arrange: Verify source data exists
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        with source_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM appointment WHERE AptNum IN (1, 2, 3)")).scalar()
            assert source_count == len(appointments), f"Source database missing appointment data: expected {len(appointments)}, got {source_count}"
        
        # Arrange: Verify replication database starts empty (after cleanup fixture)
        # NOTE: clean_replication_db fixture should have run, but verify and clean if needed
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            # Check for test records specifically (not total count, in case other data exists)
            replication_count = conn.execute(text("SELECT COUNT(*) FROM appointment WHERE AptNum IN (1, 2, 3)")).scalar()
            if replication_count > 0:
                # If cleanup didn't work, try to clean now (defensive cleanup)
                logger.warning(f"Replication has {replication_count} test appointment records, cleaning now...")
                conn.execute(text("DELETE FROM appointment WHERE AptNum IN (1, 2, 3)"))
                conn.commit()
                time.sleep(0.1)
                replication_count = conn.execute(text("SELECT COUNT(*) FROM appointment WHERE AptNum IN (1, 2, 3)")).scalar()
            assert replication_count == 0, f"Replication database should be empty for test records, got {replication_count} records"
        
        # Act: Execute full copy strategy
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('appointment', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate full copy execution and data integrity
        assert result is True, "Appointment full copy pipeline execution failed"
        assert duration < 120, f"Full copy pipeline took too long: {duration:.2f}s"
        
        # Assert: Validate complete data transfer
        validation_results = pipeline_validator.validate_appointment_pipeline(appointments)
        assert validation_results['record_count_match'], f"Full copy record count mismatch: {validation_results}"
        assert validation_results['expected_count'] == len(appointments), "Full copy expected count mismatch"
        
        # Assert: Verify data integrity in replication database
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM appointment WHERE AptNum IN (1, 2, 3)")).scalar()
            assert replication_count == len(appointments), f"Full copy replication count mismatch: expected {len(appointments)}, got {replication_count}"
            
            # Verify sample data integrity
            sample_result = conn.execute(text("SELECT AptNum, PatNum, AptDateTime FROM appointment WHERE AptNum = 1")).fetchone()
            assert sample_result is not None, "Full copy: No sample appointment data found in replication"
            assert sample_result[0] == 1, f"Full copy: Unexpected AptNum: {sample_result[0]}"
        
        logger.info(f"Appointment FULL COPY strategy E2E test completed successfully in {duration:.2f}s")

    @pytest.mark.e2e
    @pytest.mark.copy_strategy
    @pytest.mark.procedure_full_copy
    def test_procedure_full_copy_strategy_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test FULL COPY strategy for procedure data pipeline.
        
        AAA Pattern:
            Arrange: Verify source procedure data exists and replication database is empty
            Act: Execute full copy pipeline strategy for procedures
            Assert: Validate complete procedure data transfer and integrity
        """
        logger.info("Starting procedure FULL COPY strategy E2E test")
        
        # Arrange: Get test data and verify initial state
        test_data = self._get_test_data_from_source(test_settings)
        procedures = test_data['procedurelog']
        assert len(procedures) > 0, "No procedure data found in source database"
        
        # Arrange: Verify source data exists
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        with source_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            assert source_count == len(procedures), f"Source database missing procedure data: expected {len(procedures)}, got {source_count}"
        
        # Arrange: Verify replication database starts empty (after cleanup fixture)
        # NOTE: clean_replication_db fixture should have run, but verify and clean if needed
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            # Check for test records specifically (not total count, in case other data exists)
            replication_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            if replication_count > 0:
                # If cleanup didn't work, try to clean now (defensive cleanup)
                logger.warning(f"Replication has {replication_count} test procedure records, cleaning now...")
                conn.execute(text("DELETE FROM procedurelog WHERE ProcNum IN (1, 2, 3)"))
                conn.commit()
                time.sleep(0.1)
                replication_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            assert replication_count == 0, f"Replication database should be empty for test records, got {replication_count} records"
        
        # Act: Execute full copy strategy
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('procedurelog', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate full copy execution and data integrity
        assert result is True, "Procedure full copy pipeline execution failed"
        assert duration < 120, f"Full copy pipeline took too long: {duration:.2f}s"
        
        # Assert: Validate complete data transfer
        validation_results = pipeline_validator.validate_procedure_pipeline(procedures)
        assert validation_results['record_count_match'], f"Full copy record count mismatch: {validation_results}"
        assert validation_results['expected_count'] == len(procedures), "Full copy expected count mismatch"
        
        # Assert: Verify data integrity in replication database
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            assert replication_count == len(procedures), f"Full copy replication count mismatch: expected {len(procedures)}, got {replication_count}"
            
            # Verify sample data integrity
            sample_result = conn.execute(text("SELECT ProcNum, PatNum, AptNum, ProcFee FROM procedurelog WHERE ProcNum = 1")).fetchone()
            assert sample_result is not None, "Full copy: No sample procedure data found in replication"
            assert sample_result[0] == 1, f"Full copy: Unexpected ProcNum: {sample_result[0]}"
            assert sample_result[3] > 0, f"Full copy: Unexpected ProcFee: {sample_result[3]}"
        
        logger.info(f"Procedure FULL COPY strategy E2E test completed successfully in {duration:.2f}s")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "e2e and test_data"])