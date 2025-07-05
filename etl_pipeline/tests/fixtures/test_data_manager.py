"""
Integration Test Data Manager

This module provides a standardized way to manage test data across all integration tests.
It uses the new architectural patterns (ConnectionFactory, Settings) and provides
consistent test data that matches the real OpenDental schema.

The manager handles:
- Inserting standardized test data into test databases
- Cleaning up test data after tests
- Managing test data across multiple database types
- Providing test data for different scenarios (minimal, full, incremental)
"""

import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import text, bindparam
from sqlalchemy.engine import Engine

from etl_pipeline.config import DatabaseType, PostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
from .test_data_definitions import (
    get_test_patient_data,
    get_test_appointment_data,
    get_incremental_test_patient_data,
    get_test_data_for_table
)

logger = logging.getLogger(__name__)

class IntegrationTestDataManager:
    """
    Manages test data for integration tests using standardized data and new architecture.
    
    This class provides a unified interface for:
    - Setting up test data in test databases
    - Cleaning up test data after tests
    - Managing test data across different database types
    - Providing consistent test data that matches real schemas
    """
    
    def __init__(self, settings):
        """
        Initialize the test data manager.
        
        Args:
            settings: Settings instance for database configuration
        """
        self.settings = settings
        self.logger = logging.getLogger(__name__)
        self._setup_connections()
    
    def _setup_connections(self):
        """Set up database connections using new ConnectionFactory."""
        # Only use new ConnectionFactory methods, no legacy or fallback logic
        # Source database (OpenDental MySQL)
        self.source_engine = ConnectionFactory.get_source_connection(self.settings)
        # Replication database (Local MySQL)
        self.replication_engine = ConnectionFactory.get_replication_connection(self.settings)
        # Analytics database (PostgreSQL)
        self.analytics_engine = ConnectionFactory.get_analytics_connection(self.settings)
        # Raw schema connection
        self.raw_engine = ConnectionFactory.get_analytics_connection(self.settings, PostgresSchema.RAW)
        logger.info("Successfully set up test database connections")
    
    def setup_patient_data(self, 
                          include_all_fields: bool = True,
                          database_types: Optional[List[DatabaseType]] = None) -> None:
        """
        Set up standardized patient test data in specified databases.
        
        Args:
            include_all_fields: If True, insert complete patient records with all fields.
                               If False, insert minimal records with only required fields.
            database_types: List of database types to insert data into.
                           If None, inserts into all databases (source, replication, analytics).
        """
        if database_types is None:
            database_types = [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS]
        
        patient_data = get_test_patient_data(include_all_fields)
        
        for db_type in database_types:
            try:
                if db_type == DatabaseType.SOURCE:
                    self._insert_patient_data_mysql(self.source_engine, patient_data, "source")
                elif db_type == DatabaseType.REPLICATION:
                    self._insert_patient_data_mysql(self.replication_engine, patient_data, "replication")
                elif db_type == DatabaseType.ANALYTICS:
                    self._insert_patient_data_postgres(self.raw_engine, patient_data, "analytics")
                
                logger.info(f"✅ Inserted {len(patient_data)} patient records into {db_type.value} database")
                
            except Exception as e:
                logger.error(f"❌ Failed to insert patient data into {db_type.value}: {e}")
                raise
    
    def setup_appointment_data(self, 
                              database_types: Optional[List[DatabaseType]] = None) -> None:
        """
        Set up standardized appointment test data in specified databases.
        
        Args:
            database_types: List of database types to insert data into.
                           If None, inserts into all databases.
        """
        if database_types is None:
            database_types = [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS]
        
        appointment_data = get_test_appointment_data()
        
        for db_type in database_types:
            try:
                if db_type == DatabaseType.SOURCE:
                    self._insert_appointment_data_mysql(self.source_engine, appointment_data, "source")
                elif db_type == DatabaseType.REPLICATION:
                    self._insert_appointment_data_mysql(self.replication_engine, appointment_data, "replication")
                elif db_type == DatabaseType.ANALYTICS:
                    self._insert_appointment_data_postgres(self.raw_engine, appointment_data, "analytics")
                
                logger.info(f"✅ Inserted {len(appointment_data)} appointment records into {db_type.value} database")
                
            except Exception as e:
                logger.error(f"❌ Failed to insert appointment data into {db_type.value}: {e}")
                raise
    
    def setup_incremental_patient_data(self, 
                                      database_types: Optional[List[DatabaseType]] = None) -> None:
        """
        Set up incremental patient test data (with newer timestamps) for incremental loading tests.
        
        Args:
            database_types: List of database types to insert data into.
                           If None, inserts into all databases.
        """
        if database_types is None:
            database_types = [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS]
        
        incremental_data = get_incremental_test_patient_data()
        
        for db_type in database_types:
            try:
                if db_type == DatabaseType.SOURCE:
                    self._insert_patient_data_mysql(self.source_engine, incremental_data, "source")
                elif db_type == DatabaseType.REPLICATION:
                    self._insert_patient_data_mysql(self.replication_engine, incremental_data, "replication")
                elif db_type == DatabaseType.ANALYTICS:
                    self._insert_patient_data_postgres(self.raw_engine, incremental_data, "analytics")
                
                logger.info(f"✅ Inserted {len(incremental_data)} incremental patient records into {db_type.value} database")
                
            except Exception as e:
                logger.error(f"❌ Failed to insert incremental patient data into {db_type.value}: {e}")
                raise
    
    def _get_table_columns(self, engine, table_name):
        """Return a set of column names for the given table in the connected MySQL database."""
        with engine.connect() as conn:
            result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}"))
            return set(row[0] for row in result)

    def _insert_patient_data_mysql(self, engine: Engine, patient_data: List[Dict[str, Any]], db_name: str) -> None:
        # Dynamically detect columns in the patient table
        table_columns = self._get_table_columns(engine, "patient")
        with engine.connect() as conn:
            # Clear existing test data first
            pat_nums = [patient['PatNum'] for patient in patient_data if 'PatNum' in patient]
            if pat_nums:
                conn.execute(
                    text("DELETE FROM patient WHERE PatNum IN :pat_nums").bindparams(bindparam("pat_nums", expanding=True)),
                    {"pat_nums": pat_nums}
                )
            # Insert test data, only using columns that exist in the table
            for patient in patient_data:
                filtered_patient = {k: v for k, v in patient.items() if k in table_columns}
                columns = ', '.join(f'`{col}`' for col in filtered_patient.keys())
                values = ', '.join(f':{col}' for col in filtered_patient.keys())
                insert_sql = f"INSERT INTO patient ({columns}) VALUES ({values})"
                try:
                    conn.execute(text(insert_sql), filtered_patient)
                except Exception as e:
                    self.logger.error(f"Failed to insert patient data into {db_name}: {e}")
    
    def _get_table_columns_postgres(self, engine, table_name, schema):
        """Return a set of column names for the given table in the connected Postgres database."""
        with engine.connect() as conn:
            result = conn.execute(text(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table_name
                """), {"schema": schema, "table_name": table_name})
            return set(row[0] for row in result)

    def _insert_patient_data_postgres(self, engine: Engine, patient_data: List[Dict[str, Any]], db_name: str) -> None:
        # Dynamically detect columns in the patient table
        table_columns = self._get_table_columns_postgres(engine, "patient", schema="raw")
        with engine.connect() as conn:
            # Clear existing test data first
            pat_nums = [patient['PatNum'] for patient in patient_data if 'PatNum' in patient]
            if pat_nums:
                conn.execute(
                    text("DELETE FROM raw.patient WHERE \"PatNum\" = ANY(:pat_nums)"),
                    {"pat_nums": pat_nums}
                )
            # Insert test data, only using columns that exist in the table
            for patient in patient_data:
                filtered_patient = {k: v for k, v in patient.items() if k in table_columns}
                
                # Convert integer boolean values to PostgreSQL boolean for specific fields
                boolean_fields = ['PatStatus', 'Gender', 'Position', 'Guarantor', 'Premed', 
                                'PlannedIsDone', 'PreferConfirmMethod', 'PreferContactMethod', 
                                'PreferRecallMethod', 'TxtMsgOk', 'HasSuperBilling', 
                                'HasSignedTil', 'ShortCodeOptIn', 'AskToArriveEarly', 
                                'PreferContactConfidential', 'SuperFamily']
                
                for field in boolean_fields:
                    if field in filtered_patient and isinstance(filtered_patient[field], int):
                        filtered_patient[field] = bool(filtered_patient[field])
                
                columns = ', '.join(f'"{col}"' for col in filtered_patient.keys())
                values = ', '.join(f':{col}' for col in filtered_patient.keys())
                insert_sql = f"INSERT INTO raw.patient ({columns}) VALUES ({values})"
                try:
                    conn.execute(text(insert_sql), filtered_patient)
                except Exception as e:
                    self.logger.error(f"Failed to insert patient data into {db_name}: {e}")
    
    def _insert_appointment_data_mysql(self, engine: Engine, appointment_data: List[Dict[str, Any]], db_name: str) -> None:
        """Insert appointment data into MySQL database."""
        with engine.connect() as conn:
            # Clear existing test data first
            apt_nums = [appointment['AptNum'] for appointment in appointment_data]
            if apt_nums:
                conn.execute(
                    text("DELETE FROM appointment WHERE AptNum IN :apt_nums").bindparams(bindparam("apt_nums", expanding=True)),
                    {"apt_nums": apt_nums}
                )
            # Insert new test data
            for appointment in appointment_data:
                fields = list(appointment.keys())
                placeholders = ', '.join([f':{field}' for field in fields])
                field_names = ', '.join([f'`{field}`' for field in fields])
                
                insert_sql = f"INSERT INTO appointment ({field_names}) VALUES ({placeholders})"
                conn.execute(text(insert_sql), appointment)
            
            conn.commit()
    
    def _insert_appointment_data_postgres(self, engine: Engine, appointment_data: List[Dict[str, Any]], db_name: str) -> None:
        """Insert appointment data into PostgreSQL database."""
        with engine.connect() as conn:
            # Clear existing test data first
            apt_nums = [appointment['AptNum'] for appointment in appointment_data]
            if apt_nums:
                conn.execute(
                    text('DELETE FROM raw.appointment WHERE "AptNum" = ANY(:apt_nums)'),
                    {"apt_nums": apt_nums}
                )
            # Insert new test data
            for appointment in appointment_data:
                # Convert integer boolean values to PostgreSQL boolean for specific fields
                appointment_copy = appointment.copy()
                boolean_fields = ['AptStatus']
                
                for field in boolean_fields:
                    if field in appointment_copy and isinstance(appointment_copy[field], int):
                        appointment_copy[field] = bool(appointment_copy[field])
                
                fields = list(appointment_copy.keys())
                placeholders = ', '.join([f':{field}' for field in fields])
                field_names = ', '.join([f'"{field}"' for field in fields])
                
                insert_sql = f'INSERT INTO raw.appointment ({field_names}) VALUES ({placeholders})'
                conn.execute(text(insert_sql), appointment_copy)
            
            conn.commit()
    
    def cleanup_patient_data(self, database_types: Optional[List[DatabaseType]] = None) -> None:
        """
        Clean up patient test data from specified databases.
        
        Args:
            database_types: List of database types to clean up.
                           If None, cleans up all databases.
        """
        if database_types is None:
            database_types = [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS]
        
        for db_type in database_types:
            try:
                if db_type == DatabaseType.SOURCE:
                    self._cleanup_patient_data_mysql(self.source_engine, "source")
                elif db_type == DatabaseType.REPLICATION:
                    self._cleanup_patient_data_mysql(self.replication_engine, "replication")
                elif db_type == DatabaseType.ANALYTICS:
                    self._cleanup_patient_data_postgres(self.raw_engine, "analytics")
                
                logger.info(f"✅ Cleaned up patient data from {db_type.value} database")
                
            except Exception as e:
                logger.error(f"❌ Failed to clean up patient data from {db_type.value}: {e}")
                raise
    
    def cleanup_appointment_data(self, database_types: Optional[List[DatabaseType]] = None) -> None:
        """
        Clean up appointment test data from specified databases.
        
        Args:
            database_types: List of database types to clean up.
                           If None, cleans up all databases.
        """
        if database_types is None:
            database_types = [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS]
        
        for db_type in database_types:
            try:
                if db_type == DatabaseType.SOURCE:
                    self._cleanup_appointment_data_mysql(self.source_engine, "source")
                elif db_type == DatabaseType.REPLICATION:
                    self._cleanup_appointment_data_mysql(self.replication_engine, "replication")
                elif db_type == DatabaseType.ANALYTICS:
                    self._cleanup_appointment_data_postgres(self.raw_engine, "analytics")
                
                logger.info(f"✅ Cleaned up appointment data from {db_type.value} database")
                
            except Exception as e:
                logger.error(f"❌ Failed to clean up appointment data from {db_type.value}: {e}")
                raise
    
    def _cleanup_patient_data_mysql(self, engine: Engine, db_name: str) -> None:
        """Clean up patient data from MySQL database."""
        with engine.connect() as conn:
            # Get all test patient PatNums
            result = conn.execute(text("SELECT PatNum FROM patient"))
            all_pat_nums = [row[0] for row in result.fetchall()]
            if all_pat_nums:
                conn.execute(
                    text("DELETE FROM patient WHERE PatNum IN :pat_nums").bindparams(bindparam("pat_nums", expanding=True)),
                    {"pat_nums": all_pat_nums}
                )
            conn.commit()
    
    def _cleanup_patient_data_postgres(self, engine: Engine, db_name: str) -> None:
        """Clean up patient data from PostgreSQL database."""
        with engine.connect() as conn:
            # Get all test patient PatNums
            result = conn.execute(text('SELECT "PatNum" FROM raw.patient'))
            all_pat_nums = [row[0] for row in result.fetchall()]
            if all_pat_nums:
                conn.execute(
                    text('DELETE FROM raw.patient WHERE "PatNum" = ANY(:pat_nums)'),
                    {"pat_nums": all_pat_nums}
                )
            conn.commit()
    
    def _cleanup_appointment_data_mysql(self, engine: Engine, db_name: str) -> None:
        """Clean up appointment data from MySQL database."""
        with engine.connect() as conn:
            # Get all test appointment AptNums
            result = conn.execute(text("SELECT AptNum FROM appointment"))
            all_apt_nums = [row[0] for row in result.fetchall()]
            if all_apt_nums:
                conn.execute(
                    text("DELETE FROM appointment WHERE AptNum IN :apt_nums").bindparams(bindparam("apt_nums", expanding=True)),
                    {"apt_nums": all_apt_nums}
                )
            conn.commit()
    
    def _cleanup_appointment_data_postgres(self, engine: Engine, db_name: str) -> None:
        """Clean up appointment data from PostgreSQL database."""
        with engine.connect() as conn:
            # Get all test appointment AptNums
            result = conn.execute(text('SELECT "AptNum" FROM raw.appointment'))
            all_apt_nums = [row[0] for row in result.fetchall()]
            if all_apt_nums:
                conn.execute(
                    text('DELETE FROM raw.appointment WHERE "AptNum" = ANY(:apt_nums)'),
                    {"apt_nums": all_apt_nums}
                )
            conn.commit()
    
    def cleanup_all_test_data(self) -> None:
        """Clean up all test data from all databases."""
        logger.info("Cleaning up all test data...")
        
        try:
            self.cleanup_patient_data()
            self.cleanup_appointment_data()
            logger.info("✅ Successfully cleaned up all test data")
        except Exception as e:
            logger.error(f"❌ Failed to clean up all test data: {e}")
            raise
    
    def get_patient_count(self, database_type: DatabaseType) -> int:
        """
        Get the count of patients in the specified database.
        
        Args:
            database_type: The database to check
            
        Returns:
            Number of patients in the database
        """
        try:
            if database_type == DatabaseType.SOURCE:
                engine = self.source_engine
                table_name = "patient"
            elif database_type == DatabaseType.REPLICATION:
                engine = self.replication_engine
                table_name = "patient"
            elif database_type == DatabaseType.ANALYTICS:
                engine = self.raw_engine
                table_name = "raw.patient"
            else:
                raise ValueError(f"Unsupported database type: {database_type}")
            
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.scalar()
                
        except Exception as e:
            logger.error(f"❌ Failed to get patient count from {database_type.value}: {e}")
            raise
    
    def get_appointment_count(self, database_type: DatabaseType) -> int:
        """
        Get the count of appointments in the specified database.
        
        Args:
            database_type: The database to check
            
        Returns:
            Number of appointments in the database
        """
        try:
            if database_type == DatabaseType.SOURCE:
                engine = self.source_engine
                table_name = "appointment"
            elif database_type == DatabaseType.REPLICATION:
                engine = self.replication_engine
                table_name = "appointment"
            elif database_type == DatabaseType.ANALYTICS:
                engine = self.raw_engine
                table_name = "raw.appointment"
            else:
                raise ValueError(f"Unsupported database type: {database_type}")
            
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.scalar()
                
        except Exception as e:
            logger.error(f"❌ Failed to get appointment count from {database_type.value}: {e}")
            raise
    
    def dispose(self) -> None:
        """Dispose of all database connections."""
        try:
            self.source_engine.dispose()
            self.replication_engine.dispose()
            self.analytics_engine.dispose()
            self.raw_engine.dispose()
            logger.info("✅ Disposed of all database connections")
        except Exception as e:
            logger.error(f"❌ Failed to dispose connections: {e}")
            raise 