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

Connection Architecture Compliance:
- ✅ Uses Settings injection for environment-agnostic operation
- ✅ Uses unified ConnectionFactory API with Settings injection
- ✅ Uses enum-based database type specification
- ✅ Supports provider pattern for dependency injection
- ✅ Environment-agnostic (works for both production and test)
"""

import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import text, bindparam
from sqlalchemy.engine import Engine

# Import connection architecture components
from etl_pipeline.config import DatabaseType, PostgresSchema, Settings
from etl_pipeline.core.connections import ConnectionFactory
from .test_data_definitions import (
    get_test_patient_data,
    get_test_appointment_data,
    get_test_procedure_data
)

logger = logging.getLogger(__name__)

class IntegrationTestDataManager:
    """
    Manages test data for integration tests using standardized data and new architecture.
    
    This class provides a unified interface for:
    - Setting up test data in test databases using Settings injection
    - Cleaning up test data after tests
    - Managing test data across different database types using enums
    - Providing consistent test data that matches real schemas
    
    Connection Architecture Compliance:
    - ✅ Uses Settings injection for environment-agnostic operation
    - ✅ Uses unified ConnectionFactory API with Settings injection
    - ✅ Uses enum-based database type specification
    - ✅ Supports provider pattern for dependency injection
    - ✅ Environment-agnostic (works for both production and test)
    """
    
    def __init__(self, settings: Settings):
        """
        Initialize the test data manager with Settings injection.
        
        Connection Architecture:
        - Uses Settings injection for environment-agnostic database connections
        - Uses unified ConnectionFactory API with Settings injection
        - Automatically uses correct environment (production/test) based on Settings
        - Uses Settings-based configuration methods for database info
        
        Args:
            settings: Settings instance for database configuration and environment detection
        """
        self.settings = settings
        self.logger = logging.getLogger(__name__)
        self._setup_connections()
    
    def _setup_connections(self):
        """Set up database connections using unified ConnectionFactory API with Settings injection."""
        # Only use ConnectionFactory methods with Settings injection, no fallback logic
        # Source database (OpenDental MySQL)
        self.source_engine = ConnectionFactory.get_source_connection(self.settings)
        # Replication database (Local MySQL)
        self.replication_engine = ConnectionFactory.get_replication_connection(self.settings)
        # Analytics database (PostgreSQL) - using unified interface
        self.analytics_engine = ConnectionFactory.get_analytics_connection(self.settings)
        # Raw schema connection - using enum for type safety
        self.raw_engine = ConnectionFactory.get_analytics_connection(self.settings, PostgresSchema.RAW)
        
        # Log connection info for debugging (using Settings-based configuration)
        if logger.isEnabledFor(logging.DEBUG):
            source_config = self.settings.get_source_connection_config()
            replication_config = self.settings.get_replication_connection_config()
            analytics_config = self.settings.get_analytics_connection_config(PostgresSchema.RAW)
            
            logger.debug(f"Test data manager initialized with Settings injection")
            logger.debug(f"Source: {source_config.get('host')}:{source_config.get('port')}/{source_config.get('database')}")
            logger.debug(f"Replication: {replication_config.get('host')}:{replication_config.get('port')}/{replication_config.get('database')}")
            logger.debug(f"Analytics: {analytics_config.get('host')}:{analytics_config.get('port')}/{analytics_config.get('database')}")
        else:
            logger.info("Successfully set up test database connections using Settings injection")
    
    def setup_patient_data(self, 
                          include_all_fields: bool = True,
                          database_types: Optional[List[DatabaseType]] = None) -> None:
        """
        Set up standardized patient test data in specified databases using enum-based specification.
        
        Connection Architecture:
        - Uses enum-based database type specification for type safety
        - Uses Settings injection for environment-agnostic operation
        - Supports unified interface for all database types
        
        Args:
            include_all_fields: If True, insert complete patient records with all fields.
                               If False, insert minimal records with only required fields.
            database_types: List of DatabaseType enums to insert data into.
                           If None, inserts into all databases (SOURCE, REPLICATION, ANALYTICS).
        """
        if database_types is None:
            database_types = [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS]
        
        patient_data = get_test_patient_data(include_all_fields)
        
        for db_type in database_types:
            try:
                if db_type == DatabaseType.SOURCE:
                    self._insert_patient_data_mysql(self.source_engine, patient_data, db_type.value)
                elif db_type == DatabaseType.REPLICATION:
                    self._insert_patient_data_mysql(self.replication_engine, patient_data, db_type.value)
                elif db_type == DatabaseType.ANALYTICS:
                    self._insert_patient_data_postgres(self.raw_engine, patient_data, db_type.value)
                else:
                    raise ValueError(f"Unsupported database type: {db_type}")
                
                logger.info(f"Inserted {len(patient_data)} patient records into {db_type.value} database")
                
            except Exception as e:
                logger.error(f"Failed to insert patient data into {db_type.value}: {e}")
                raise
    
    def setup_appointment_data(self, 
                              database_types: Optional[List[DatabaseType]] = None) -> None:
        """
        Set up standardized appointment test data in specified databases using enum-based specification.
        
        Connection Architecture:
        - Uses enum-based database type specification for type safety
        - Uses Settings injection for environment-agnostic operation
        - Supports unified interface for all database types
        
        Args:
            database_types: List of DatabaseType enums to insert data into.
                           If None, inserts into all databases.
        """
        if database_types is None:
            database_types = [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS]
        
        appointment_data = get_test_appointment_data()
        
        for db_type in database_types:
            try:
                if db_type == DatabaseType.SOURCE:
                    self._insert_appointment_data_mysql(self.source_engine, appointment_data, db_type.value)
                elif db_type == DatabaseType.REPLICATION:
                    self._insert_appointment_data_mysql(self.replication_engine, appointment_data, db_type.value)
                elif db_type == DatabaseType.ANALYTICS:
                    self._insert_appointment_data_postgres(self.raw_engine, appointment_data, db_type.value)
                else:
                    raise ValueError(f"Unsupported database type: {db_type}")
                
                logger.info(f"Inserted {len(appointment_data)} appointment records into {db_type.value} database")
                
            except Exception as e:
                logger.error(f"Failed to insert appointment data into {db_type.value}: {e}")
                raise
    
    def setup_procedure_data(self, 
                            database_types: Optional[List[DatabaseType]] = None) -> None:
        """
        Set up standardized procedure test data in specified databases using enum-based specification.
        
        Connection Architecture:
        - Uses enum-based database type specification for type safety
        - Uses Settings injection for environment-agnostic operation
        - Supports unified interface for all database types
        
        Args:
            database_types: List of DatabaseType enums to insert data into.
                           If None, inserts into all databases.
        """
        if database_types is None:
            database_types = [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS]
        
        procedure_data = get_test_procedure_data()
        
        for db_type in database_types:
            try:
                if db_type == DatabaseType.SOURCE:
                    self._insert_procedure_data_mysql(self.source_engine, procedure_data, db_type.value)
                elif db_type == DatabaseType.REPLICATION:
                    self._insert_procedure_data_mysql(self.replication_engine, procedure_data, db_type.value)
                elif db_type == DatabaseType.ANALYTICS:
                    self._insert_procedure_data_postgres(self.raw_engine, procedure_data, db_type.value)
                else:
                    raise ValueError(f"Unsupported database type: {db_type}")
                
                logger.info(f"Inserted {len(procedure_data)} procedure records into {db_type.value} database")
                
            except Exception as e:
                logger.error(f"Failed to insert procedure data into {db_type.value}: {e}")
                raise
    

    
    def _get_table_columns(self, engine, table_name):
        """Return a set of column names for the given table in the connected MySQL database."""
        with engine.connect() as conn:
            # Debug: Check what database we're connected to
            db_result = conn.execute(text("SELECT DATABASE()"))
            current_db = db_result.scalar()
            logger.info(f"Checking table '{table_name}' in database: {current_db}")
            
            result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}"))
            return set(row[0] for row in result)

    def _insert_patient_data_mysql(self, engine: Engine, patient_data: List[Dict[str, Any]], db_name: str) -> None:
        # Check if table exists before trying to insert data
        with engine.connect() as conn:
            # Debug: Check if table exists
            result = conn.execute(text("SHOW TABLES LIKE 'patient'"))
            table_exists = result.fetchone() is not None
            logger.info(f"Table 'patient' exists in {db_name}: {table_exists}")
            
            if not table_exists:
                logger.error(f"Table 'patient' does not exist in {db_name}. Available tables:")
                result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result.fetchall()]
                logger.error(f"Available tables: {tables}")
                raise Exception(f"Table 'patient' does not exist in {db_name}")
        
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
            
            # Commit the transaction
            conn.commit()
    
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
            
            # Commit the transaction
            conn.commit()
    
    def _insert_appointment_data_mysql(self, engine: Engine, appointment_data: List[Dict[str, Any]], db_name: str) -> None:
        """Insert appointment data into MySQL database."""
        # Dynamically detect columns in the appointment table
        table_columns = self._get_table_columns(engine, "appointment")
        with engine.connect() as conn:
            # Clear existing test data first
            apt_nums = [appointment['AptNum'] for appointment in appointment_data]
            if apt_nums:
                conn.execute(
                    text("DELETE FROM appointment WHERE AptNum IN :apt_nums").bindparams(bindparam("apt_nums", expanding=True)),
                    {"apt_nums": apt_nums}
                )
            # Insert new test data, only using columns that exist in the table
            for appointment in appointment_data:
                filtered_appointment = {k: v for k, v in appointment.items() if k in table_columns}
                columns = ', '.join(f'`{col}`' for col in filtered_appointment.keys())
                values = ', '.join(f':{col}' for col in filtered_appointment.keys())
                insert_sql = f"INSERT INTO appointment ({columns}) VALUES ({values})"
                try:
                    conn.execute(text(insert_sql), filtered_appointment)
                except Exception as e:
                    self.logger.error(f"Failed to insert appointment data into {db_name}: {e}")
            
            conn.commit()
    
    def _insert_appointment_data_postgres(self, engine: Engine, appointment_data: List[Dict[str, Any]], db_name: str) -> None:
        """Insert appointment data into PostgreSQL database."""
        # Dynamically detect columns in the appointment table
        table_columns = self._get_table_columns_postgres(engine, "appointment", schema="raw")
        with engine.connect() as conn:
            # Clear existing test data first
            apt_nums = [appointment['AptNum'] for appointment in appointment_data]
            if apt_nums:
                conn.execute(
                    text('DELETE FROM raw.appointment WHERE "AptNum" = ANY(:apt_nums)'),
                    {"apt_nums": apt_nums}
                )
            # Insert new test data, only using columns that exist in the table
            for appointment in appointment_data:
                filtered_appointment = {k: v for k, v in appointment.items() if k in table_columns}
                
                # Convert integer boolean values to PostgreSQL boolean for specific fields
                boolean_fields = ['AptStatus']
                
                for field in boolean_fields:
                    if field in filtered_appointment and isinstance(filtered_appointment[field], int):
                        filtered_appointment[field] = bool(filtered_appointment[field])
                
                columns = ', '.join(f'"{col}"' for col in filtered_appointment.keys())
                values = ', '.join(f':{col}' for col in filtered_appointment.keys())
                insert_sql = f'INSERT INTO raw.appointment ({columns}) VALUES ({values})'
                try:
                    conn.execute(text(insert_sql), filtered_appointment)
                except Exception as e:
                    self.logger.error(f"Failed to insert appointment data into {db_name}: {e}")
            
            conn.commit()
    
    def _insert_procedure_data_mysql(self, engine: Engine, procedure_data: List[Dict[str, Any]], db_name: str) -> None:
        """Insert procedure data into MySQL database."""
        # Dynamically detect columns in the procedurelog table
        table_columns = self._get_table_columns(engine, "procedurelog")
        with engine.connect() as conn:
            # Clear existing test data first
            proc_nums = [procedure['ProcNum'] for procedure in procedure_data]
            if proc_nums:
                conn.execute(
                    text("DELETE FROM procedurelog WHERE ProcNum IN :proc_nums").bindparams(bindparam("proc_nums", expanding=True)),
                    {"proc_nums": proc_nums}
                )
            # Insert new test data, only using columns that exist in the table
            for procedure in procedure_data:
                filtered_procedure = {k: v for k, v in procedure.items() if k in table_columns}
                columns = ', '.join(f'`{col}`' for col in filtered_procedure.keys())
                values = ', '.join(f':{col}' for col in filtered_procedure.keys())
                insert_sql = f"INSERT INTO procedurelog ({columns}) VALUES ({values})"
                try:
                    conn.execute(text(insert_sql), filtered_procedure)
                except Exception as e:
                    self.logger.error(f"Failed to insert procedure data into {db_name}: {e}")
            
            conn.commit()
    
    def _insert_procedure_data_postgres(self, engine: Engine, procedure_data: List[Dict[str, Any]], db_name: str) -> None:
        """Insert procedure data into PostgreSQL database."""
        # Dynamically detect columns in the procedurelog table
        table_columns = self._get_table_columns_postgres(engine, "procedurelog", schema="raw")
        with engine.connect() as conn:
            # Clear existing test data first
            proc_nums = [procedure['ProcNum'] for procedure in procedure_data]
            if proc_nums:
                conn.execute(
                    text('DELETE FROM raw.procedurelog WHERE "ProcNum" = ANY(:proc_nums)'),
                    {"proc_nums": proc_nums}
                )
            # Insert new test data, only using columns that exist in the table
            for procedure in procedure_data:
                filtered_procedure = {k: v for k, v in procedure.items() if k in table_columns}
                
                # Convert integer boolean values to PostgreSQL boolean for specific fields
                boolean_fields = ['ProcStatus']
                
                for field in boolean_fields:
                    if field in filtered_procedure and isinstance(filtered_procedure[field], int):
                        filtered_procedure[field] = bool(filtered_procedure[field])
                
                columns = ', '.join(f'"{col}"' for col in filtered_procedure.keys())
                values = ', '.join(f':{col}' for col in filtered_procedure.keys())
                insert_sql = f'INSERT INTO raw.procedurelog ({columns}) VALUES ({values})'
                try:
                    conn.execute(text(insert_sql), filtered_procedure)
                except Exception as e:
                    self.logger.error(f"Failed to insert procedure data into {db_name}: {e}")
            
            conn.commit()
    
    def cleanup_patient_data(self, database_types: Optional[List[DatabaseType]] = None) -> None:
        """
        Clean up patient test data from specified databases using enum-based specification.
        
        Connection Architecture:
        - Uses enum-based database type specification for type safety
        - Uses Settings injection for environment-agnostic operation
        - Supports unified interface for all database types
        
        Args:
            database_types: List of DatabaseType enums to clean up.
                           If None, cleans up all databases.
        """
        if database_types is None:
            database_types = [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS]
        
        for db_type in database_types:
            try:
                if db_type == DatabaseType.SOURCE:
                    self._cleanup_patient_data_mysql(self.source_engine, db_type.value)
                elif db_type == DatabaseType.REPLICATION:
                    self._cleanup_patient_data_mysql(self.replication_engine, db_type.value)
                elif db_type == DatabaseType.ANALYTICS:
                    self._cleanup_patient_data_postgres(self.raw_engine, db_type.value)
                else:
                    raise ValueError(f"Unsupported database type: {db_type}")
                
                logger.info(f"Cleaned up patient data from {db_type.value} database")
                
            except Exception as e:
                logger.error(f"Failed to clean up patient data from {db_type.value}: {e}")
                raise
    
    def cleanup_appointment_data(self, database_types: Optional[List[DatabaseType]] = None) -> None:
        """
        Clean up appointment test data from specified databases using enum-based specification.
        
        Connection Architecture:
        - Uses enum-based database type specification for type safety
        - Uses Settings injection for environment-agnostic operation
        - Supports unified interface for all database types
        
        Args:
            database_types: List of DatabaseType enums to clean up.
                           If None, cleans up all databases.
        """
        if database_types is None:
            database_types = [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS]
        
        for db_type in database_types:
            try:
                if db_type == DatabaseType.SOURCE:
                    self._cleanup_appointment_data_mysql(self.source_engine, db_type.value)
                elif db_type == DatabaseType.REPLICATION:
                    self._cleanup_appointment_data_mysql(self.replication_engine, db_type.value)
                elif db_type == DatabaseType.ANALYTICS:
                    self._cleanup_appointment_data_postgres(self.raw_engine, db_type.value)
                else:
                    raise ValueError(f"Unsupported database type: {db_type}")
                
                logger.info(f"Cleaned up appointment data from {db_type.value} database")
                
            except Exception as e:
                logger.error(f"Failed to clean up appointment data from {db_type.value}: {e}")
                raise
    
    def cleanup_procedure_data(self, database_types: Optional[List[DatabaseType]] = None) -> None:
        """
        Clean up procedure test data from specified databases using enum-based specification.
        
        Connection Architecture:
        - Uses enum-based database type specification for type safety
        - Uses Settings injection for environment-agnostic operation
        - Supports unified interface for all database types
        
        Args:
            database_types: List of DatabaseType enums to clean up.
                           If None, cleans up all databases.
        """
        if database_types is None:
            database_types = [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS]
        
        for db_type in database_types:
            try:
                if db_type == DatabaseType.SOURCE:
                    self._cleanup_procedure_data_mysql(self.source_engine, db_type.value)
                elif db_type == DatabaseType.REPLICATION:
                    self._cleanup_procedure_data_mysql(self.replication_engine, db_type.value)
                elif db_type == DatabaseType.ANALYTICS:
                    self._cleanup_procedure_data_postgres(self.raw_engine, db_type.value)
                else:
                    raise ValueError(f"Unsupported database type: {db_type}")
                
                logger.info(f"Cleaned up procedure data from {db_type.value} database")
                
            except Exception as e:
                logger.error(f"Failed to clean up procedure data from {db_type.value}: {e}")
                raise
    
    def _cleanup_patient_data_mysql(self, engine: Engine, db_name: str) -> None:
        """Clean up patient data from MySQL database."""
        with engine.connect() as conn:
            # Check if table exists before trying to clean it up
            result = conn.execute(text("SHOW TABLES LIKE 'patient'"))
            if not result.fetchone():
                logger.info(f"Table 'patient' doesn't exist in {db_name}, skipping cleanup")
                return
            
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
            # Check if table exists before trying to clean it up
            result = conn.execute(text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = 'patient')"))
            if not result.scalar():
                logger.info(f"Table 'raw.patient' doesn't exist in {db_name}, skipping cleanup")
                return
            
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
            # Check if table exists before trying to clean it up
            result = conn.execute(text("SHOW TABLES LIKE 'appointment'"))
            if not result.fetchone():
                logger.info(f"Table 'appointment' doesn't exist in {db_name}, skipping cleanup")
                return
            
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
            # Check if table exists before trying to clean it up
            result = conn.execute(text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = 'appointment')"))
            if not result.scalar():
                logger.info(f"Table 'raw.appointment' doesn't exist in {db_name}, skipping cleanup")
                return
            
            # Get all test appointment AptNums
            result = conn.execute(text('SELECT "AptNum" FROM raw.appointment'))
            all_apt_nums = [row[0] for row in result.fetchall()]
            if all_apt_nums:
                conn.execute(
                    text('DELETE FROM raw.appointment WHERE "AptNum" = ANY(:apt_nums)'),
                    {"apt_nums": all_apt_nums}
                )
            conn.commit()
    
    def _cleanup_procedure_data_mysql(self, engine: Engine, db_name: str) -> None:
        """Clean up procedure data from MySQL database."""
        with engine.connect() as conn:
            # Check if table exists before trying to clean it up
            result = conn.execute(text("SHOW TABLES LIKE 'procedurelog'"))
            if not result.fetchone():
                logger.info(f"Table 'procedurelog' doesn't exist in {db_name}, skipping cleanup")
                return
            
            # Get all test procedure ProcNums
            result = conn.execute(text("SELECT ProcNum FROM procedurelog"))
            all_proc_nums = [row[0] for row in result.fetchall()]
            if all_proc_nums:
                conn.execute(
                    text("DELETE FROM procedurelog WHERE ProcNum IN :proc_nums").bindparams(bindparam("proc_nums", expanding=True)),
                    {"proc_nums": all_proc_nums}
                )
            conn.commit()
    
    def _cleanup_procedure_data_postgres(self, engine: Engine, db_name: str) -> None:
        """Clean up procedure data from PostgreSQL database."""
        with engine.connect() as conn:
            # Check if table exists before trying to clean it up
            result = conn.execute(text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = 'procedurelog')"))
            if not result.scalar():
                logger.info(f"Table 'raw.procedurelog' doesn't exist in {db_name}, skipping cleanup")
                return
            
            # Get all test procedure ProcNums
            result = conn.execute(text('SELECT "ProcNum" FROM raw.procedurelog'))
            all_proc_nums = [row[0] for row in result.fetchall()]
            if all_proc_nums:
                conn.execute(
                    text('DELETE FROM raw.procedurelog WHERE "ProcNum" = ANY(:proc_nums)'),
                    {"proc_nums": all_proc_nums}
                )
            conn.commit()
    
    def cleanup_all_test_data(self) -> None:
        """Clean up all test data from all databases using Settings injection."""
        logger.info("Cleaning up all test data using Settings injection...")
        
        try:
            self.cleanup_patient_data()
            self.cleanup_appointment_data()
            self.cleanup_procedure_data()
            logger.info("Successfully cleaned up all test data")
        except Exception as e:
            logger.error(f"Failed to clean up all test data: {e}")
            raise
    
    def get_patient_count(self, database_type: DatabaseType) -> int:
        """
        Get the count of patients in the specified database using enum-based specification.
        
        Connection Architecture:
        - Uses enum-based database type specification for type safety
        - Uses Settings injection for environment-agnostic operation
        - Supports unified interface for all database types
        
        Args:
            database_type: The DatabaseType enum to check
            
        Returns:
            Number of patients in the database
            
        Raises:
            ValueError: If database_type is not supported
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
                return result.scalar() or 0
                
        except Exception as e:
            logger.error(f"Failed to get patient count from {database_type.value}: {e}")
            raise
    
    def get_appointment_count(self, database_type: DatabaseType) -> int:
        """
        Get the count of appointments in the specified database using enum-based specification.
        
        Connection Architecture:
        - Uses enum-based database type specification for type safety
        - Uses Settings injection for environment-agnostic operation
        - Supports unified interface for all database types
        
        Args:
            database_type: The DatabaseType enum to check
            
        Returns:
            Number of appointments in the database
            
        Raises:
            ValueError: If database_type is not supported
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
                return result.scalar() or 0
                
        except Exception as e:
            logger.error(f"Failed to get appointment count from {database_type.value}: {e}")
            raise
    
    def dispose(self) -> None:
        """Dispose of all database connections using Settings injection."""
        try:
            self.source_engine.dispose()
            self.replication_engine.dispose()
            self.analytics_engine.dispose()
            self.raw_engine.dispose()
            logger.info("Disposed of all database connections using Settings injection")
        except Exception as e:
            logger.error(f"Failed to dispose connections: {e}")
            raise 