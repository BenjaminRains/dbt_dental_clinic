"""
DEPRECATION NOTICE - REFACTORING IN PROGRESS
============================================

This file is part of the ETL Pipeline Schema Analysis Refactoring Plan.
See: docs/refactoring_plan_schema_analysis.md

PLANNED CHANGES:
- This class will be enhanced to become the SINGLE source of truth for ALL schema analysis
- Will add comprehensive analysis methods (relationships, usage patterns, importance)
- Will add master methods (analyze_complete_schema, generate_complete_configuration)
- Will become mandatory dependency for all components
- Will eliminate duplicate code across the entire pipeline

TIMELINE: Phase 1 of refactoring plan
STATUS: Enhancement in progress

ENHANCED SCHEMA DISCOVERY - SINGLE SOURCE OF TRUTH
=================================================
This enhanced SchemaDiscovery class is the SINGLE source of truth for ALL schema analysis
in the ETL pipeline. It provides comprehensive analysis, smart caching, and configuration
generation for the entire pipeline.

Key Features:
- Smart three-tier caching system (schema, config, analysis)
- Comprehensive table relationship analysis
- Usage pattern detection and table importance scoring
- Complete pipeline configuration generation
- Specific exception types for better error handling
- Master analysis methods for batch processing
- Support for any target database (not just MySQL replication)

Architecture:
- Single SchemaDiscovery instance serves entire pipeline
- All components use SchemaDiscovery for schema information
- No duplicate schema analysis anywhere in codebase
- Configuration-driven approach with intelligent defaults
"""
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
import hashlib
import re
from datetime import datetime
import time
import json
import os
import yaml
import logging
import glob
import os.path

# Use the new logger architecture
from etl_pipeline.config.logging import get_logger
from etl_pipeline.config.settings import settings

# Import ConnectionManager from connections.py
from .connections import ConnectionManager

logger = logging.getLogger(__name__)

# Specific Exception Types for Better Error Handling
class SchemaDiscoveryError(Exception):
    """Base exception for SchemaDiscovery operations."""
    pass

class SchemaNotFoundError(SchemaDiscoveryError):
    """Raised when a table schema is not found."""
    pass

class SchemaAnalysisError(SchemaDiscoveryError):
    """Raised when schema analysis fails."""
    pass

class ConfigurationGenerationError(SchemaDiscoveryError):
    """Raised when configuration generation fails."""
    pass

class SchemaDiscovery:
    """
    Enhanced SchemaDiscovery - Single Source of Truth for ALL Schema Analysis
    
    This class provides comprehensive schema analysis, relationship discovery,
    usage pattern analysis, and pipeline configuration generation for the entire
    ETL pipeline. It uses smart caching and batch processing for optimal performance.
    """
    
    def __init__(self, source_engine: Engine, source_db: str, dbt_project_root: str = None):
        """
        Initialize enhanced schema discovery for comprehensive analysis.
        
        Args:
            source_engine: Source MySQL database engine (OpenDental)
            source_db: Source database name
            dbt_project_root: Path to dbt project root (for model discovery)
        """
        self.source_engine = source_engine
        self.source_db = source_db
        self.source_inspector = inspect(source_engine)
        
        # Smart Three-Tier Caching System
        self._schema_cache = {}      # Raw schema data
        self._config_cache = {}      # Pipeline configurations  
        self._analysis_cache = {}    # Relationship/usage analysis
        
        # Use the new settings system
        self.settings = settings
        
        # Analysis metadata
        self._analysis_timestamp = None
        self._analysis_version = "3.0"
        
        # Add connection manager for efficient database access
        self.connection_manager = ConnectionManager(source_engine)
        
        # DBT model discovery
        self.dbt_project_root = dbt_project_root
        self._modeled_tables_cache = None
        
        logger.info(f"Enhanced SchemaDiscovery initialized for {source_db}")
    
    # ============================================================================
    # SMART CACHING METHODS
    # ============================================================================
    
    def _get_cached_schema(self, table_name: str) -> Optional[Dict]:
        """Get schema from cache if available."""
        return self._schema_cache.get(table_name)
    
    def _cache_schema(self, table_name: str, schema_data: Dict) -> None:
        """Cache schema data."""
        self._schema_cache[table_name] = schema_data
    
    def _get_cached_config(self, table_name: str) -> Optional[Dict]:
        """Get configuration from cache if available."""
        return self._config_cache.get(table_name)
    
    def _cache_config(self, table_name: str, config_data: Dict) -> None:
        """Cache configuration data."""
        self._config_cache[table_name] = config_data
    
    def _get_cached_analysis(self, analysis_type: str) -> Optional[Dict]:
        """Get analysis data from cache if available."""
        return self._analysis_cache.get(analysis_type)
    
    def _cache_analysis(self, analysis_type: str, analysis_data: Dict) -> None:
        """Cache analysis data."""
        self._analysis_cache[analysis_type] = analysis_data
    
    def clear_cache(self, cache_type: str = None) -> None:
        """
        Clear cache data.
        
        Args:
            cache_type: 'schema', 'config', 'analysis', or None for all
        """
        if cache_type == 'schema':
            self._schema_cache.clear()
        elif cache_type == 'config':
            self._config_cache.clear()
        elif cache_type == 'analysis':
            self._analysis_cache.clear()
        else:
            self._schema_cache.clear()
            self._config_cache.clear()
            self._analysis_cache.clear()
        
        logger.info(f"Cache cleared: {cache_type or 'all'}")
    
    # ============================================================================
    # ENHANCED SCHEMA ANALYSIS METHODS
    # ============================================================================
    
    def get_table_schema(self, table_name: str) -> Dict:
        """
        Get the complete schema information for a table with enhanced caching.
        Includes CREATE TABLE statement, indexes, constraints, and detailed column information.
        """
        # Check cache first
        cached_schema = self._get_cached_schema(table_name)
        if cached_schema:
            logger.debug(f"Returning cached schema for {table_name}")
            return cached_schema
        
        try:
            logger.info(f"Getting schema information for {table_name}...")
            start_time = time.time()
            
            # Use connection manager for efficient database access
            with self.connection_manager as conn_mgr:
                conn = conn_mgr.get_connection()
                conn.execute(text(f"USE {self.source_db}"))
                
                # Get CREATE TABLE statement
                result = conn_mgr.execute_with_retry(f"SHOW CREATE TABLE {table_name}")
                row = result.fetchone()
                
                if not row:
                    raise SchemaNotFoundError(f"Table {table_name} not found in {self.source_db}")
                
                create_statement = row[1]
                
                # Get table metadata
                metadata = self._get_table_metadata_with_conn(conn, table_name)
                
                # Get indexes
                indexes = self._get_table_indexes_with_conn(conn, table_name)
                
                # Get foreign keys
                foreign_keys = self._get_foreign_keys_with_conn(conn, table_name)
                
                # Get detailed column information
                columns = self._get_detailed_columns_with_conn(conn, table_name)
                
                schema_info = {
                    'table_name': table_name,
                    'create_statement': create_statement,
                    'metadata': metadata,
                    'indexes': indexes,
                    'foreign_keys': foreign_keys,
                    'columns': columns,
                    'schema_hash': self._calculate_schema_hash(create_statement),
                    'analysis_timestamp': datetime.now().isoformat(),
                    'analysis_version': self._analysis_version
                }
                
                # Cache the schema info
                self._cache_schema(table_name, schema_info)
                
                elapsed = time.time() - start_time
                logger.info(f"Completed schema discovery for {table_name} in {elapsed:.2f}s")
                
                return schema_info
                
        except SchemaNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {str(e)}")
            
            # Check if this is a "table not found" error
            error_str = str(e).lower()
            if any(phrase in error_str for phrase in ["doesn't exist", "not found", "table", "1146"]):
                raise SchemaNotFoundError(f"Table {table_name} not found in {self.source_db}")
            
            raise SchemaAnalysisError(f"Schema analysis failed for {table_name}: {str(e)}")
    
    def analyze_table_relationships(self, tables: List[str]) -> Dict[str, Dict]:
        """
        Analyze relationships between tables (foreign keys, dependencies).
        Uses efficient batch processing with single connection.
        """
        cache_key = f"relationships_{hash(tuple(sorted(tables)))}"
        cached = self._get_cached_analysis(cache_key)
        if cached:
            return cached
        
        logger.info(f"Analyzing relationships for {len(tables)} tables...")
        relationships = {}
        
        # Use single connection for all operations
        with self.connection_manager as conn_mgr:
            conn = conn_mgr.get_connection()
            conn.execute(text(f"USE {self.source_db}"))
            
            # Fetch all schemas once upfront to avoid redundant database queries
            schemas = {}
            for table_name in tables:
                try:
                    schemas[table_name] = self._get_table_schema_with_conn(conn, table_name)
                except Exception as e:
                    logger.error(f"Error getting schema for {table_name}: {str(e)}")
                    # Create empty schema for failed tables
                    schemas[table_name] = {
                        'foreign_keys': [],
                        'columns': []
                    }
            
            # Now analyze relationships using the cached schemas
            for table_name in tables:
                try:
                    schema = schemas[table_name]
                    foreign_keys = schema.get('foreign_keys', [])
                    
                    # Build dependency information
                    dependencies = []
                    referenced_by = []
                    
                    # Find tables this table depends on
                    for fk in foreign_keys:
                        referenced_table = fk['referenced_table']
                        dependencies.append({
                            'table': referenced_table,
                            'column': fk['column'],
                            'referenced_column': fk['referenced_column']
                        })
                    
                    # Check if this table is referenced by others (using cached schemas)
                    for other_table in tables:
                        if other_table != table_name:
                            other_schema = schemas[other_table]
                            for fk in other_schema.get('foreign_keys', []):
                                if fk['referenced_table'] == table_name:
                                    referenced_by.append({
                                        'table': other_table,
                                        'column': fk['column'],
                                        'referenced_column': fk['referenced_column']
                                    })
                    
                    relationships[table_name] = {
                        'dependencies': dependencies,
                        'referenced_by': referenced_by,
                        'dependency_count': len(dependencies),
                        'reference_count': len(referenced_by),
                        'is_lookup_table': len(dependencies) == 0 and len(referenced_by) > 0,
                        'is_standalone': len(dependencies) == 0 and len(referenced_by) == 0
                    }
                    
                except Exception as e:
                    logger.error(f"Error analyzing relationships for {table_name}: {str(e)}")
                    relationships[table_name] = {
                        'dependencies': [],
                        'referenced_by': [],
                        'dependency_count': 0,
                        'reference_count': 0,
                        'is_lookup_table': False,
                        'is_standalone': True,
                        'error': str(e)
                    }
        
        # Cache the analysis
        self._cache_analysis(cache_key, relationships)
        return relationships
    
    def analyze_table_usage_patterns(self, tables: List[str]) -> Dict[str, Dict]:
        """
        Analyze usage patterns and characteristics of tables.
        Uses efficient batch processing with single connection.
        """
        cache_key = f"usage_patterns_{hash(tuple(sorted(tables)))}"
        cached = self._get_cached_analysis(cache_key)
        if cached:
            return cached
        
        logger.info(f"Analyzing usage patterns for {len(tables)} tables...")
        usage_patterns = {}
        
        # Use single connection for all operations
        with self.connection_manager as conn_mgr:
            conn = conn_mgr.get_connection()
            conn.execute(text(f"USE {self.source_db}"))
            
            # Fetch all schemas and size info once upfront to avoid redundant database queries
            schemas = {}
            size_infos = {}
            
            for table_name in tables:
                try:
                    schemas[table_name] = self._get_table_schema_with_conn(conn, table_name)
                    size_infos[table_name] = self._get_table_size_info_with_conn(conn, table_name)
                except Exception as e:
                    logger.error(f"Error getting data for {table_name}: {str(e)}")
                    # Create empty data for failed tables
                    schemas[table_name] = {'columns': []}
                    size_infos[table_name] = {
                        'total_size_mb': 0,
                        'row_count': 0
                    }
            
            # Now analyze usage patterns using the cached data
            for table_name in tables:
                try:
                    schema = schemas[table_name]
                    size_info = size_infos[table_name]
                    columns = schema.get('columns', [])
                    
                    # Analyze column patterns
                    timestamp_columns = [col for col in columns if 'timestamp' in col['type'].lower() or 'datetime' in col['type'].lower()]
                    boolean_columns = [col for col in columns if col['type'].lower() == 'tinyint(1)']
                    text_columns = [col for col in columns if 'text' in col['type'].lower() or 'varchar' in col['type'].lower()]
                    
                    # Determine update frequency based on table characteristics
                    update_frequency = self._determine_update_frequency(table_name, schema, size_info)
                    
                    # Determine table type
                    table_type = self._determine_table_type(table_name, schema, size_info)
                    
                    usage_patterns[table_name] = {
                        'size_mb': size_info.get('total_size_mb', 0),
                        'row_count': size_info.get('row_count', 0),
                        'column_count': len(columns),
                        'timestamp_columns': len(timestamp_columns),
                        'boolean_columns': len(boolean_columns),
                        'text_columns': len(text_columns),
                        'update_frequency': update_frequency,
                        'table_type': table_type,
                        'has_auto_increment': any(col.get('extra', '').lower() == 'auto_increment' for col in columns),
                        'has_primary_key': any(col.get('key_type') == 'PRI' for col in columns)
                    }
                    
                except Exception as e:
                    logger.error(f"Error analyzing usage patterns for {table_name}: {str(e)}")
                    usage_patterns[table_name] = {
                        'size_mb': 0,
                        'row_count': 0,
                        'column_count': 0,
                        'timestamp_columns': 0,
                        'boolean_columns': 0,
                        'text_columns': 0,
                        'update_frequency': 'unknown',
                        'table_type': 'unknown',
                        'has_auto_increment': False,
                        'has_primary_key': False,
                        'error': str(e)
                    }
        
        # Cache the analysis
        self._cache_analysis(cache_key, usage_patterns)
        return usage_patterns
    
    def determine_table_importance(self, tables: List[str]) -> Dict[str, str]:
        """
        Determine the importance level of tables for ETL processing.
        
        Args:
            tables: List of table names to analyze
            
        Returns:
            Dict mapping table names to importance levels ('critical', 'important', 'reference', 'audit', 'standard')
        """
        cache_key = f"importance_{hash(tuple(sorted(tables)))}"
        cached = self._get_cached_analysis(cache_key)
        if cached:
            return cached
        
        logger.info(f"Determining importance for {len(tables)} tables...")
        
        # Get analysis data
        relationships = self.analyze_table_relationships(tables)
        usage_patterns = self.analyze_table_usage_patterns(tables)
        
        importance_scores = {}
        
        for table_name in tables:
            try:
                rel_info = relationships.get(table_name, {})
                usage_info = usage_patterns.get(table_name, {})
                
                # Score based on multiple factors
                score = 0
                
                # Size factor (larger tables are more important)
                size_mb = usage_info.get('size_mb', 0)
                if size_mb > 100:
                    score += 3
                elif size_mb > 10:
                    score += 2
                elif size_mb > 1:
                    score += 1
                
                # Dependency factor (tables with many dependencies are more important)
                dependency_count = rel_info.get('dependency_count', 0)
                if dependency_count > 5:
                    score += 3
                elif dependency_count > 2:
                    score += 2
                elif dependency_count > 0:
                    score += 1
                
                # Reference factor (tables referenced by many others are more important)
                reference_count = rel_info.get('reference_count', 0)
                if reference_count > 10:
                    score += 3
                elif reference_count > 5:
                    score += 2
                elif reference_count > 0:
                    score += 1
                
                # Table type factor
                table_type = usage_info.get('table_type', 'unknown')
                if table_type == 'transaction':
                    score += 2
                elif table_type == 'master':
                    score += 3
                
                # Determine importance level based on score
                if score >= 8:
                    importance = 'critical'
                elif score >= 5:
                    importance = 'important'
                elif rel_info.get('is_lookup_table', False):
                    importance = 'reference'
                elif 'audit' in table_name.lower() or 'log' in table_name.lower():
                    importance = 'audit'
                else:
                    importance = 'standard'
                
                importance_scores[table_name] = importance
                
            except Exception as e:
                logger.error(f"Error determining importance for {table_name}: {str(e)}")
                importance_scores[table_name] = 'standard'
        
        # Cache the analysis
        self._cache_analysis(cache_key, importance_scores)
        return importance_scores
    
    # ============================================================================
    # MASTER ANALYSIS METHODS
    # ============================================================================
    
    def analyze_complete_schema(self, tables: List[str] = None) -> Dict:
        """
        Master method that performs ALL schema analysis in one efficient pass.
        
        Args:
            tables: List of tables to analyze (None for all tables)
            
        Returns:
            Complete analysis results with metadata
        """
        if tables is None:
            tables = self.discover_all_tables()
            tables = self._filter_excluded_tables(tables)
        
        logger.info(f"Performing complete schema analysis for {len(tables)} tables...")
        start_time = time.time()
        
        # Perform all analysis in one pass for efficiency
        relationships = self.analyze_table_relationships(tables)
        usage_patterns = self.analyze_table_usage_patterns(tables)
        importance_scores = self.determine_table_importance(tables)
        
        # Generate pipeline configurations for all tables
        pipeline_configs = {}
        for table in tables:
            try:
                pipeline_configs[table] = self.get_pipeline_configuration(table)
            except Exception as e:
                logger.error(f"Error generating pipeline config for {table}: {str(e)}")
                pipeline_configs[table] = self._get_default_pipeline_configuration(table)
        
        # Calculate summary statistics
        summary_stats = self._calculate_summary_statistics(
            relationships, usage_patterns, importance_scores, pipeline_configs
        )
        
        # Update analysis metadata
        self._analysis_timestamp = datetime.now().isoformat()
        
        analysis_results = {
            'metadata': {
                'timestamp': self._analysis_timestamp,
                'total_tables': len(tables),
                'source_database': self.source_db,
                'analysis_version': self._analysis_version,
                'analysis_duration_seconds': time.time() - start_time
            },
            'tables': tables,
            'relationships': relationships,
            'usage_patterns': usage_patterns,
            'importance_scores': importance_scores,
            'pipeline_configs': pipeline_configs,
            'summary_statistics': summary_stats
        }
        
        logger.info(f"Complete schema analysis finished in {time.time() - start_time:.2f}s")
        return analysis_results
    
    def generate_complete_configuration(self, output_dir: str = None) -> Dict:
        """
        Generate the final tables.yml configuration file.
        
        Args:
            output_dir: Directory to save configuration and analysis data
            
        Returns:
            Configuration dictionary ready for YAML serialization
        """
        try:
            logger.info("Generating complete configuration...")
            
            # Perform complete analysis
            analysis = self.analyze_complete_schema()
            
            # Generate clean, simplified configuration structure
            config = {'tables': {}}
            
            for table in analysis['tables']:
                table_config = analysis['pipeline_configs'][table]
                
                # Add analysis metadata to configuration
                table_config.update({
                    'schema_hash': self.get_table_schema(table)['schema_hash'],
                    'last_analyzed': analysis['metadata']['timestamp'],
                    'analysis_version': analysis['metadata']['analysis_version']
                })
                
                config['tables'][table] = table_config
            
            # Save configuration and analysis data if output directory provided
            if output_dir:
                self._save_complete_configuration(config, analysis, output_dir)
            
            logger.info(f"Configuration generated for {len(config['tables'])} tables")
            return config
            
        except Exception as e:
            logger.error(f"Error generating complete configuration: {str(e)}")
            raise ConfigurationGenerationError(f"Configuration generation failed: {str(e)}")
    
    # ============================================================================
    # PIPELINE CONFIGURATION METHODS
    # ============================================================================
    
    def get_pipeline_configuration(self, table_name: str) -> Dict:
        """
        Generate complete ETL pipeline configuration for a table.
        
        Args:
            table_name: Name of the table to configure
            
        Returns:
            Complete pipeline configuration dictionary
        """
        # Check cache first
        cached_config = self._get_cached_config(table_name)
        if cached_config:
            return cached_config
        
        try:
            # Get all table information
            schema_info = self.get_table_schema(table_name)
            size_info = self.get_table_size_info(table_name)
            incremental_columns = self.get_incremental_columns(table_name)
            
            # Get the best incremental column
            best_incremental = self._get_best_incremental_column(incremental_columns)
            
            # Get dependencies from foreign keys
            dependencies = [fk['referenced_table'] for fk in schema_info['foreign_keys']]
            
            # Determine configuration based on analysis
            usage_metrics = self._get_table_usage_metrics(table_name, schema_info, size_info)
            importance = self._determine_single_table_importance(table_name, schema_info, size_info)
            
            # Check if table is modeled by dbt (NEW LOGIC)
            is_modeled = self.is_table_modeled_by_dbt(table_name)
            dbt_model_types = self.get_dbt_model_types(table_name)
            
            config = {
                # Core ETL configuration
                'incremental_column': best_incremental,
                'batch_size': self._get_recommended_batch_size(usage_metrics),
                'extraction_strategy': self._get_extraction_strategy(usage_metrics, importance),
                
                # Table metadata
                'table_importance': importance,
                'estimated_size_mb': size_info['total_size_mb'],
                'estimated_rows': size_info['row_count'],
                'dependencies': dependencies,
                
                # DBT modeling information (NEW)
                'is_modeled': is_modeled,
                'dbt_model_types': dbt_model_types,
                
                # Optional configurations
                'column_overrides': self._get_column_overrides(schema_info),
                'monitoring': self._get_monitoring_config(importance, size_info)
            }
            
            # Cache the configuration
            self._cache_config(table_name, config)
            return config
            
        except Exception as e:
            logger.error(f"Error generating pipeline configuration for {table_name}: {str(e)}")
            return self._get_default_pipeline_configuration(table_name)
    
    # ============================================================================
    # HELPER METHODS
    # ============================================================================
    
    def _filter_excluded_tables(self, tables: List[str]) -> List[str]:
        """Filter out tables that should be excluded from analysis."""
        # Add any table exclusion logic here
        excluded_patterns = [
            r'^temp_',
            r'^tmp_',
            r'_backup$',
            r'_old$'
        ]
        
        filtered_tables = []
        for table in tables:
            should_exclude = False
            for pattern in excluded_patterns:
                if re.match(pattern, table, re.IGNORECASE):
                    should_exclude = True
                    break
            
            if not should_exclude:
                filtered_tables.append(table)
        
        return filtered_tables
    
    def _get_best_incremental_column(self, incremental_columns: List[Dict]) -> Optional[str]:
        """Get the best incremental column from the list."""
        if not incremental_columns:
            return None
        
        # Prioritize timestamp/datetime columns
        timestamp_cols = [col for col in incremental_columns 
                         if 'timestamp' in col['data_type'].lower() or 'datetime' in col['data_type'].lower()]
        
        if timestamp_cols:
            return timestamp_cols[0]['column_name']
        
        # Fall back to first column
        return incremental_columns[0]['column_name']
    
    def _get_table_usage_metrics(self, table_name: str, schema_info: Dict, size_info: Dict) -> Dict:
        """Get usage metrics for a table."""
        columns = schema_info.get('columns', [])
        
        return {
            'size_mb': size_info.get('total_size_mb', 0),
            'row_count': size_info.get('row_count', 0),
            'column_count': len(columns),
            'update_frequency': self._determine_update_frequency(table_name, schema_info, size_info),
            'table_type': self._determine_table_type(table_name, schema_info, size_info)
        }
    
    def _determine_update_frequency(self, table_name: str, schema_info: Dict, size_info: Dict) -> str:
        """Determine update frequency based on table characteristics."""
        # Simple heuristic based on table name and characteristics
        table_lower = table_name.lower()
        
        if any(keyword in table_lower for keyword in ['log', 'audit', 'history', 'event']):
            return 'high'
        elif any(keyword in table_lower for keyword in ['patient', 'appointment', 'procedure']):
            return 'medium'
        else:
            return 'low'
    
    def _determine_table_type(self, table_name: str, schema_info: Dict, size_info: Dict) -> str:
        """Determine table type based on characteristics."""
        table_lower = table_name.lower()
        
        if any(keyword in table_lower for keyword in ['log', 'audit', 'history']):
            return 'audit'
        elif any(keyword in table_lower for keyword in ['patient', 'appointment', 'procedure']):
            return 'transaction'
        elif size_info.get('row_count', 0) < 1000:
            return 'lookup'
        else:
            return 'master'
    
    def _determine_single_table_importance(self, table_name: str, schema_info: Dict, size_info: Dict) -> str:
        """Determine importance for a single table."""
        # Use the same logic as the batch method but for a single table
        tables = [table_name]
        importance_scores = self.determine_table_importance(tables)
        return importance_scores.get(table_name, 'standard')
    
    def _get_recommended_batch_size(self, usage_metrics: Dict) -> int:
        """Determine optimal batch size based on table characteristics."""
        size_mb = usage_metrics.get('size_mb', 0)
        update_freq = usage_metrics.get('update_frequency', 'low')
        
        if size_mb < 1:
            return 5000
        elif size_mb < 100:
            return 2000 if update_freq == 'high' else 3000
        elif size_mb < 1000:
            return 1000 if update_freq == 'high' else 2000
        else:
            return 500 if update_freq == 'high' else 1000
    
    def _get_extraction_strategy(self, usage_metrics: Dict, importance: str) -> str:
        """Determine optimal extraction strategy."""
        size_mb = usage_metrics.get('size_mb', 0)
        update_freq = usage_metrics.get('update_frequency', 'low')
        
        if size_mb < 1:
            return 'full_table'
        elif size_mb < 100:
            return 'incremental'
        elif size_mb < 1000:
            return 'chunked_incremental' if update_freq == 'high' else 'incremental'
        else:
            return 'streaming_incremental'
    
    def _get_column_overrides(self, schema_info: Dict) -> Dict:
        """Get column overrides for special handling."""
        overrides = {}
        columns = schema_info.get('columns', [])
        
        for col in columns:
            col_name = col['name']
            col_type = col['type'].lower()
            
            # Handle boolean conversions
            if col_type == 'tinyint(1)':
                overrides[col_name] = {'conversion_rule': 'tinyint_to_boolean'}
            
            # Handle decimal precision
            elif 'decimal' in col_type:
                overrides[col_name] = {'conversion_rule': 'decimal_round', 'precision': 2}
        
        return overrides
    
    def _get_monitoring_config(self, importance: str, size_info: Dict) -> Dict:
        """Generate monitoring configuration based on table importance."""
        return {
            'alert_on_failure': importance in ['critical', 'important'],
            'max_extraction_time_minutes': min(180, max(5, int(size_info['total_size_mb'] / 10))),
            'data_quality_threshold': 0.99 if importance == 'critical' else 0.95
        }
    
    def _get_default_pipeline_configuration(self, table_name: str) -> Dict:
        """Get default configuration for tables that fail analysis."""
        return {
            'incremental_column': None,
            'batch_size': 5000,
            'extraction_strategy': 'full_table',
            'table_importance': 'standard',
            'estimated_size_mb': 0,
            'estimated_rows': 0,
            'dependencies': [],
            'is_modeled': self.is_table_modeled_by_dbt(table_name),
            'dbt_model_types': self.get_dbt_model_types(table_name),
            'column_overrides': {},
            'monitoring': {
                'alert_on_failure': False,
                'max_extraction_time_minutes': 30,
                'data_quality_threshold': 0.95
            }
        }
    
    def _calculate_summary_statistics(self, relationships: Dict, usage_patterns: Dict, 
                                    importance_scores: Dict, pipeline_configs: Dict) -> Dict:
        """Calculate summary statistics from analysis results."""
        total_tables = len(importance_scores)
        
        # Importance distribution
        importance_dist = {}
        for importance in importance_scores.values():
            importance_dist[importance] = importance_dist.get(importance, 0) + 1
        
        # Size statistics
        total_size_mb = sum(usage_patterns.get(table, {}).get('size_mb', 0) for table in usage_patterns)
        total_rows = sum(usage_patterns.get(table, {}).get('row_count', 0) for table in usage_patterns)
        
        # Monitoring statistics
        monitored_tables = sum(1 for config in pipeline_configs.values() 
                             if config.get('monitoring', {}).get('alert_on_failure', False))
        
        return {
            'total_tables': total_tables,
            'total_size_mb': total_size_mb,
            'total_rows': total_rows,
            'importance_distribution': importance_dist,
            'monitored_tables': monitored_tables,
            'average_size_mb': total_size_mb / total_tables if total_tables > 0 else 0,
            'average_rows': total_rows / total_tables if total_tables > 0 else 0
        }
    
    def _save_complete_configuration(self, config: Dict, analysis: Dict, output_dir: str) -> None:
        """Save configuration and analysis data to files."""
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Save tables.yml
        tables_yml_path = os.path.join(output_dir, 'tables.yml')
        with open(tables_yml_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        
        # Save detailed analysis
        analysis_path = os.path.join(output_dir, f'schema_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(analysis_path, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        
        logger.info(f"Configuration saved to {tables_yml_path}")
        logger.info(f"Analysis data saved to {analysis_path}")
    
    # ============================================================================
    # COMPATIBILITY METHODS (for existing code)
    # ============================================================================
    
    def replicate_schema(self, source_table: str, target_engine: Engine, target_db: str, target_table: str = None, drop_if_exists: bool = True) -> bool:
        """
        Replicate a table's schema to any target database.
        Updated to accept target engine and database as parameters.
        """
        try:
            target_table = target_table or source_table
            
            # Get the CREATE TABLE statement from source
            create_statement = self.get_table_schema(source_table)['create_statement']
            
            # Modify the CREATE statement for the target database
            target_create_statement = self._adapt_create_statement_for_target(
                create_statement, target_table
            )
            
            with target_engine.begin() as conn:
                conn.execute(text(f"USE {target_db}"))
                
                # Drop table if it exists and requested
                if drop_if_exists:
                    conn.execute(text(f"DROP TABLE IF EXISTS `{target_table}`"))
                    logger.info(f"Dropped existing table {target_table} in target")
                
                # Create the exact replica
                conn.execute(text(target_create_statement))
                
                logger.info(f"Created exact replica of {source_table} as {target_table} in {target_db}")
                return True
                
        except Exception as e:
            logger.error(f"Error replicating schema for {source_table}: {str(e)}")
            return False
    
    def _get_table_metadata_with_conn(self, conn, table_name: str) -> Dict:
        """Get table metadata using provided connection."""
        result = conn.execute(text(f"SHOW TABLE STATUS LIKE '{table_name}'"))
        row = result.fetchone()
        
        if not row:
            return {
                'engine': 'InnoDB',
                'charset': 'utf8mb4',
                'collation': 'utf8mb4_general_ci',
                'auto_increment': None,
                'row_count': 0
            }
        
        return {
            'engine': row.Engine or 'InnoDB',
            'charset': getattr(row, 'Collation', 'utf8mb4_general_ci').split('_')[0] if hasattr(row, 'Collation') else 'utf8mb4',
            'collation': getattr(row, 'Collation', 'utf8mb4_general_ci') if hasattr(row, 'Collation') else 'utf8mb4_general_ci',
            'auto_increment': getattr(row, 'Auto_increment', None) if hasattr(row, 'Auto_increment') else None,
            'row_count': getattr(row, 'Rows', 0) if hasattr(row, 'Rows') else 0
        }
    
    def _get_table_indexes_with_conn(self, conn, table_name: str) -> List[Dict]:
        """Get all indexes for a table using provided connection."""
        query = text("""
            SELECT 
                index_name,
                GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns,
                non_unique,
                index_type
            FROM information_schema.statistics
            WHERE table_schema = :db_name
            AND table_name = :table_name
            GROUP BY index_name, non_unique, index_type
        """)
        
        result = conn.execute(query.bindparams(db_name=self.source_db, table_name=table_name))
        
        indexes = []
        for row in result:
            indexes.append({
                'name': row.index_name,
                'columns': row.columns.split(','),
                'is_unique': not row.non_unique,
                'type': row.index_type
            })
        
        return indexes
    
    def _get_foreign_keys_with_conn(self, conn, table_name: str) -> List[Dict]:
        """Get all foreign key constraints for a table using provided connection."""
        query = text("""
            SELECT 
                constraint_name,
                column_name,
                referenced_table_name,
                referenced_column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = :db_name
            AND table_name = :table_name
            AND referenced_table_name IS NOT NULL
        """)
        
        result = conn.execute(query.bindparams(db_name=self.source_db, table_name=table_name))
        
        foreign_keys = []
        for row in result:
            foreign_keys.append({
                'name': row.constraint_name,
                'column': row.column_name,
                'referenced_table': row.referenced_table_name,
                'referenced_column': row.referenced_column_name
            })
        
        return foreign_keys
    
    def _get_detailed_columns_with_conn(self, conn, table_name: str) -> List[Dict]:
        """Get detailed information about all columns in a table using provided connection."""
        query = text("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                extra,
                column_comment,
                column_key
            FROM information_schema.columns
            WHERE table_schema = :db_name
            AND table_name = :table_name
            ORDER BY ordinal_position
        """)
        
        result = conn.execute(query.bindparams(db_name=self.source_db, table_name=table_name))
        
        columns = []
        for row in result:
            columns.append({
                'name': row.column_name,
                'type': row.data_type,
                'is_nullable': row.is_nullable == 'YES',
                'default': row.column_default,
                'extra': row.extra,
                'comment': row.column_comment,
                'key_type': row.column_key
            })
        
        return columns
    
    def _get_table_size_info_with_conn(self, conn, table_name: str) -> Dict:
        """Get size information for a table using provided connection."""
        try:
            query = text("""
                    SELECT 
                        table_rows as row_count,
                        data_length as data_size_bytes,
                        index_length as index_size_bytes,
                        data_length + index_length as total_size_bytes
                    FROM information_schema.tables
                    WHERE table_schema = :db_name
                    AND table_name = :table_name
            """)
            
            result = conn.execute(query.bindparams(db_name=self.source_db, table_name=table_name))
            
            row = result.fetchone()
            if not row:
                return {
                    'row_count': 0,
                    'data_size_bytes': 0,
                    'index_size_bytes': 0,
                    'total_size_bytes': 0,
                    'data_size_mb': 0,
                    'index_size_mb': 0,
                    'total_size_mb': 0
                }
            
            # Convert bytes to MB
            data_size_mb = row.data_size_bytes / (1024 * 1024)
            index_size_mb = row.index_size_bytes / (1024 * 1024)
            total_size_mb = row.total_size_bytes / (1024 * 1024)
            
            return {
                'row_count': row.row_count,
                'data_size_bytes': row.data_size_bytes,
                'index_size_bytes': row.index_size_bytes,
                'total_size_bytes': row.total_size_bytes,
                'data_size_mb': round(data_size_mb, 2),
                'index_size_mb': round(index_size_mb, 2),
                'total_size_mb': round(total_size_mb, 2)
            }
            
        except Exception as e:
            logger.error(f"Error getting size info for {table_name}: {str(e)}")
            return {
                'row_count': 0,
                'data_size_bytes': 0,
                'index_size_bytes': 0,
                'total_size_bytes': 0,
                'data_size_mb': 0,
                'index_size_mb': 0,
                'total_size_mb': 0
            }
    
    def _calculate_schema_hash(self, create_statement: str) -> str:
        """Calculate a hash of the schema for change detection."""
        # Normalize the CREATE statement by removing variable parts
        normalized = re.sub(r'AUTO_INCREMENT=\d+', 'AUTO_INCREMENT=1', create_statement)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def has_schema_changed(self, table_name: str, stored_hash: str) -> bool:
        """Check if the table schema has changed."""
        try:
            current_schema = self.get_table_schema(table_name)
            current_hash = current_schema['schema_hash']
            return current_hash != stored_hash
        except Exception as e:
            logger.error(f"Error checking schema changes for {table_name}: {str(e)}")
            return True  # Assume changed if we can't determine
    
    def get_incremental_columns(self, table_name: str) -> List[Dict]:
        """
        Get all columns that can be used for incremental loading.
        Updated to use connection manager.
        """
        try:
            with self.connection_manager as conn_mgr:
                conn = conn_mgr.get_connection()
                conn.execute(text(f"USE {self.source_db}"))
                columns = self._get_detailed_columns_with_conn(conn, table_name)
            
            # All columns are considered for incremental loading
            incremental_columns = []
            for col in columns:
                incremental_columns.append({
                    'column_name': col['name'],
                    'data_type': col['type'],
                    'default': col['default'],
                    'extra': col['extra'],
                    'comment': col.get('comment', ''),
                    'priority': 1  # All columns have equal priority
                })
            
            return incremental_columns
            
        except Exception as e:
            logger.error(f"Error getting incremental columns for {table_name}: {str(e)}")
            return []
    
    def discover_all_tables(self) -> List[str]:
        """Get a list of all tables in the source database (updated to use connection manager)."""
        try:
            with self.connection_manager as conn_mgr:
                conn = conn_mgr.get_connection()
                conn.execute(text(f"USE {self.source_db}"))
                result = conn.execute(text("SHOW TABLES"))
                return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error discovering tables: {str(e)}")
            return []
    
    def get_table_size_info(self, table_name: str) -> Dict:
        """Get size information for a table (updated to use connection manager)."""
        try:
            with self.connection_manager as conn_mgr:
                conn = conn_mgr.get_connection()
                conn.execute(text(f"USE {self.source_db}"))
                return self._get_table_size_info_with_conn(conn, table_name)
        except Exception as e:
            logger.error(f"Error getting size info for {table_name}: {str(e)}")
            return {
                'row_count': 0,
                'data_size_bytes': 0,
                'index_size_bytes': 0,
                'total_size_bytes': 0,
                'data_size_mb': 0,
                'index_size_mb': 0,
                'total_size_mb': 0
            }
    
    def _adapt_create_statement_for_target(self, create_statement: str, target_table: str) -> str:
        """
        Adapt the CREATE statement for the target database.
        Ensures compatibility while preserving the exact structure.
        """
        # First, extract the table definition part (everything after CREATE TABLE)
        parts = create_statement.split('CREATE TABLE', 1)
        if len(parts) != 2:
            raise ValueError("Invalid CREATE TABLE statement")
            
        # Get the table definition part and remove any existing table name
        table_def = parts[1].strip()
        table_def = re.sub(r'^`?\w+`?\s+', '', table_def)
        
        # Create the new statement with the target table name
        return f"CREATE TABLE `{target_table}` {table_def}"
    
    def _get_table_schema_with_conn(self, conn, table_name: str) -> Dict:
        """
        Get the complete schema information for a table using provided connection.
        This method avoids creating nested connection contexts.
        """
        # Check cache first
        cached_schema = self._get_cached_schema(table_name)
        if cached_schema:
            logger.debug(f"Returning cached schema for {table_name}")
            return cached_schema
        
        try:
            logger.info(f"Getting schema information for {table_name}...")
            start_time = time.time()
            
            # Get CREATE TABLE statement
            result = conn.execute(text(f"SHOW CREATE TABLE {table_name}"))
            row = result.fetchone()
            
            if not row:
                raise SchemaNotFoundError(f"Table {table_name} not found in {self.source_db}")
            
            create_statement = row[1]
            
            # Get table metadata
            metadata = self._get_table_metadata_with_conn(conn, table_name)
            
            # Get indexes
            indexes = self._get_table_indexes_with_conn(conn, table_name)
            
            # Get foreign keys
            foreign_keys = self._get_foreign_keys_with_conn(conn, table_name)
            
            # Get detailed column information
            columns = self._get_detailed_columns_with_conn(conn, table_name)
            
            schema_info = {
                'table_name': table_name,
                'create_statement': create_statement,
                'metadata': metadata,
                'indexes': indexes,
                'foreign_keys': foreign_keys,
                'columns': columns,
                'schema_hash': self._calculate_schema_hash(create_statement),
                'analysis_timestamp': datetime.now().isoformat(),
                'analysis_version': self._analysis_version
            }
            
            # Cache the schema info
            self._cache_schema(table_name, schema_info)
            
            elapsed = time.time() - start_time
            logger.info(f"Completed schema discovery for {table_name} in {elapsed:.2f}s")
            
            return schema_info
            
        except SchemaNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {str(e)}")
            
            # Check if this is a "table not found" error
            error_str = str(e).lower()
            if any(phrase in error_str for phrase in ["doesn't exist", "not found", "table", "1146"]):
                raise SchemaNotFoundError(f"Table {table_name} not found in {self.source_db}")
            
            raise SchemaAnalysisError(f"Schema analysis failed for {table_name}: {str(e)}")

    def _discover_dbt_models(self) -> Dict[str, List[str]]:
        """
        Discover all dbt models and map them to source tables.
        
        Returns:
            Dict mapping source table names to list of dbt model types
        """
        if self._modeled_tables_cache is not None:
            return self._modeled_tables_cache
            
        if not self.dbt_project_root:
            logger.warning("No dbt project root provided, assuming no models")
            self._modeled_tables_cache = {}
            return self._modeled_tables_cache
        
        modeled_tables = {}
        
        try:
            # Scan staging models (stg_opendental__{table_name}.sql)
            staging_pattern = os.path.join(self.dbt_project_root, "models", "staging", "opendental", "stg_opendental__*.sql")
            staging_files = glob.glob(staging_pattern)
            
            for file_path in staging_files:
                filename = os.path.basename(file_path)
                if filename.startswith("stg_opendental__") and filename.endswith(".sql"):
                    # Extract table name: stg_opendental__patient.sql -> patient
                    table_name = filename[16:-4]  # Remove "stg_opendental__" and ".sql"
                    if table_name not in modeled_tables:
                        modeled_tables[table_name] = []
                    modeled_tables[table_name].append("staging")
            
            # Scan marts models (dim_*, fact_*, mart_*)
            marts_pattern = os.path.join(self.dbt_project_root, "models", "marts", "*.sql")
            marts_files = glob.glob(marts_pattern)
            
            # We could also scan intermediate models if needed
            # intermediate_pattern = os.path.join(self.dbt_project_root, "models", "intermediate", "**", "*.sql")
            # intermediate_files = glob.glob(intermediate_pattern, recursive=True)
            
            logger.info(f"Discovered {len(modeled_tables)} tables with dbt models")
            logger.debug(f"Modeled tables: {list(modeled_tables.keys())}")
            
        except Exception as e:
            logger.error(f"Error discovering dbt models: {str(e)}")
            modeled_tables = {}
        
        self._modeled_tables_cache = modeled_tables
        return modeled_tables
    
    def is_table_modeled_by_dbt(self, table_name: str) -> bool:
        """
        Check if a table is modeled by dbt.
        
        Args:
            table_name: Name of the source table
            
        Returns:
            True if the table has dbt models, False otherwise
        """
        modeled_tables = self._discover_dbt_models()
        return table_name in modeled_tables
    
    def get_dbt_model_types(self, table_name: str) -> List[str]:
        """
        Get the types of dbt models for a table.
        
        Args:
            table_name: Name of the source table
            
        Returns:
            List of model types (e.g., ['staging', 'mart'])
        """
        modeled_tables = self._discover_dbt_models()
        return modeled_tables.get(table_name, [])