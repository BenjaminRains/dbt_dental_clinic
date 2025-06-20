"""
OpenDental dlt source - replaces all your custom extraction logic
This single file replaces: schema_discovery.py, mysql_extractor.py, incremental.py, base_extractor.py
"""

import dlt
from dlt.sources.sql_database import sql_database
from typing import Dict, List, Optional, Iterator, Any
import os
from sqlalchemy import create_engine, text, inspect
import logging
from etl_pipeline.core.connections import ConnectionFactory

from ..config.settings import get_table_configs, get_database_config

logger = logging.getLogger(__name__)

@dlt.source(name="opendental")
def opendental_source(
    importance_filter: Optional[List[str]] = None,
    table_filter: Optional[List[str]] = None
):
    """
    Main OpenDental source that replaces your entire extraction infrastructure.
    
    This automatically handles:
    - Schema discovery (replaces schema_discovery.py)
    - Type conversion (replaces postgres_schema.py) 
    - Incremental loading (replaces incremental.py)
    - Connection management (replaces connections.py)
    
    Args:
        importance_filter: Filter by table importance ['critical', 'important', 'audit', 'reference']
        table_filter: Specific table names to process
    """
    
    # Load configurations (from your existing tables.yml)
    table_configs = get_table_configs()
    
    # Get source engine from ConnectionFactory
    source_engine = ConnectionFactory.get_opendental_source_connection()
    
    # Filter tables based on criteria
    filtered_tables = _filter_tables(table_configs, importance_filter, table_filter)
    
    logger.info(f"Processing {len(filtered_tables)} tables")
    
    # Use dlt's sql_database source as the foundation
    source = sql_database(
        credentials=source_engine.url,
        table_names=list(filtered_tables.keys()),
        reflection_level="minimal"  # Faster startup
    )
    
    # Apply your business configurations to each table
    for resource in source.resources.values():
        table_name = resource.name
        config = filtered_tables.get(table_name, {})
        
        # Apply incremental loading based on your config
        _apply_incremental_config(resource, config)
        
        # Apply write disposition
        _apply_write_disposition(resource, config)
        
        # Apply primary key hints
        _apply_primary_key(resource, table_name, source_engine)
        
        logger.info(f"Configured {table_name}: {config.get('extraction_strategy', 'full_table')}")
    
    return source

def _filter_tables(
    table_configs: Dict[str, Dict], 
    importance_filter: Optional[List[str]], 
    table_filter: Optional[List[str]]
) -> Dict[str, Dict]:
    """Filter tables based on importance or specific names"""
    
    if table_filter:
        # Specific tables requested
        return {name: config for name, config in table_configs.items() if name in table_filter}
    
    if importance_filter:
        # Filter by importance
        return {
            name: config for name, config in table_configs.items()
            if config.get('table_importance') in importance_filter
        }
    
    # Default: all tables
    return table_configs

def _apply_incremental_config(resource, config: Dict):
    """Apply incremental loading configuration"""
    
    incremental_column = config.get('incremental_column')
    extraction_strategy = config.get('extraction_strategy', 'full_table')
    
    if incremental_column and extraction_strategy == 'incremental':
        # dlt automatically manages incremental state
        resource.apply_hints(
            incremental=dlt.sources.incremental(
                cursor_path=incremental_column,
                initial_value=None  # Start from beginning on first run
            )
        )
        logger.debug(f"Applied incremental loading: {resource.name}.{incremental_column}")

def _apply_write_disposition(resource, config: Dict):
    """Apply write disposition based on extraction strategy"""
    
    extraction_strategy = config.get('extraction_strategy', 'full_table')
    incremental_column = config.get('incremental_column')
    
    if extraction_strategy == 'incremental' and incremental_column:
        # Use merge for incremental tables
        write_disposition = "merge"
    else:
        # Use replace for full table loads
        write_disposition = "replace"
    
    resource.apply_hints(write_disposition=write_disposition)
    logger.debug(f"Applied write disposition: {resource.name} -> {write_disposition}")

def _apply_primary_key(resource, table_name: str, engine):
    """Auto-detect and apply primary key"""
    
    try:
        inspector = inspect(engine)
        pk_constraint = inspector.get_pk_constraint(table_name)
        
        if pk_constraint and pk_constraint.get('constrained_columns'):
            primary_key = pk_constraint['constrained_columns']
            resource.apply_hints(primary_key=primary_key)
            logger.debug(f"Applied primary key: {table_name} -> {primary_key}")
        
    except Exception as e:
        logger.warning(f"Could not detect primary key for {table_name}: {e}")

# Critical tables source (replaces your get_critical_tables logic)
@dlt.source(name="opendental_critical")
def opendental_critical_source():
    """Source for critical tables only - equivalent to your Phase 1"""
    return opendental_source(importance_filter=['critical'])

# Incremental source (for scheduled updates)
@dlt.source(name="opendental_incremental") 
def opendental_incremental_source():
    """Source for incremental updates only"""
    table_configs = get_table_configs()
    
    # Get tables with incremental strategy
    incremental_tables = [
        name for name, config in table_configs.items()
        if config.get('extraction_strategy') == 'incremental'
    ]
    
    return opendental_source(table_filter=incremental_tables)

# Custom resource example (for tables that need special handling)
@dlt.resource(
    table_name="patient_enriched",
    primary_key="PatNum",
    write_disposition="merge"
)
def patient_with_computed_fields():
    """
    Example custom resource for complex business logic.
    Most tables won't need this - the sql_database source handles them automatically.
    """
    
    # Get source engine from ConnectionFactory
    source_engine = ConnectionFactory.get_opendental_source_connection()
    
    with source_engine.connect() as conn:
        # Custom query with business logic
        query = text("""
            SELECT 
                *,
                CASE 
                    WHEN Birthdate IS NOT NULL 
                    THEN YEAR(CURDATE()) - YEAR(Birthdate) 
                    ELSE NULL 
                END as computed_age,
                CASE 
                    WHEN DateFirstVisit IS NOT NULL 
                    THEN DATEDIFF(CURDATE(), DateFirstVisit) 
                    ELSE NULL 
                END as days_since_first_visit
            FROM patient
        """)
        
        result = conn.execute(query)
        for row in result:
            yield dict(row)

# Utility functions that replace parts of your core modules
def get_table_row_counts(table_names: List[str]) -> Dict[str, int]:
    """Get row counts for validation - replaces part of your metrics collection"""
    
    db_config = get_database_config('source')
    connection_string = (
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    
    engine = create_engine(connection_string)
    counts = {}
    
    with engine.connect() as conn:
        for table_name in table_names:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                counts[table_name] = result.scalar()
            except Exception as e:
                logger.warning(f"Could not get count for {table_name}: {e}")
                counts[table_name] = 0
    
    engine.dispose()
    return counts

def validate_table_exists(table_name: str) -> bool:
    """Validate table exists - replaces schema validation logic"""
    
    db_config = get_database_config('source')
    connection_string = (
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    
    try:
        engine = create_engine(connection_string)
        inspector = inspect(engine)
        exists = inspector.has_table(table_name)
        engine.dispose()
        return exists
    except Exception as e:
        logger.error(f"Error checking if table {table_name} exists: {e}")
        return False