"""
PostgreSQL Loader - Refactored with Strategy Pattern
====================================================

ROLE IN PIPELINE:
This is a COMPONENT (not the orchestrator!) called by TableProcessor for the load phase.
It handles loading data from replication MySQL to analytics PostgreSQL.

CALL HIERARCHY:
PipelineOrchestrator → TableProcessor → PostgresLoaderRefactored ← YOU ARE HERE
                                      ↓
                              (Coordinates internal strategies)

REFACTORING GOAL:
Consolidate 5 duplicate load methods into a clean Strategy Pattern architecture
that eliminates code duplication and ensures consistent behavior (especially HYBRID FIX).

CURRENT STATE: 5 methods × ~250 lines = ~1,250 lines (80% duplicate code)
TARGET STATE: 1 coordinator + 3 strategies = ~600 lines (50% reduction)

ARCHITECTURE:
1. PostgresLoaderRefactored - Loading component (coordinates strategies internally)
2. LoadPreparation - Universal pre-processing (config, schema, query building with HYBRID FIX)
3. LoadStrategy (ABC) - Abstract base for execution strategies
4. Concrete Strategies - StandardStrategy, StreamingStrategy, ChunkedStrategy
5. LoadFinalization - Universal post-processing (tracking, metrics, validation)

KEY IMPROVEMENTS:
- ✅ HYBRID FIX implemented ONCE, applies to ALL strategies
- ✅ Eliminates ~800 lines of duplicate code
- ✅ Consistent error handling and logging
- ✅ Easy to test individual strategies
- ✅ Simple to add new strategies (e.g., Delta Lake, Iceberg)
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import time
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl_pipeline.config import get_settings, DatabaseType
from etl_pipeline.config.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# SCHEMA CACHE
# ============================================================================

class SchemaCache:
    """
    Cache for MySQL schema information to avoid repeated expensive queries.
    
    This class caches schema analysis results to improve performance
    when loading multiple tables or retrying failed operations.
    
    Migrated from original postgres_loader.py (line 155)
    """
    
    def __init__(self):
        self._cache = {}
        self._cache_timestamps = {}
        self._cache_ttl = 3600  # 1 hour TTL
    
    def get_cached_schema(self, table_name: str) -> Optional[Dict]:
        """
        Get cached schema for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Cached schema dict or None if not cached/expired
        """
        if table_name not in self._cache:
            return None
        
        # Check if cache is still valid
        timestamp = self._cache_timestamps.get(table_name, 0)
        if time.time() - timestamp > self._cache_ttl:
            # Cache expired, remove entry
            del self._cache[table_name]
            if table_name in self._cache_timestamps:
                del self._cache_timestamps[table_name]
            return None
        
        return self._cache[table_name]
    
    def cache_schema(self, table_name: str, schema: Dict) -> None:
        """
        Cache schema for a table.
        
        Args:
            table_name: Name of the table
            schema: Schema dictionary to cache
        """
        self._cache[table_name] = schema
        self._cache_timestamps[table_name] = time.time()
        logger.debug(f"Cached schema for table {table_name}")
    
    def invalidate_cache(self, table_name: Optional[str] = None) -> None:
        """
        Invalidate cache for a specific table or all tables.
        
        Args:
            table_name: Name of the table to invalidate, or None for all tables
        """
        if table_name:
            if table_name in self._cache:
                del self._cache[table_name]
            if table_name in self._cache_timestamps:
                del self._cache_timestamps[table_name]
            logger.debug(f"Invalidated cache for table {table_name}")
        else:
            self._cache.clear()
            self._cache_timestamps.clear()
            logger.debug("Invalidated all schema cache entries")
    
    def get_cache_stats(self) -> Dict:
        """
        Get cache statistics.
        
        Returns:
            Dict with cache statistics
        """
        return {
            'cached_tables': len(self._cache),
            'cache_size': sum(len(str(schema)) for schema in self._cache.values()),
            'oldest_entry': min(self._cache_timestamps.values()) if self._cache_timestamps else None,
            'newest_entry': max(self._cache_timestamps.values()) if self._cache_timestamps else None
        }


# ============================================================================
# ENUMS & DATA CLASSES
# ============================================================================

class LoadStrategyType(Enum):
    """Available loading strategies based on table size."""
    STANDARD = "standard"      # < 50MB - Batch loading with fetchall()
    STREAMING = "streaming"    # 50-200MB - Generator-based streaming
    CHUNKED = "chunked"        # 200-500MB - Paginated LIMIT/OFFSET
    COPY_CSV = "copy_csv"      # > 500MB - Export CSV, use COPY command
    PARALLEL = "parallel"      # > 1M rows - ThreadPoolExecutor parallel chunks


@dataclass
class LoadPreparation:
    """
    Container for all pre-processing results.
    
    Created by _prepare_table_load() and passed to strategies.
    Contains everything a strategy needs to execute.
    
    FIELD SOURCES (all from tables.yml via Settings.get_table_config()):
    - primary_column: Local name for table_config['primary_incremental_column']
    - incremental_columns: From table_config['incremental_columns'] (list)
    - incremental_strategy: From table_config['incremental_strategy'] (e.g., 'or_logic', 'single_column')
    - batch_size: From table_config['batch_size']
    - estimated_rows: From table_config['estimated_rows']
    - estimated_size_mb: From table_config['estimated_size_mb']
    """
    table_name: str
    table_config: Dict[str, Any]
    mysql_schema: Dict[str, Any]
    query: str
    primary_column: Optional[str]          # From table_config.get('primary_incremental_column')
    incremental_columns: List[str]         # From table_config.get('incremental_columns', [])
    incremental_strategy: str              # From table_config.get('incremental_strategy', 'or_logic')
    should_truncate: bool
    force_full: bool
    batch_size: int                        # From table_config.get('batch_size', 10000)
    estimated_rows: int                    # From table_config.get('estimated_rows', 0)
    estimated_size_mb: float               # From table_config.get('estimated_size_mb', 0.0)


@dataclass
class LoadResult:
    """
    Container for load execution results.
    
    Returned by strategies and used by _finalize_table_load().
    """
    success: bool
    rows_loaded: int
    strategy_used: str
    duration: float
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


# ============================================================================
# LOAD STRATEGY - ABSTRACT BASE CLASS
# ============================================================================

class LoadStrategy(ABC):
    """
    Abstract base class for load execution strategies.
    
    Each strategy implements ONLY the loading logic - no query building,
    no schema validation, no tracking updates. Just data movement.
    
    RESPONSIBILITIES:
    - Execute the provided query against source database
    - Stream/batch/chunk the results appropriately
    - Insert data into target database
    - Return LoadResult with rows_loaded count
    
    NOT RESPONSIBLE FOR:
    - Query building (done in LoadPreparation)
    - Schema validation (done in LoadPreparation)
    - Tracking updates (done in LoadFinalization)
    - HYBRID FIX logic (done in LoadPreparation)
    """
    
    def __init__(self, 
                 source_engine: Engine,
                 target_engine: Engine,
                 target_schema: str,
                 bulk_insert_method: Any = None):
        """
        Initialize strategy with database connections.
        
        Args:
            source_engine: SQLAlchemy engine for MySQL replication
            target_engine: SQLAlchemy engine for PostgreSQL analytics
            target_schema: Schema name in target database (e.g., 'raw')
            bulk_insert_method: Reference to bulk_insert_optimized method
        """
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.target_schema = target_schema
        self.bulk_insert = bulk_insert_method
    
    @abstractmethod
    def execute(self, load_prep: LoadPreparation) -> LoadResult:
        """
        Execute the loading strategy.
        
        Args:
            load_prep: LoadPreparation object with query and configuration
            
        Returns:
            LoadResult with success status and rows loaded
        """
        pass
    
    def _convert_row_to_dict(self, row: Any, column_names: List[str]) -> Dict[str, Any]:
        """
        Helper: Convert SQLAlchemy row to dictionary.
        
        This will be shared across all strategies.
        """
        # TODO: Implement row conversion logic
        # Handle special cases: datetime, Decimal, None, etc.
        pass


# ============================================================================
# CONCRETE STRATEGIES
# ============================================================================

class StandardLoadStrategy(LoadStrategy):
    """
    Standard loading for small tables (< 50MB).
    
    APPROACH:
    - Execute query and fetchall() results into memory
    - Process in batches of 10,000 rows
    - Use bulk_insert_optimized for insertion
    
    BEST FOR:
    - Small tables with < 100K rows
    - Tables that fit comfortably in memory
    - Quick one-shot loads
    
    PERFORMANCE:
    - Fastest for small datasets
    - Memory usage: O(n) where n = total rows
    - Single transaction for entire load
    """
    
    def execute(self, load_prep: LoadPreparation) -> LoadResult:
        """
        Execute standard batch loading strategy.
        
        IMPLEMENTATION NOTES:
        - Use result.fetchall() to get all rows at once
        - Batch insert in chunks of 10,000
        - Single transaction for all inserts
        - Handle truncation if load_prep.should_truncate
        """
        start_time = time.time()
        rows_loaded = 0
        
        try:
            logger.info(f"[StandardStrategy] Loading {load_prep.table_name} with batch inserts")
            
            # TODO: Implement standard loading logic
            # 1. Execute query: result = source_conn.execute(text(load_prep.query))
            # 2. Fetch all: rows = result.fetchall()
            # 3. Convert rows to dicts
            # 4. Batch insert in chunks of 10,000
            # 5. Commit transaction
            
            return LoadResult(
                success=True,
                rows_loaded=rows_loaded,
                strategy_used="standard",
                duration=time.time() - start_time
            )
            
        except Exception as e:
            logger.error(f"[StandardStrategy] Error loading {load_prep.table_name}: {str(e)}")
            return LoadResult(
                success=False,
                rows_loaded=rows_loaded,
                strategy_used="standard",
                duration=time.time() - start_time,
                error=str(e)
            )


class StreamingLoadStrategy(LoadStrategy):
    """
    Streaming loading for medium tables (50-200MB).
    
    APPROACH:
    - Use generator to stream rows from MySQL
    - Process in batches of 10,000 rows
    - Memory-efficient for larger datasets
    
    BEST FOR:
    - Medium tables 100K-1M rows
    - Tables too large for fetchall()
    - Memory-constrained environments
    
    PERFORMANCE:
    - Balanced memory usage and speed
    - Memory usage: O(batch_size) = constant
    - Multiple transactions (one per batch)
    """
    
    def execute(self, load_prep: LoadPreparation) -> LoadResult:
        """
        Execute streaming loading strategy.
        
        IMPLEMENTATION NOTES:
        - Use result iterator (don't fetchall)
        - Accumulate rows in batches
        - Flush batch when size reaches 10,000
        - Multiple commits (one per batch)
        """
        start_time = time.time()
        rows_loaded = 0
        
        try:
            logger.info(f"[StreamingStrategy] Loading {load_prep.table_name} with streaming")
            
            # TODO: Implement streaming loading logic
            # 1. Execute query: result = source_conn.execute(text(load_prep.query))
            # 2. Iterate: for row in result: (generator, memory efficient)
            # 3. Accumulate batch
            # 4. Insert batch when size reaches 10,000
            # 5. Continue until all rows processed
            
            return LoadResult(
                success=True,
                rows_loaded=rows_loaded,
                strategy_used="streaming",
                duration=time.time() - start_time
            )
            
        except Exception as e:
            logger.error(f"[StreamingStrategy] Error loading {load_prep.table_name}: {str(e)}")
            return LoadResult(
                success=False,
                rows_loaded=rows_loaded,
                strategy_used="streaming",
                duration=time.time() - start_time,
                error=str(e)
            )


class ChunkedLoadStrategy(LoadStrategy):
    """
    Chunked loading for large tables (200-500MB).
    
    APPROACH:
    - Use LIMIT/OFFSET pagination
    - Process in chunks of 25,000 rows
    - Progress tracking per chunk
    
    BEST FOR:
    - Large tables 1M-10M rows
    - Long-running loads that need progress tracking
    - Resumable loads (can restart from last chunk)
    
    PERFORMANCE:
    - Memory usage: O(chunk_size) = constant
    - Multiple queries (one per chunk)
    - Overhead from pagination queries
    """
    
    def execute(self, load_prep: LoadPreparation) -> LoadResult:
        """
        Execute chunked loading strategy with pagination.
        
        IMPLEMENTATION NOTES:
        - Use LIMIT/OFFSET to paginate results
        - Process chunks of 25,000 rows (configurable)
        - Log progress after each chunk
        - Can be interrupted and resumed
        """
        start_time = time.time()
        rows_loaded = 0
        
        try:
            logger.info(f"[ChunkedStrategy] Loading {load_prep.table_name} with pagination")
            
            # TODO: Implement chunked loading logic
            # 1. Calculate total chunks based on estimated_rows
            # 2. Loop: for offset in range(0, total_rows, chunk_size):
            # 3. Execute query with LIMIT chunk_size OFFSET offset
            # 4. Convert and insert chunk
            # 5. Log progress: "Chunk X of Y (Z% complete)"
            
            return LoadResult(
                success=True,
                rows_loaded=rows_loaded,
                strategy_used="chunked",
                duration=time.time() - start_time
            )
            
        except Exception as e:
            logger.error(f"[ChunkedStrategy] Error loading {load_prep.table_name}: {str(e)}")
            return LoadResult(
                success=False,
                rows_loaded=rows_loaded,
                strategy_used="chunked",
                duration=time.time() - start_time,
                error=str(e)
            )


class CopyCSVLoadStrategy(LoadStrategy):
    """
    CSV COPY loading for very large tables (> 500MB).
    
    APPROACH:
    - Export MySQL results to temporary CSV file
    - Use PostgreSQL COPY FROM to bulk load
    - Fastest for very large datasets
    
    BEST FOR:
    - Very large tables > 10M rows
    - Full refresh operations
    - Maximum load speed required
    
    PERFORMANCE:
    - Fastest loading method available
    - Memory usage: O(1) - streams through temp file
    - Requires disk space for temp CSV
    - Single transaction via COPY
    """
    
    def execute(self, load_prep: LoadPreparation) -> LoadResult:
        """
        Execute CSV export + COPY loading strategy.
        
        IMPLEMENTATION NOTES:
        - Create temp CSV file
        - Stream MySQL results to CSV
        - Use PostgreSQL COPY FROM CSV
        - Clean up temp file
        - Handle column mapping for MySQL → PostgreSQL
        """
        start_time = time.time()
        rows_loaded = 0
        
        try:
            logger.info(f"[CopyCSVStrategy] Loading {load_prep.table_name} via CSV COPY")
            
            # TODO: Implement CSV COPY loading logic
            # 1. Create temp file: tempfile.NamedTemporaryFile(mode='w', suffix='.csv')
            # 2. Execute query and stream to CSV
            # 3. Build COPY command: COPY table FROM file WITH (FORMAT CSV, HEADER)
            # 4. Execute COPY in PostgreSQL
            # 5. Clean up temp file
            
            return LoadResult(
                success=True,
                rows_loaded=rows_loaded,
                strategy_used="copy_csv",
                duration=time.time() - start_time
            )
            
        except Exception as e:
            logger.error(f"[CopyCSVStrategy] Error loading {load_prep.table_name}: {str(e)}")
            return LoadResult(
                success=False,
                rows_loaded=rows_loaded,
                strategy_used="copy_csv",
                duration=time.time() - start_time,
                error=str(e)
            )


class ParallelLoadStrategy(LoadStrategy):
    """
    Parallel loading for massive tables (> 1M rows).
    
    APPROACH:
    - Split data into N chunks based on primary key ranges
    - Process chunks in parallel using ThreadPoolExecutor
    - Aggregate results from all threads
    
    BEST FOR:
    - Massive tables > 50M rows
    - Systems with multiple CPU cores
    - When load time is critical
    
    PERFORMANCE:
    - Fastest for massive datasets (if CPU/network allows)
    - Memory usage: O(num_workers × chunk_size)
    - Complexity: Requires coordination between threads
    - Risk: Can overwhelm source/target databases
    """
    
    def execute(self, load_prep: LoadPreparation) -> LoadResult:
        """
        Execute parallel loading strategy with thread pool.
        
        IMPLEMENTATION NOTES:
        - Calculate primary key ranges for chunks
        - Use ThreadPoolExecutor with 4-8 workers
        - Each worker processes one chunk
        - Aggregate rows_loaded from all workers
        - Handle partial failures (some chunks succeed, some fail)
        """
        start_time = time.time()
        rows_loaded = 0
        
        try:
            logger.info(f"[ParallelStrategy] Loading {load_prep.table_name} with parallel workers")
            
            # TODO: Implement parallel loading logic
            # 1. Determine primary key ranges
            # 2. Split into N chunks (N = num_workers)
            # 3. ThreadPoolExecutor: executor.submit(process_chunk, chunk_range)
            # 4. Wait for all futures: as_completed(futures)
            # 5. Aggregate results
            
            return LoadResult(
                success=True,
                rows_loaded=rows_loaded,
                strategy_used="parallel",
                duration=time.time() - start_time
            )
            
        except Exception as e:
            logger.error(f"[ParallelStrategy] Error loading {load_prep.table_name}: {str(e)}")
            return LoadResult(
                success=False,
                rows_loaded=rows_loaded,
                strategy_used="parallel",
                duration=time.time() - start_time,
                error=str(e)
            )


# ============================================================================
# MAIN LOADING COMPONENT CLASS
# ============================================================================

class PostgresLoaderRefactored:
    """
    Refactored PostgreSQL loader using Strategy Pattern.
    
    ROLE: Loading component called by TableProcessor (NOT the pipeline orchestrator!)
    
    RESPONSIBILITIES:
    1. Coordinate internal loading strategies (not the overall ETL pipeline)
    2. Select appropriate strategy based on table size
    3. Coordinate pre-processing, execution, and post-processing for the load phase
    4. Maintain backward compatibility with existing PostgresLoader interface
    
    NOT RESPONSIBLE FOR:
    - Pipeline orchestration (that's PipelineOrchestrator's job)
    - Extract phase (that's SimpleMySQLReplicator's job)
    - Actual data movement (delegated to internal strategies)
    
    POSTGRES_SCHEMA INTEGRATION:
    - PostgresSchema (schema_adapter) is INJECTED into this component
    - Used for: schema extraction, table creation, data type conversion
    - Operations:
      * schema_adapter.get_table_schema_from_mysql() - Get MySQL schema
      * schema_adapter.ensure_table_exists() - Create/validate PostgreSQL table
      * schema_adapter.convert_row_data_types() - Transform data types
    - Schema operations happen INSIDE this loader, NOT in orchestrator
    - Orchestrator doesn't know about PostgresSchema - it's internal to the loader
    
    KEY METHODS:
    - load_table() - Main entry point (coordinates internal strategies)
    - _prepare_table_load() - Universal pre-processing with HYBRID FIX
    - _select_strategy() - Choose appropriate internal strategy
    - _finalize_table_load() - Universal post-processing
    """
    
    def __init__(self,
                 replication_engine: Engine,
                 analytics_engine: Engine,
                 settings: Any,
                 schema_adapter: Any,
                 table_configs: Optional[Dict] = None):
        """
        Initialize loader with database connections and configuration.
        
        Args:
            replication_engine: SQLAlchemy engine for MySQL replication
            analytics_engine: SQLAlchemy engine for PostgreSQL analytics
            settings: Settings object with configuration
            schema_adapter: PostgresSchema adapter for schema operations
            table_configs: Optional pre-loaded table configurations (from tables.yml)
        """
        self.replication_engine = replication_engine
        self.analytics_engine = analytics_engine
        self.settings = settings
        self.schema_adapter = schema_adapter
        
        # Get target schema name
        self.analytics_schema = settings.get_schema_name(DatabaseType.ANALYTICS) or 'raw'
        
        # Initialize schema cache for performance optimization
        self.schema_cache = SchemaCache()
        logger.info("Initialized schema cache for performance optimization")
        
        # Load table configurations from tables.yml
        self.table_configs = table_configs or {}
        if not self.table_configs:
            logger.warning("No table configurations provided, will load from settings as needed")
        
        # Initialize strategy instances (reusable)
        self._init_strategies()
    
    def _init_strategies(self):
        """
        Initialize all strategy instances.
        
        Strategies are stateless and reusable, so we create them once.
        """
        self.strategies = {
            LoadStrategyType.STANDARD: StandardLoadStrategy(
                self.replication_engine,
                self.analytics_engine,
                self.analytics_schema,
                self.bulk_insert_optimized  # Pass reference to bulk insert method
            ),
            LoadStrategyType.STREAMING: StreamingLoadStrategy(
                self.replication_engine,
                self.analytics_engine,
                self.analytics_schema,
                self.bulk_insert_optimized
            ),
            LoadStrategyType.CHUNKED: ChunkedLoadStrategy(
                self.replication_engine,
                self.analytics_engine,
                self.analytics_schema,
                self.bulk_insert_optimized
            ),
            LoadStrategyType.COPY_CSV: CopyCSVLoadStrategy(
                self.replication_engine,
                self.analytics_engine,
                self.analytics_schema,
                self.bulk_insert_optimized
            ),
            LoadStrategyType.PARALLEL: ParallelLoadStrategy(
                self.replication_engine,
                self.analytics_engine,
                self.analytics_schema,
                self.bulk_insert_optimized
            ),
        }
    
    # ========================================================================
    # MAIN LOADING METHOD (Called by TableProcessor)
    # ========================================================================
    
    def load_table(self, table_name: str, force_full: bool = False) -> Tuple[bool, Dict[str, Any]]:
        """
        Main entry point for loading a table from replication to analytics.
        
        Called by: TableProcessor._load_to_analytics()
        
        This is the ONLY public method needed. It coordinates the internal
        loading process using the Strategy Pattern.
        
        PROCESS:
        1. Pre-processing: Configuration, schema, query building with HYBRID FIX
        2. Strategy Selection: Choose appropriate strategy based on table size
        3. Execution: Delegate to strategy for actual data movement
        4. Post-processing: Tracking updates, validation, metrics
        
        Args:
            table_name: Name of table to load
            force_full: If True, force full refresh (truncate and reload all)
            
        Returns:
            Tuple of (success: bool, metadata: Dict)
        """
        start_time = time.time()
        
        try:
            logger.info(f"Starting load for table: {table_name} (force_full={force_full})")
            
            # ================================================================
            # STEP 1: UNIVERSAL PRE-PROCESSING
            # ================================================================
            # This includes:
            # - Getting table configuration
            # - Validating schema exists
            # - Building query with HYBRID FIX fallback
            # - Estimating table size
            # - Determining batch size
            
            load_prep = self._prepare_table_load(table_name, force_full)
            
            if not load_prep:
                return False, {
                    'error': 'Failed to prepare table load',
                    'duration': time.time() - start_time
                }
            
            # ================================================================
            # STEP 2: STRATEGY SELECTION
            # ================================================================
            # Choose appropriate strategy based on table size
            
            strategy_type = self._select_strategy(load_prep)
            strategy = self.strategies[strategy_type]
            
            logger.info(f"Selected strategy: {strategy_type.value} for {table_name} "
                       f"({load_prep.estimated_size_mb:.2f}MB, {load_prep.estimated_rows:,} rows)")
            
            # ================================================================
            # STEP 3: EXECUTE STRATEGY
            # ================================================================
            # Delegate actual loading to the selected strategy
            
            load_result = strategy.execute(load_prep)
            
            # ================================================================
            # STEP 4: UNIVERSAL POST-PROCESSING
            # ================================================================
            # This includes:
            # - Updating tracking tables
            # - Calculating performance metrics
            # - Running validation checks
            # - Creating result metadata
            
            final_result = self._finalize_table_load(table_name, load_result, load_prep, start_time)
            
            return final_result
            
        except Exception as e:
            logger.error(f"Error in load_table for {table_name}: {str(e)}")
            return False, {
                'error': str(e),
                'duration': time.time() - start_time,
                'table_name': table_name
            }
    
    # ========================================================================
    # PREPARATION - UNIVERSAL PRE-PROCESSING
    # ========================================================================
    
    def _prepare_table_load(self, table_name: str, force_full: bool) -> Optional[LoadPreparation]:
        """
        Universal pre-processing for all load methods.
        
        This is where the HYBRID FIX is implemented ONCE for ALL strategies.
        
        RESPONSIBILITIES:
        1. Get table configuration from settings (via self.settings.get_table_config())
        2. Validate MySQL schema exists
        3. Ensure PostgreSQL table exists
        4. Build incremental query
        5. ✅ HYBRID FIX: Check if incremental returns 0 rows but data is missing
        6. Estimate table size and determine batch size
        
        VARIABLE SOURCES:
        - table_config: from tables.yml via self.settings.get_table_config(table_name)
        - primary_column: from table_config.get('primary_incremental_column')
        - incremental_columns: from table_config.get('incremental_columns', [])
        - incremental_strategy: from table_config.get('incremental_strategy', 'or_logic')
        - batch_size: from table_config.get('batch_size', 10000)
        - estimated_rows: from table_config.get('estimated_rows', 0)
        - estimated_size_mb: from table_config.get('estimated_size_mb', 0)
        
        NOTE: These values come from tables.yml, NOT passed from simplemysqlreplicator.py
        
        Returns:
            LoadPreparation object with all necessary configuration,
            or None if preparation failed
        """
        try:
            logger.debug(f"[Preparation] Starting for {table_name}")
            
            # ============================================================
            # 1. GET TABLE CONFIGURATION
            # ============================================================
            # TODO: Implement
            # table_config = self.settings.get_table_config(table_name)
            # if not table_config:
            #     raise ConfigurationError(f"No configuration for {table_name}")
            
            # ============================================================
            # 2. VALIDATE SCHEMA (Uses PostgresSchema - schema_adapter)
            # ============================================================
            # Get MySQL schema with caching to avoid repeated expensive queries
            mysql_schema = self._get_cached_schema(table_name)
            if mysql_schema is None:
                logger.error(f"Failed to get schema for table {table_name}")
                return None
            
            # Create or verify PostgreSQL table using schema_adapter
            # This is where PostgresSchema integration happens
            if self.schema_adapter is not None:
                if not self.schema_adapter.ensure_table_exists(table_name, mysql_schema):
                    logger.error(f"Failed to ensure table exists for {table_name}")
                    return None
            else:
                logger.info(f"Skipping table creation in test environment for {table_name}")
            
            # ============================================================
            # 3. BUILD INCREMENTAL QUERY
            # ============================================================
            # TODO: Implement
            # primary_column = self._get_primary_incremental_column(table_config)
            # incremental_columns = table_config.get('incremental_columns', [])
            # query = self._build_load_query(table_name, incremental_columns, force_full)
            
            # ============================================================
            # 4. ✅ UNIVERSAL HYBRID FIX
            # ============================================================
            # This is THE KEY IMPROVEMENT - implemented ONCE, applies to ALL
            
            should_force_full = force_full
            
            if not force_full:
                # Check if incremental query will return 0 rows
                # TODO: Implement count check
                # row_count = self._execute_count_query(query)
                
                # if row_count == 0:
                #     # Verify if analytics actually needs updating
                #     needs_updating, _, _, _ = self._check_analytics_needs_updating(table_name)
                #     
                #     if needs_updating:
                #         logger.warning(
                #             f"[HYBRID FIX] Incremental query returns 0 rows for {table_name}, "
                #             f"but analytics needs updating. Forcing full load."
                #         )
                #         # Rebuild query without incremental WHERE clause
                #         query = self._build_full_load_query(table_name)
                #         should_force_full = True
                pass
            
            # ============================================================
            # 5. ESTIMATE TABLE SIZE
            # ============================================================
            # TODO: Implement
            # estimated_rows = self._estimate_row_count(table_name)
            # estimated_size_mb = self._estimate_table_size_mb(table_name)
            
            # ============================================================
            # 6. DETERMINE BATCH SIZE
            # ============================================================
            # TODO: Implement based on table size and available memory
            # batch_size = self._calculate_optimal_batch_size(estimated_size_mb)
            
            # ============================================================
            # 7. CREATE PREPARATION OBJECT
            # ============================================================
            
            # TODO: Return actual LoadPreparation object
            # return LoadPreparation(
            #     table_name=table_name,
            #     table_config=table_config,
            #     mysql_schema=mysql_schema,
            #     query=query,
            #     primary_column=primary_column,  # From table_config.get('primary_incremental_column')
            #     incremental_columns=incremental_columns,  # From table_config.get('incremental_columns', [])
            #     incremental_strategy=incremental_strategy,  # From table_config.get('incremental_strategy', 'or_logic')
            #     should_truncate=should_force_full,
            #     force_full=should_force_full,
            #     batch_size=batch_size,  # From table_config.get('batch_size', 10000)
            #     estimated_rows=estimated_rows,  # From table_config.get('estimated_rows', 0)
            #     estimated_size_mb=estimated_size_mb  # From table_config.get('estimated_size_mb', 0.0)
            # )
            
            return None  # Placeholder until implemented
            
        except Exception as e:
            logger.error(f"[Preparation] Error preparing {table_name}: {str(e)}")
            return None
    
    # ========================================================================
    # STRATEGY SELECTION
    # ========================================================================
    
    def _select_strategy(self, load_prep: LoadPreparation) -> LoadStrategyType:
        """
        Select appropriate loading strategy based on table characteristics.
        
        SELECTION CRITERIA:
        - < 50MB: Standard (batch loading)
        - 50-200MB: Streaming (generator-based)
        - 200-500MB: Chunked (paginated)
        - > 500MB: CopyCSV (bulk COPY)
        - > 1M rows: Parallel (if enabled)
        
        Args:
            load_prep: LoadPreparation with table size estimates
            
        Returns:
            LoadStrategyType enum value
        """
        size_mb = load_prep.estimated_size_mb
        rows = load_prep.estimated_rows
        
        # Priority: Parallel for massive tables (if enabled)
        if rows > 1_000_000:
            # TODO: Check if parallel is enabled in config
            # if self.settings.get('enable_parallel_loading', False):
            #     return LoadStrategyType.PARALLEL
            pass
        
        # Size-based selection
        if size_mb > 500:
            return LoadStrategyType.COPY_CSV
        elif size_mb > 200:
            return LoadStrategyType.CHUNKED
        elif size_mb > 50:
            return LoadStrategyType.STREAMING
        else:
            return LoadStrategyType.STANDARD
    
    # ========================================================================
    # FINALIZATION - UNIVERSAL POST-PROCESSING
    # ========================================================================
    
    def _finalize_table_load(self,
                            table_name: str,
                            load_result: LoadResult,
                            load_prep: LoadPreparation,
                            start_time: float) -> Tuple[bool, Dict[str, Any]]:
        """
        Universal post-processing for all load methods.
        
        RESPONSIBILITIES:
        1. Update tracking tables (etl_load_status)
        2. Calculate performance metrics (rows/sec, MB/sec)
        3. Run validation checks (row count comparison)
        4. Create standardized result metadata
        5. Log final summary
        
        Args:
            table_name: Name of loaded table
            load_result: Result from strategy execution
            load_prep: Preparation object with configuration
            start_time: Timestamp when load started
            
        Returns:
            Tuple of (success: bool, metadata: Dict)
        """
        try:
            logger.debug(f"[Finalization] Starting for {table_name}")
            
            # ============================================================
            # 1. UPDATE TRACKING TABLES
            # ============================================================
            # TODO: Implement
            # if load_prep.primary_column:
            #     self._update_load_status_hybrid(...)
            # else:
            #     self._update_load_status(...)
            
            # ============================================================
            # 2. CALCULATE PERFORMANCE METRICS
            # ============================================================
            duration = time.time() - start_time
            rows_per_sec = load_result.rows_loaded / duration if duration > 0 else 0
            mb_per_sec = load_prep.estimated_size_mb / duration if duration > 0 else 0
            
            # ============================================================
            # 3. RUN VALIDATION CHECKS
            # ============================================================
            # TODO: Implement post-load validation
            # - Compare row counts: replication vs analytics
            # - Check for data quality issues
            # - Verify primary key integrity
            
            # ============================================================
            # 4. CREATE RESULT METADATA
            # ============================================================
            metadata = {
                'rows_loaded': load_result.rows_loaded,
                'strategy_used': load_result.strategy_used,
                'duration': duration,
                'rows_per_second': rows_per_sec,
                'mb_per_second': mb_per_sec,
                'force_full_applied': load_prep.force_full,
                'primary_column': load_prep.primary_column,
                'estimated_rows': load_prep.estimated_rows,
                'estimated_size_mb': load_prep.estimated_size_mb
            }
            
            if load_result.error:
                metadata['error'] = load_result.error
            
            # ============================================================
            # 5. LOG FINAL SUMMARY
            # ============================================================
            if load_result.success:
                logger.info(
                    f"✅ Successfully loaded {table_name}: "
                    f"{load_result.rows_loaded:,} rows in {duration:.2f}s "
                    f"({rows_per_sec:.0f} rows/sec) using {load_result.strategy_used} strategy"
                )
            else:
                logger.error(
                    f"❌ Failed to load {table_name}: {load_result.error}"
                )
            
            return load_result.success, metadata
            
        except Exception as e:
            logger.error(f"[Finalization] Error finalizing {table_name}: {str(e)}")
            return False, {
                'error': f"Finalization error: {str(e)}",
                'rows_loaded': load_result.rows_loaded,
                'duration': time.time() - start_time
            }
    
    # ========================================================================
    # HELPER METHODS
    # ========================================================================
    # These methods are migrated from original postgres_loader.py
    
    def get_table_config(self, table_name: str) -> Dict:
        """
        Get table configuration from loaded configs or settings.
        
        Migrated from original postgres_loader.py (line 403)
        """
        # First try local table_configs
        config = self.table_configs.get(table_name, {})
        
        # Fallback to settings if not in local configs
        if not config and self.settings:
            config = self.settings.get_table_config(table_name)
        
        if not config:
            logger.warning(f"No configuration found for table {table_name}")
            return {}
        
        return config
    
    def bulk_insert_optimized(self, table_name: str, rows_data: List[Dict], 
                             chunk_size: int = 25000, use_upsert: bool = True) -> bool:
        """
        Optimized bulk INSERT using executemany with strategy-based chunk sizes.
        
        PHASE 1 IMPROVEMENT: Uses optimized batch sizes based on insert strategy:
        - simple_insert: 50,000 rows (no conflicts, larger batches OK)
        - optimized_upsert: 5,000 rows (reduce conflict overhead)
        - copy_csv: 100,000 rows (COPY can handle large batches)
        
        Migrated from original postgres_loader.py (line 583)
        """
        try:
            if not rows_data:
                logger.info(f"No data to insert for {table_name}")
                return True
            
            # Determine optimal batch size based on strategy
            if use_upsert:
                # For UPSERT operations, use smaller batches to reduce conflict overhead
                optimal_batch_size = min(chunk_size, 5000)  # Cap at 5K for UPSERT
                logger.debug(f"Using UPSERT-optimized batch size: {optimal_batch_size}")
            else:
                # For simple INSERT, can use larger batches
                optimal_batch_size = min(chunk_size, 50000)  # Cap at 50K for INSERT
                logger.debug(f"Using INSERT-optimized batch size: {optimal_batch_size}")
            
            # Process in optimized chunks
            total_inserted = 0
            for i in range(0, len(rows_data), optimal_batch_size):
                chunk = rows_data[i:i + optimal_batch_size]
                
                # Build optimized INSERT statement
                columns = ', '.join([f'"{col}"' for col in chunk[0].keys()])
                placeholders = ', '.join([f':{col}' for col in chunk[0].keys()])
                
                if use_upsert:
                    # Use UPSERT to handle duplicate keys
                    insert_sql = self._build_upsert_sql(table_name, list(chunk[0].keys()))
                    logger.debug(f"Using UPSERT for {table_name} chunk {i//optimal_batch_size + 1} with {len(chunk)} rows")
                else:
                    # Use simple INSERT (for full loads where table is truncated)
                    insert_sql = f"""
                        INSERT INTO {self.target_schema}.{table_name} ({columns})
                        VALUES ({placeholders})
                    """
                    logger.debug(f"Using simple INSERT for {table_name} chunk {i//optimal_batch_size + 1} with {len(chunk)} rows")
                
                # Use executemany for bulk operation
                try:
                    with self.target_engine.begin() as conn:
                        conn.execute(text(insert_sql), chunk)
                except Exception as e:
                    if "duplicate key" in str(e).lower() or "unique_violation" in str(e).lower():
                        logger.error(f"Duplicate key violation in {table_name} chunk {i//optimal_batch_size + 1}: {str(e)}")
                        logger.error(f"First few rows in chunk: {chunk[:3] if chunk else 'No data'}")
                        raise
                    else:
                        raise
                
                total_inserted += len(chunk)
                logger.debug(f"Bulk inserted {len(chunk)} rows for {table_name} (total: {total_inserted})")
            
            logger.info(f"Successfully bulk inserted {total_inserted} rows for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error in bulk insert for {table_name}: {str(e)}")
            return False
    
    def _build_upsert_sql(self, table_name: str, columns: List[str]) -> str:
        """
        Build UPSERT SQL for PostgreSQL with ON CONFLICT DO UPDATE.
        
        TODO: Implement based on primary key from table config
        """
        # Placeholder - needs primary key determination
        columns_list = ', '.join([f'"{col}"' for col in columns])
        placeholders = ', '.join([f':{col}' for col in columns])
        
        # Simple version - assumes first column is unique
        update_set = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in columns[1:]])
        
        return f"""
            INSERT INTO {self.target_schema}.{table_name} ({columns_list})
            VALUES ({placeholders})
            ON CONFLICT ("{columns[0]}") DO UPDATE SET
                {update_set}
        """
    
    def _get_cached_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get table schema from cache or MySQL.
        
        Migrated from original postgres_loader.py (line 438)
        """
        # Check cache first
        cached_schema = self.schema_cache.get_cached_schema(table_name)
        if cached_schema is not None:
            logger.debug(f"Using cached schema for table {table_name}")
            return cached_schema
        
        # If not in cache, get from schema adapter and cache it
        try:
            logger.debug(f"Fetching schema for table {table_name} from MySQL")
            mysql_schema = self.schema_adapter.get_table_schema_from_mysql(table_name)
            
            # Cache the schema for future use
            self.schema_cache.cache_schema(table_name, mysql_schema)
            logger.debug(f"Cached schema for table {table_name}")
            
            return mysql_schema
            
        except Exception as e:
            logger.error(f"Error getting schema for table {table_name}: {str(e)}")
            return None
    
    def _get_primary_incremental_column(self, table_config: Dict) -> Optional[str]:
        """
        Extract primary_incremental_column from table configuration.
        
        This is a helper that retrieves the 'primary_incremental_column' field
        from tables.yml configuration and validates it.
        
        Example from tables.yml:
            medication:
              primary_incremental_column: MedicationNum
              incremental_columns:
                - MedicationNum
                - DateTStamp
              incremental_strategy: or_logic
        
        Returns:
            Column name (e.g., 'MedicationNum') or None if not configured
        
        TODO: Migrate from original postgres_loader.py (line 3163)
        """
        primary_column = table_config.get('primary_incremental_column')
        
        # Check if primary column is valid (not null, 'none', or empty)
        if primary_column and primary_column != 'none' and primary_column.strip():
            return primary_column
        
        # Fallback: if no primary column specified, return None to use multi-column logic
        return None
    
    def _build_load_query(self, table_name: str, incremental_columns: List[str], 
                         force_full: bool) -> str:
        """
        Build incremental or full load query.
        
        Delegates to _build_enhanced_load_query for actual implementation.
        Migrated from original postgres_loader.py (line 2401)
        """
        # Get table configuration for primary column and strategy
        table_config = self.get_table_config(table_name)
        primary_column = self._get_primary_incremental_column(table_config) if table_config else None
        incremental_strategy = table_config.get('incremental_strategy', 'or_logic') if table_config else 'or_logic'
        
        return self._build_enhanced_load_query(
            table_name, 
            incremental_columns, 
            primary_column, 
            force_full, 
            incremental_strategy
        )
    
    def _build_full_load_query(self, table_name: str) -> str:
        """
        Build full load query (no WHERE clause).
        
        Migrated from original postgres_loader.py
        """
        # Get replication database name from settings
        replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
        replication_db = replication_config.get('database', 'opendental_replication')
        
        return f"SELECT * FROM `{replication_db}`.`{table_name}`"
    
    def _build_enhanced_load_query(self, table_name: str, incremental_columns: List[str], 
                                  primary_column: Optional[str] = None,
                                  force_full: bool = False, incremental_strategy: str = 'or_logic') -> str:
        """
        Build enhanced load query with improved incremental logic.
        
        This is the CORE query building method that handles:
        - Full vs incremental loading
        - Multiple incremental column strategies (or_logic, and_logic, single_column)
        - Integer primary key handling
        - Timestamp column handling
        
        Migrated from original postgres_loader.py (line 2266)
        """
        # Get replication database name from settings
        replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
        replication_db = replication_config.get('database', 'opendental_replication')

        # ENHANCED: Respect the incremental_strategy configuration
        # Don't override incremental_columns - use what was passed in
        table_config = self.get_table_config(table_name)
        primary_incremental_column = table_config.get('primary_incremental_column') if table_config else None
        
        # Validate incremental columns
        if not incremental_columns:
            logger.warning(f"No incremental columns provided for {table_name}, falling back to primary column")
            if primary_incremental_column:
                incremental_columns = [primary_incremental_column]
            else:
                logger.error(f"No incremental columns available for {table_name}")
                return f"SELECT * FROM `{replication_db}`.`{table_name}`"

        if force_full:
            return f"SELECT * FROM `{replication_db}`.`{table_name}`"

        # Get last analytics load time to determine what's new
        last_analytics_load = self._get_loaded_at_time(table_name)
        
        # FIXED: Handle integer primary keys correctly
        # Get the last primary value for integer primary keys
        last_primary_value = self._get_last_primary_value(table_name)
        
        # If no previous load record exists, check if the table has data in analytics
        if not last_analytics_load and not last_primary_value:
            # Check if the table has any data in the analytics database
            try:
                with self.analytics_engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {self.analytics_schema}.{table_name}")).scalar()
                    if result and result > 0:
                        # Table has data, get the maximum primary key value
                        if primary_incremental_column:
                            max_result = conn.execute(text(f"""
                                SELECT MAX("{primary_incremental_column}") 
                                FROM {self.analytics_schema}.{table_name}
                                WHERE "{primary_incremental_column}" IS NOT NULL
                            """)).scalar()
                            if max_result:
                                last_primary_value = str(max_result)
                                logger.info(f"No previous load record found for {table_name}, but table has {result} rows. Using max {primary_incremental_column}: {last_primary_value}")
                            else:
                                logger.info(f"No previous load record found for {table_name}, but table has {result} rows. No valid {primary_incremental_column} values found.")
                        else:
                            logger.info(f"No previous load record found for {table_name}, but table has {result} rows. No primary incremental column configured.")
                    else:
                        # Table is empty, perform full load
                        logger.info(f"No previous analytics load found for {table_name} and table is empty, performing full load")
                        return f"SELECT * FROM `{replication_db}`.`{table_name}`"
            except Exception as e:
                logger.warning(f"Error checking analytics table data for {table_name}: {str(e)}")
                # Fall back to full load if we can't check
                logger.info(f"No previous analytics load found for {table_name}, performing full load")
                return f"SELECT * FROM `{replication_db}`.`{table_name}`"
        
        # If still no previous load info, perform full load
        if not last_analytics_load and not last_primary_value:
            logger.info(f"No previous analytics load found for {table_name}, performing full load")
            return f"SELECT * FROM `{replication_db}`.`{table_name}`"
        
        # ENHANCED: Implement proper multi-column incremental logic based on strategy
        if incremental_strategy == 'or_logic':
            # Use OR logic for multiple columns
            conditions = []
            for col in incremental_columns:
                if col == primary_incremental_column and last_primary_value and self._is_integer_column(table_name, col):
                    # Handle integer primary key columns
                    conditions.append(f"`{col}` > {last_primary_value}")
                elif last_analytics_load:
                    # Handle timestamp columns only if we have a valid analytics load time
                    conditions.append(f"(`{col}` > '{last_analytics_load}' AND `{col}` != '0001-01-01 00:00:00')")
                # If no valid analytics load time, skip timestamp conditions
            where_clause = " OR ".join(conditions) if conditions else "1=1"  # Default to all records if no conditions
        elif incremental_strategy == 'and_logic':
            # Use AND logic for multiple columns
            conditions = []
            for col in incremental_columns:
                if col == primary_incremental_column and last_primary_value and self._is_integer_column(table_name, col):
                    # Handle integer primary key columns
                    conditions.append(f"`{col}` > {last_primary_value}")
                elif last_analytics_load:
                    # Handle timestamp columns only if we have a valid analytics load time
                    conditions.append(f"(`{col}` > '{last_analytics_load}' AND `{col}` != '0001-01-01 00:00:00')")
                # If no valid analytics load time, skip timestamp conditions
            where_clause = " AND ".join(conditions) if conditions else "1=1"  # Default to all records if no conditions
        elif incremental_strategy == 'single_column':
            # Use only the primary incremental column
            if primary_incremental_column and primary_incremental_column in incremental_columns:
                if last_primary_value and self._is_integer_column(table_name, primary_incremental_column):
                    # Handle integer primary key columns
                    where_clause = f"`{primary_incremental_column}` > {last_primary_value}"
                elif last_analytics_load:
                    # Handle timestamp columns only if we have a valid analytics load time
                    where_clause = f"`{primary_incremental_column}` > '{last_analytics_load}' AND `{primary_incremental_column}` != '0001-01-01 00:00:00'"
                else:
                    # No valid conditions, use full load
                    where_clause = "1=1"
            else:
                # Fallback to first column
                if last_primary_value and self._is_integer_column(table_name, incremental_columns[0]):
                    where_clause = f"`{incremental_columns[0]}` > {last_primary_value}"
                elif last_analytics_load:
                    where_clause = f"`{incremental_columns[0]}` > '{last_analytics_load}' AND `{incremental_columns[0]}` != '0001-01-01 00:00:00'"
                else:
                    # No valid conditions, use full load
                    where_clause = "1=1"
        else:
            # Default to OR logic
            conditions = []
            for col in incremental_columns:
                if col == primary_incremental_column and last_primary_value and self._is_integer_column(table_name, col):
                    # Handle integer primary key columns
                    conditions.append(f"`{col}` > {last_primary_value}")
                elif last_analytics_load:
                    # Handle timestamp columns only if we have a valid analytics load time
                    conditions.append(f"(`{col}` > '{last_analytics_load}' AND `{col}` != '0001-01-01 00:00:00')")
                # If no valid analytics load time, skip timestamp conditions
            where_clause = " OR ".join(conditions) if conditions else "1=1"  # Default to all records if no conditions

        logger.info(f"Using {incremental_strategy} strategy for {table_name} with columns: {incremental_columns}")
        logger.info(f"Analytics load time: {last_analytics_load}")
        logger.info(f"Last primary value: {last_primary_value}")
        logger.info(f"Where clause: {where_clause}")
        
        return f"SELECT * FROM `{replication_db}`.`{table_name}` WHERE {where_clause}"
    
    def _get_loaded_at_time(self, table_name: str) -> Optional[Any]:
        """
        Get last load time from analytics database's etl_load_status table.
        
        Migrated from original postgres_loader.py (line 1355)
        """
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT _loaded_at
                    FROM {self.analytics_schema}.etl_load_status
                    WHERE table_name = :table_name
                    AND load_status = 'success'
                    ORDER BY _loaded_at DESC
                    LIMIT 1
                """), {"table_name": table_name}).scalar()
                
                return result
        except Exception as e:
            logger.error(f"Error getting last load time for {table_name}: {str(e)}")
            return None

    def _get_analytics_row_count(self, table_name: str) -> int:
        """
        Get the current row count of a table in the analytics database.
        
        Migrated from original postgres_loader.py (line 1373)
        """
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.analytics_schema}.{table_name}")).scalar()
                return result or 0
        except Exception as e:
            logger.error(f"Error getting row count for {table_name} in analytics: {str(e)}")
            return 0

    def _get_last_primary_value(self, table_name: str) -> Optional[str]:
        """
        Get the last primary column value for incremental loading.
        
        Migrated from original postgres_loader.py (line 3114)
        """
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT last_primary_value, primary_column_name
                    FROM {self.analytics_schema}.etl_load_status
                    WHERE table_name = :table_name
                    AND load_status = 'success'
                    ORDER BY _loaded_at DESC
                    LIMIT 1
                """), {"table_name": table_name}).fetchone()
                
                if result:
                    last_primary_value, primary_column_name = result
                    logger.debug(f"Retrieved last primary value for {table_name}: {last_primary_value} (column: {primary_column_name})")
                    
                    # NEW: If tracking has NULL but we know the primary column, compute from analytics data
                    if last_primary_value is None and primary_column_name:
                        try:
                            computed_max = conn.execute(text(f"""
                                SELECT MAX("{primary_column_name}")
                                FROM {self.analytics_schema}.{table_name}
                            """)).scalar()
                            if computed_max is not None:
                                last_primary_value = str(computed_max)
                                logger.info(f"Computed last_primary_value for {table_name} from analytics data: {last_primary_value}")
                            else:
                                logger.info(f"Analytics table {self.analytics_schema}.{table_name} is empty; leaving last_primary_value as NULL")
                        except Exception as compute_err:
                            logger.warning(f"Could not compute MAX({primary_column_name}) for {table_name}: {str(compute_err)}")
                    
                    return last_primary_value
                else:
                    return None
        except Exception as e:
            logger.error(f"Error getting last primary value for {table_name}: {str(e)}")
            return None

    def _is_integer_column(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column is an integer type based on table configuration.
        
        Migrated from original postgres_loader.py (line 3178)
        """
        try:
            table_config = self.get_table_config(table_name)
            if not table_config:
                return False
            
            # Check if this is the primary key and if it's likely an integer
            primary_key = table_config.get('primary_key')
            primary_incremental_column = table_config.get('primary_incremental_column')
            
            # Check if this column is either the primary key or the primary incremental column
            if column_name == primary_key or column_name == primary_incremental_column:
                # Common integer primary key patterns
                integer_primary_patterns = [
                    'Num', 'ID', 'Id', 'id'
                ]
                return any(pattern in column_name for pattern in integer_primary_patterns)
            
            # For non-primary keys, check if it's explicitly marked as integer
            # This is a simplified check - in a real implementation, you might want to
            # query the database schema to get the actual column type
            return False
            
        except Exception as e:
            logger.warning(f"Error checking if column {column_name} is integer for table {table_name}: {str(e)}")
            return False

    def _get_last_copy_time_from_replication(self, table_name: str) -> Optional[Any]:
        """
        Get last copy time from replication database's etl_copy_status table.
        
        Migrated from original postgres_loader.py (line 3466)
        """
        try:
            with self.replication_engine.connect() as conn:
                # Use the replication database name explicitly
                replication_db = self.replication_engine.url.database
                result = conn.execute(text(f"""
                    SELECT last_copied
                    FROM `{replication_db}`.etl_copy_status
                    WHERE table_name = :table_name
                    AND copy_status = 'success'
                    ORDER BY last_copied DESC
                    LIMIT 1
                """), {"table_name": table_name}).scalar()
                return result
        except Exception as e:
            logger.error(f"Error getting last copy time from replication for {table_name}: {str(e)}")
            return None

    def _check_analytics_needs_updating(self, table_name: str) -> Tuple[bool, Optional[str], Optional[str], bool]:
        """
        Check if analytics database needs updating from replication database.
        
        ENHANCED: Check for actual new data in replication since last analytics load,
        not just timestamp comparison.
        
        This is the CORE of the HYBRID FIX detection logic.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            Tuple[bool, Optional[str], Optional[str], bool]: 
                (needs_updating, replication_primary_value, analytics_primary_value, force_full_load_recommended)
        
        Migrated from original postgres_loader.py (lines 3508-3679)
        """
        try:
            # Get last copy time from replication database
            replication_copy_time = self._get_last_copy_time_from_replication(table_name)
            
            # Get last load time from analytics database
            analytics_load_time = self._get_loaded_at_time(table_name)
            
            # Get current row count in analytics table
            analytics_row_count = self._get_analytics_row_count(table_name)
            
            if not replication_copy_time:
                logger.info(f"No replication data found for {table_name}, analytics cannot be updated")
                return False, None, None, False
            
            # Check if analytics table is empty but has a non-initial load time
            force_full_load_recommended = False
            if analytics_row_count == 0 and analytics_load_time is not None:
                logger.info(f"Analytics table {table_name} is empty but has load timestamp {analytics_load_time}, recommending full load")
                force_full_load_recommended = True
            
            if analytics_load_time is None:
                # Analytics has never been loaded, check if replication has data
                logger.info(f"Analytics has never been loaded for {table_name}, checking if replication has data")
                
                # Check if replication has any data for this table
                try:
                    with self.replication_engine.connect() as conn:
                        # Use the replication database name explicitly
                        replication_db = self.replication_engine.url.database
                        result = conn.execute(text(f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`"))
                        replication_row_count = result.scalar()
                        
                    if replication_row_count > 0:
                        logger.info(f"Replication has {replication_row_count} rows for {table_name}, analytics needs full sync")
                        return True, str(replication_copy_time), None, True
                    else:
                        logger.info(f"Replication has no data for {table_name}, nothing to load")
                        return False, str(replication_copy_time), None, False
                except Exception as e:
                    logger.error(f"Error checking replication data for {table_name}: {str(e)}")
                    return False, str(replication_copy_time), None, False
            
            # ENHANCED: Check if there's new data in replication since last analytics load
            # Instead of just comparing timestamps, check if there are actually new records
            try:
                with self.replication_engine.connect() as conn:
                    # Use the replication database name explicitly
                    replication_db = self.replication_engine.url.database
                    
                    # Get table configuration for incremental columns
                    table_config = self.get_table_config(table_name)
                    incremental_columns = table_config.get('incremental_columns', []) if table_config else []
                    
                    # ENHANCED: Check for actual new records in replication since last analytics load
                    # Don't rely on replication copy time comparison - check actual data timestamps
                    if incremental_columns and analytics_load_time:
                        # Get incremental strategy from table configuration
                        incremental_strategy = table_config.get('incremental_strategy', 'or_logic')
                        
                        # FIXED: Check if there are new records in replication since last analytics load
                        # The issue is that records with timestamps older than analytics_load_time might still be new
                        # if they weren't in analytics before. We need to check if replication has more records than analytics.
                        
                        # First, check if replication has more records than analytics (this catches all new records regardless of timestamp)
                        replication_count_query = f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`"
                        replication_count = conn.execute(text(replication_count_query)).scalar()
                    
                        if replication_count > analytics_row_count:
                            logger.info(f"Replication has {replication_count} rows vs analytics {analytics_row_count} rows for {table_name}, analytics needs updating")
                            return True, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                        
                        # If row counts are the same, check for records with timestamps newer than analytics load time
                        # FIXED: Handle integer primary keys correctly
                        primary_incremental_column = table_config.get('primary_incremental_column')
                        last_primary_value = self._get_last_primary_value(table_name)
                        
                        conditions = []
                        for col in incremental_columns:
                            if col == primary_incremental_column and last_primary_value and self._is_integer_column(table_name, col):
                                # Handle integer primary key columns
                                conditions.append(f"`{col}` > {last_primary_value}")
                            else:
                                # Handle timestamp columns
                                conditions.append(f"({col} > '{analytics_load_time}' AND {col} != '0001-01-01 00:00:00')")
                        
                        # Use strategy-based logic
                        if incremental_strategy == 'or_logic':
                            where_clause = " OR ".join(conditions)
                        elif incremental_strategy == 'and_logic':
                            where_clause = " AND ".join(conditions)
                        elif incremental_strategy == 'single_column':
                            # Use only the primary incremental column
                            if primary_incremental_column and primary_incremental_column in incremental_columns:
                                if last_primary_value and self._is_integer_column(table_name, primary_incremental_column):
                                    where_clause = f"`{primary_incremental_column}` > {last_primary_value}"
                                else:
                                    where_clause = f"({primary_incremental_column} > '{analytics_load_time}' AND {primary_incremental_column} != '0001-01-01 00:00:00')"
                            else:
                                # Fallback to first column
                                if last_primary_value and self._is_integer_column(table_name, incremental_columns[0]):
                                    where_clause = f"`{incremental_columns[0]}` > {last_primary_value}"
                                else:
                                    where_clause = f"({incremental_columns[0]} > '{analytics_load_time}' AND {incremental_columns[0]} != '0001-01-01 00:00:00')"
                        else:
                            # Default to OR logic
                            where_clause = " OR ".join(conditions)
                        
                        new_records_query = f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}` WHERE {where_clause}"
                        
                        result = conn.execute(text(new_records_query))
                        new_records_count = result.scalar()
                        
                        if new_records_count > 0:
                            logger.info(f"Found {new_records_count} new records in replication for {table_name} since last analytics load")
                            return True, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                        else:
                            logger.info(f"No new records found in replication for {table_name} since last analytics load")
                            return False, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                    
                    # Fallback: Only use replication copy time comparison if no incremental columns available
                    if replication_copy_time and analytics_load_time and replication_copy_time > analytics_load_time:
                        logger.info(f"Replication copy time ({replication_copy_time}) is newer than analytics load time ({analytics_load_time}), but no incremental columns configured. Checking for any data differences.")
                        # Check if replication has more rows than analytics
                        replication_count_query = f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`"
                        
                        replication_count = conn.execute(text(replication_count_query)).scalar()
                        analytics_count = self._get_analytics_row_count(table_name)
                        
                        if replication_count > analytics_count:
                            logger.info(f"Replication has {replication_count} rows vs analytics {analytics_count} rows for {table_name}, analytics needs updating")
                            return True, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                        else:
                            logger.info(f"Replication and analytics have same row count for {table_name} ({replication_count}), no update needed")
                            return False, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                    
                    else:
                        # Fallback to timestamp comparison if no incremental columns or no analytics load time
                        if replication_copy_time > analytics_load_time or force_full_load_recommended:
                            logger.info(f"Analytics needs updating for {table_name}: "
                                       f"replication_copy_time={replication_copy_time}, analytics_load_time={analytics_load_time}, "
                                       f"analytics_row_count={analytics_row_count}, force_full_load_recommended={force_full_load_recommended}")
                            return True, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                        else:
                            logger.info(f"Analytics is up to date for {table_name}: "
                                       f"replication_copy_time={replication_copy_time}, analytics_load_time={analytics_load_time}")
                            return False, str(replication_copy_time), str(analytics_load_time), False
                            
            except Exception as e:
                logger.error(f"Error checking for new records in replication for {table_name}: {str(e)}")
                # Fallback to timestamp comparison
                if replication_copy_time > analytics_load_time or force_full_load_recommended:
                    return True, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                else:
                    return False, str(replication_copy_time), str(analytics_load_time), False
                
        except Exception as e:
            logger.error(f"Error checking if analytics needs updating for {table_name}: {str(e)}")
            return False, None, None, False
    
    def _execute_count_query(self, query: str) -> int:
        """
        Execute query and return row count.
        
        Helper for HYBRID FIX to check if incremental query returns 0 rows.
        
        TODO: Implement
        """
        pass
    
    def _update_load_status_hybrid(self, table_name: str, rows_loaded: int, **kwargs):
        """
        Update tracking with hybrid strategy (primary value tracking).
        
        TODO: Migrate from original postgres_loader.py
        """
        pass
    
    def _update_load_status(self, table_name: str, rows_loaded: int, status: str):
        """
        Update tracking with standard strategy (timestamp only).
        
        TODO: Migrate from original postgres_loader.py
        """
        pass


# ============================================================================
# VARIABLE SOURCE DOCUMENTATION
# ============================================================================
"""
UNDERSTANDING THE DATA FLOW:

WHERE DO THE VARIABLES COME FROM?

1. TABLES.YML CONFIGURATION (Primary Source)
   ├── Location: etl_pipeline/etl_pipeline/config/tables.yml
   ├── Accessed via: self.settings.get_table_config(table_name)
   └── Fields used:
       ├── primary_incremental_column: 'MedicationNum'  (stored as primary_column locally)
       ├── incremental_columns: ['MedicationNum', 'DateTStamp']
       ├── incremental_strategy: 'or_logic' | 'single_column' | 'and_logic'
       ├── batch_size: 10000
       ├── estimated_rows: 1090
       ├── estimated_size_mb: 0.11
       ├── extraction_strategy: 'incremental' | 'full_table'
       └── primary_key: 'MedicationNum'

2. PIPELINE ORCHESTRATION (Call Hierarchy)
   ├── PipelineOrchestrator (orchestration/pipeline_orchestrator.py)
   │   └── Main entry point, coordinates overall ETL flow
   ├── TableProcessor (orchestration/table_processor.py)
   │   ├── Handles individual table ETL processing
   │   ├── Calls: SimpleMySQLReplicator for extraction (source → replication)
   │   └── Calls: PostgresLoader for loading (replication → analytics)
   ├── SimpleMySQLReplicator (core/simple_mysql_replicator.py)
   │   └── Component: Extracts data from source MySQL to replication MySQL
   └── PostgresLoader (loaders/postgres_loader.py)
       └── Component: Loads data from replication MySQL to PostgreSQL analytics

3. POSTGRES_LOADER.PY (Current Implementation - A COMPONENT, NOT AN ORCHESTRATOR)
   ├── Called by: TableProcessor._load_to_analytics()
   ├── Signature: loader.load_table(table_name, force_full=False)
   ├── Gets config: table_config = self.get_table_config(table_name)
   ├── Extracts primary: primary_column = self._get_primary_incremental_column(table_config)
   ├── Extracts list: incremental_columns = table_config.get('incremental_columns', [])
   └── Builds query: query = self._build_load_query(table_name, incremental_columns, force_full)

NAMING CONVENTIONS:

Field Name in tables.yml          Local Variable Name        Purpose
------------------------          -------------------        -------
primary_incremental_column   →    primary_column            Main column for tracking (e.g., 'MedicationNum')
incremental_columns          →    incremental_columns       List of columns for WHERE clause
incremental_strategy         →    incremental_strategy      How to combine columns (or_logic, single_column)
batch_size                   →    batch_size                Rows per batch insert
estimated_rows               →    estimated_rows            For strategy selection
estimated_size_mb            →    estimated_size_mb         For strategy selection
primary_key                  →    (not used in loading)     For schema/dbt reference only

WHY "primary_column" NOT "primary_incremental_column"?

The original code uses "primary_column" as a shorter local variable name:
  
  # Line 1581 in postgres_loader.py
  primary_column = self._get_primary_incremental_column(table_config)
  
This is stored in LoadPreparation as "primary_column" for brevity,
but it represents the value from table_config['primary_incremental_column'].

EXAMPLE DATA FLOW FOR MEDICATION TABLE:

1. tables.yml contains:
   medication:
     primary_incremental_column: MedicationNum
     incremental_columns: [MedicationNum, DateTStamp]
     incremental_strategy: or_logic
     batch_size: 10000
     estimated_rows: 1090
     estimated_size_mb: 0.11

2. Pipeline orchestration flow:
   PipelineOrchestrator.run_table('medication', force_full=False)
   └── TableProcessor.process_table('medication', force_full=False)
       ├── Extract Phase: SimpleMySQLReplicator.copy_table('medication')
       │   └── Copies from source → replication database
       └── Load Phase: PostgresLoader.load_table('medication', force_full=False)
           └── Copies from replication → analytics database

3. postgres_loader.py (or refactored version) retrieves:
   table_config = self.settings.get_table_config('medication')
   primary_column = self._get_primary_incremental_column(table_config)  # Returns 'MedicationNum'
   incremental_columns = table_config.get('incremental_columns')  # Returns ['MedicationNum', 'DateTStamp']

4. LoadPreparation stores (in refactored version):
   LoadPreparation(
     table_name='medication',
     primary_column='MedicationNum',  # From primary_incremental_column field
     incremental_columns=['MedicationNum', 'DateTStamp'],
     incremental_strategy='or_logic',
     batch_size=10000,
     estimated_rows=1090,
     estimated_size_mb=0.11
   )

5. Strategy uses these values to:
   - Build WHERE clause: MedicationNum > 1545 OR DateTStamp > '2025-10-11'
   - Determine batch size: 10,000 rows per insert
   - Update tracking: last_primary_value = max(MedicationNum)
"""


# ============================================================================
# MIGRATION NOTES
# ============================================================================
"""
MIGRATION PLAN FROM OLD postgres_loader.py:

PHASE 1: Structure Setup ✅ (This file)
- Define Strategy Pattern classes
- Create high-level orchestration flow
- Document responsibilities clearly

PHASE 2: Migrate Helper Methods
From postgres_loader.py, migrate:
- _get_cached_schema() - Line 438
- _build_load_query() - Line 2381
- _build_enhanced_load_query() - Line 2153
- _check_analytics_needs_updating() - Line 3508
- _get_primary_incremental_column() - Line 3001
- _filter_valid_incremental_columns() - Line 1238
- bulk_insert_optimized() - Line 583
- _update_load_status_hybrid() - Line 3565
- _update_load_status() - Line 3001

PHASE 3: Implement Strategies
Priority order (based on usage):
1. StandardLoadStrategy - 95% of loads
2. StreamingLoadStrategy - 5% of loads
3. ChunkedLoadStrategy - 11% of loads
4. CopyCSVLoadStrategy - 0% usage (consider removing)
5. ParallelLoadStrategy - 0% usage (consider removing)

PHASE 4: Testing
- Unit tests for each strategy
- Integration tests with test database
- Performance benchmarks
- Stale state recovery tests (HYBRID FIX)

PHASE 5: Deployment
- Run side-by-side with old loader
- Compare results
- Gradual rollout
- Deprecate old loader

ESTIMATED EFFORT:
- Phase 1: 2 hours (DONE)
- Phase 2: 4-6 hours
- Phase 3: 8-12 hours
- Phase 4: 6-8 hours
- Phase 5: 2-4 hours
Total: 22-32 hours (~3-4 days)

CODE REDUCTION:
Current: ~1,250 lines across 5 methods
Target: ~600 lines in organized structure
Savings: ~650 lines (52% reduction)
"""

