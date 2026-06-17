"""
PostgreSQL Loader - Refactored with Strategy Pattern
====================================================

ROLE IN PIPELINE:
This is a COMPONENT (not the orchestrator!) called by TableProcessor for the load phase.
It handles loading data from replication MySQL to analytics PostgreSQL.

CALL HIERARCHY:
PipelineOrchestrator → TableProcessor → PostgresLoader ← YOU ARE HERE
                                      ↓
                              (Coordinates internal strategies)

REFACTORING GOAL:
Consolidate 5 duplicate load methods into a clean Strategy Pattern architecture
that eliminates code duplication and ensures consistent behavior (especially incremental fallback logic).

CURRENT STATE: 5 methods × ~250 lines = ~1,250 lines (80% duplicate code)
TARGET STATE: 1 coordinator + 3 strategies = ~600 lines (50% reduction)

ARCHITECTURE:
1. PostgresLoader - Loading component (coordinates strategies internally)
2. LoadPreparation - Universal pre-processing (config, schema, query building with incremental fallback)
3. LoadStrategy (ABC) - Abstract base for execution strategies
4. Concrete Strategies - StandardStrategy, StreamingStrategy, CopyCSVStrategy
5. LoadFinalization - Universal post-processing (tracking, metrics, validation)

KEY IMPROVEMENTS:
- ✅ Incremental fallback logic (zero-row detection → full load) implemented ONCE, applies to ALL strategies
- ✅ Eliminates ~800 lines of duplicate code
- ✅ Consistent error handling and logging
- ✅ Easy to test individual strategies
- ✅ Simple to add new strategies (e.g., Delta Lake, Iceberg)
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date
import csv
import tempfile
import os
from dataclasses import dataclass
from enum import Enum
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl_pipeline.config import get_settings, DatabaseType, PostgresSchema as ConfigPostgresSchema
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
    COPY_CSV = "copy_csv"      # > 200MB - Export CSV, PostgreSQL COPY
    PARALLEL = "parallel"      # > 1M rows - ThreadPoolExecutor (future)


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
    - Incremental fallback logic (done in LoadPreparation)
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
        # Basic conversion using column order; handle SQLAlchemy Row and tuples
        if hasattr(row, "_mapping"):
            mapping = row._mapping
            return {col: mapping[col] for col in column_names}
        return {col: row[idx] for idx, col in enumerate(column_names)}


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
            # Optional truncate for full loads
            if load_prep.should_truncate:
                with self.target_engine.begin() as tgt_conn:
                    tgt_conn.execute(text(f'TRUNCATE TABLE {self.target_schema}.{load_prep.table_name}'))
            # Execute source query and fetch all
            with self.source_engine.connect() as src_conn:
                result = src_conn.execute(text(load_prep.query))
                column_names = list(result.keys())
                rows = result.fetchall()
            if not rows:
                return LoadResult(
                    success=True,
                    rows_loaded=0,
                    strategy_used="standard",
                    duration=time.time() - start_time
                )
            # Convert rows to list of dicts
            converted: List[Dict[str, Any]] = [self._convert_row_to_dict(r, column_names) for r in rows]
            # Insert in batches using provided bulk_insert helper
            batch_size = max(1000, min(load_prep.batch_size, 50000))
            use_upsert = not load_prep.should_truncate
            total_inserted = 0
            for i in range(0, len(converted), batch_size):
                chunk = converted[i:i + batch_size]
                ok = self.bulk_insert(load_prep.table_name, chunk, chunk_size=batch_size, use_upsert=use_upsert)
                if not ok:
                    raise RuntimeError("bulk_insert_optimized returned False")
                total_inserted += len(chunk)
            rows_loaded = total_inserted
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
            # Optional truncate for full loads
            if load_prep.should_truncate:
                with self.target_engine.begin() as tgt_conn:
                    tgt_conn.execute(text(f'TRUNCATE TABLE {self.target_schema}.{load_prep.table_name}'))
            batch_size = max(1000, min(load_prep.batch_size, 25000))
            use_upsert = not load_prep.should_truncate
            pending: List[Dict[str, Any]] = []
            column_names: List[str] = []
            with self.source_engine.connect() as src_conn:
                result = src_conn.execute(text(load_prep.query))
                column_names = list(result.keys())
                for row in result:  # generator iteration
                    pending.append(self._convert_row_to_dict(row, column_names))
                    if len(pending) >= batch_size:
                        ok = self.bulk_insert(load_prep.table_name, pending, chunk_size=batch_size, use_upsert=use_upsert)
                        if not ok:
                            raise RuntimeError("bulk_insert_optimized returned False")
                        rows_loaded += len(pending)
                        pending.clear()
            # Flush any remaining rows
            if pending:
                ok = self.bulk_insert(load_prep.table_name, pending, chunk_size=batch_size, use_upsert=use_upsert)
                if not ok:
                    raise RuntimeError("bulk_insert_optimized returned False")
                rows_loaded += len(pending)
                pending.clear()
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


def _serialize_value_for_copy_csv(value: Any) -> str:
    """
    Serialize a Python value (after PostgresSchema conversion) for PostgreSQL COPY CSV.
    Handles only formatting: None → empty (NULL), datetime/date → ISO string, else str.
    Schema/type edge cases (e.g. MySQL zero-datetime) are handled in PostgresSchema.
    """
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return "" if value == "" else str(value)


class CopyCSVLoadStrategy(LoadStrategy):
    """
    COPY CSV loading for very large tables (> 500MB).

    APPROACH:
    - Stream rows from MySQL and write to a temporary CSV file
    - Use PostgreSQL COPY for fast bulk ingestion
    - Truncate target when force_full; otherwise upsert is not supported (fallback recommended)

    Type/schema edge cases (e.g. MySQL 0000-00-00 → PostgreSQL NULL) are handled via
    schema_adapter.convert_row_data_types() from postgres_schema.py, not in this strategy.
    """

    def __init__(
        self,
        source_engine: Any,
        target_engine: Any,
        target_schema: str,
        bulk_insert_method: Any = None,
        schema_adapter: Any = None,
    ):
        super().__init__(source_engine, target_engine, target_schema, bulk_insert_method)
        self.schema_adapter = schema_adapter

    def execute(self, load_prep: LoadPreparation) -> LoadResult:
        start_time = time.time()
        rows_loaded = 0
        tmp_path = None
        count_before = 0
        try:
            logger.info(f"[CopyCsvStrategy] Loading {load_prep.table_name} via CSV COPY")

            # Truncate when full load; for incremental, capture current row count for verification
            if load_prep.should_truncate:
                with self.target_engine.begin() as tgt_conn:
                    tgt_conn.execute(text(f'TRUNCATE TABLE {self.target_schema}.{load_prep.table_name}'))
            else:
                with self.target_engine.connect() as conn:
                    count_before = conn.execute(text(
                        f'SELECT COUNT(*) FROM {self.target_schema}.{load_prep.table_name}'
                    )).scalar() or 0

            table_name = load_prep.table_name
            # Execute source query and stream to temp CSV
            with self.source_engine.connect() as src_conn:
                result = src_conn.execute(text(load_prep.query))
                column_names = list(result.keys())

                fd, tmp_path = tempfile.mkstemp(prefix=f"etl_{table_name}_", suffix=".csv")
                os.close(fd)

                with open(tmp_path, mode="w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    # Do not write header; COPY will use column list
                    for row in result:
                        if hasattr(row, "_mapping"):
                            row_dict = {col: row._mapping[col] for col in column_names}
                        else:
                            row_dict = {col: row[idx] for idx, col in enumerate(column_names)}
                        # Use PostgresSchema for type/schema edge cases (e.g. 0000-00-00 → NULL)
                        if self.schema_adapter is not None:
                            row_dict = self.schema_adapter.convert_row_data_types(table_name, row_dict)
                        data = [_serialize_value_for_copy_csv(row_dict[col]) for col in column_names]
                        writer.writerow(data)
                        rows_loaded += 1

            # COPY into Postgres
            columns_csv = ",".join(f'"{c}"' for c in column_names)
            copy_sql = f"COPY {self.target_schema}.{load_prep.table_name} ({columns_csv}) FROM STDIN WITH (FORMAT CSV)"

            with self.target_engine.begin() as target_conn:
                with open(tmp_path, "r", encoding="utf-8") as f:
                    with target_conn.connection.cursor() as cur:
                        cur.copy_expert(copy_sql, f)
                        # Note: rows_loaded tracking happens during CSV write phase above
                        # begin() context manager auto-commits on success

            # Verify rows are visible in analytics before we report success (ensures commit is done and data is in raw)
            with self.target_engine.connect() as verify_conn:
                verify_result = verify_conn.execute(text(
                    f'SELECT COUNT(*) FROM {self.target_schema}.{load_prep.table_name}'
                ))
                actual_count = verify_result.scalar()
            expected_count = rows_loaded if load_prep.should_truncate else (count_before + rows_loaded)
            if actual_count != expected_count:
                raise RuntimeError(
                    f"Post-load count mismatch for {load_prep.table_name}: "
                    f"expected {expected_count:,} rows in {self.target_schema}.{load_prep.table_name}, got {actual_count:,}"
                )
            logger.info(
                f"Verified {rows_loaded:,} rows in {self.target_schema}.{load_prep.table_name} (analytics)"
            )

            return LoadResult(
                success=True,
                rows_loaded=rows_loaded,
                strategy_used="copy_csv",
                duration=time.time() - start_time
            )
        except Exception as e:
            logger.error(f"[CopyCsvStrategy] Error loading {load_prep.table_name}: {str(e)}")
            return LoadResult(
                success=False,
                rows_loaded=rows_loaded,
                strategy_used="copy_csv",
                duration=time.time() - start_time,
                error=str(e)
            )
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass


class ParallelLoadStrategy(LoadStrategy):
    """
    Parallel loading for massive tables (> 1M rows).
    
    APPROACH:
    - Split data into N chunks based on primary key ranges
    - Process chunks in parallel using ThreadPoolExecutor
    - Aggregate results from all threads
    
    Falls back to the configured strategy when incremental load, no primary key,
    or PK bounds cannot be determined.
    """
    
    def __init__(
        self,
        source_engine: Engine,
        target_engine: Engine,
        target_schema: str,
        bulk_insert_method: Any = None,
        fallback_strategy: Optional[LoadStrategy] = None,
        num_workers: int = 4,
    ):
        super().__init__(source_engine, target_engine, target_schema, bulk_insert_method)
        self.fallback_strategy = fallback_strategy
        self.num_workers = max(1, min(num_workers, 16))

    def _replication_db(self) -> str:
        return self.source_engine.url.database

    def _get_pk_bounds(self, table_name: str, pk_col: str) -> Tuple[Optional[int], Optional[int]]:
        db = self._replication_db()
        with self.source_engine.connect() as conn:
            row = conn.execute(
                text(
                    f"SELECT MIN(`{pk_col}`), MAX(`{pk_col}`) "
                    f"FROM `{db}`.`{table_name}` WHERE `{pk_col}` IS NOT NULL"
                )
            ).fetchone()
        if not row or row[0] is None or row[1] is None:
            return None, None
        return int(row[0]), int(row[1])

    @staticmethod
    def _split_pk_ranges(min_pk: int, max_pk: int, num_workers: int) -> List[Tuple[int, int]]:
        span = max_pk - min_pk + 1
        if span <= num_workers:
            return [(pk, pk) for pk in range(min_pk, max_pk + 1)]
        chunk_size = max(1, span // num_workers)
        ranges: List[Tuple[int, int]] = []
        start = min_pk
        for idx in range(num_workers):
            end = start + chunk_size - 1 if idx < num_workers - 1 else max_pk
            ranges.append((start, end))
            start = end + 1
        return ranges

    def _load_chunk(
        self,
        load_prep: LoadPreparation,
        pk_col: str,
        low: int,
        high: int,
        batch_size: int,
    ) -> int:
        db = self._replication_db()
        chunk_query = (
            f"SELECT * FROM `{db}`.`{load_prep.table_name}` "
            f"WHERE `{pk_col}` >= {low} AND `{pk_col}` <= {high}"
        )
        rows_loaded = 0
        pending: List[Dict[str, Any]] = []
        with self.source_engine.connect() as src_conn:
            result = src_conn.execution_options(stream_results=True).execute(text(chunk_query))
            column_names = list(result.keys())
            for row in result:
                pending.append(self._convert_row_to_dict(row, column_names))
                if len(pending) >= batch_size:
                    if not self.bulk_insert(
                        load_prep.table_name, pending, chunk_size=batch_size, use_upsert=False
                    ):
                        raise RuntimeError("bulk_insert_optimized returned False")
                    rows_loaded += len(pending)
                    pending.clear()
        if pending:
            if not self.bulk_insert(
                load_prep.table_name, pending, chunk_size=batch_size, use_upsert=False
            ):
                raise RuntimeError("bulk_insert_optimized returned False")
            rows_loaded += len(pending)
        return rows_loaded

    def execute(self, load_prep: LoadPreparation) -> LoadResult:
        """Execute parallel loading by PK range, or delegate to fallback strategy."""
        start_time = time.time()
        rows_loaded = 0

        if self.fallback_strategy is None:
            return LoadResult(
                success=False,
                rows_loaded=0,
                strategy_used="parallel",
                duration=time.time() - start_time,
                error="Parallel strategy has no fallback configured",
            )

        if not load_prep.should_truncate:
            logger.info(
                f"[ParallelStrategy] Incremental load for {load_prep.table_name}; using fallback"
            )
            return self.fallback_strategy.execute(load_prep)

        pk_col = load_prep.primary_column
        if not pk_col:
            logger.warning(
                f"[ParallelStrategy] No primary column for {load_prep.table_name}; using fallback"
            )
            return self.fallback_strategy.execute(load_prep)

        try:
            logger.info(
                f"[ParallelStrategy] Loading {load_prep.table_name} with "
                f"{self.num_workers} workers on `{pk_col}`"
            )

            if load_prep.should_truncate:
                with self.target_engine.begin() as tgt_conn:
                    tgt_conn.execute(
                        text(f"TRUNCATE TABLE {self.target_schema}.{load_prep.table_name}")
                    )

            min_pk, max_pk = self._get_pk_bounds(load_prep.table_name, pk_col)
            if min_pk is None:
                logger.warning(
                    f"[ParallelStrategy] No PK bounds for {load_prep.table_name}; using fallback"
                )
                return self.fallback_strategy.execute(load_prep)

            ranges = self._split_pk_ranges(min_pk, max_pk, self.num_workers)
            batch_size = max(1000, min(load_prep.batch_size, 25000))
            errors: List[str] = []

            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                futures = {
                    executor.submit(
                        self._load_chunk, load_prep, pk_col, low, high, batch_size
                    ): (low, high)
                    for low, high in ranges
                }
                for future in as_completed(futures):
                    low, high = futures[future]
                    try:
                        rows_loaded += future.result()
                    except Exception as exc:
                        msg = f"chunk {low}-{high}: {exc}"
                        logger.error(f"[ParallelStrategy] {load_prep.table_name} {msg}")
                        errors.append(msg)

            if errors:
                return LoadResult(
                    success=False,
                    rows_loaded=rows_loaded,
                    strategy_used="parallel",
                    duration=time.time() - start_time,
                    error="; ".join(errors),
                )

            return LoadResult(
                success=True,
                rows_loaded=rows_loaded,
                strategy_used="parallel",
                duration=time.time() - start_time,
            )

        except Exception as e:
            logger.error(f"[ParallelStrategy] Error loading {load_prep.table_name}: {str(e)}")
            return LoadResult(
                success=False,
                rows_loaded=rows_loaded,
                strategy_used="parallel",
                duration=time.time() - start_time,
                error=str(e),
            )


# ============================================================================
# MAIN LOADING COMPONENT CLASS
# ============================================================================

class PostgresLoader:
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
    - _prepare_table_load() - Universal pre-processing with incremental fallback detection
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
        
        # Get target schema name using settings database config
        analytics_config = self.settings.get_database_config(DatabaseType.ANALYTICS, ConfigPostgresSchema.RAW)
        self.analytics_schema = analytics_config.get('schema', 'raw')
        # Backward-compat attribute used by bulk_insert_optimized
        self.target_schema = self.analytics_schema
        # Backward-compat target engine attribute expected by bulk_insert_optimized
        self.target_engine = self.analytics_engine
        
        # Initialize schema cache for performance optimization
        self.schema_cache = SchemaCache()
        logger.debug("Initialized schema cache for performance optimization")
        
        # Load table configurations from tables.yml
        self.table_configs = table_configs or {}
        if not self.table_configs:
            logger.debug("No table configurations provided, will load from settings as needed")
        
        # Initialize strategy instances (reusable)
        self._init_strategies()
    
    def _init_strategies(self):
        """
        Initialize all strategy instances.
        
        Strategies are stateless and reusable, so we create them once.
        """
        standard = StandardLoadStrategy(
            self.replication_engine,
            self.analytics_engine,
            self.analytics_schema,
            self.bulk_insert_optimized,
        )
        streaming = StreamingLoadStrategy(
            self.replication_engine,
            self.analytics_engine,
            self.analytics_schema,
            self.bulk_insert_optimized,
        )
        copy_csv = CopyCSVLoadStrategy(
            self.replication_engine,
            self.analytics_engine,
            self.analytics_schema,
            self.bulk_insert_optimized,
            self.schema_adapter,
        )
        parallel = ParallelLoadStrategy(
            self.replication_engine,
            self.analytics_engine,
            self.analytics_schema,
            self.bulk_insert_optimized,
            fallback_strategy=copy_csv,
            num_workers=self._get_parallel_workers(),
        )
        self.strategies = {
            LoadStrategyType.STANDARD: standard,
            LoadStrategyType.STREAMING: streaming,
            LoadStrategyType.COPY_CSV: copy_csv,
            LoadStrategyType.PARALLEL: parallel,
        }

    def _parallel_loading_enabled(self) -> bool:
        """Whether parallel load strategy is enabled in pipeline config."""
        return bool(self.settings.get_pipeline_setting("stages.load.enable_parallel_loading", False))

    def _get_parallel_workers(self) -> int:
        """Worker count for parallel load strategy (clamped 1–16)."""
        raw = self.settings.get_pipeline_setting("stages.load.parallel_workers", 4)
        try:
            workers = int(raw)
        except (TypeError, ValueError):
            workers = 4
        return max(1, min(workers, 16))

    def _get_parallel_min_rows(self) -> int:
        """Minimum estimated rows before parallel strategy is considered."""
        raw = self.settings.get_pipeline_setting("stages.load.parallel_min_rows", 1_000_000)
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 1_000_000
    
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
        1. Pre-processing: Configuration, schema, query building with incremental fallback detection
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
            # - Building query with incremental fallback detection
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
        
        This is where incremental fallback logic is implemented ONCE for ALL strategies.
        If an incremental query returns 0 rows but analytics needs updating, automatically
        falls back to a full load to ensure data consistency.
        
        RESPONSIBILITIES:
        1. Get table configuration from settings (via self.settings.get_table_config())
        2. Validate MySQL schema exists
        3. Ensure PostgreSQL table exists
        4. Build incremental query
        5. Detect stale state: Check if incremental returns 0 rows but analytics needs updating
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
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration for {table_name}")
                return None
            
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
            primary_column = self._get_primary_incremental_column(table_config)
            incremental_columns = table_config.get('incremental_columns', [])
            incremental_strategy = table_config.get('incremental_strategy', 'or_logic')
            query = self._build_load_query(table_name, incremental_columns, force_full)
            
            # ============================================================
            # 4. INCREMENTAL FALLBACK DETECTION
            # ============================================================
            # Detect stale state: If incremental query returns 0 rows but analytics
            # needs updating (e.g., due to data corruption, missed loads, or reset),
            # automatically fall back to full load to ensure data consistency.
            # This prevents silent data gaps and is implemented ONCE for all strategies.
            
            should_force_full = force_full
            
            if not force_full:
                # Check if incremental query will return 0 rows
                row_count = self._execute_count_query(query)
                if row_count == 0:
                    # Verify if analytics actually needs updating (stale state detection)
                    needs_updating, _, _, force_full_recommended = self._check_analytics_needs_updating(table_name)
                    if needs_updating:
                        logger.warning(
                            f"Incremental query returned 0 rows for {table_name}, "
                            f"but analytics needs updating. Falling back to full load to ensure data consistency."
                        )
                        # Rebuild query without incremental WHERE clause
                        query = self._build_full_load_query(table_name)
                        should_force_full = True or force_full_recommended
            
            # ============================================================
            # 5. ESTIMATE TABLE SIZE
            # ============================================================
            estimated_rows = int(table_config.get('estimated_rows', 0) or 0)
            estimated_size_mb = float(table_config.get('estimated_size_mb', 0.0) or 0.0)
            
            # ============================================================
            # 6. DETERMINE BATCH SIZE
            # ============================================================
            batch_size = int(table_config.get('batch_size', 10000) or 10000)
            
            # ============================================================
            # 7. CREATE PREPARATION OBJECT
            # ============================================================
            
            return LoadPreparation(
                table_name=table_name,
                table_config=table_config,
                mysql_schema=mysql_schema,
                query=query,
                primary_column=primary_column,
                incremental_columns=incremental_columns,
                incremental_strategy=incremental_strategy,
                should_truncate=should_force_full,
                force_full=should_force_full,
                batch_size=batch_size,
                estimated_rows=estimated_rows,
                estimated_size_mb=estimated_size_mb
            )
            
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
        - > 200MB: CopyCSV (bulk COPY; best throughput for large tables)
        - > 1M rows: Parallel (if enabled in future)
        
        Args:
            load_prep: LoadPreparation with table size estimates
            
        Returns:
            LoadStrategyType enum value
        """
        size_mb = load_prep.estimated_size_mb
        rows = load_prep.estimated_rows
        
        # Priority: Parallel for massive tables when enabled in pipeline config
        if rows >= self._get_parallel_min_rows() and self._parallel_loading_enabled():
            return LoadStrategyType.PARALLEL
        
        # Size-based selection: use copy_csv for large tables (>200MB) for best throughput
        if size_mb > 200:
            return LoadStrategyType.COPY_CSV
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
            try:
                self._ensure_tracking_record_exists(table_name)
            except Exception:
                pass
            
            # Compute last primary value if we have a primary column and loaded rows
            last_primary_value = None
            if load_prep.primary_column and load_result.success and load_result.rows_loaded > 0:
                try:
                    with self.analytics_engine.connect() as conn:
                        # Get the max primary value from the analytics table that was just loaded
                        max_result = conn.execute(text(f"""
                            SELECT MAX("{load_prep.primary_column}") 
                            FROM {self.analytics_schema}.{table_name}
                            WHERE "{load_prep.primary_column}" IS NOT NULL
                        """)).scalar()
                        if max_result is not None:
                            last_primary_value = str(max_result)
                            logger.debug(f"Computed last primary value for {table_name}: {last_primary_value} (column: {load_prep.primary_column})")
                except Exception as e:
                    logger.warning(f"Could not compute last primary value for {table_name}: {str(e)}")
                    # Continue without primary value - timestamp tracking will be used as fallback
            
            if load_prep.primary_column:
                self._update_load_status_hybrid(
                    table_name=table_name,
                    rows_loaded=load_result.rows_loaded,
                    load_status='success' if load_result.success else 'failed',
                    last_timestamp=datetime.utcnow(),
                    last_primary_value=last_primary_value,
                    primary_column_name=load_prep.primary_column
                )
            else:
                self._update_load_status(
                    table_name=table_name,
                    rows_loaded=load_result.rows_loaded,
                    status='success' if load_result.success else 'failed'
                )
            
            # ============================================================
            # 2. CALCULATE PERFORMANCE METRICS
            # ============================================================
            duration = time.time() - start_time
            rows_per_sec = load_result.rows_loaded / duration if duration > 0 else 0
            mb_per_sec = load_prep.estimated_size_mb / duration if duration > 0 else 0
            
            # ============================================================
            # 3. RUN VALIDATION CHECKS
            # ============================================================
            validation = self._validate_post_load(table_name, load_prep, load_result)
            for warning in validation.get("warnings", []):
                logger.warning(f"[Finalization] {table_name}: {warning}")
            
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
                'estimated_size_mb': load_prep.estimated_size_mb,
                'post_load_validation': validation,
            }
            
            if load_result.error:
                metadata['error'] = load_result.error
            
            # ============================================================
            # 5. LOG FINAL SUMMARY
            # ============================================================
            if load_result.success:
                logger.debug(
                    f"✅ Successfully loaded {table_name} into analytics: "
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
                
                # Use executemany for bulk operation with type conversion via schema adapter
                try:
                    # Convert types per row if adapter is available
                    if self.schema_adapter is not None:
                        try:
                            chunk = [self.schema_adapter.convert_row_data_types(table_name, row) for row in chunk]
                        except Exception as conv_err:
                            logger.warning(f"Type conversion failed for some rows in {table_name}: {conv_err}")
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
    
    def _build_upsert_sql(self, table_name: str, column_names: List[str]) -> str:
        """
        Build PostgreSQL UPSERT SQL with dynamic primary key handling and change detection.
        
        Uses IS DISTINCT FROM in WHERE clause to prevent no-op updates when data hasn't changed.
        This reduces MVCC churn and improves pipeline health metrics by only updating rows
        when values actually differ.
        """
        # Get primary key from table configuration
        table_config = self.get_table_config(table_name)
        primary_key = table_config.get('primary_key', 'id')
        logger.debug(f"Building UPSERT for {table_name} with primary key: {primary_key}")
        columns = ', '.join([f'"{col}"' for col in column_names])
        placeholders = ', '.join([f':{col}' for col in column_names])
        # Update all columns except the primary key
        update_columns = [f'"{col}" = EXCLUDED."{col}"' for col in column_names if col != primary_key]
        update_clause = ', '.join(update_columns) if update_columns else ''
        
        # Build change detection WHERE clause using IS DISTINCT FROM
        # Only update if at least one column value has changed
        # IS DISTINCT FROM handles NULL comparisons correctly
        # NOTE: In ON CONFLICT DO UPDATE WHERE clause, we need to use a table alias
        # to qualify column names and avoid ambiguity between table and EXCLUDED columns
        table_alias = 'target'
        change_detection_conditions = [
            f'{table_alias}."{col}" IS DISTINCT FROM EXCLUDED."{col}"'
            for col in column_names
            if col != primary_key
        ]
        
        # If we have columns to check, add WHERE clause; otherwise update always (shouldn't happen)
        where_clause = ''
        if change_detection_conditions:
            conditions_str = ' OR '.join(change_detection_conditions)
            where_clause = f'\n            WHERE ({conditions_str})'
        
        return f"""
            INSERT INTO {self.target_schema}.{table_name} AS {table_alias} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ("{primary_key}") DO UPDATE SET
                {update_clause}{where_clause}
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
        
        # Validate incremental columns against actual replication table schema
        # This filters out columns that don't exist (e.g., 'SecDateTEntry' vs actual column names)
        incremental_columns = self._validate_incremental_columns(table_name, incremental_columns)
        
        # Validate incremental columns
        if not incremental_columns:
            logger.warning(f"No incremental columns provided for {table_name}, falling back to primary column")
            if primary_incremental_column:
                # Validate the primary column exists too
                validated_primary = self._validate_incremental_columns(table_name, [primary_incremental_column])
                if validated_primary:
                    incremental_columns = validated_primary
                else:
                    logger.error(f"Primary incremental column '{primary_incremental_column}' does not exist in replication table {table_name}")
                    return f"SELECT * FROM `{replication_db}`.`{table_name}`"
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
                # Only use primary key predicate if value is numeric
                def _is_numeric_value(val: object) -> bool:
                    try:
                        float(str(val))
                        return True
                    except Exception:
                        return False

                if last_primary_value and self._is_integer_column(table_name, primary_incremental_column) and _is_numeric_value(last_primary_value):
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
                # Only use primary key predicate if value is numeric
                def _is_numeric_value(val: object) -> bool:
                    try:
                        float(str(val))
                        return True
                    except Exception:
                        return False

                if col == primary_incremental_column and last_primary_value and self._is_integer_column(table_name, col) and _is_numeric_value(last_primary_value):
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

    def _get_replication_row_count(self, table_name: str) -> int:
        """Get total row count for a table in the replication database."""
        if self.replication_engine is None:
            return 0
        try:
            replication_db = self.replication_engine.url.database
            with self.replication_engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`")
                ).scalar()
                return result or 0
        except Exception as e:
            logger.error(f"Error getting row count for {table_name} in replication: {str(e)}")
            return 0

    def _count_null_primary_keys(self, table_name: str, primary_column: str) -> int:
        """Count NULL primary-key values in the analytics table after load."""
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(
                    text(
                        f'SELECT COUNT(*) FROM {self.analytics_schema}.{table_name} '
                        f'WHERE "{primary_column}" IS NULL'
                    )
                ).scalar()
                return int(result or 0)
        except Exception as e:
            logger.warning(
                f"Could not check NULL primary keys for {table_name}.{primary_column}: {e}"
            )
            return 0

    def _validate_post_load(
        self,
        table_name: str,
        load_prep: LoadPreparation,
        load_result: LoadResult,
    ) -> Dict[str, Any]:
        """
        Post-load validation: replication vs analytics row counts and PK integrity.

        Full loads expect analytics row count to match replication. Incremental loads
        only warn when analytics exceeds replication. Warnings are logged and returned
        in metadata; they do not fail the load.
        """
        validation: Dict[str, Any] = {
            "row_count_match": None,
            "replication_rows": None,
            "analytics_rows": None,
            "primary_key_nulls": None,
            "warnings": [],
        }
        if not load_result.success:
            return validation

        analytics_rows = self._get_analytics_row_count(table_name)
        validation["analytics_rows"] = analytics_rows

        if self.replication_engine is None:
            return validation

        replication_rows = self._get_replication_row_count(table_name)
        validation["replication_rows"] = replication_rows

        if load_prep.should_truncate:
            validation["row_count_match"] = analytics_rows == replication_rows
            if not validation["row_count_match"]:
                validation["warnings"].append(
                    f"Full load row count mismatch: replication={replication_rows:,}, "
                    f"analytics={analytics_rows:,}"
                )
        elif analytics_rows > replication_rows:
            validation["row_count_match"] = False
            validation["warnings"].append(
                f"Analytics row count ({analytics_rows:,}) exceeds replication "
                f"({replication_rows:,}) after incremental load"
            )
        else:
            validation["row_count_match"] = True

        if load_prep.primary_column:
            null_pks = self._count_null_primary_keys(table_name, load_prep.primary_column)
            validation["primary_key_nulls"] = null_pks
            if null_pks > 0:
                validation["warnings"].append(
                    f"Found {null_pks:,} NULL values in primary column "
                    f"{load_prep.primary_column}"
                )

        return validation

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

                    # Helper to check integer-likeness
                    def _is_integer_like(val: object) -> bool:
                        if isinstance(val, int):
                            return True
                        s = str(val)
                        return s.isdigit()

                    # Pull configured primary incremental column for this table
                    table_config = self.get_table_config(table_name) or {}
                    configured_primary = table_config.get('primary_incremental_column') or table_config.get('primary_key')

                    # Prefer the configured primary column over stored 'timestamp' or mismatched values
                    # If the stored primary_column_name is missing, 'timestamp', or doesn't match configured_primary,
                    # use the configured_primary to recompute a valid integer last_primary_value when appropriate.
                    selected_column = configured_primary or primary_column_name

                    # Decide if we need to recompute from analytics data
                    needs_recompute = False

                    # If we have a configured integer primary and the stored value is missing or not integer-like, recompute
                    if selected_column and self._is_integer_column(table_name, selected_column):
                        if last_primary_value is None or not _is_integer_like(last_primary_value):
                            needs_recompute = True

                    # Also force recompute when the stored column name is clearly not a real PK (e.g., 'timestamp')
                    if primary_column_name and primary_column_name.lower() == 'timestamp' and selected_column:
                        if self._is_integer_column(table_name, selected_column):
                            needs_recompute = True

                    if needs_recompute and selected_column:
                        try:
                            computed_max = conn.execute(text(f"""
                                SELECT MAX("{selected_column}")
                                FROM {self.analytics_schema}.{table_name}
                                WHERE "{selected_column}" IS NOT NULL
                            """)).scalar()
                            if computed_max is not None:
                                last_primary_value = str(computed_max)
                                logger.info(
                                    f"Computed last_primary_value for {table_name} from analytics data using {selected_column}: {last_primary_value}"
                                )
                            else:
                                # Analytics table is empty - clear the invalid stored value
                                last_primary_value = None
                                logger.info(
                                    f"Analytics table {self.analytics_schema}.{table_name} has no non-null values for {selected_column}; setting last_primary_value to NULL"
                                )
                        except Exception as compute_err:
                            logger.warning(f"Could not compute MAX({selected_column}) for {table_name}: {str(compute_err)}")

                    return last_primary_value
                else:
                    return None
        except Exception as e:
            logger.error(f"Error getting last primary value for {table_name}: {str(e)}")
            return None

    def _validate_incremental_columns(self, table_name: str, incremental_columns: List[str]) -> List[str]:
        """
        Validate incremental columns against actual replication table schema.
        
        Filters out columns that don't exist in the replication table to prevent
        SQL errors like "Unknown column 'X' in 'where clause'".
        
        Args:
            table_name: Name of the table to validate against
            incremental_columns: List of column names to validate
            
        Returns:
            Filtered list containing only columns that actually exist in the replication table
        """
        if not incremental_columns:
            return []
        
        try:
            replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
            replication_db = replication_config.get('database', 'opendental_replication')
            
            # For MySQL replication, query INFORMATION_SCHEMA directly for reliability
            # This works across all MySQL versions and doesn't depend on SQLAlchemy's schema handling
            with self.replication_engine.connect() as conn:
                # Query INFORMATION_SCHEMA to get actual column names
                query = text(f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = :db_name 
                    AND TABLE_NAME = :table_name
                """)
                result = conn.execute(query, {"db_name": replication_db, "table_name": table_name})
                actual_columns = [row[0] for row in result]
            
            # Filter to only include columns that exist
            valid_columns = [col for col in incremental_columns if col in actual_columns]
            
            # Log any columns that were filtered out
            invalid_columns = [col for col in incremental_columns if col not in actual_columns]
            if invalid_columns:
                logger.warning(
                    f"Filtered out {len(invalid_columns)} non-existent columns for {table_name}: {invalid_columns}. "
                    f"Valid columns: {valid_columns}"
                )
            
            return valid_columns
            
        except Exception as e:
            logger.warning(
                f"Error validating incremental columns for {table_name}: {str(e)}. "
                f"Using original columns list as-is."
            )
            # On error, return original list (fail-safe behavior)
            return incremental_columns

    def _filter_valid_incremental_columns(self, table_name: str, columns: List[str]) -> List[str]:
        """
        Filter out columns with data quality issues.
        
        This method performs data quality validation by sampling column data
        to check for date ranges, null counts, and other quality indicators.
        
        Args:
            table_name: Name of the table
            columns: List of incremental columns to validate
            
        Returns:
            List[str]: Filtered list of valid incremental columns
        """
        if not columns:
            return []
        
        # In test environment, skip data quality validation if replication_engine is None
        if self.replication_engine is None:
            logger.info(f"Skipping data quality validation for {table_name} in test environment")
            return columns
        
        valid_columns = []
        
        try:
            # Get the actual database name from the connection
            with self.replication_engine.connect() as conn:
                # Get the current database name from the connection
                result = conn.execute(text("SELECT DATABASE()"))
                actual_db = result.scalar()
                logger.debug(f"Actual database for {table_name}: {actual_db}")
                
                for column in columns:
                    # Check for data quality issues by sampling the column
                    sample_query = f"""
                        SELECT MIN({column}), MAX({column}), COUNT(*)
                        FROM `{actual_db}`.`{table_name}`
                        WHERE {column} IS NOT NULL
                        LIMIT 1000
                    """
                    
                    result = conn.execute(text(sample_query))
                    row = result.fetchone()
                    
                    if row and row[0] and row[1]:
                        min_date = row[0]
                        max_date = row[1]
                        count = row[2]
                        
                        # Skip columns with obviously bad dates
                        if isinstance(min_date, (datetime, date)):
                            if min_date.year < 2000 or max_date.year > 2030:
                                logger.warning(f"Column {column} in {table_name} has bad date range: {min_date} to {max_date}")
                                continue
                        
                        # Skip columns with too many NULL values (indicating poor data quality)
                        if count < 100:  # Less than 100 non-null values in sample
                            logger.warning(f"Column {column} in {table_name} has poor data quality: only {count} non-null values")
                            continue
                        
                        valid_columns.append(column)
                        logger.debug(f"Column {column} in {table_name} passed data quality validation")
                    else:
                        logger.warning(f"Column {column} in {table_name} has no valid data")
                        
        except Exception as e:
            logger.warning(f"Could not validate data quality for columns in {table_name}: {str(e)}")
            # If validation fails, return original columns (fail open)
            return columns
        
        return valid_columns

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
        
        This is the core of the stale state detection logic used for incremental fallback.
        
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
            # This indicates data loss and requires a full reload
            force_full_load_recommended = False
            if analytics_row_count == 0 and analytics_load_time is not None:
                logger.info(f"Analytics table {table_name} is empty but has load timestamp {analytics_load_time}, recommending full load")
                force_full_load_recommended = True
            
            if analytics_load_time is None:
                # Analytics has never been loaded, but check if table actually has data
                # (in case load tracking was lost but data exists)
                if analytics_row_count > 0:
                    # Table has data but no load record - try incremental based on max primary key
                    logger.info(f"Analytics table {table_name} has {analytics_row_count} rows but no load timestamp, will attempt incremental load")
                    # Don't force full load - let _build_load_query handle it based on actual table state
                    force_full_load_recommended = False
                else:
                    # Analytics has never been loaded and table is empty, check if replication has data
                    logger.info(f"Analytics has never been loaded for {table_name} (table is empty), checking if replication has data")
                    
                    # Check if replication has any data for this table
                    try:
                        with self.replication_engine.connect() as conn:
                            # Use the replication database name explicitly
                            replication_db = self.replication_engine.url.database
                            result = conn.execute(text(f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`"))
                            replication_row_count = result.scalar()
                            
                        if replication_row_count > 0:
                            logger.info(f"Replication has {replication_row_count} rows for {table_name}, analytics needs full sync (initial load)")
                            # First-time load: needs full load, this is expected
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
                    
                    # Validate incremental columns against actual replication table schema
                    incremental_columns = self._validate_incremental_columns(table_name, incremental_columns)
                    
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
                            # Replication has more rows - this is normal incremental scenario
                            # Only force full load if table was empty (data loss scenario)
                            # Otherwise, use incremental load to add new rows
                            if force_full_load_recommended:
                                logger.info(f"Replication has {replication_count} rows vs analytics {analytics_row_count} rows for {table_name}, but analytics table is empty with timestamp - forcing full load")
                            else:
                                logger.info(f"Replication has {replication_count} rows vs analytics {analytics_row_count} rows for {table_name}, will use incremental load")
                            return True, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                        
                        # If row counts are the same, check for records with timestamps newer than analytics load time
                        # FIXED: Handle integer primary keys correctly
                        primary_incremental_column = table_config.get('primary_incremental_column')
                        last_primary_value = self._get_last_primary_value(table_name)

                        def _is_numeric_value(val: object) -> bool:
                            """Only use non-datetime values for integer column comparisons."""
                            if val is None:
                                return False
                            s = str(val).strip()
                            if not s:
                                return False
                            # Reject datetime-like strings (e.g. '2026-01-07 23:31:54')
                            if '-' in s and (':' in s or len(s) == 10):
                                return False
                            try:
                                float(s)
                                return True
                            except ValueError:
                                return False

                        conditions = []
                        for col in incremental_columns:
                            if col == primary_incremental_column and last_primary_value and self._is_integer_column(table_name, col) and _is_numeric_value(last_primary_value):
                                # Handle integer primary key columns - only with numeric values
                                conditions.append(f"`{col}` > {last_primary_value}")
                            else:
                                # Handle timestamp columns
                                conditions.append(f"(`{col}` > '{analytics_load_time}' AND `{col}` != '0001-01-01 00:00:00')")
                        
                        # Use strategy-based logic
                        if incremental_strategy == 'or_logic':
                            where_clause = " OR ".join(conditions)
                        elif incremental_strategy == 'and_logic':
                            where_clause = " AND ".join(conditions)
                        elif incremental_strategy == 'single_column':
                            # Use only the primary incremental column
                            if primary_incremental_column and primary_incremental_column in incremental_columns:
                                if last_primary_value and self._is_integer_column(table_name, primary_incremental_column) and _is_numeric_value(last_primary_value):
                                    where_clause = f"`{primary_incremental_column}` > {last_primary_value}"
                                else:
                                    where_clause = f"(`{primary_incremental_column}` > '{analytics_load_time}' AND `{primary_incremental_column}` != '0001-01-01 00:00:00')"
                            else:
                                # Fallback to first column
                                if last_primary_value and self._is_integer_column(table_name, incremental_columns[0]) and _is_numeric_value(last_primary_value):
                                    where_clause = f"`{incremental_columns[0]}` > {last_primary_value}"
                                else:
                                    where_clause = f"(`{incremental_columns[0]}` > '{analytics_load_time}' AND `{incremental_columns[0]}` != '0001-01-01 00:00:00')"
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
                            logger.info(
                                f"No new records in replication for {table_name} since last analytics load "
                                f"(replication and analytics in sync; table already has full data)"
                            )
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
                        # Fallback: no incremental columns or no analytics load time
                        # When analytics_load_time is None (e.g. table has data but no load record), use row-count comparison
                        if analytics_load_time is None:
                            replication_count_query = f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`"
                            replication_count = conn.execute(text(replication_count_query)).scalar()
                            needs_updating = (replication_count > analytics_row_count) or force_full_load_recommended
                            logger.info(f"Analytics has no load timestamp for {table_name}: replication={replication_count}, analytics={analytics_row_count}, needs_updating={needs_updating}")
                        else:
                            needs_updating = (replication_copy_time > analytics_load_time) or force_full_load_recommended
                            if needs_updating:
                                logger.info(f"Analytics needs updating for {table_name}: "
                                           f"replication_copy_time={replication_copy_time}, analytics_load_time={analytics_load_time}, "
                                           f"analytics_row_count={analytics_row_count}, force_full_load_recommended={force_full_load_recommended}")
                            else:
                                logger.info(f"Analytics is up to date for {table_name}: "
                                           f"replication_copy_time={replication_copy_time}, analytics_load_time={analytics_load_time}")
                        if needs_updating:
                            return True, str(replication_copy_time), str(analytics_load_time) if analytics_load_time is not None else None, force_full_load_recommended
                        else:
                            return False, str(replication_copy_time), str(analytics_load_time) if analytics_load_time is not None else None, False
                            
            except Exception as e:
                logger.error(f"Error checking for new records in replication for {table_name}: {str(e)}")
                # Fallback: avoid comparing datetime to None
                if analytics_load_time is not None and replication_copy_time is not None and (replication_copy_time > analytics_load_time or force_full_load_recommended):
                    return True, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                if analytics_load_time is None and replication_copy_time is not None:
                    # Table has data but no load timestamp - assume needs updating so load is attempted (row-count check will happen in load path)
                    return True, str(replication_copy_time), None, force_full_load_recommended
                return False, str(replication_copy_time) if replication_copy_time else None, str(analytics_load_time) if analytics_load_time else None, False
                
        except Exception as e:
            logger.error(f"Error checking if analytics needs updating for {table_name}: {str(e)}")
            return False, None, None, False
    
    def _execute_count_query(self, query: str) -> int:
        """
        Execute query and return row count.
        
        Helper for incremental fallback detection to check if incremental query returns 0 rows.
        """
        try:
            # Wrap the query to count rows at source (replication/MySQL)
            count_sql = f"SELECT COUNT(*) AS cnt FROM ({query}) AS subq"
            with self.replication_engine.connect() as conn:
                result = conn.execute(text(count_sql)).scalar()
                return int(result or 0)
        except Exception as e:
            logger.warning(f"Count query failed, defaulting to 0: {str(e)}")
            return 0
    
    def _update_load_status_hybrid(self, table_name: str, rows_loaded: int, **kwargs):
        """
        Update tracking with hybrid strategy (primary value tracking).
        """
        load_status = kwargs.get('load_status', 'success')
        last_timestamp: Optional[datetime] = kwargs.get('last_timestamp')
        last_primary_value: Optional[str] = kwargs.get('last_primary_value')
        primary_column_name: Optional[str] = kwargs.get('primary_column_name')
        try:
            # Determine stored value and column (ported simplified behavior)
            stored_value = None
            stored_column = 'hybrid'
            if last_timestamp and last_primary_value:
                timestamp_str = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                stored_value = f"{timestamp_str}|{last_primary_value}"
            elif last_timestamp:
                stored_value = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                stored_column = 'timestamp'
            elif last_primary_value:
                stored_value = str(last_primary_value)
                stored_column = primary_column_name or 'id'
            else:
                stored_value = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                stored_column = 'timestamp'
            with self.analytics_engine.connect() as conn:
                # For 0-row loads, don't advance _loaded_at timestamp
                if rows_loaded > 0:
                    conn.execute(text(f"""
                        INSERT INTO {self.analytics_schema}.etl_load_status 
                        (table_name, rows_loaded, load_status, last_primary_value, primary_column_name, _loaded_at)
                        VALUES (:table_name, :rows_loaded, :load_status, :last_primary_value, :primary_column_name, NOW())
                        ON CONFLICT (table_name) DO UPDATE SET
                            last_primary_value = :last_primary_value,
                            primary_column_name = :primary_column_name,
                            rows_loaded = :rows_loaded,
                            load_status = :load_status,
                            _loaded_at = NOW()
                    """), {
                        "table_name": table_name,
                        "rows_loaded": rows_loaded,
                        "load_status": load_status,
                        "last_primary_value": stored_value,
                        "primary_column_name": stored_column
                    })
                else:
                    # Do not advance timestamp on 0-row loads
                    # Preserve existing _loaded_at, last_primary_value, and primary_column_name when no rows were loaded
                    # Only update load_status and rows_loaded to indicate the table was checked but had no new data
                    # By omitting these fields from the UPDATE clause, PostgreSQL preserves their existing values
                    conn.execute(text(f"""
                        INSERT INTO {self.analytics_schema}.etl_load_status 
                        (table_name, rows_loaded, load_status, last_primary_value, primary_column_name, _loaded_at)
                        VALUES (:table_name, 0, :load_status, NULL, NULL, CURRENT_TIMESTAMP)
                        ON CONFLICT (table_name) DO UPDATE SET
                            rows_loaded = 0,
                            load_status = :load_status
                            -- _loaded_at, last_primary_value, and primary_column_name are omitted to preserve existing values
                            -- This prevents misleading timestamps when incremental loads find no new data
                    """), {
                        "table_name": table_name,
                        "load_status": load_status
                    })
                conn.commit()
            logger.info(f"Updated hybrid load status for {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error updating hybrid load status for {table_name}: {str(e)}")
            return False
    
    def _update_load_status(self, table_name: str, rows_loaded: int, status: str):
        """
        Update tracking with standard strategy (timestamp only).
        """
        try:
            with self.analytics_engine.connect() as conn:
                if rows_loaded > 0:
                    conn.execute(text(f"""
                        INSERT INTO {self.analytics_schema}.etl_load_status (
                            table_name, last_primary_value, primary_column_name,
                            rows_loaded, load_status, _loaded_at
                        ) VALUES (
                            :table_name, NULL, NULL,
                            :rows_loaded, :load_status, CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (table_name) DO UPDATE SET
                            rows_loaded = :rows_loaded,
                            load_status = :load_status,
                            _loaded_at = CURRENT_TIMESTAMP
                    """), {
                        "table_name": table_name,
                        "rows_loaded": rows_loaded,
                        "load_status": status
                    })
                    conn.commit()
                else:
                    # Do not advance timestamp on 0-row loads
                    conn.execute(text(f"""
                        INSERT INTO {self.analytics_schema}.etl_load_status (
                            table_name, last_primary_value, primary_column_name,
                            rows_loaded, load_status, _loaded_at
                        ) VALUES (
                            :table_name, NULL, NULL,
                            :rows_loaded, :load_status, COALESCE(
                                (SELECT _loaded_at FROM {self.analytics_schema}.etl_load_status WHERE table_name = :table_name),
                                CURRENT_TIMESTAMP
                            )
                        )
                        ON CONFLICT (table_name) DO UPDATE SET
                            rows_loaded = :rows_loaded,
                            load_status = :load_status
                    """), {
                        "table_name": table_name,
                        "rows_loaded": rows_loaded,
                        "load_status": status
                    })
                    conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating load status for {table_name}: {str(e)}")
            return False

    def _ensure_tracking_record_exists(self, table_name: str) -> bool:
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.analytics_schema}.etl_load_status 
                    WHERE table_name = :table_name
                """), {"table_name": table_name}).scalar()
                if result == 0:
                    conn.execute(text(f"""
                        INSERT INTO {self.analytics_schema}.etl_load_status (
                            table_name, last_primary_value, primary_column_name,
                            rows_loaded, load_status, _loaded_at
                        ) VALUES (
                            :table_name, NULL, NULL,
                            0, 'pending', CURRENT_TIMESTAMP
                        )
                    """), {"table_name": table_name})
                    conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error ensuring tracking record for {table_name}: {str(e)}")
            return False


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
3. CopyCSVLoadStrategy - large tables (>200MB); chunked strategy removed
4. ParallelLoadStrategy - 0% usage (future)

PHASE 4: Testing
- Unit tests for each strategy
- Integration tests with test database
- Performance benchmarks
- Stale state recovery tests (incremental fallback detection)

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

