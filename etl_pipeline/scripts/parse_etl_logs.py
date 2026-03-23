#!/usr/bin/env python3
"""
ETL Pipeline Log Parser

Parse and reorganize ETL pipeline log files, grouping logs by table name
for easier analysis. Extracts and organizes all ERROR and WARNING messages
(ALL ERRORS and ALL WARNINGS sections with counts and chronological lists).

Usage:
    python scripts/parse_etl_logs.py <log_file_path>
    python scripts/parse_etl_logs.py -Filepath logs/etl_pipeline/etl_pipeline_run_20260101_151617.log
"""

import argparse
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class LogEntry:
    """Represents a single log entry."""
    timestamp: datetime
    logger: str
    level: str
    message: str
    raw_line: str
    table_name: Optional[str] = None
    phase: Optional[str] = None  # 'extract', 'load', 'system', 'error'


@dataclass
class TableProcessingInfo:
    """Tracks processing information for a table."""
    table_name: str
    category: Optional[str] = None  # 'large', 'medium', 'small', 'tiny'
    status: str = "incomplete"  # 'completed', 'failed', 'incomplete'
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_minutes: Optional[float] = None
    
    # Extract phase metrics
    extract_rows: Optional[int] = None
    extract_duration: Optional[float] = None
    extract_rate: Optional[float] = None  # rows/sec
    
    # Load phase metrics
    load_rows: Optional[int] = None
    load_duration: Optional[float] = None
    load_rate: Optional[float] = None  # rows/sec
    load_strategy: Optional[str] = None
    
    # Logs
    log_entries: List[LogEntry] = field(default_factory=list)
    errors: List[LogEntry] = field(default_factory=list)


class ETLLogParser:
    """
    Parse and reorganize ETL pipeline log files.
    
    Features:
    - Group logs by table name
    - Extract performance metrics
    - Generate summary reports
    """
    
    # Log line pattern: YYYY-MM-DD HH:MM:SS - logger - LEVEL - message
    LOG_PATTERN = re.compile(
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - ([^-]+) - (\w+) - (.+)'
    )
    
    # Table identification patterns
    TABLE_START_PATTERN = re.compile(r'Starting ETL pipeline for table: (\w+)')
    TABLE_PROCESSING_PATTERN = re.compile(r'Processing (\w+) \((\w+) category\)')
    TABLE_COMPLETE_PATTERN = re.compile(r'Successfully completed ETL pipeline for table: (\w+) in ([\d.]+) minutes')
    TABLE_PERF_PATTERN = re.compile(r'Performance metrics for (\w+):')
    TABLE_EXTRACT_PATTERN = re.compile(r'Successfully extracted (\w+)')
    TABLE_LOAD_PATTERN = re.compile(r'Successfully loaded (\w+):')
    # Summary line when load skipped (0 new rows): "document: 0 new rows in 0.1 min (replication and analytics in sync, load skipped)"
    TABLE_SUMMARY_SKIP_PATTERN = re.compile(r'^(\w+): 0 new rows in [\d.]+ min \(replication and analytics in sync')
    # Summary line when load completed with rows: "paysplit: 105 rows in 0.4 min (copy_csv, 5 rows/sec)"
    TABLE_SUMMARY_LOAD_PATTERN = re.compile(
        r'^(\w+): ([\d,]+) rows in ([\d.]+) min \((\w+), ([\d.]+) rows/sec\)'
    )
    TABLE_ERROR_PATTERN = re.compile(r'(?:ERROR|Failed|Exception|Error).*(?:for table: |table )(\w+)', re.IGNORECASE)
    # Additional patterns for table names in log messages
    TABLE_CHUNKED_PATTERN = re.compile(r'\[ChunkedStrategy\] (\w+):')  # "[ChunkedStrategy] table:"
    TABLE_BULK_INSERT_PATTERN = re.compile(r'Successfully bulk inserted .* for (\w+)')
    TABLE_COLUMN_PATTERN = re.compile(r'Column (\w+)\.')  # "Column table."
    TABLE_BATCH_PATTERN = re.compile(r'Batch \d+:.*\[(\w+)\]')  # "Batch 1: ... [table]"
    TABLE_COPY_STATUS_PATTERN = re.compile(r'(?:Updated copy status|copy status).*?for (\w+)')
    TABLE_PRE_COMMIT_PATTERN = re.compile(r'(?:Pre-commit|Post-commit) replication count for (\w+)')  # "Pre-commit replication count for table"
    TABLE_BULK_EXECUTEMANY_PATTERN = re.compile(r'Bulk executemany affected rowcount=.* for (\w+)')  # "Bulk executemany ... for table"
    
    # Metrics extraction patterns
    ROWS_PATTERN = re.compile(r'(\d{1,3}(?:,\d{3})*|\d+) rows')
    DURATION_SECONDS_PATTERN = re.compile(r'(\d+\.?\d*)s(?:econds?)?')
    ROWS_PER_SEC_PATTERN = re.compile(r'(\d{1,3}(?:,\d{3})*|\d+) rows/sec')
    STRATEGY_PATTERN = re.compile(r'using (\w+(?:_\w+)?) strategy')
    
    # Message filter patterns (messages to exclude from output)
    FILTER_PATTERNS = [
        re.compile(r'Replication target: host=localhost'),
    ]
    
    def __init__(self, log_file_path: str):
        """Initialize parser with log file path."""
        log_path = Path(log_file_path.strip())
        script_file = Path(__file__).resolve()
        project_root = script_file.parent.parent.parent  # scripts -> etl_pipeline -> project root
        tried: List[Path] = []

        # If path is relative, try multiple resolution strategies
        if not log_path.is_absolute():
            # Strategy 1: relative to current working directory
            cwd_path = (Path.cwd() / log_path).resolve()
            tried.append(cwd_path)
            if cwd_path.exists():
                log_path = cwd_path
            else:
                # Strategy 2: relative to project root
                root_path = (project_root / log_path).resolve()
                tried.append(root_path)
                if root_path.exists():
                    log_path = root_path
                elif len(log_path.parts) == 1:
                    # Strategy 3: filename only -> try known ETL log dirs (pipeline writes to etl_pipeline/logs/etl_pipeline/)
                    search_dirs = [
                        project_root / "logs" / "etl_pipeline",
                        project_root / "etl_pipeline" / "logs" / "etl_pipeline",  # actual pipeline default
                        Path.cwd() / "logs" / "etl_pipeline",
                        Path.cwd() / "etl_pipeline" / "logs" / "etl_pipeline",
                    ]
                    for log_dir in search_dirs:
                        default_log = (log_dir / log_path.name).resolve()
                        tried.append(default_log)
                        if default_log.exists():
                            log_path = default_log
                            break
                    else:
                        log_path = root_path  # use for error message
                else:
                    log_path = root_path
        else:
            log_path = log_path.resolve()
            tried.append(log_path)

        if not log_path.exists():
            tried_str = "\n  ".join(str(p) for p in tried)
            raise FileNotFoundError(
                f"Log file not found: {log_file_path.strip()!r}\n"
                f"  Resolved path: {log_path}\n"
                f"  Current working directory: {Path.cwd()}\n"
                f"  Tried:\n  {tried_str}"
            )
        
        self.log_file_path = log_path
        self.tables: Dict[str, TableProcessingInfo] = {}
        self.system_logs: List[LogEntry] = []
        self.all_errors: List[Tuple[Optional[str], LogEntry]] = []  # (table_name or None, entry)
        self.all_warnings: List[Tuple[Optional[str], LogEntry]] = []  # (table_name or None, entry)
        self.current_table_context: Optional[str] = None
        # Track connection messages per table (to keep only first occurrence)
        self.table_connection_messages_seen: Dict[str, set] = {}  # table_name -> set of connection message types
        
    def parse(self) -> Dict[str, TableProcessingInfo]:
        """
        Parse log file and group entries by table.
        
        Returns:
            Dictionary mapping table names to TableProcessingInfo objects
        """
        print(f"Parsing log file: {self.log_file_path}")
        
        with open(self.log_file_path, 'r', encoding='utf-8', errors='replace') as f:
            for line_num, line in enumerate(f, 1):
                line = line.rstrip()
                if not line:
                    continue
                
                entry = self._parse_log_line(line)
                if not entry:
                    continue
                
                # Filter out unwanted messages
                if self._should_filter_message(entry):
                    continue
                
                # Determine table context for this entry
                table_name = self._identify_table(entry)
                if table_name:
                    # Update current table context
                    if table_name not in self.tables:
                        self.tables[table_name] = TableProcessingInfo(table_name=table_name)
                    
                    entry.table_name = table_name
                    # Check for duplicate connection messages before adding (keep only first per table)
                    if not self._should_filter_duplicate_connection(entry, table_name):
                        self.tables[table_name].log_entries.append(entry)
                    self.current_table_context = table_name
                    
                    # Extract metrics and status
                    self._extract_table_info(entry, table_name)
                    
                    # Track errors (all errors also go to global all_errors)
                    if self._is_error_entry(entry):
                        self.tables[table_name].errors.append(entry)
                        self.all_errors.append((table_name, entry))
                    # Track warnings
                    if entry.level == 'WARNING':
                        self.all_warnings.append((table_name, entry))
                else:
                    # System log or unassociated log
                    # Only assign to current context if it's truly a generic log (no table mentions)
                    # Check if the message mentions any known table names to avoid mis-assignment
                    mentions_known_table = False
                    if self.current_table_context:
                        # Check if message mentions a different table explicitly
                        for known_table in self.tables.keys():
                            if known_table != self.current_table_context and known_table in entry.message:
                                # This log mentions a different table, don't assign to current context
                                mentions_known_table = True
                                break
                    
                    # Only assign to current context if:
                    # 1. We have a current context
                    # 2. The message doesn't explicitly mention a different table
                    # 3. It's a generic system log (connection, schema initialization, etc.)
                    if (self.current_table_context and 
                        self.current_table_context in self.tables and 
                        not mentions_known_table and
                        self._is_generic_log(entry)):
                        entry.table_name = self.current_table_context
                        ctx = self.current_table_context
                        # Check for duplicate connection messages before adding (keep only first per table)
                        if not self._should_filter_duplicate_connection(entry, ctx):
                            self.tables[ctx].log_entries.append(entry)
                        # Track errors (including those assigned to context)
                        if self._is_error_entry(entry):
                            self.tables[ctx].errors.append(entry)
                            self.all_errors.append((ctx, entry))
                        if entry.level == 'WARNING':
                            self.all_warnings.append((ctx, entry))
                    else:
                        self.system_logs.append(entry)
                        # Track unassociated system errors
                        if self._is_error_entry(entry):
                            self.all_errors.append((None, entry))
                        if entry.level == 'WARNING':
                            self.all_warnings.append((None, entry))
        
        # Finalize table statuses
        self._finalize_table_statuses()
        
        print(f"Parsed {len(self.tables)} tables from log file")
        return self.tables
    
    def _should_filter_message(self, entry: LogEntry) -> bool:
        """Check if a log entry should be filtered out (not included in output)."""
        for pattern in self.FILTER_PATTERNS:
            if pattern.search(entry.message):
                return True
        return False

    def _is_error_entry(self, entry: LogEntry) -> bool:
        """Check if a log entry represents an error (catches all error types)."""
        if entry.level in ('ERROR', 'CRITICAL'):
            return True
        msg = entry.message.lower()
        # Exclude known informational patterns (verification logs, not failures)
        if 'pre-commit replication count for' in msg or 'post-commit replication count for' in msg:
            return False
        error_indicators = [
            'error', 'failed', 'exception', 'traceback',
            'programmingerror', 'operationalerror', 'integrityerror',
            'sql syntax', 'syntax error',
        ]
        if any(ind in msg for ind in error_indicators):
            return True
        # MySQL error codes: match as whole words so "1062437" doesn't match "1062"
        if re.search(r'\b1064\b', msg) or re.search(r'\b1062\b', msg):
            return True
        return False
    
    def _should_filter_duplicate_connection(self, entry: LogEntry, table_name: str) -> bool:
        """
        Check if a connection message should be filtered (duplicate).
        Keep only the first connection message per table.
        
        Args:
            entry: The log entry to check
            table_name: The table this entry belongs to
        
        Returns:
            True if this is a duplicate connection message that should be filtered
        """
        # Check if this is a connection creation message
        if 'Successfully created' not in entry.message or 'connection' not in entry.message.lower():
            return False  # Not a connection message, don't filter
        
        # Initialize tracking for this table if needed
        if table_name not in self.table_connection_messages_seen:
            self.table_connection_messages_seen[table_name] = set()
        
        # Extract connection type (MySQL or PostgreSQL) to track separately
        if 'MySQL connection' in entry.message:
            connection_type = 'mysql'
        elif 'PostgreSQL connection' in entry.message:
            connection_type = 'postgresql'
        else:
            connection_type = 'other'
        
        # Check if we've seen this connection type for this table
        if connection_type in self.table_connection_messages_seen[table_name]:
            return True  # Filter duplicate - we've already seen this connection type for this table
        
        # Mark this connection type as seen for this table (keep first occurrence)
        self.table_connection_messages_seen[table_name].add(connection_type)
        return False  # Keep first occurrence
    
    def _is_generic_log(self, entry: LogEntry) -> bool:
        """Check if a log entry is a generic system log that could belong to any table."""
        generic_patterns = [
            'Successfully created',
            'initialized',
            'requires GLOBAL privileges',
            'Replication target:',
            'Environment validation',
            'Using unified',
            'Metrics tables',
            'Loaded configuration',
            'PerformanceOptimizations',
            'PostgresSchema initialized',
            'schema cache',
            'No table configurations',
        ]
        message_lower = entry.message.lower()
        return any(pattern.lower() in message_lower for pattern in generic_patterns)
    
    def _parse_log_line(self, line: str) -> Optional[LogEntry]:
        """Parse a single log line into a LogEntry object."""
        match = self.LOG_PATTERN.match(line)
        if not match:
            return None
        
        timestamp_str, logger, level, message = match.groups()
        
        try:
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
        
        return LogEntry(
            timestamp=timestamp,
            logger=logger.strip(),
            level=level.strip(),
            message=message.strip(),
            raw_line=line
        )
    
    def _identify_table(self, entry: LogEntry) -> Optional[str]:
        """Identify table name from log entry."""
        # Check for explicit table start
        match = self.TABLE_START_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        # Check for table in processing message
        match = self.TABLE_PROCESSING_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        # Check for table in completion message
        match = self.TABLE_COMPLETE_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        # Check for table in performance metrics
        match = self.TABLE_PERF_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        # Check for table in extract message
        match = self.TABLE_EXTRACT_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        # Check for table in load message
        match = self.TABLE_LOAD_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        # Check for table in chunked strategy messages
        match = self.TABLE_CHUNKED_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        # Check for table in load-skipped summary line ("table: 0 new rows in X min (replication and analytics in sync...)")
        match = self.TABLE_SUMMARY_SKIP_PATTERN.search(entry.message.strip())
        if match:
            return match.group(1)

        # Check for table in load summary line with rows ("table: N rows in X min (strategy, Y rows/sec)")
        match = self.TABLE_SUMMARY_LOAD_PATTERN.search(entry.message.strip())
        if match:
            return match.group(1)

        # Check for table in bulk insert messages
        match = self.TABLE_BULK_INSERT_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        # Check for table in column messages (e.g., "Column paysplit.IsDiscount")
        match = self.TABLE_COLUMN_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        # Check for table in copy status messages
        match = self.TABLE_COPY_STATUS_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        # Check for table in pre/post-commit messages
        match = self.TABLE_PRE_COMMIT_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        # Check for table in bulk executemany messages
        match = self.TABLE_BULK_EXECUTEMANY_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        # Check for table in error message
        match = self.TABLE_ERROR_PATTERN.search(entry.message)
        if match:
            return match.group(1)
        
        return None
    
    def _detect_phase_transition(self, message: str, current_phase: Optional[str]) -> Optional[str]:
        """
        Detect phase transitions based on strong markers.
        
        Only returns a new phase if there's a clear transition marker.
        Generic system messages (privilege warnings, etc.) return None
        to maintain current phase.
        
        Args:
            message: The log message
            current_phase: Current phase state
            
        Returns:
            New phase name if transition detected, None to keep current phase
        """
        message_lower = message.lower()
        
        # Completion phase markers (strong - always transitions)
        if any(marker in message_lower for marker in [
            'successfully completed etl pipeline'
        ]):
            return 'completion'
        
        # Strong load phase transition markers
        if any(marker in message_lower for marker in [
            'successfully loaded',
            'loading to analytics',
            'successfully bulk inserted',
            '[chunkedstrategy]'
        ]):
            return 'load'
        
        # Strong extract phase transition markers
        if any(marker in message_lower for marker in [
            'successfully extracted',
            'extracting to replication',
            'bulk executemany',
            'pre-commit replication count',
            'post-commit replication count'
        ]):
            return 'extract'
        
        # Generic system messages (privilege warnings, connections, etc.)
        # don't indicate phase transitions - return None to keep current phase
        generic_markers = [
            'requires global privileges',
            'replication target:',
            'successfully created',
            'initialized',
            'loaded configuration',
            'environment validation',
            'column .* identified as',
            'performance metrics for',
            'updated copy status',
            'postgresschema initialized',
            'initialized schema cache',
            'no table configurations provided'
        ]
        if any(marker in message_lower for marker in generic_markers):
            return None  # Keep current phase
        
        # If we have a current phase, keep it for unknown messages
        # Otherwise default to initialization
        if current_phase:
            return None  # Keep current phase
        else:
            return 'initialization'  # Default for first messages
    
    def _extract_table_info(self, entry: LogEntry, table_name: str):
        """Extract table processing information from log entry."""
        info = self.tables[table_name]
        
        # Extract category from processing message
        match = self.TABLE_PROCESSING_PATTERN.search(entry.message)
        if match:
            info.category = match.group(2)
            if not info.start_time:
                info.start_time = entry.timestamp
        
        # Extract start time
        if self.TABLE_START_PATTERN.search(entry.message):
            info.start_time = entry.timestamp
        
        # Extract completion info
        match = self.TABLE_COMPLETE_PATTERN.search(entry.message)
        if match:
            info.status = "completed"
            info.end_time = entry.timestamp
            try:
                info.duration_minutes = float(match.group(2))
            except ValueError:
                pass
        
        # Extract extract phase metrics
        if "Performance metrics for" in entry.message and table_name in entry.message:
            # Extract rows
            rows_match = self.ROWS_PATTERN.search(entry.message)
            if rows_match:
                rows_str = rows_match.group(1).replace(',', '')
                try:
                    info.extract_rows = int(rows_str)
                except ValueError:
                    pass
            
            # Extract duration
            duration_match = self.DURATION_SECONDS_PATTERN.search(entry.message)
            if duration_match:
                try:
                    info.extract_duration = float(duration_match.group(1))
                except ValueError:
                    pass
            
            # Extract rate
            rate_match = self.ROWS_PER_SEC_PATTERN.search(entry.message)
            if rate_match:
                rate_str = rate_match.group(1).replace(',', '')
                try:
                    info.extract_rate = float(rate_str)
                except ValueError:
                    pass
        
        # Extract load phase metrics from "Successfully loaded" message
        if "Successfully loaded" in entry.message and table_name in entry.message:
            # Extract rows
            rows_match = self.ROWS_PATTERN.search(entry.message)
            if rows_match:
                rows_str = rows_match.group(1).replace(',', '')
                try:
                    info.load_rows = int(rows_str)
                except ValueError:
                    pass
            
            # Extract duration
            duration_match = self.DURATION_SECONDS_PATTERN.search(entry.message)
            if duration_match:
                try:
                    info.load_duration = float(duration_match.group(1))
                except ValueError:
                    pass
            
            # Extract rate
            rate_match = self.ROWS_PER_SEC_PATTERN.search(entry.message)
            if rate_match:
                rate_str = rate_match.group(1).replace(',', '')
                try:
                    info.load_rate = float(rate_str)
                except ValueError:
                    pass
            
            # Extract strategy
            strategy_match = self.STRATEGY_PATTERN.search(entry.message)
            if strategy_match:
                info.load_strategy = strategy_match.group(1)

        # Extract load-skipped summary: "table: 0 new rows in X min (replication and analytics in sync, load skipped)"
        if self.TABLE_SUMMARY_SKIP_PATTERN.search(entry.message.strip()) and table_name in entry.message:
            info.load_rows = 0
            info.load_strategy = "skipped_no_new_data"
            info.status = "completed"
            info.end_time = entry.timestamp
            duration_match = re.search(r'0 new rows in ([\d.]+) min', entry.message)
            if duration_match:
                try:
                    info.load_duration = float(duration_match.group(1)) * 60.0  # min -> seconds
                except ValueError:
                    pass

        # Extract load summary with rows: "table: N rows in X min (strategy, Y rows/sec)"
        load_summary_match = self.TABLE_SUMMARY_LOAD_PATTERN.search(entry.message.strip())
        if load_summary_match and table_name in entry.message:
            try:
                info.load_rows = int(load_summary_match.group(2).replace(',', ''))
                info.load_duration = float(load_summary_match.group(3)) * 60.0  # min -> seconds
                info.load_strategy = load_summary_match.group(4)
                info.load_rate = float(load_summary_match.group(5))
                info.status = "completed"
                info.end_time = entry.timestamp
            except (ValueError, IndexError):
                pass
    
    def _finalize_table_statuses(self):
        """Finalize table statuses after parsing."""
        for table_name, info in self.tables.items():
            # If we have errors but no completion, mark as failed
            if info.errors and info.status == "incomplete":
                info.status = "failed"
            
            # Calculate duration if we have start and end times
            if info.start_time and info.end_time:
                delta = info.end_time - info.start_time
                info.duration_minutes = delta.total_seconds() / 60.0
            elif info.start_time and not info.duration_minutes:
                # If we have start time but no end time, we can't calculate duration
                pass
    
    def _build_errors_section(self) -> List[str]:
        """Build errors section: all errors organized at top for quick review."""
        lines = []
        if not self.all_errors:
            return lines
        lines.append("=" * 80)
        lines.append("ALL ERRORS")
        lines.append("=" * 80)
        lines.append("")
        # Sort by timestamp, then by table (None last)
        sorted_errors = sorted(
            self.all_errors,
            key=lambda x: (x[1].timestamp, x[0] or "")
        )
        for table_name, entry in sorted_errors:
            timestamp_str = entry.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            table_prefix = f"[{table_name}] " if table_name else "[system] "
            lines.append(f"  {timestamp_str} {table_prefix}{entry.message}")
        lines.append("")
        lines.append("=" * 80)
        lines.append("")
        return lines

    @staticmethod
    def _normalize_warning_message(msg: str) -> str:
        """Normalize a warning message so similar messages (e.g. differing only by table name) group together."""
        s = msg
        # "Could not track performance metrics for <table>:" or "for <table>_load:"
        s = re.sub(r'\bfor\s+(\w+)(_load)?:\s*', r'for *\2: ', s)
        # "Performance below threshold for <table>: N records/sec"
        s = re.sub(r':\s*\d+(,\d+)*\s*records/sec', r': * records/sec', s)
        return s

    def _build_warnings_section(self) -> List[str]:
        """Build warnings section: grouped by normalized pattern, by message type, then chronological list."""
        lines = []
        if not self.all_warnings:
            return lines
        lines.append("=" * 80)
        lines.append("ALL WARNINGS")
        lines.append("=" * 80)
        lines.append("")

        # Group by exact message: (message -> [(table_name, entry), ...])
        by_message: Dict[str, List[Tuple[Optional[str], LogEntry]]] = defaultdict(list)
        for table_name, entry in self.all_warnings:
            by_message[entry.message].append((table_name, entry))

        # Group by normalized pattern for a compact summary
        by_pattern: Dict[str, List[Tuple[Optional[str], LogEntry]]] = defaultdict(list)
        for table_name, entry in self.all_warnings:
            pattern = self._normalize_warning_message(entry.message)
            by_pattern[pattern].append((table_name, entry))

        # Summary
        lines.append(f"Total: {len(self.all_warnings)} warning(s), {len(by_message)} unique message type(s), {len(by_pattern)} pattern(s)")
        lines.append("")

        # By normalized pattern (collapses table names / numbers so you see few lines)
        lines.append("By pattern (normalized; table names and numbers collapsed):")
        lines.append("-" * 80)
        sorted_patterns = sorted(
            by_pattern.items(),
            key=lambda x: len(x[1]),
            reverse=True
        )
        for pattern, occurrences in sorted_patterns[:30]:  # top 30 patterns
            count = len(occurrences)
            _, entry = occurrences[0]
            ts = entry.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            lines.append(f"  [{count:5}x] {ts}  {pattern}")
        if len(sorted_patterns) > 30:
            lines.append(f"  ... and {len(sorted_patterns) - 30} more pattern(s)")
        lines.append("")

        # By message type (count + one example)
        lines.append("By message type (exact; count then one example):")
        lines.append("-" * 80)
        # Sort by count descending so most frequent appear first
        sorted_groups = sorted(
            by_message.items(),
            key=lambda x: len(x[1]),
            reverse=True
        )
        for message, occurrences in sorted_groups:
            count = len(occurrences)
            table_name, entry = occurrences[0]
            timestamp_str = entry.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            table_prefix = f"[{table_name}] " if table_name else "[system] "
            lines.append(f"  [{count:4}x] {timestamp_str} {table_prefix}{message}")
        lines.append("")

        # Chronological list (truncated when very long)
        lines.append("Chronological list:")
        lines.append("-" * 80)
        sorted_warnings = sorted(
            self.all_warnings,
            key=lambda x: (x[1].timestamp, x[0] or "")
        )
        max_chrono = 500
        to_show = sorted_warnings if len(sorted_warnings) <= max_chrono else sorted_warnings[:max_chrono]
        for table_name, entry in to_show:
            timestamp_str = entry.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            table_prefix = f"[{table_name}] " if table_name else "[system] "
            lines.append(f"  {timestamp_str} {table_prefix}{entry.message}")
        if len(sorted_warnings) > max_chrono:
            lines.append(f"  ... ({len(sorted_warnings) - max_chrono} more; see raw log for full list)")
        lines.append("")
        lines.append("=" * 80)
        lines.append("")
        return lines

    def _build_summary_lines(self) -> List[str]:
        """Build summary block lines: category counts, status, time window, slowest tables."""
        lines = []
        lines.append("=" * 80)
        lines.append("ETL PIPELINE RUN SUMMARY")
        lines.append("=" * 80)
        lines.append("")

        n = len(self.tables)
        lines.append(f"Tables processed: {n}")
        if self.all_errors:
            lines.append(f"Errors: {len(self.all_errors)} (see ALL ERRORS section above)")
        if self.all_warnings:
            lines.append(f"Warnings: {len(self.all_warnings)} (see ALL WARNINGS section above)")
        completed = sum(1 for info in self.tables.values() if info.status == "completed")
        failed = sum(1 for info in self.tables.values() if info.status == "failed")
        incomplete = sum(1 for info in self.tables.values() if info.status == "incomplete")
        if failed == 0 and incomplete == 0:
            lines.append("Status: All completed")
        else:
            parts = []
            if completed:
                parts.append(f"{completed} completed")
            if failed:
                parts.append(f"{failed} failed")
            if incomplete:
                parts.append(f"{incomplete} incomplete")
            lines.append(f"Status: {', '.join(parts)}")
        lines.append("")

        # Time window
        start_times = [info.start_time for info in self.tables.values() if info.start_time]
        end_times = [info.end_time for info in self.tables.values() if info.end_time]
        if start_times and end_times:
            first_ts = min(start_times)
            last_ts = max(end_times)
            total_mins = (last_ts - first_ts).total_seconds() / 60.0
            lines.append(f"Time window: {first_ts.strftime('%Y-%m-%d %H:%M:%S')} -> {last_ts.strftime('%Y-%m-%d %H:%M:%S')} ({total_mins:.1f} minutes)")
        else:
            lines.append("Time window: (could not determine from log)")
        lines.append("")

        # Category breakdown
        by_cat = defaultdict(int)
        for info in self.tables.values():
            cat = (info.category or "unknown").lower()
            by_cat[cat] += 1
        lines.append("Category breakdown:")
        for cat in ("large", "medium", "small", "tiny"):
            count = by_cat.get(cat, 0)
            notes = ""
            if cat == "large":
                notes = " (highest volume tables)"
            elif cat == "tiny":
                notes = " (fast, small datasets)"
            lines.append(f"  {cat:8} {count:4}  {notes}")
        if by_cat.get("unknown", 0):
            lines.append(f"  {'unknown':8} {by_cat['unknown']:4}")
        lines.append("")

        # Slowest tables (top 10 by duration_minutes)
        with_duration = [(info.table_name, info.duration_minutes, info.category) for info in self.tables.values() if info.duration_minutes]
        with_duration.sort(key=lambda x: x[1], reverse=True)
        top = with_duration[:10]
        if top:
            lines.append("Slowest tables (by duration):")
            for name, mins, cat in top:
                cat_str = cat or "?"
                lines.append(f"  {name:25} {mins:.2f} min  ({cat_str})")
        lines.append("")

        # Note about load metrics gap if many tables lack load metrics
        no_load_metrics = sum(1 for info in self.tables.values() if info.load_rows is None)
        if no_load_metrics > 0:
            lines.append(f"Note: {no_load_metrics} table(s) have 'Load: No metrics available' (load completed; parser did not capture load line).")
            lines.append("")

        lines.append("=" * 80)
        lines.append("")
        return lines

    def output_grouped(self, output_path: Optional[str] = None) -> str:
        """
        Output logs grouped by table (Format 1).
        
        Args:
            output_path: Optional output file path. If not provided, uses same
                        directory as input with _parsed suffix.
        
        Returns:
            Path to output file
        """
        if output_path is None:
            # Same directory as input with _parsed suffix
            output_path = self.log_file_path.parent / f"{self.log_file_path.stem}_parsed.txt"
        else:
            output_path = Path(output_path)
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"Writing grouped output to: {output_path}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            # Write errors at top (if any)
            for line in self._build_errors_section():
                f.write(line + "\n")
            # Write warnings section (organized by message type, then chronological)
            for line in self._build_warnings_section():
                f.write(line + "\n")
            # Write summary block
            for line in self._build_summary_lines():
                f.write(line + "\n")

            # Sort tables by start time (or table name if no start time)
            sorted_tables = sorted(
                self.tables.items(),
                key=lambda x: (x[1].start_time or datetime.min, x[0])
            )
            
            for table_name, info in sorted_tables:
                self._write_table_section(f, info)
        
        print(f"Output written to: {output_path}")
        return str(output_path)
    
    def _write_table_section(self, f, info: TableProcessingInfo):
        """Write a table section to the output file."""
        # Header
        f.write("=" * 80 + "\n")
        f.write(f"TABLE: {info.table_name}\n")
        
        # Status line
        category_str = info.category or "unknown"
        status_icon = "✅" if info.status == "completed" else "❌" if info.status == "failed" else "⏳"
        duration_str = f"{info.duration_minutes:.2f} minutes" if info.duration_minutes else "unknown"
        f.write(f"Category: {category_str} | Status: {status_icon} {info.status.title()} | Duration: {duration_str}\n")
        f.write("=" * 80 + "\n\n")
        
        # Log entries (all logs for this table) with phase separators
        current_phase = None
        for entry in info.log_entries:
            # Detect phase transitions (only changes on strong markers)
            new_phase = self._detect_phase_transition(entry.message, current_phase)
            
            # Update phase only if a transition was detected
            if new_phase is not None:
                phase = new_phase
            else:
                # Keep current phase, or default to initialization if none set
                phase = current_phase if current_phase else 'initialization'
            
            # Insert phase separator comment if phase changed
            if phase != current_phase:
                if current_phase is not None:
                    f.write("\n")  # Blank line before new phase
                f.write(f"# {'-' * 76}\n")
                f.write(f"# {phase.upper()} PHASE\n")
                f.write(f"# {'-' * 76}\n\n")
                current_phase = phase
            
            timestamp_str = entry.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"[{timestamp_str}] {entry.message}\n")
        
        # Metrics section
        f.write("\nMETRICS:\n")
        if info.extract_rows is not None:
            extract_str = f"  Extract: {info.extract_rows:,} rows"
            if info.extract_duration:
                extract_str += f" in {info.extract_duration:.2f}s"
            if info.extract_rate:
                extract_str += f" ({info.extract_rate:,.0f} rows/sec)"
            f.write(extract_str + "\n")
        else:
            f.write("  Extract: No metrics available\n")
        
        if info.load_rows is not None:
            load_str = f"  Load:    {info.load_rows:,} rows"
            if info.load_duration:
                load_str += f" in {info.load_duration:.2f}s"
            if info.load_rate:
                load_str += f" ({info.load_rate:,.0f} rows/sec)"
            if info.load_strategy:
                load_str += f" using {info.load_strategy} strategy"
            f.write(load_str + "\n")
        else:
            f.write("  Load:    No metrics available\n")
        
        if info.duration_minutes:
            f.write(f"  Total:   {info.duration_minutes:.2f} minutes\n")
        
        # Errors section
        if info.errors:
            f.write("\nERRORS:\n")
            for error in info.errors:
                timestamp_str = error.timestamp.strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"  [{timestamp_str}] {error.message}\n")
        
        f.write("\n\n")


def main():
    """Main entry point."""
    arg_parser = argparse.ArgumentParser(
        description="Parse ETL pipeline log files and output grouped report with errors and warnings."
    )
    arg_parser.add_argument(
        "log_file_path",
        nargs="?",
        default=None,
        help="Path to the ETL log file",
    )
    arg_parser.add_argument(
        "-Filepath", "--filepath",
        dest="filepath",
        default=None,
        help="Path to the ETL log file (alternative to positional argument)",
    )
    args = arg_parser.parse_args()
    log_file_path = args.filepath or args.log_file_path
    if not log_file_path:
        arg_parser.error("Log file path required. Use: parse_etl_logs.py <path> or -Filepath <path>")
    log_file_path = log_file_path.strip()
    try:
        parser = ETLLogParser(log_file_path)
        parser.parse()
        output_path = parser.output_grouped()
        print(f"\n✓ Successfully parsed log file")
        print(f"  Input:  {log_file_path}")
        print(f"  Output: {output_path}")
        print(f"  Tables: {len(parser.tables)}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
