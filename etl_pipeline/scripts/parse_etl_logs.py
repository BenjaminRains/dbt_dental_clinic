#!/usr/bin/env python3
"""
ETL Pipeline Log Parser

Parse and reorganize ETL pipeline log files, grouping logs by table name
for easier analysis.

Usage:
    python scripts/parse_etl_logs.py <log_file_path>
    python scripts/parse_etl_logs.py logs/etl_pipeline/etl_pipeline_run_20260101_151617.log
"""

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
        log_path = Path(log_file_path)
        
        # If path is relative, try multiple resolution strategies
        if not log_path.is_absolute():
            # Strategy 1: Try relative to current working directory first
            cwd_path = (Path.cwd() / log_path).resolve()
            if cwd_path.exists():
                log_path = cwd_path
            else:
                # Strategy 2: Try relative to project root
                script_file = Path(__file__).resolve()
                project_root = script_file.parent.parent.parent  # Go up 3 levels: scripts -> etl_pipeline -> project root
                log_path = (project_root / log_path).resolve()
        else:
            # Absolute path - just resolve it
            log_path = log_path.resolve()
        
        if not log_path.exists():
            raise FileNotFoundError(
                f"Log file not found: {log_file_path}\n"
                f"  Resolved path: {log_path}\n"
                f"  Current working directory: {Path.cwd()}\n"
                f"  Tried: {Path.cwd() / log_file_path} (from CWD) and {script_file.parent.parent.parent / log_file_path} (from project root)"
            )
        
        self.log_file_path = log_path
        self.tables: Dict[str, TableProcessingInfo] = {}
        self.system_logs: List[LogEntry] = []
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
                    
                    # Track errors
                    if entry.level in ('ERROR', 'CRITICAL') or 'error' in entry.message.lower() or 'failed' in entry.message.lower():
                        self.tables[table_name].errors.append(entry)
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
                        # Check for duplicate connection messages before adding (keep only first per table)
                        if not self._should_filter_duplicate_connection(entry, self.current_table_context):
                            self.tables[self.current_table_context].log_entries.append(entry)
                    else:
                        self.system_logs.append(entry)
        
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
        
        # Extract load phase metrics
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
    if len(sys.argv) < 2:
        print("Usage: python scripts/parse_etl_logs.py <log_file_path>")
        sys.exit(1)
    
    log_file_path = sys.argv[1]
    
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
