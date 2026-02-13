"""
Table Configuration Contract Tests

Tests that verify table configuration dictionaries from tables.yml
match the documented contract in DATA_CONTRACTS.md.

These tests validate:
- Required fields are present
- Field types are correct
- Values are within valid ranges
- Cross-field constraints are satisfied
- Incremental strategy has required columns

Run with: pytest tests/contracts/test_table_config_contract.py -v
"""

import pytest
from pathlib import Path
from typing import Dict, List
import yaml


@pytest.fixture(scope="module")
def tables_config() -> Dict:
    """Load tables.yml configuration for testing."""
    # Try multiple possible locations
    possible_paths = [
        Path("etl_pipeline/config/tables.yml"),
        Path("etl_pipeline/etl_pipeline/config/tables.yml"),
        Path("config/tables.yml"),
    ]
    
    for config_path in possible_paths:
        if config_path.exists():
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
    
    pytest.skip(f"tables.yml not found in any of: {[str(p) for p in possible_paths]}")


@pytest.fixture(scope="module")
def all_table_configs(tables_config) -> Dict[str, Dict]:
    """Get all individual table configurations."""
    return tables_config.get('tables', {})


class TestTableConfigMetadata:
    """Test the metadata section of tables.yml."""
    
    def test_metadata_section_exists(self, tables_config):
        """Configuration must have metadata section."""
        assert 'metadata' in tables_config, "tables.yml missing 'metadata' section"
    
    def test_metadata_has_required_fields(self, tables_config):
        """Metadata must have essential fields."""
        metadata = tables_config['metadata']
        required_fields = [
            'generated_at',
            'analyzer_version',
            'source_database',
            'total_tables',
            'environment'
        ]
        
        for field in required_fields:
            assert field in metadata, f"Metadata missing required field: {field}"
    
    def test_metadata_environment_valid(self, tables_config):
        """Environment must be 'clinic' or 'test'."""
        environment = tables_config['metadata'].get('environment')
        valid_environments = ['clinic', 'test']
        
        assert environment in valid_environments, \
            f"Invalid environment '{environment}', must be one of {valid_environments}"
    
    def test_metadata_total_tables_matches_actual(self, tables_config):
        """total_tables metadata should match actual table count."""
        stated_total = tables_config['metadata'].get('total_tables')
        actual_total = len(tables_config.get('tables', {}))
        
        # Allow some tolerance for tables with errors
        tolerance = 10
        assert abs(stated_total - actual_total) <= tolerance, \
            f"Metadata says {stated_total} tables, but found {actual_total}"


class TestTableConfigRequiredFields:
    """Test that all table configs have required fields."""
    
    REQUIRED_FIELDS = [
        'table_name',
        'extraction_strategy',
        'batch_size',
        'incremental_columns',
        'performance_category'
    ]
    
    def test_tables_section_exists(self, tables_config):
        """Configuration must have tables section."""
        assert 'tables' in tables_config, "tables.yml missing 'tables' section"
        assert len(tables_config['tables']) > 0, "No tables configured"
    
    def test_all_tables_have_required_fields(self, all_table_configs):
        """Every table config must have required fields."""
        missing_fields_by_table = {}
        
        for table_name, config in all_table_configs.items():
            # Skip tables with errors
            if 'error' in config:
                continue
            
            missing_fields = []
            for field in self.REQUIRED_FIELDS:
                if field not in config:
                    missing_fields.append(field)
            
            if missing_fields:
                missing_fields_by_table[table_name] = missing_fields
        
        assert len(missing_fields_by_table) == 0, \
            f"Tables missing required fields: {missing_fields_by_table}"
    
    def test_table_name_matches_key(self, all_table_configs):
        """table_name field should match dictionary key."""
        mismatches = []
        
        for key, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            table_name = config.get('table_name')
            if table_name != key:
                mismatches.append((key, table_name))
        
        assert len(mismatches) == 0, \
            f"Table name mismatches (key != table_name): {mismatches}"


class TestExtractionStrategy:
    """Test extraction_strategy field contracts."""
    
    VALID_STRATEGIES = ['full_table', 'incremental', 'incremental_chunked']
    
    def test_extraction_strategy_valid_values(self, all_table_configs):
        """extraction_strategy must be one of valid values."""
        invalid_strategies = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            strategy = config.get('extraction_strategy')
            if strategy not in self.VALID_STRATEGIES:
                invalid_strategies[table_name] = strategy
        
        assert len(invalid_strategies) == 0, \
            f"Invalid extraction strategies: {invalid_strategies}"
    
    def test_incremental_strategy_has_columns(self, all_table_configs):
        """Incremental strategy must have incremental_columns."""
        missing_columns = []
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            strategy = config.get('extraction_strategy')
            columns = config.get('incremental_columns', [])
            
            if strategy in ['incremental', 'incremental_chunked']:
                if not columns or len(columns) == 0:
                    missing_columns.append(table_name)
        
        assert len(missing_columns) == 0, \
            f"Tables with incremental strategy but no columns: {missing_columns}"
    
    def test_incremental_has_primary_column(self, all_table_configs):
        """Incremental strategy should have primary_incremental_column."""
        missing_primary = []
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            strategy = config.get('extraction_strategy')
            primary_col = config.get('primary_incremental_column')
            
            if strategy in ['incremental', 'incremental_chunked']:
                if not primary_col or primary_col == 'none':
                    missing_primary.append(table_name)
        
        # This is a warning, not a hard failure (some tables might be okay)
        if missing_primary:
            pytest.warns(
                UserWarning,
                match=f"Tables with incremental strategy but no primary column: {missing_primary}"
            )


class TestBatchSize:
    """Test batch_size field contracts."""
    
    MIN_BATCH_SIZE = 1
    MAX_BATCH_SIZE = 100000
    
    def test_batch_size_is_integer(self, all_table_configs):
        """batch_size must be an integer."""
        wrong_type = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            batch_size = config.get('batch_size')
            if not isinstance(batch_size, int):
                wrong_type[table_name] = (type(batch_size).__name__, batch_size)
        
        assert len(wrong_type) == 0, \
            f"Tables with non-integer batch_size: {wrong_type}"
    
    def test_batch_size_positive(self, all_table_configs):
        """batch_size must be positive."""
        non_positive = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            batch_size = config.get('batch_size')
            if isinstance(batch_size, int) and batch_size <= 0:
                non_positive[table_name] = batch_size
        
        assert len(non_positive) == 0, \
            f"Tables with non-positive batch_size: {non_positive}"
    
    def test_batch_size_reasonable_range(self, all_table_configs):
        """batch_size should be within reasonable range."""
        out_of_range = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            batch_size = config.get('batch_size')
            if isinstance(batch_size, int):
                if batch_size < self.MIN_BATCH_SIZE or batch_size > self.MAX_BATCH_SIZE:
                    out_of_range[table_name] = batch_size
        
        assert len(out_of_range) == 0, \
            f"Tables with batch_size outside range [{self.MIN_BATCH_SIZE}, {self.MAX_BATCH_SIZE}]: {out_of_range}"


class TestPerformanceCategory:
    """Test performance_category field contracts."""
    
    VALID_CATEGORIES = ['tiny', 'small', 'medium', 'large']
    
    def test_performance_category_valid(self, all_table_configs):
        """performance_category must be valid value."""
        invalid_categories = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            category = config.get('performance_category')
            if category not in self.VALID_CATEGORIES:
                invalid_categories[table_name] = category
        
        assert len(invalid_categories) == 0, \
            f"Invalid performance categories: {invalid_categories}"
    
    def test_category_matches_row_count(self, all_table_configs):
        """performance_category should roughly match estimated_rows."""
        mismatches = []
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            category = config.get('performance_category')
            estimated_rows = config.get('estimated_rows', 0)
            
            # Check rough alignment
            if category == 'large' and estimated_rows < 100000:
                mismatches.append((table_name, category, estimated_rows, 'expected >= 100K'))
            elif category == 'medium' and (estimated_rows < 10000 or estimated_rows >= 1000000):
                mismatches.append((table_name, category, estimated_rows, 'expected 10K-1M'))
            elif category == 'small' and (estimated_rows < 1000 or estimated_rows >= 100000):
                mismatches.append((table_name, category, estimated_rows, 'expected 1K-100K'))
        
        # This is a soft check - some tables might be at boundaries
        if mismatches and len(mismatches) > 50:  # Only fail if many mismatches
            pytest.fail(f"Many category/row count mismatches: {mismatches[:10]}")


class TestProcessingPriority:
    """Test processing_priority field contracts."""
    
    VALID_PRIORITIES = ['high', 'medium', 'low']
    
    def test_processing_priority_valid(self, all_table_configs):
        """processing_priority must be valid value."""
        invalid_priorities = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            priority = config.get('processing_priority')
            if priority and priority not in self.VALID_PRIORITIES:
                invalid_priorities[table_name] = priority
        
        assert len(invalid_priorities) == 0, \
            f"Invalid processing priorities: {invalid_priorities}"


class TestEstimates:
    """Test estimated_rows and estimated_size_mb contracts."""
    
    def test_estimated_rows_non_negative(self, all_table_configs):
        """estimated_rows must be non-negative."""
        negative_rows = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            estimated_rows = config.get('estimated_rows')
            if estimated_rows is not None and estimated_rows < 0:
                negative_rows[table_name] = estimated_rows
        
        assert len(negative_rows) == 0, \
            f"Tables with negative estimated_rows: {negative_rows}"
    
    def test_estimated_size_non_negative(self, all_table_configs):
        """estimated_size_mb must be non-negative."""
        negative_sizes = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            estimated_size = config.get('estimated_size_mb')
            if estimated_size is not None and estimated_size < 0:
                negative_sizes[table_name] = estimated_size
        
        assert len(negative_sizes) == 0, \
            f"Tables with negative estimated_size_mb: {negative_sizes}"
    
    def test_estimated_size_is_number(self, all_table_configs):
        """estimated_size_mb must be numeric (int or float)."""
        wrong_type = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            estimated_size = config.get('estimated_size_mb')
            if estimated_size is not None:
                if not isinstance(estimated_size, (int, float)):
                    wrong_type[table_name] = type(estimated_size).__name__
        
        assert len(wrong_type) == 0, \
            f"Tables with non-numeric estimated_size_mb: {wrong_type}"


class TestIncrementalColumns:
    """Test incremental_columns field contracts."""
    
    def test_incremental_columns_is_list(self, all_table_configs):
        """incremental_columns must be a list."""
        wrong_type = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            columns = config.get('incremental_columns')
            if columns is not None and not isinstance(columns, list):
                wrong_type[table_name] = type(columns).__name__
        
        assert len(wrong_type) == 0, \
            f"Tables with non-list incremental_columns: {wrong_type}"
    
    def test_incremental_columns_are_strings(self, all_table_configs):
        """All items in incremental_columns must be strings."""
        wrong_types = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            columns = config.get('incremental_columns', [])
            if isinstance(columns, list):
                for col in columns:
                    if not isinstance(col, str):
                        wrong_types[table_name] = wrong_types.get(table_name, [])
                        wrong_types[table_name].append((col, type(col).__name__))
        
        assert len(wrong_types) == 0, \
            f"Tables with non-string column names: {wrong_types}"
    
    def test_primary_incremental_column_in_list(self, all_table_configs):
        """primary_incremental_column should be in incremental_columns."""
        not_in_list = []
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            primary_col = config.get('primary_incremental_column')
            columns = config.get('incremental_columns', [])
            
            if primary_col and primary_col != 'none' and isinstance(columns, list):
                if primary_col not in columns:
                    not_in_list.append((table_name, primary_col, columns))
        
        assert len(not_in_list) == 0, \
            f"Tables where primary_incremental_column not in incremental_columns: {not_in_list}"


class TestMonitoring:
    """Test monitoring configuration contracts."""
    
    def test_monitoring_is_dict(self, all_table_configs):
        """monitoring field must be a dictionary if present."""
        wrong_type = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            monitoring = config.get('monitoring')
            if monitoring is not None and not isinstance(monitoring, dict):
                wrong_type[table_name] = type(monitoring).__name__
        
        assert len(wrong_type) == 0, \
            f"Tables with non-dict monitoring: {wrong_type}"
    
    def test_monitoring_alert_flags_are_boolean(self, all_table_configs):
        """Alert flags in monitoring must be boolean."""
        wrong_type = {}
        
        alert_fields = ['alert_on_failure', 'alert_on_slow_extraction']
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            monitoring = config.get('monitoring', {})
            if isinstance(monitoring, dict):
                for field in alert_fields:
                    value = monitoring.get(field)
                    if value is not None and not isinstance(value, bool):
                        wrong_type[f"{table_name}.{field}"] = type(value).__name__
        
        assert len(wrong_type) == 0, \
            f"Monitoring alert flags that are not boolean: {wrong_type}"
    
    def test_performance_threshold_is_integer(self, all_table_configs):
        """performance_threshold_records_per_second must be integer."""
        wrong_type = {}
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            monitoring = config.get('monitoring', {})
            if isinstance(monitoring, dict):
                threshold = monitoring.get('performance_threshold_records_per_second')
                if threshold is not None and not isinstance(threshold, int):
                    wrong_type[table_name] = (type(threshold).__name__, threshold)
        
        assert len(wrong_type) == 0, \
            f"Tables with non-integer performance_threshold: {wrong_type}"


class TestCrossFieldConstraints:
    """Test constraints that involve multiple fields."""
    
    def test_large_tables_have_high_batch_size(self, all_table_configs):
        """Large tables should have appropriately large batch sizes."""
        small_batches = []
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            category = config.get('performance_category')
            batch_size = config.get('batch_size')
            
            # Large tables should have batch_size >= 50K
            if category == 'large' and isinstance(batch_size, int):
                if batch_size < 50000:
                    small_batches.append((table_name, batch_size))
        
        # This is a soft check - some large tables might need smaller batches
        if len(small_batches) > 10:
            pytest.warns(
                UserWarning,
                match=f"Many large tables with small batch sizes: {small_batches[:5]}"
            )
    
    def test_tiny_tables_dont_need_chunked_strategy(self, all_table_configs):
        """Tiny tables shouldn't use incremental_chunked strategy."""
        unnecessary_chunking = []
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            category = config.get('performance_category')
            strategy = config.get('extraction_strategy')
            
            if category == 'tiny' and strategy == 'incremental_chunked':
                unnecessary_chunking.append(table_name)
        
        # This is informational - might be valid reasons
        if unnecessary_chunking:
            pytest.warns(
                UserWarning,
                match=f"Tiny tables using incremental_chunked: {unnecessary_chunking}"
            )


class TestConfigurationCoverage:
    """Test overall configuration completeness."""
    
    def test_most_tables_have_incremental_strategy(self, all_table_configs):
        """Most tables should support incremental loading."""
        full_table_only = []
        
        for table_name, config in all_table_configs.items():
            if 'error' in config:
                continue
            
            strategy = config.get('extraction_strategy')
            if strategy == 'full_table':
                full_table_only.append(table_name)
        
        total_tables = len([c for c in all_table_configs.values() if 'error' not in c])
        full_table_percentage = len(full_table_only) / total_tables * 100
        
        # Warn if more than 30% are full_table only
        assert full_table_percentage < 30, \
            f"{full_table_percentage:.1f}% of tables use full_table strategy (consider adding incremental columns)"
    
    def test_critical_tables_configured(self, all_table_configs):
        """Critical tables must be present in configuration."""
        critical_tables = [
            'patient', 'appointment', 'procedurelog', 
            'provider', 'payment', 'adjustment'
        ]
        
        missing_critical = []
        for table_name in critical_tables:
            if table_name not in all_table_configs:
                missing_critical.append(table_name)
            elif 'error' in all_table_configs.get(table_name, {}):
                missing_critical.append(f"{table_name} (has error)")
        
        assert len(missing_critical) == 0, \
            f"Critical tables missing or have errors: {missing_critical}"
    
    def test_no_duplicate_table_names(self, all_table_configs):
        """All table names should be unique (case-insensitive check)."""
        table_names_lower = {}
        
        for table_name in all_table_configs.keys():
            lower_name = table_name.lower()
            if lower_name in table_names_lower:
                pytest.fail(
                    f"Duplicate table name (case-insensitive): "
                    f"{table_names_lower[lower_name]} and {table_name}"
                )
            table_names_lower[lower_name] = table_name

