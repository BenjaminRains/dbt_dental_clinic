#!/usr/bin/env python3
"""
Test copy methods configuration for performance categories.
"""

import pytest
import sys
import os
from unittest.mock import patch, MagicMock, mock_open
import yaml

# Add the etl_pipeline directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
from tests.fixtures.env_fixtures import test_settings


class TestCopyMethods:
    """Test that copy methods are correctly configured for performance categories."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock table configurations with different performance categories
        self.mock_table_configs = {
            'tiny_table': {
                'performance_category': 'tiny',
                'estimated_rows': 5000,
                'estimated_size_mb': 0.5
            },
            'small_table': {
                'performance_category': 'small', 
                'estimated_rows': 50000,
                'estimated_size_mb': 5.0
            },
            'medium_table': {
                'performance_category': 'medium',
                'estimated_rows': 500000, 
                'estimated_size_mb': 50.0
            },
            'large_table': {
                'performance_category': 'large',
                'estimated_rows': 2000000,
                'estimated_size_mb': 200.0
            }
        }
        
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine), \
             patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
            
            # Mock the _load_configuration method at the class level to intercept during __init__
            with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=self.mock_table_configs):
                # Create replicator instance
                self.replicator = SimpleMySQLReplicator(settings=test_settings)
    
    def test_performance_category_based_copy_methods(self):
        """Test that copy methods correctly map to performance categories."""
        
        # Test tables with explicit performance categories
        assert self.replicator.get_copy_method('tiny_table') == 'tiny'
        assert self.replicator.get_copy_method('small_table') == 'small'
        assert self.replicator.get_copy_method('medium_table') == 'medium'
        assert self.replicator.get_copy_method('large_table') == 'large'
    
    def test_missing_performance_category_raises_error(self):
        """Test that missing performance_category raises ValueError."""
        
        # Test that tables without performance_category raise ValueError
        with pytest.raises(ValueError, match="missing performance_category in configuration"):
            self.replicator.get_copy_method('unknown_table')
        
        # Add a table without performance_category to test
        self.replicator.table_configs['no_category_table'] = {
            'estimated_rows': 5000,
            'estimated_size_mb': 0.5
        }
        
        with pytest.raises(ValueError, match="missing performance_category in configuration"):
            self.replicator.get_copy_method('no_category_table')
    
    def test_invalid_performance_category_raises_error(self):
        """Test that invalid performance_category raises ValueError."""
        
        # Add a table with invalid performance_category
        self.replicator.table_configs['invalid_category_table'] = {
            'performance_category': 'invalid',
            'estimated_rows': 5000,
            'estimated_size_mb': 0.5
        }
        
        with pytest.raises(ValueError, match="Invalid performance_category"):
            self.replicator.get_copy_method('invalid_category_table')


if __name__ == '__main__':
    # Run the tests
    pytest.main([__file__, '-v']) 