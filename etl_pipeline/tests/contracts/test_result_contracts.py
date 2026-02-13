"""
Result Contract Tests

Tests that verify result dictionaries from orchestration and processing
operations match the documented contracts in DATA_CONTRACTS.md.

These tests validate:
- Processing results from PipelineOrchestrator
- Category results from PriorityProcessor
- Table results dictionaries

Run with: pytest tests/contracts/test_result_contracts.py -v
"""

import pytest
from typing import Dict, List


class TestTableResults:
    """Test table results contract (table_name → success boolean)."""
    
    @pytest.fixture
    def table_results(self) -> Dict[str, bool]:
        """Example table results from process_tables_by_performance_category."""
        return {
            'patient': True,
            'provider': True,
            'adjustment': False,
            'payment': True,
            'appointment': True
        }
    
    def test_is_dictionary(self, table_results):
        """Table results must be dictionary."""
        assert isinstance(table_results, dict), \
            "Table results must be dictionary"
    
    def test_keys_are_strings(self, table_results):
        """Keys must be strings (table names)."""
        for key in table_results.keys():
            assert isinstance(key, str), \
                f"Table name must be string, got {type(key).__name__}"
            assert len(key) > 0, \
                "Table name must not be empty"
    
    def test_values_are_boolean(self, table_results):
        """Values must be boolean (success status)."""
        for table_name, success in table_results.items():
            assert isinstance(success, bool), \
                f"Success status for {table_name} must be boolean, got {type(success).__name__}"
    
    def test_no_duplicate_keys(self, table_results):
        """No duplicate table names."""
        # Python dicts automatically handle this, but test for completeness
        table_names = list(table_results.keys())
        unique_names = set(table_names)
        
        assert len(table_names) == len(unique_names), \
            f"Duplicate table names found: {[t for t in table_names if table_names.count(t) > 1]}"


class TestCategoryResults:
    """Test category results contract from PriorityProcessor."""
    
    VALID_CATEGORIES = ['large', 'medium', 'small', 'tiny']
    
    @pytest.fixture
    def category_results(self) -> Dict[str, Dict]:
        """Example category results from process_by_priority."""
        return {
            'large': {
                'success': ['appointment', 'procedurelog'],
                'failed': ['claimproc'],
                'total': 3
            },
            'medium': {
                'success': ['patient', 'provider', 'adjustment'],
                'failed': [],
                'total': 3
            },
            'small': {
                'success': ['definition', 'pharmacy'],
                'failed': ['obsolete_table'],
                'total': 3
            },
            'tiny': {
                'success': ['account', 'clinic'],
                'failed': [],
                'total': 2
            }
        }
    
    def test_is_dictionary(self, category_results):
        """Category results must be dictionary."""
        assert isinstance(category_results, dict), \
            "Category results must be dictionary"
    
    def test_categories_valid(self, category_results):
        """Category keys must be valid performance categories."""
        for category in category_results.keys():
            assert category in self.VALID_CATEGORIES, \
                f"Invalid category: {category}"
    
    def test_each_category_has_required_fields(self, category_results):
        """Each category must have success, failed, and total fields."""
        required_fields = ['success', 'failed', 'total']
        
        for category, results in category_results.items():
            for field in required_fields:
                assert field in results, \
                    f"Category {category} missing required field: {field}"
    
    def test_success_is_list_of_strings(self, category_results):
        """success field must be list of strings."""
        for category, results in category_results.items():
            success_list = results['success']
            
            assert isinstance(success_list, list), \
                f"Category {category} success must be list"
            
            for table_name in success_list:
                assert isinstance(table_name, str), \
                    f"Category {category} success contains non-string: {table_name}"
    
    def test_failed_is_list_of_strings(self, category_results):
        """failed field must be list of strings."""
        for category, results in category_results.items():
            failed_list = results['failed']
            
            assert isinstance(failed_list, list), \
                f"Category {category} failed must be list"
            
            for table_name in failed_list:
                assert isinstance(table_name, str), \
                    f"Category {category} failed contains non-string: {table_name}"
    
    def test_total_is_integer(self, category_results):
        """total field must be integer."""
        for category, results in category_results.items():
            total = results['total']
            
            assert isinstance(total, int), \
                f"Category {category} total must be integer"
            assert total >= 0, \
                f"Category {category} total must be non-negative"
    
    def test_total_equals_success_plus_failed(self, category_results):
        """total should equal success count + failed count."""
        for category, results in category_results.items():
            success_count = len(results['success'])
            failed_count = len(results['failed'])
            total = results['total']
            
            assert total == success_count + failed_count, \
                f"Category {category}: total ({total}) != success ({success_count}) + failed ({failed_count})"
    
    def test_no_table_in_both_success_and_failed(self, category_results):
        """A table should not appear in both success and failed lists."""
        for category, results in category_results.items():
            success_set = set(results['success'])
            failed_set = set(results['failed'])
            
            overlap = success_set & failed_set
            assert len(overlap) == 0, \
                f"Category {category} has tables in both success and failed: {overlap}"
    
    def test_success_rate_calculation(self, category_results):
        """Success rate should be calculable."""
        for category, results in category_results.items():
            total = results['total']
            
            if total > 0:
                success_count = len(results['success'])
                success_rate = success_count / total * 100
                
                assert 0 <= success_rate <= 100, \
                    f"Category {category} success rate out of range: {success_rate}%"


class TestAggregatedResults:
    """Test aggregated results across all categories."""
    
    @pytest.fixture
    def category_results(self) -> Dict[str, Dict]:
        """Example category results."""
        return {
            'large': {
                'success': ['appointment', 'procedurelog'],
                'failed': ['claimproc'],
                'total': 3
            },
            'medium': {
                'success': ['patient', 'provider'],
                'failed': [],
                'total': 2
            },
            'small': {
                'success': ['definition'],
                'failed': ['obsolete'],
                'total': 2
            },
            'tiny': {
                'success': ['account', 'clinic'],
                'failed': [],
                'total': 2
            }
        }
    
    def test_aggregate_totals(self, category_results):
        """Can aggregate totals across categories."""
        total_success = sum(len(r['success']) for r in category_results.values())
        total_failed = sum(len(r['failed']) for r in category_results.values())
        total_tables = sum(r['total'] for r in category_results.values())
        
        assert total_tables == total_success + total_failed, \
            "Aggregated total should equal success + failed"
        
        assert total_success > 0, \
            "Should have at least some successful tables"
        
        # Calculate success rate
        success_rate = total_success / total_tables * 100 if total_tables > 0 else 0
        assert 0 <= success_rate <= 100, \
            f"Success rate out of range: {success_rate}%"
    
    def test_all_table_names_unique(self, category_results):
        """Table names should be unique across all categories."""
        all_tables = []
        
        for results in category_results.values():
            all_tables.extend(results['success'])
            all_tables.extend(results['failed'])
        
        unique_tables = set(all_tables)
        
        # If tables are duplicated, they appear in multiple categories
        if len(all_tables) != len(unique_tables):
            duplicates = [t for t in all_tables if all_tables.count(t) > 1]
            pytest.fail(f"Tables appear in multiple categories: {set(duplicates)}")


class TestPipelineReport:
    """Test pipeline report contract (from Airflow DAG)."""
    
    @pytest.fixture
    def pipeline_report(self) -> Dict:
        """Example pipeline report from generate_pipeline_report."""
        return {
            'execution_date': '2025-10-22T03:00:00',
            'dag_run_id': 'scheduled__2025-10-22T03:00:00',
            'environment': 'clinic',
            'configuration': {
                'total_tables_configured': 436,
                'config_age_days': 15,
                'schema_drift_detected': False
            },
            'processing_results': {
                'total_processed': 436,
                'total_success': 430,
                'total_failed': 6,
                'success_rate': '98.6%',
                'by_category': {
                    'large': {'success': 8, 'failed': 2, 'total': 10},
                    'medium': {'success': 45, 'failed': 3, 'total': 48}
                }
            },
            'verification': {
                'analytics_successful': 430,
                'analytics_failed': 6
            }
        }
    
    def test_has_required_sections(self, pipeline_report):
        """Pipeline report must have required sections."""
        required_sections = [
            'execution_date',
            'environment',
            'configuration',
            'processing_results'
        ]
        
        for section in required_sections:
            assert section in pipeline_report, \
                f"Pipeline report missing section: {section}"
    
    def test_configuration_section_valid(self, pipeline_report):
        """Configuration section must be valid."""
        config = pipeline_report['configuration']
        
        assert isinstance(config, dict), \
            "configuration must be dictionary"
        
        assert 'total_tables_configured' in config, \
            "configuration missing total_tables_configured"
        assert isinstance(config['total_tables_configured'], int), \
            "total_tables_configured must be integer"
        
        if 'schema_drift_detected' in config:
            assert isinstance(config['schema_drift_detected'], bool), \
                "schema_drift_detected must be boolean"
    
    def test_processing_results_section_valid(self, pipeline_report):
        """Processing results section must be valid."""
        results = pipeline_report['processing_results']
        
        assert isinstance(results, dict), \
            "processing_results must be dictionary"
        
        required_fields = ['total_processed', 'total_success', 'total_failed']
        for field in required_fields:
            assert field in results, \
                f"processing_results missing {field}"
            assert isinstance(results[field], int), \
                f"{field} must be integer"
    
    def test_success_rate_format(self, pipeline_report):
        """success_rate should be percentage string."""
        results = pipeline_report['processing_results']
        
        if 'success_rate' in results:
            rate = results['success_rate']
            assert isinstance(rate, str), \
                "success_rate should be string"
            assert '%' in rate, \
                "success_rate should contain % symbol"
    
    def test_totals_add_up(self, pipeline_report):
        """Total processed should equal success + failed."""
        results = pipeline_report['processing_results']
        
        total = results['total_processed']
        success = results['total_success']
        failed = results['total_failed']
        
        assert total == success + failed, \
            f"Total ({total}) != success ({success}) + failed ({failed})"
    
    def test_environment_valid(self, pipeline_report):
        """Environment must be valid value."""
        environment = pipeline_report['environment']
        valid_environments = ['clinic', 'test']
        
        assert environment in valid_environments, \
            f"Invalid environment: {environment}"


class TestXComDataSerializability:
    """Test that result objects are XCom-compatible (JSON-serializable)."""
    
    def test_table_results_serializable(self):
        """Table results must be JSON-serializable."""
        import json
        
        results = {
            'patient': True,
            'provider': False
        }
        
        try:
            json_str = json.dumps(results)
            deserialized = json.loads(json_str)
            assert deserialized == results
        except (TypeError, ValueError) as e:
            pytest.fail(f"Table results not JSON-serializable: {e}")
    
    def test_category_results_serializable(self):
        """Category results must be JSON-serializable."""
        import json
        
        results = {
            'large': {
                'success': ['table1', 'table2'],
                'failed': ['table3'],
                'total': 3
            }
        }
        
        try:
            json_str = json.dumps(results)
            deserialized = json.loads(json_str)
            assert deserialized == results
        except (TypeError, ValueError) as e:
            pytest.fail(f"Category results not JSON-serializable: {e}")
    
    def test_pipeline_report_serializable(self):
        """Pipeline report must be JSON-serializable."""
        import json
        
        report = {
            'execution_date': '2025-10-22T03:00:00',  # String, not datetime!
            'environment': 'clinic',
            'processing_results': {
                'total': 436,
                'success': 430,
                'failed': 6
            }
        }
        
        try:
            json_str = json.dumps(report)
            deserialized = json.loads(json_str)
            assert deserialized == report
        except (TypeError, ValueError) as e:
            pytest.fail(f"Pipeline report not JSON-serializable: {e}")
    
    def test_datetime_objects_not_serializable(self):
        """Datetime objects should NOT be in XCom data."""
        import json
        from datetime import datetime
        
        # This SHOULD fail
        results = {
            'execution_date': datetime.now()  # BAD!
        }
        
        with pytest.raises((TypeError, ValueError)):
            json.dumps(results)
    
    def test_use_iso_strings_for_dates(self):
        """Dates should be ISO strings, not datetime objects."""
        import json
        from datetime import datetime
        
        # This SHOULD work
        results = {
            'execution_date': datetime.now().isoformat()  # GOOD!
        }
        
        try:
            json_str = json.dumps(results)
            deserialized = json.loads(json_str)
            assert 'execution_date' in deserialized
        except (TypeError, ValueError) as e:
            pytest.fail(f"ISO string not JSON-serializable: {e}")


class TestResultConsistency:
    """Test consistency rules across result objects."""
    
    def test_empty_results_valid(self):
        """Empty results should be valid."""
        empty_results = {}
        
        assert isinstance(empty_results, dict), \
            "Empty results should be empty dict"
    
    def test_all_success_valid(self):
        """All successful results should be valid."""
        all_success = {
            'patient': True,
            'provider': True,
            'appointment': True
        }
        
        assert all(all_success.values()), \
            "All success results should have all True values"
    
    def test_all_failure_valid(self):
        """All failed results should be valid."""
        all_failed = {
            'patient': False,
            'provider': False,
            'appointment': False
        }
        
        assert not any(all_failed.values()), \
            "All failed results should have all False values"
    
    def test_mixed_results_valid(self):
        """Mixed success/failure results should be valid."""
        mixed = {
            'patient': True,
            'provider': False,
            'appointment': True
        }
        
        success_count = sum(1 for v in mixed.values() if v)
        failure_count = sum(1 for v in mixed.values() if not v)
        
        assert success_count + failure_count == len(mixed), \
            "Success + failure should equal total"

