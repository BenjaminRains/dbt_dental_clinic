"""
Unit tests for Phase 1 replica fidelity config generation in OpenDentalSchemaAnalyzer.

Covers sync_profile, replicator_watermark_column, lookback_resync, and
sync-profile-aware primary incremental column / loader strategy selection.
"""
import tempfile
from unittest.mock import Mock, patch

import pytest

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import (
    IN_PLACE_UPDATE_TABLES,
    LOOKBACK_RESYNC_BY_TABLE,
    OpenDentalSchemaAnalyzer,
)


@pytest.fixture
def analyzer():
    with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
         patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
        mock_factory.get_source_connection.return_value = Mock()
        mock_inspect.return_value = Mock()
        yield OpenDentalSchemaAnalyzer()


class TestDetermineSyncProfile:
    @pytest.mark.parametrize('table_name', sorted(IN_PLACE_UPDATE_TABLES))
    def test_mutation_seed_tables_are_in_place(self, analyzer, table_name):
        assert analyzer.determine_sync_profile(table_name, is_modeled=False) == 'in_place_updates'

    def test_modeled_table_is_in_place_when_not_in_seed_list(self, analyzer):
        assert analyzer.determine_sync_profile('definition', is_modeled=True) == 'in_place_updates'

    def test_unmodeled_non_seed_table_is_append_only(self, analyzer):
        assert analyzer.determine_sync_profile('definition', is_modeled=False) == 'append_only'


class TestBuildLookbackResyncConfig:
    def test_procedurelog_gets_config_when_in_place(self, analyzer):
        config = analyzer.build_lookback_resync_config('procedurelog', 'in_place_updates')
        assert config == LOOKBACK_RESYNC_BY_TABLE['procedurelog']
        assert config['enabled'] is True
        assert config['window_days'] == 30
        assert config['predicate_columns'] == ['DateComplete', 'ProcDate']

    def test_mutation_table_without_entry_returns_none(self, analyzer):
        assert analyzer.build_lookback_resync_config('patient', 'in_place_updates') is None

    @pytest.mark.parametrize(
        'table_name, predicate_columns',
        [
            ('payment', ['PayDate']),
            ('claimproc', ['DateCP', 'ProcDate']),
            ('adjustment', ['AdjDate', 'ProcDate']),
            ('claim', ['DateService', 'DateSent', 'DateReceived']),
            ('paysplit', ['DatePay', 'ProcDate']),
        ],
    )
    def test_financial_mutation_tables_get_lookback(self, analyzer, table_name, predicate_columns):
        config = analyzer.build_lookback_resync_config(table_name, 'in_place_updates')
        assert config == LOOKBACK_RESYNC_BY_TABLE[table_name]
        assert config['enabled'] is True
        assert config['window_days'] == 30
        assert config['predicate_columns'] == predicate_columns

    def test_append_only_profile_returns_none(self, analyzer):
        assert analyzer.build_lookback_resync_config('procedurelog', 'append_only') is None


class TestSelectTimestampIncrementalColumn:
    def test_prefers_datetstamp_over_secdateedit(self, analyzer):
        columns = ['SecDateTEdit', 'DateTStamp']
        assert analyzer._select_timestamp_incremental_column('payment', columns) == 'DateTStamp'

    def test_uses_secdateedit_when_datetstamp_missing(self, analyzer):
        columns = ['PayNum', 'SecDateTEdit']
        schema_info = {'primary_keys': ['PayNum']}
        assert analyzer._select_timestamp_incremental_column(
            'payment', columns, schema_info
        ) == 'SecDateTEdit'

    def test_skips_primary_key_in_non_priority_fallback(self, analyzer):
        columns = ['ClaimProcNum', 'CustomDateTime']
        schema_info = {'primary_keys': ['ClaimProcNum']}
        assert analyzer._select_timestamp_incremental_column(
            'claimproc', columns, schema_info
        ) == 'CustomDateTime'


class TestSelectPrimaryIncrementalColumn:
    @pytest.fixture
    def procedurelog_schema(self):
        return {
            'primary_keys': ['ProcNum'],
            'columns': {
                'ProcNum': {'type': 'bigint'},
                'DateTStamp': {'type': 'datetime'},
            },
        }

    def test_in_place_uses_timestamp_not_pk(self, analyzer, procedurelog_schema):
        columns = ['ProcNum', 'DateTStamp', 'SecDateTEdit']
        selected = analyzer.select_primary_incremental_column(
            'procedurelog', columns, procedurelog_schema, sync_profile='in_place_updates'
        )
        assert selected == 'DateTStamp'

    def test_append_only_prefers_pk(self, analyzer, procedurelog_schema):
        columns = ['ProcNum', 'DateTStamp', 'SecDateTEdit']
        selected = analyzer.select_primary_incremental_column(
            'procedurelog', columns, procedurelog_schema, sync_profile='append_only'
        )
        assert selected == 'ProcNum'

    def test_in_place_falls_back_to_pk_when_no_timestamp(self, analyzer):
        schema_info = {'primary_keys': ['PayNum'], 'columns': {'PayNum': {'type': 'bigint'}}}
        columns = ['PayNum']
        selected = analyzer.select_primary_incremental_column(
            'payment', columns, schema_info, sync_profile='in_place_updates'
        )
        assert selected == 'PayNum'

    def test_securitylog_append_only_uses_securitylognum(self, analyzer):
        schema_info = {'primary_keys': ['SecurityLogNum'], 'columns': {}}
        selected = analyzer.select_primary_incremental_column(
            'securitylog', ['DateTStamp'], schema_info, sync_profile='append_only'
        )
        assert selected == 'SecurityLogNum'


class TestDetermineIncrementalStrategyReplicaFidelity:
    def test_in_place_multi_column_uses_or_logic(self, analyzer):
        strategy = analyzer.determine_incremental_strategy(
            'claimproc',
            {},
            ['DateTStamp', 'SecDateTEdit'],
            sync_profile='in_place_updates',
        )
        assert strategy == 'or_logic'

    def test_append_only_multi_column_still_uses_or_logic(self, analyzer):
        strategy = analyzer.determine_incremental_strategy(
            'appointment',
            {},
            ['DateTStamp', 'SecDateTEdit'],
            sync_profile='append_only',
        )
        assert strategy == 'or_logic'

    def test_mutation_table_single_column_uses_single_column(self, analyzer):
        strategy = analyzer.determine_incremental_strategy(
            'procedurelog',
            {},
            ['DateTStamp'],
            sync_profile='in_place_updates',
        )
        assert strategy == 'single_column'


class TestDetermineExtractionStrategyWithSyncProfile:
    def test_tiny_in_place_table_uses_incremental_with_timestamp_watermark(self, analyzer):
        performance_chars = {'performance_category': 'tiny'}
        strategy = analyzer.determine_extraction_strategy(
            'procedurelog',
            {'primary_keys': ['ProcNum']},
            {'estimated_row_count': 100},
            performance_chars,
            incremental_columns=['ProcNum', 'DateTStamp'],
            sync_profile='in_place_updates',
            primary_incremental_column='DateTStamp',
        )
        assert strategy == 'incremental'

    def test_tiny_append_only_pk_table_uses_full_table(self, analyzer):
        performance_chars = {'performance_category': 'tiny'}
        strategy = analyzer.determine_extraction_strategy(
            'appointment',
            {'primary_keys': ['AptNum']},
            {'estimated_row_count': 100},
            performance_chars,
            incremental_columns=['AptNum', 'DateTStamp'],
            sync_profile='append_only',
            primary_incremental_column='AptNum',
        )
        assert strategy == 'full_table'


class TestGenerateConfigurationReplicaFidelityFields:
    @pytest.fixture
    def replica_fidelity_config(self, mock_settings_with_dict_provider, mock_dbt_models, mock_environment_variables):
        test_tables = ['patient', 'definition', 'procedurelog', 'payment', 'claimproc', 'adjustment']

        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:

            mock_factory.get_source_connection.return_value = Mock()
            mock_inspect.return_value = Mock()

            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            mock_conn_manager_instance.execute_with_retry.return_value = Mock()

            analyzer = OpenDentalSchemaAnalyzer()
            analyzer.discover_all_tables = lambda: test_tables
            analyzer.discover_dbt_models = lambda: mock_dbt_models

            pk_by_table = {
                'procedurelog': 'ProcNum',
                'payment': 'PayNum',
                'claimproc': 'ClaimProcNum',
                'adjustment': 'AdjNum',
                'patient': 'PatNum',
                'definition': 'DefNum',
            }

            def mock_get_batch_schema_info(table_names):
                return {
                    table_name: {
                        'table_name': table_name,
                        'columns': {
                            pk_by_table[table_name]: {'type': 'bigint'},
                            'DateTStamp': {'type': 'datetime'},
                        },
                        'primary_keys': [pk_by_table[table_name]],
                        'foreign_keys': [],
                        'indexes': [],
                    }
                    for table_name in table_names
                }

            def mock_get_batch_size_info(table_names):
                return {
                    table_name: {
                        'table_name': table_name,
                        'estimated_row_count': 50000,
                        'size_mb': 5.0,
                        'source': 'information_schema_estimate',
                    }
                    for table_name in table_names
                }

            analyzer.get_batch_schema_info = mock_get_batch_schema_info
            analyzer.get_batch_size_info = mock_get_batch_size_info

            def mock_find_incremental_columns(table_name, schema_info):
                pk = schema_info.get('primary_keys', ['id'])[0]
                return [pk, 'DateTStamp', 'SecDateTEdit']

            analyzer.find_incremental_columns = mock_find_incremental_columns

            with tempfile.TemporaryDirectory() as temp_dir:
                config = analyzer.generate_complete_configuration(temp_dir)
                yield config

    def test_metadata_documents_replica_fidelity_version(self, replica_fidelity_config):
        metadata = replica_fidelity_config['metadata']
        assert metadata['configuration_version'] == '4.1'
        assert metadata['analyzer_version'] == '4.1_replica_fidelity'
        assert 'sync_profile_classification' in metadata['optimization_features']
        assert 'replicator_watermark_column' in metadata['optimization_features']
        assert 'lookback_resync_config' in metadata['optimization_features']

    def test_procedurelog_emits_full_replica_fidelity_block(self, replica_fidelity_config):
        procedurelog = replica_fidelity_config['tables']['procedurelog']
        assert procedurelog['sync_profile'] == 'in_place_updates'
        assert procedurelog['primary_incremental_column'] == 'DateTStamp'
        assert procedurelog['replicator_watermark_column'] == 'DateTStamp'
        assert procedurelog['incremental_strategy'] == 'or_logic'
        assert procedurelog['lookback_resync']['window_days'] == 30
        assert procedurelog['lookback_resync']['predicate_columns'] == ['DateComplete', 'ProcDate']

    def test_append_only_definition_uses_pk_watermark(self, replica_fidelity_config):
        definition = replica_fidelity_config['tables']['definition']
        assert definition['sync_profile'] == 'append_only'
        assert definition['primary_incremental_column'] == 'DefNum'
        assert definition['replicator_watermark_column'] == 'DefNum'
        assert 'lookback_resync' not in definition

    def test_mutation_payment_has_lookback_block(self, replica_fidelity_config):
        payment = replica_fidelity_config['tables']['payment']
        assert payment['sync_profile'] == 'in_place_updates'
        assert payment['primary_incremental_column'] == 'DateTStamp'
        assert payment['lookback_resync']['window_days'] == 30
        assert payment['lookback_resync']['predicate_columns'] == ['PayDate']

    def test_mutation_claimproc_has_lookback_block(self, replica_fidelity_config):
        claimproc = replica_fidelity_config['tables']['claimproc']
        assert claimproc['lookback_resync']['predicate_columns'] == ['DateCP', 'ProcDate']

    def test_mutation_adjustment_has_lookback_block(self, replica_fidelity_config):
        adjustment = replica_fidelity_config['tables']['adjustment']
        assert adjustment['lookback_resync']['predicate_columns'] == ['AdjDate', 'ProcDate']

    def test_watermark_column_always_matches_primary_incremental_column(self, replica_fidelity_config):
        for table_config in replica_fidelity_config['tables'].values():
            assert table_config['replicator_watermark_column'] == table_config['primary_incremental_column']
