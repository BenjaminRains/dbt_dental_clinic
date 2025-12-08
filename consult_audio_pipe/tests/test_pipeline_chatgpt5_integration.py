"""
Tests for pipeline integration with ChatGPT 5 functionality
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock, call
from dental_consultation_pipeline.pipeline import (
    run_full_pipeline,
    setup_logging,
    log_status
)

class TestPipelineChatGPT5Integration:
    """Test pipeline integration with ChatGPT 5 using AAA approach"""
    
    @pytest.fixture
    def mock_analysis_module(self):
        """Mock the analysis module for testing"""
        with patch('dental_consultation_pipeline.pipeline.analyze_clean_transcripts') as mock_analyze:
            with patch('dental_consultation_pipeline.pipeline.analyze_clean_transcripts_with_both_models') as mock_analyze_both:
                yield {
                    'analyze': mock_analyze,
                    'analyze_both': mock_analyze_both
                }
    
    @pytest.fixture
    def mock_other_modules(self):
        """Mock other pipeline modules for testing"""
        with patch('dental_consultation_pipeline.pipeline.transcribe_all_audio_files') as mock_transcribe:
            with patch('dental_consultation_pipeline.pipeline.clean_transcripts') as mock_clean:
                with patch('dental_consultation_pipeline.pipeline.convert_all_formats') as mock_convert:
                    yield {
                        'transcribe': mock_transcribe,
                        'clean': mock_clean,
                        'convert': mock_convert
                    }
    
    @pytest.fixture
    def mock_directories(self):
        """Mock directory structure for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create mock directory structure
            (temp_path / "raw_audio").mkdir()
            (temp_path / "transcripts").mkdir()
            (temp_path / "transcripts_clean").mkdir()
            (temp_path / "api_claude_summary_analysis").mkdir()
            (temp_path / "html_output").mkdir()
            (temp_path / "pdf_output").mkdir()
            
            with patch('dental_consultation_pipeline.pipeline.PROJECT_ROOT', temp_path):
                yield temp_path

    def test_pipeline_uses_claude_by_default(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test that pipeline uses Claude by default when USE_BOTH_MODELS is not set using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].return_value = []
        mock_other_modules['clean'].return_value = []
        mock_other_modules['convert'].return_value = None
        mock_analysis_module['analyze'].return_value = []
        
        # Act
        with patch('os.getenv', return_value='false'):
            run_full_pipeline()
        
        # Assert
        mock_analysis_module['analyze'].assert_called_once()
        mock_analysis_module['analyze_both'].assert_not_called()

    def test_pipeline_uses_both_models_when_env_set(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test that pipeline uses both models when USE_BOTH_MODELS is set to true using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].return_value = []
        mock_other_modules['clean'].return_value = []
        mock_other_modules['convert'].return_value = None
        mock_analysis_module['analyze_both'].return_value = []
        
        # Act
        with patch('os.getenv', return_value='true'):
            run_full_pipeline()
        
        # Assert
        mock_analysis_module['analyze_both'].assert_called_once()
        mock_analysis_module['analyze'].assert_not_called()

    def test_pipeline_uses_both_models_case_insensitive(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test that pipeline uses both models when USE_BOTH_MODELS is set to TRUE (case insensitive) using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].return_value = []
        mock_other_modules['clean'].return_value = []
        mock_other_modules['convert'].return_value = None
        mock_analysis_module['analyze_both'].return_value = []
        
        # Act
        with patch('os.getenv', return_value='TRUE'):
            run_full_pipeline()
        
        # Assert
        mock_analysis_module['analyze_both'].assert_called_once()
        mock_analysis_module['analyze'].assert_not_called()

    def test_pipeline_uses_claude_when_env_not_set(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test that pipeline uses Claude when USE_BOTH_MODELS environment variable is not set using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].return_value = []
        mock_other_modules['clean'].return_value = []
        mock_other_modules['convert'].return_value = None
        mock_analysis_module['analyze'].return_value = []
        
        # Act
        with patch('os.getenv', return_value=None):
            run_full_pipeline()
        
        # Assert
        mock_analysis_module['analyze'].assert_called_once()
        mock_analysis_module['analyze_both'].assert_not_called()

    def test_pipeline_uses_claude_when_env_set_to_false(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test that pipeline uses Claude when USE_BOTH_MODELS is set to false using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].return_value = []
        mock_other_modules['clean'].return_value = []
        mock_other_modules['convert'].return_value = None
        mock_analysis_module['analyze'].return_value = []
        
        # Act
        with patch('os.getenv', return_value='false'):
            run_full_pipeline()
        
        # Assert
        mock_analysis_module['analyze'].assert_called_once()
        mock_analysis_module['analyze_both'].assert_not_called()

    def test_pipeline_logs_correct_model_usage(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test that pipeline logs correct model usage information using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].return_value = []
        mock_other_modules['clean'].return_value = []
        mock_other_modules['convert'].return_value = None
        mock_analysis_module['analyze_both'].return_value = []
        
        # Act
        with patch('os.getenv', return_value='true'):
            with patch('dental_consultation_pipeline.pipeline.log_status') as mock_log:
                run_full_pipeline()
        
        # Assert
        # Check that the correct log message was called
        log_calls = [call.args[0] for call in mock_log.call_args_list]
        assert any("Using both Claude and ChatGPT 5 models for analysis" in call for call in log_calls)

    def test_pipeline_logs_claude_usage_by_default(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test that pipeline logs Claude usage by default using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].return_value = []
        mock_other_modules['clean'].return_value = []
        mock_other_modules['convert'].return_value = None
        mock_analysis_module['analyze'].return_value = []
        
        # Act
        with patch('os.getenv', return_value='false'):
            with patch('dental_consultation_pipeline.pipeline.log_status') as mock_log:
                run_full_pipeline()
        
        # Assert
        # Check that the default Claude message is in the logs
        log_calls = [call.args[0] for call in mock_log.call_args_list]
        assert any("Using Anthropic Claude API for analysis and summaries" in call for call in log_calls)

    def test_pipeline_handles_analysis_exception_gracefully(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test that pipeline handles analysis exceptions gracefully using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].return_value = []
        mock_other_modules['clean'].return_value = []
        mock_analysis_module['analyze'].side_effect = Exception("Analysis failed")
        
        # Act
        with patch('os.getenv', return_value='false'):
            with patch('dental_consultation_pipeline.pipeline.log_status') as mock_log:
                run_full_pipeline()
        
        # Assert
        # Check that error was logged
        log_calls = [call.args[0] for call in mock_log.call_args_list]
        error_calls = [call for call in log_calls if "Failed to run LLM analysis" in call]
        assert len(error_calls) > 0

    def test_pipeline_handles_both_models_analysis_exception_gracefully(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test that pipeline handles both models analysis exceptions gracefully using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].return_value = []
        mock_other_modules['clean'].return_value = []
        mock_analysis_module['analyze_both'].side_effect = Exception("Both models analysis failed")
        
        # Act
        with patch('os.getenv', return_value='true'):
            with patch('dental_consultation_pipeline.pipeline.log_status') as mock_log:
                run_full_pipeline()
        
        # Assert
        # Check that error was logged
        log_calls = [call.args[0] for call in mock_log.call_args_list]
        error_calls = [call for call in log_calls if "Failed to run LLM analysis" in call]
        assert len(error_calls) > 0

    def test_pipeline_imports_correct_modules(self):
        """Test that pipeline imports the correct analysis modules using AAA approach"""
        # Arrange & Act
        # This test verifies that the pipeline can import the analysis modules
        try:
            from dental_consultation_pipeline.analysis import analyze_clean_transcripts
            from dental_consultation_pipeline.analysis import analyze_clean_transcripts_with_both_models
            import_successful = True
        except ImportError:
            import_successful = False
        
        # Assert
        assert import_successful

    def test_pipeline_environment_variable_handling(self):
        """Test pipeline environment variable handling using AAA approach"""
        # Arrange
        test_cases = [
            ('true', True),
            ('TRUE', True),
            ('True', True),
            ('false', False),
            ('FALSE', False),
            ('False', False),
            (None, False),
            ('', False),
            ('random_value', False),
        ]
        
        for env_value, expected_result in test_cases:
            # Act
            with patch('os.getenv', return_value=env_value):
                result = os.getenv("USE_BOTH_MODELS", "false").lower() == "true"
            
            # Assert
            assert result == expected_result

    def test_pipeline_logging_setup(self):
        """Test pipeline logging setup using AAA approach"""
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            (temp_path / "logs").mkdir()
            
            with patch('dental_consultation_pipeline.pipeline.LOGS_DIR', temp_path / "logs"):
                # Act
                log_path = setup_logging()
                
                # Assert
                assert log_path.exists()
                assert log_path.parent == temp_path / "logs"
                assert log_path.name.startswith("pipeline_run_")

    def test_pipeline_log_status_function(self):
        """Test pipeline log_status function using AAA approach"""
        # Arrange
        test_message = "Test log message"
        
        # Act
        with patch('dental_consultation_pipeline.pipeline.logging') as mock_logging:
            log_status(test_message, "INFO")
            
            # Assert
            mock_logging.info.assert_called_once_with(test_message)

    def test_pipeline_log_status_different_levels(self):
        """Test pipeline log_status function with different levels using AAA approach"""
        # Arrange
        test_message = "Test log message"
        test_cases = [
            ("INFO", "info"),
            ("WARNING", "warning"),
            ("ERROR", "error"),
            ("DEBUG", "debug"),
            ("UNKNOWN", "info"),  # Default to info for unknown levels
        ]
        
        for level, expected_method in test_cases:
            # Act
            with patch('dental_consultation_pipeline.pipeline.logging') as mock_logging:
                log_status(test_message, level)
                
                # Assert
                getattr(mock_logging, expected_method).assert_called_once_with(test_message)

    def test_pipeline_integration_with_chatgpt5_analysis_module(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test full pipeline integration with ChatGPT 5 analysis module using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].return_value = ["file1.m4a", "file2.m4a"]
        mock_other_modules['clean'].return_value = ["transcript1_clean.txt", "transcript2_clean.txt"]
        mock_other_modules['convert'].return_value = None
        mock_analysis_module['analyze_both'].return_value = ["result1", "result2"]
        
        # Act
        with patch('os.getenv', return_value='true'):
            with patch('dental_consultation_pipeline.pipeline.log_status') as mock_log:
                run_full_pipeline()
        
        # Assert
        mock_analysis_module['analyze_both'].assert_called_once()
        mock_other_modules['transcribe'].assert_called_once()
        mock_other_modules['clean'].assert_called_once()
        mock_other_modules['convert'].assert_called_once()
        
        # Check that success messages were logged
        log_calls = [call.args[0] for call in mock_log.call_args_list]
        assert any("Successfully transcribed" in call for call in log_calls)
        assert any("Applied corrections" in call for call in log_calls)
        assert any("LLM analysis completed" in call for call in log_calls)

    def test_pipeline_aborts_on_transcription_failure(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test that pipeline aborts on transcription failure using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].side_effect = Exception("Transcription failed")
        
        # Act
        with patch('os.getenv', return_value='true'):
            with patch('dental_consultation_pipeline.pipeline.log_status') as mock_log:
                run_full_pipeline()
        
        # Assert
        # Check that pipeline aborted and didn't continue to analysis
        mock_analysis_module['analyze_both'].assert_not_called()
        mock_analysis_module['analyze'].assert_not_called()
        
        # Check that abort message was logged
        log_calls = [call.args[0] for call in mock_log.call_args_list]
        assert any("Pipeline aborted due to transcription failure" in call for call in log_calls)

    def test_pipeline_aborts_on_cleaning_failure(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test that pipeline aborts on cleaning failure using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].return_value = ["file1.m4a"]
        mock_other_modules['clean'].side_effect = Exception("Cleaning failed")
        
        # Act
        with patch('os.getenv', return_value='true'):
            with patch('dental_consultation_pipeline.pipeline.log_status') as mock_log:
                run_full_pipeline()
        
        # Assert
        # Check that pipeline aborted and didn't continue to analysis
        mock_analysis_module['analyze_both'].assert_not_called()
        mock_analysis_module['analyze'].assert_not_called()
        
        # Check that abort message was logged
        log_calls = [call.args[0] for call in mock_log.call_args_list]
        assert any("Pipeline aborted due to transcript cleaning failure" in call for call in log_calls)

    def test_pipeline_continues_on_conversion_failure(self, mock_analysis_module, mock_other_modules, mock_directories):
        """Test that pipeline continues even if conversion fails using AAA approach"""
        # Arrange
        mock_other_modules['transcribe'].return_value = []
        mock_other_modules['clean'].return_value = []
        mock_other_modules['convert'].side_effect = Exception("Conversion failed")
        mock_analysis_module['analyze'].return_value = []
        
        # Act
        with patch('os.getenv', return_value='false'):
            with patch('dental_consultation_pipeline.pipeline.log_status') as mock_log:
                run_full_pipeline()
        
        # Assert
        # Check that analysis was still called
        mock_analysis_module['analyze'].assert_called_once()
        
        # Check that conversion error was logged but pipeline continued
        log_calls = [call.args[0] for call in mock_log.call_args_list]
        assert any("Failed to convert markdown files" in call for call in log_calls)
        assert any("Pipeline complete" in call for call in log_calls)
