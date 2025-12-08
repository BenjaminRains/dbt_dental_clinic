"""
Comprehensive tests for ChatGPT 5 integration in the analysis module
"""

import pytest
import tempfile
import os
import json
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open, call
from dental_consultation_pipeline.analysis import (
    call_openai_api_analysis,
    call_openai_api_summary,
    generate_summary_with_llm,
    generate_analysis_with_llm,
    generate_summary_with_chatgpt5,
    generate_analysis_with_chatgpt5,
    save_summary_to_markdown,
    save_analysis_to_markdown,
    process_transcript_with_llm,
    analyze_clean_transcripts,
    analyze_clean_transcripts_with_both_models,
    calculate_token_usage,
    LLM_CONFIG,
    OpenAI
)

class TestChatGPT5Integration:
    """Test ChatGPT 5 integration functionality using AAA approach"""
    
    @pytest.fixture
    def mock_openai_client(self):
        """Mock OpenAI client for testing"""
        with patch('dental_consultation_pipeline.analysis.OpenAI') as mock_openai:
            mock_client = MagicMock()
            mock_openai.return_value = mock_client
            yield mock_client
    
    @pytest.fixture
    def mock_json_debug_dir(self):
        """Mock JSON debug directory for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch('dental_consultation_pipeline.analysis.JSON_DEBUG_DIR', Path(temp_dir)):
                yield Path(temp_dir)
    
    @pytest.fixture
    def mock_output_dir(self):
        """Mock output directory for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch('dental_consultation_pipeline.analysis.OUTPUT_DIR', Path(temp_dir)):
                yield Path(temp_dir)

    def test_call_openai_api_analysis_success(self, mock_openai_client, mock_json_debug_dir):
        """Test successful ChatGPT 5 analysis API call using AAA approach"""
        # Arrange
        transcript = "Test transcript content"
        patient_name = "Test Patient"
        expected_response = {
            "choices": [{"message": {"content": "Test analysis content"}}],
            "usage": {"input_tokens": 100, "output_tokens": 200}
        }
        # Create a proper mock response that can be serialized
        mock_response = MagicMock()
        mock_response.model_dump.return_value = expected_response
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Test analysis content"
        mock_openai_client.chat.completions.create.return_value = mock_response
        
        # Mock the API key and config - need to mock both the API key and model
        with patch('dental_consultation_pipeline.analysis.LLM_CONFIG') as mock_config:
            # Mock the config to return different values based on the key
            def mock_getitem(key):
                if key == "openai_api_key":
                    return "test-api-key"
                elif key == "openai_model":
                    return "gpt-5o"
                else:
                    return None
            mock_config.__getitem__.side_effect = mock_getitem
            mock_config.get.side_effect = mock_getitem
            
            # Act
            result = call_openai_api_analysis(transcript, patient_name)
            
            # Assert
            assert result == "Test analysis content"
            mock_openai_client.chat.completions.create.assert_called_once()
            call_args = mock_openai_client.chat.completions.create.call_args
            # Check keyword arguments (kwargs) - model is a keyword argument
            assert call_args[1]['model'] == 'gpt-4o'
            assert call_args[1]['max_tokens'] == 2000
            assert call_args[1]['messages'][0]['role'] == 'user'
            assert 'Test transcript content' in call_args[1]['messages'][0]['content']
            
            # Check if JSON response was saved
            json_file = mock_json_debug_dir / f"{patient_name}_chatgpt5_analysis_response.json"
            assert json_file.exists()

    def test_call_openai_api_analysis_no_api_key(self, mock_openai_client):
        """Test ChatGPT 5 analysis API call without API key using AAA approach"""
        # Arrange
        with patch('dental_consultation_pipeline.analysis.LLM_CONFIG') as mock_config:
            mock_config.__getitem__.return_value = None
            mock_config.get.return_value = None
            transcript = "Test transcript"
            patient_name = "Test Patient"
        
        # Act
        result = call_openai_api_analysis(transcript, patient_name)
        
        # Assert
        assert result is None
        mock_openai_client.chat.completions.create.assert_not_called()

    def test_call_openai_api_analysis_openai_not_available(self):
        """Test ChatGPT 5 analysis API call when OpenAI client is not available using AAA approach"""
        # Arrange
        with patch('dental_consultation_pipeline.analysis.OpenAI', None):
            transcript = "Test transcript"
            patient_name = "Test Patient"
        
        # Act
        result = call_openai_api_analysis(transcript, patient_name)
        
        # Assert
        assert result is None

    def test_call_openai_api_analysis_api_error(self, mock_openai_client):
        """Test ChatGPT 5 analysis API call with API error using AAA approach"""
        # Arrange
        mock_openai_client.chat.completions.create.side_effect = Exception("API Error")
        transcript = "Test transcript"
        patient_name = "Test Patient"
        
        # Act
        result = call_openai_api_analysis(transcript, patient_name)
        
        # Assert
        assert result is None

    def test_call_openai_api_summary_success(self, mock_openai_client, mock_json_debug_dir):
        """Test successful ChatGPT 5 summary API call using AAA approach"""
        # Arrange
        transcript = "Test transcript content"
        patient_name = "Test Patient"
        expected_response = {
            "choices": [{"message": {"content": "Test summary content"}}],
            "usage": {"input_tokens": 80, "output_tokens": 150}
        }
        # Create a proper mock response that can be serialized
        mock_response = MagicMock()
        mock_response.model_dump.return_value = expected_response
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Test summary content"
        mock_openai_client.chat.completions.create.return_value = mock_response
        
        # Mock the API key and config - need to mock both the API key and model
        with patch('dental_consultation_pipeline.analysis.LLM_CONFIG') as mock_config:
            # Mock the config to return different values based on the key
            def mock_getitem(key):
                if key == "openai_api_key":
                    return "test-api-key"
                elif key == "openai_model":
                    return "gpt-5o"
                else:
                    return None
            mock_config.__getitem__.side_effect = mock_getitem
            mock_config.get.side_effect = mock_getitem
            
            # Act
            result = call_openai_api_summary(transcript, patient_name)
            
            # Assert
            assert result == "Test summary content"
            mock_openai_client.chat.completions.create.assert_called_once()
            call_args = mock_openai_client.chat.completions.create.call_args
            # Check keyword arguments (kwargs) - model is a keyword argument
            assert call_args[1]['model'] == 'gpt-4o'
            assert call_args[1]['max_tokens'] == 1500
            assert call_args[1]['messages'][0]['role'] == 'user'
            assert 'Test transcript content' in call_args[1]['messages'][0]['content']
            
            # Check if JSON response was saved
            json_file = mock_json_debug_dir / f"{patient_name}_chatgpt5_summary_response.json"
            assert json_file.exists()

    def test_call_openai_api_summary_no_api_key(self, mock_openai_client):
        """Test ChatGPT 5 summary API call without API key using AAA approach"""
        # Arrange
        with patch('dental_consultation_pipeline.analysis.LLM_CONFIG') as mock_config:
            mock_config.__getitem__.return_value = None
            mock_config.get.return_value = None
            transcript = "Test transcript"
            patient_name = "Test Patient"
        
        # Act
        result = call_openai_api_summary(transcript, patient_name)
        
        # Assert
        assert result is None
        mock_openai_client.chat.completions.create.assert_not_called()

    def test_generate_summary_with_llm_chatgpt5(self, mock_openai_client):
        """Test generate_summary_with_llm using ChatGPT 5 model using AAA approach"""
        # Arrange
        transcript = "Test transcript"
        patient_name = "Test Patient"
        # Create a proper mock response that can be serialized
        mock_response = MagicMock()
        mock_response.model_dump.return_value = {"choices": [{"message": {"content": "Test summary"}}]}
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Test summary"
        mock_openai_client.chat.completions.create.return_value = mock_response
        
        # Mock the API key
        with patch('dental_consultation_pipeline.analysis.LLM_CONFIG') as mock_config:
            mock_config.__getitem__.return_value = "test-api-key"
            mock_config.get.return_value = "test-api-key"
            
            # Act
            result = generate_summary_with_llm(transcript, patient_name, "chatgpt5")
            
            # Assert
            assert result == "Test summary"
            mock_openai_client.chat.completions.create.assert_called_once()

    def test_generate_summary_with_llm_claude(self):
        """Test generate_summary_with_llm using Claude model using AAA approach"""
        # Arrange
        transcript = "Test transcript"
        patient_name = "Test Patient"
        
        with patch('dental_consultation_pipeline.analysis.call_anthropic_api_summary') as mock_claude:
            mock_claude.return_value = "Claude summary"
            
            # Act
            result = generate_summary_with_llm(transcript, patient_name, "claude")
            
            # Assert
            assert result == "Claude summary"
            mock_claude.assert_called_once_with(transcript, patient_name)

    def test_generate_summary_with_llm_unknown_model(self):
        """Test generate_summary_with_llm with unknown model using AAA approach"""
        # Arrange
        transcript = "Test transcript"
        patient_name = "Test Patient"
        
        with patch('dental_consultation_pipeline.analysis.call_anthropic_api_summary') as mock_claude:
            mock_claude.return_value = "Default summary"
            
            # Act
            result = generate_summary_with_llm(transcript, patient_name, "unknown_model")
            
            # Assert
            assert result == "Default summary"
            mock_claude.assert_called_once_with(transcript, patient_name)

    def test_generate_analysis_with_llm_chatgpt5(self, mock_openai_client):
        """Test generate_analysis_with_llm using ChatGPT 5 model using AAA approach"""
        # Arrange
        transcript = "Test transcript"
        patient_name = "Test Patient"
        # Create a proper mock response that can be serialized
        mock_response = MagicMock()
        mock_response.model_dump.return_value = {"choices": [{"message": {"content": "Test analysis"}}]}
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Test analysis"
        mock_openai_client.chat.completions.create.return_value = mock_response
        
        # Mock the API key
        with patch('dental_consultation_pipeline.analysis.LLM_CONFIG') as mock_config:
            mock_config.__getitem__.return_value = "test-api-key"
            mock_config.get.return_value = "test-api-key"
            
            # Act
            result = generate_analysis_with_llm(transcript, patient_name, "chatgpt5")
            
            # Assert
            assert result == "Test analysis"
            mock_openai_client.chat.completions.create.assert_called_once()

    def test_generate_analysis_with_llm_claude(self):
        """Test generate_analysis_with_llm using Claude model using AAA approach"""
        # Arrange
        transcript = "Test transcript"
        patient_name = "Test Patient"
        
        with patch('dental_consultation_pipeline.analysis.call_anthropic_api_analysis') as mock_claude:
            mock_claude.return_value = "Claude analysis"
            
            # Act
            result = generate_analysis_with_llm(transcript, patient_name, "claude")
            
            # Assert
            assert result == "Claude analysis"
            mock_claude.assert_called_once_with(transcript, patient_name)

    def test_generate_summary_with_chatgpt5(self, mock_openai_client):
        """Test generate_summary_with_chatgpt5 function using AAA approach"""
        # Arrange
        transcript = "Test transcript"
        patient_name = "Test Patient"
        # Create a proper mock response that can be serialized
        mock_response = MagicMock()
        mock_response.model_dump.return_value = {"choices": [{"message": {"content": "ChatGPT 5 summary"}}]}
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "ChatGPT 5 summary"
        mock_openai_client.chat.completions.create.return_value = mock_response
        
        # Mock the API key
        with patch('dental_consultation_pipeline.analysis.LLM_CONFIG') as mock_config:
            mock_config.__getitem__.return_value = "test-api-key"
            mock_config.get.return_value = "test-api-key"
            
            # Act
            result = generate_summary_with_chatgpt5(transcript, patient_name)
            
            # Assert
            assert result == "ChatGPT 5 summary"
            mock_openai_client.chat.completions.create.assert_called_once()

    def test_generate_analysis_with_chatgpt5(self, mock_openai_client):
        """Test generate_analysis_with_chatgpt5 function using AAA approach"""
        # Arrange
        transcript = "Test transcript"
        patient_name = "Test Patient"
        # Create a proper mock response that can be serialized
        mock_response = MagicMock()
        mock_response.model_dump.return_value = {"choices": [{"message": {"content": "ChatGPT 5 analysis"}}]}
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "ChatGPT 5 analysis"
        mock_openai_client.chat.completions.create.return_value = mock_response
        
        # Mock the API key
        with patch('dental_consultation_pipeline.analysis.LLM_CONFIG') as mock_config:
            mock_config.__getitem__.return_value = "test-api-key"
            mock_config.get.return_value = "test-api-key"
            
            # Act
            result = generate_analysis_with_chatgpt5(transcript, patient_name)
            
            # Assert
            assert result == "ChatGPT 5 analysis"
            mock_openai_client.chat.completions.create.assert_called_once()

    def test_save_summary_to_markdown_chatgpt5(self, mock_output_dir):
        """Test saving ChatGPT 5 summary to markdown using AAA approach"""
        # Arrange
        summary_content = "# Test Summary\n\nThis is a test summary."
        patient_name = "Test Patient"
        
        # Act
        result = save_summary_to_markdown(summary_content, patient_name, "chatgpt5")
        
        # Assert
        assert result is not None
        expected_file = mock_output_dir / "Test Patient_chatgpt5_summary.md"
        assert expected_file.exists()
        
        with open(expected_file, 'r', encoding='utf-8') as f:
            content = f.read()
        assert content == summary_content

    def test_save_summary_to_markdown_claude(self, mock_output_dir):
        """Test saving Claude summary to markdown using AAA approach"""
        # Arrange
        summary_content = "# Test Summary\n\nThis is a test summary."
        patient_name = "Test Patient"
        
        # Act
        result = save_summary_to_markdown(summary_content, patient_name, "claude")
        
        # Assert
        assert result is not None
        expected_file = mock_output_dir / "Test Patient_summary.md"
        assert expected_file.exists()
        
        with open(expected_file, 'r', encoding='utf-8') as f:
            content = f.read()
        assert content == summary_content

    def test_save_analysis_to_markdown_chatgpt5(self, mock_output_dir):
        """Test saving ChatGPT 5 analysis to markdown using AAA approach"""
        # Arrange
        analysis_content = "# Test Analysis\n\nThis is a test analysis."
        patient_name = "Test Patient"
        
        # Act
        result = save_analysis_to_markdown(analysis_content, patient_name, "chatgpt5")
        
        # Assert
        assert result is not None
        expected_file = mock_output_dir / "Test Patient_chatgpt5_analysis.md"
        assert expected_file.exists()
        
        with open(expected_file, 'r', encoding='utf-8') as f:
            content = f.read()
        assert content == analysis_content

    def test_save_analysis_to_markdown_claude(self, mock_output_dir):
        """Test saving Claude analysis to markdown using AAA approach"""
        # Arrange
        analysis_content = "# Test Analysis\n\nThis is a test analysis."
        patient_name = "Test Patient"
        
        # Act
        result = save_analysis_to_markdown(analysis_content, patient_name, "claude")
        
        # Assert
        assert result is not None
        expected_file = mock_output_dir / "Test Patient_analysis.md"
        assert expected_file.exists()
        
        with open(expected_file, 'r', encoding='utf-8') as f:
            content = f.read()
        assert content == analysis_content

    @patch('dental_consultation_pipeline.analysis.generate_summary_with_llm')
    @patch('dental_consultation_pipeline.analysis.generate_analysis_with_llm')
    @patch('dental_consultation_pipeline.analysis.save_summary_to_markdown')
    @patch('dental_consultation_pipeline.analysis.save_analysis_to_markdown')
    @patch('dental_consultation_pipeline.analysis.read_transcript')
    def test_process_transcript_with_llm_chatgpt5_success(self, mock_read, mock_save_analysis, 
                                                        mock_save_summary, mock_generate_analysis, 
                                                        mock_generate_summary, mock_output_dir):
        """Test successful transcript processing with ChatGPT 5 using AAA approach"""
        # Arrange
        transcript_path = Path("test_transcript_clean.txt")
        mock_read.return_value = "Test transcript content"
        mock_generate_summary.return_value = "Test summary"
        mock_generate_analysis.return_value = "Test analysis"
        mock_save_summary.return_value = mock_output_dir / "test_summary.md"
        mock_save_analysis.return_value = mock_output_dir / "test_analysis.md"
        
        # Act
        result = process_transcript_with_llm(transcript_path, "chatgpt5")
        
        # Assert
        assert result is True
        mock_generate_summary.assert_called_once_with("Test transcript content", "test_transcript", "chatgpt5")
        mock_generate_analysis.assert_called_once_with("Test transcript content", "test_transcript", "chatgpt5")
        mock_save_summary.assert_called_once_with("Test summary", "test_transcript", "chatgpt5")
        mock_save_analysis.assert_called_once_with("Test analysis", "test_transcript", "chatgpt5")

    @patch('dental_consultation_pipeline.analysis.generate_summary_with_llm')
    @patch('dental_consultation_pipeline.analysis.generate_analysis_with_llm')
    @patch('dental_consultation_pipeline.analysis.save_summary_to_markdown')
    @patch('dental_consultation_pipeline.analysis.save_analysis_to_markdown')
    @patch('dental_consultation_pipeline.analysis.read_transcript')
    def test_process_transcript_with_llm_chatgpt5_partial_failure(self, mock_read, mock_save_analysis, 
                                                                 mock_save_summary, mock_generate_analysis, 
                                                                 mock_generate_summary, mock_output_dir):
        """Test transcript processing with ChatGPT 5 partial failure using AAA approach"""
        # Arrange
        transcript_path = Path("test_transcript_clean.txt")
        mock_read.return_value = "Test transcript content"
        mock_generate_summary.return_value = "Test summary"
        mock_generate_analysis.return_value = None  # Analysis fails
        mock_save_summary.return_value = mock_output_dir / "test_summary.md"
        mock_save_analysis.return_value = None
        
        # Act
        result = process_transcript_with_llm(transcript_path, "chatgpt5")
        
        # Assert
        assert result is False
        mock_generate_summary.assert_called_once()
        mock_generate_analysis.assert_called_once()
        mock_save_summary.assert_called_once()
        mock_save_analysis.assert_not_called()

    @patch('dental_consultation_pipeline.analysis.get_clean_transcript_files')
    @patch('dental_consultation_pipeline.analysis.process_transcript_with_llm')
    @patch('dental_consultation_pipeline.analysis.extract_patient_name')
    def test_analyze_clean_transcripts_chatgpt5(self, mock_extract_name, mock_process, mock_get_files):
        """Test analyze_clean_transcripts with ChatGPT 5 model using AAA approach"""
        # Arrange
        mock_get_files.return_value = [Path("test1_clean.txt"), Path("test2_clean.txt")]
        mock_extract_name.side_effect = ["Test Patient 1", "Test Patient 2"]
        mock_process.side_effect = [True, True]
        
        with patch('dental_consultation_pipeline.analysis.PROJECT_ROOT') as mock_root:
            mock_audio_dir = MagicMock()
            mock_audio_dir.exists.return_value = True
            mock_audio_dir.iterdir.return_value = [
                MagicMock(stem="test1", suffix=".m4a"),
                MagicMock(stem="test2", suffix=".m4a")
            ]
            mock_root.__truediv__.return_value = mock_audio_dir
            
            with patch('dental_consultation_pipeline.analysis.OUTPUT_DIR') as mock_output:
                mock_output.__truediv__.return_value.exists.return_value = False
                
                # Act
                result = analyze_clean_transcripts("chatgpt5")
                
                # Assert
                assert len(result) == 2
                assert mock_process.call_count == 2
                mock_process.assert_any_call(Path("test1_clean.txt"), "chatgpt5")
                mock_process.assert_any_call(Path("test2_clean.txt"), "chatgpt5")

    @patch('dental_consultation_pipeline.analysis.analyze_clean_transcripts')
    def test_analyze_clean_transcripts_with_both_models(self, mock_analyze):
        """Test analyze_clean_transcripts_with_both_models using AAA approach"""
        # Arrange
        mock_analyze.side_effect = [["claude_result1", "claude_result2"], ["chatgpt5_result1"]]
        
        # Act
        result = analyze_clean_transcripts_with_both_models()
        
        # Assert
        assert len(result) == 3
        assert mock_analyze.call_count == 2
        mock_analyze.assert_any_call("claude")
        mock_analyze.assert_any_call("chatgpt5")

    def test_calculate_token_usage_with_chatgpt5_files(self, mock_json_debug_dir):
        """Test calculate_token_usage with ChatGPT 5 JSON files using AAA approach"""
        # Arrange
        claude_response = {
            "usage": {"input_tokens": 100, "output_tokens": 200}
        }
        chatgpt5_response = {
            "usage": {"input_tokens": 150, "output_tokens": 300}
        }
        
        # Create test JSON files
        with open(mock_json_debug_dir / "test_claude_response.json", 'w') as f:
            json.dump(claude_response, f)
        with open(mock_json_debug_dir / "test_chatgpt5_response.json", 'w') as f:
            json.dump(chatgpt5_response, f)
        
        # Act
        with patch('builtins.print') as mock_print:
            calculate_token_usage()
        
        # Assert
        # Verify that the function processed both files
        assert mock_print.call_count > 0
        # Check that cost calculations were performed
        calls = [call.args[0] for call in mock_print.call_args_list]
        cost_calls = [call for call in calls if '$' in call]
        assert len(cost_calls) > 0

    def test_calculate_token_usage_no_files(self):
        """Test calculate_token_usage with no JSON files using AAA approach"""
        # Arrange
        with patch('dental_consultation_pipeline.analysis.JSON_DEBUG_DIR') as mock_dir:
            mock_dir.exists.return_value = False
            
            # Act
            with patch('builtins.print') as mock_print:
                calculate_token_usage()
            
            # Assert
            mock_print.assert_called_with("❌ No JSON debug directory found. Run analysis first to generate token data.")

    def test_calculate_token_usage_empty_directory(self):
        """Test calculate_token_usage with empty directory using AAA approach"""
        # Arrange
        with patch('dental_consultation_pipeline.analysis.JSON_DEBUG_DIR') as mock_dir:
            mock_dir.exists.return_value = True
            mock_dir.glob.return_value = []
            
            # Act
            with patch('builtins.print') as mock_print:
                calculate_token_usage()
            
            # Assert
            mock_print.assert_called_with("❌ No JSON response files found. Run analysis first to generate token data.")

    def test_model_configuration(self):
        """Test LLM configuration for ChatGPT 5 using AAA approach"""
        # Arrange & Act
        config = LLM_CONFIG
        
        # Assert
        assert 'openai_api_key' in config
        assert 'openai_model' in config
        assert config['openai_model'] == 'gpt-4o'

    def test_openai_client_import_success(self):
        """Test successful OpenAI client import using AAA approach"""
        # Arrange & Act
        # This test verifies that the OpenAI client can be imported
        # Note: This test is skipped if openai package is not installed
        try:
            from dental_consultation_pipeline.analysis import OpenAI
            # Assert
            if OpenAI is None:
                pytest.skip("OpenAI package not installed - skipping import test")
            assert OpenAI is not None
        except ImportError:
            # Skip this test if openai package is not installed
            pytest.skip("OpenAI package not installed - skipping import test")

    def test_openai_client_import_failure(self):
        """Test OpenAI client import failure using AAA approach"""
        # Arrange
        with patch('dental_consultation_pipeline.analysis.OpenAI', None):
            # Act
            result = call_openai_api_analysis("test", "test")
            
            # Assert
            assert result is None

    def test_filename_patterns_for_chatgpt5(self, mock_output_dir):
        """Test filename patterns for ChatGPT 5 outputs using AAA approach"""
        # Arrange
        test_cases = [
            ("chatgpt5", "Test Patient", "Test Patient_chatgpt5_summary.md", "Test Patient_chatgpt5_analysis.md"),
            ("gpt5", "Test Patient", "Test Patient_chatgpt5_summary.md", "Test Patient_chatgpt5_analysis.md"),
            ("gpt-5", "Test Patient", "Test Patient_chatgpt5_summary.md", "Test Patient_chatgpt5_analysis.md"),
            ("claude", "Test Patient", "Test Patient_summary.md", "Test Patient_analysis.md"),
        ]
        
        for model, patient_name, expected_summary, expected_analysis in test_cases:
            # Act
            summary_result = save_summary_to_markdown("Test content", patient_name, model)
            analysis_result = save_analysis_to_markdown("Test content", patient_name, model)
            
            # Assert
            assert summary_result is not None
            assert analysis_result is not None
            assert summary_result.name == expected_summary
            assert analysis_result.name == expected_analysis

    def test_error_handling_in_chatgpt5_functions(self, mock_openai_client):
        """Test error handling in ChatGPT 5 functions using AAA approach"""
        # Arrange
        mock_openai_client.chat.completions.create.side_effect = Exception("Network error")
        
        # Mock the API key
        with patch('dental_consultation_pipeline.analysis.LLM_CONFIG') as mock_config:
            mock_config.__getitem__.return_value = "test-api-key"
            mock_config.get.return_value = "test-api-key"
            
            # Act
            summary_result = call_openai_api_summary("test", "test")
            analysis_result = call_openai_api_analysis("test", "test")
            
            # Assert
            assert summary_result is None
            assert analysis_result is None

    def test_api_key_validation(self):
        """Test API key validation for ChatGPT 5 using AAA approach"""
        # Arrange
        with patch('dental_consultation_pipeline.analysis.LLM_CONFIG') as mock_config:
            mock_config.__getitem__.return_value = None
            
            # Act
            summary_result = call_openai_api_summary("test", "test")
            analysis_result = call_openai_api_analysis("test", "test")
            
            # Assert
            assert summary_result is None
            assert analysis_result is None

    @patch('dental_consultation_pipeline.analysis.generate_summary_with_llm')
    @patch('dental_consultation_pipeline.analysis.generate_analysis_with_llm')
    @patch('dental_consultation_pipeline.analysis.save_summary_to_markdown')
    @patch('dental_consultation_pipeline.analysis.save_analysis_to_markdown')
    @patch('dental_consultation_pipeline.analysis.read_transcript')
    def test_process_transcript_with_llm_model_parameter(self, mock_read, mock_save_analysis, 
                                                        mock_save_summary, mock_generate_analysis, 
                                                        mock_generate_summary, mock_output_dir):
        """Test process_transcript_with_llm with different model parameters using AAA approach"""
        # Arrange
        transcript_path = Path("test_transcript_clean.txt")
        mock_read.return_value = "Test transcript content"
        mock_generate_summary.return_value = "Test summary"
        mock_generate_analysis.return_value = "Test analysis"
        mock_save_summary.return_value = mock_output_dir / "test_summary.md"
        mock_save_analysis.return_value = mock_output_dir / "test_analysis.md"
        
        test_models = ["chatgpt5", "gpt5", "gpt-5", "claude"]
        
        for model in test_models:
            # Act
            result = process_transcript_with_llm(transcript_path, model)
            
            # Assert
            assert result is True
            mock_generate_summary.assert_called_with("Test transcript content", "test_transcript", model)
            mock_generate_analysis.assert_called_with("Test transcript content", "test_transcript", model)
            
            # Reset mocks for next iteration
            mock_generate_summary.reset_mock()
            mock_generate_analysis.reset_mock()
