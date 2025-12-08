"""
Comprehensive tests for the analysis module
"""

import pytest
import tempfile
import os
import json
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
from dental_consultation_pipeline.analysis import (
    get_clean_transcript_files,
    extract_patient_name,
    read_transcript,
    generate_summary_with_llm,
    generate_analysis_with_llm,
    save_summary_to_markdown,
    save_analysis_to_markdown,
    process_transcript_with_llm,
    analyze_clean_transcripts,
    calculate_token_usage,
    call_anthropic_api_analysis,
    call_anthropic_api_summary,
    main,
    LLM_CONFIG
)

class TestAnalysisModule:
    """Test the analysis module functionality"""
    
    def test_get_clean_transcript_files(self):
        """Test getting clean transcript files"""
        # This will test the function that finds clean transcript files
        files = get_clean_transcript_files()
        assert isinstance(files, list)
        # All files should end with _clean.txt
        for file in files:
            assert file.name.endswith('_clean.txt')
    
    def test_get_clean_transcript_files_directory_not_exists(self):
        """Test getting clean transcript files when directory doesn't exist"""
        with patch('dental_consultation_pipeline.analysis.TRANSCRIPTS_CLEAN_DIR') as mock_dir:
            mock_dir.exists.return_value = False
            files = get_clean_transcript_files()
            assert files == []
    
    def test_extract_patient_name(self):
        """Test patient name extraction from filenames"""
        test_cases = [
            ("Andrew woulters consult_clean.txt", "Andrew woulters"),
            ("Christina King consult_clean.txt", "Christina King"),
            ("consult1_clean.txt", "Consult 1"),
            ("Voice 250618_115741_clean.txt", "Voice Recording 250618_115741"),
            ("test_file.txt", "test_file"),
        ]
        
        for filename, expected in test_cases:
            result = extract_patient_name(filename)
            assert result == expected
    
    def test_extract_patient_name_edge_cases(self):
        """Test patient name extraction edge cases"""
        test_cases = [
            ("Andrew woulters cinsult_clean.txt", "Andrew woulters"),  # Typo in 'consult'
            ("John Doe photos_clean.txt", "John Doe"),  # 'photos' suffix
            ("Voice 250618_115741.txt", "Voice Recording 250618_115741"),  # No _clean suffix
            ("consult1.txt", "Consult 1"),  # No _clean suffix
            ("", ""),  # Empty string
            ("_clean.txt", ""),  # Only suffix
            ("test_clean_clean.txt", "test_clean"),  # Double _clean - only first one removed
            ("Andrew woulters consult cinsult_clean.txt", "Andrew woulters consult"),  # Multiple suffixes - only first match removed
        ]
        
        for filename, expected in test_cases:
            result = extract_patient_name(filename)
            assert result == expected
    
    def test_read_transcript(self):
        """Test reading transcript files"""
        # Create a temporary test file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            test_content = "This is a test transcript content."
            f.write(test_content)
            temp_file = f.name
        
        try:
            # Test reading the file
            content = read_transcript(Path(temp_file))
            assert content == test_content
        finally:
            # Clean up
            os.unlink(temp_file)
    
    def test_read_transcript_nonexistent(self):
        """Test reading a non-existent transcript file"""
        result = read_transcript(Path("nonexistent_file.txt"))
        assert result is None
    
    def test_read_transcript_permission_error(self):
        """Test reading transcript with permission error"""
        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            result = read_transcript(Path("test.txt"))
            assert result is None
    
    def test_read_transcript_unicode_error(self):
        """Test reading transcript with encoding error"""
        with patch('builtins.open', side_effect=UnicodeDecodeError("utf-8", b"", 0, 1, "Invalid UTF-8")):
            result = read_transcript(Path("test.txt"))
            assert result is None
    
    @patch('dental_consultation_pipeline.analysis.requests.post')
    def test_call_anthropic_api_analysis_success(self, mock_post):
        """Test successful analysis API call"""
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": [{"text": "Test analysis content"}],
            "usage": {"input_tokens": 1000, "output_tokens": 500}
        }
        mock_post.return_value = mock_response
        
        # Mock file operations
        with patch('builtins.open', mock_open()) as mock_file:
            with patch('dental_consultation_pipeline.analysis.JSON_DEBUG_DIR') as mock_dir:
                mock_dir.mkdir.return_value = None
                
                # Test the function
                result = call_anthropic_api_analysis("Test transcript", "Test Patient")
                assert result == "Test analysis content"
                
                # Verify API call
                mock_post.assert_called_once()
                call_args = mock_post.call_args
                assert call_args[1]['json']['model'] == LLM_CONFIG["anthropic_model"]
                assert "Test transcript" in call_args[1]['json']['messages'][0]['content']
    
    @patch('dental_consultation_pipeline.analysis.requests.post')
    def test_call_anthropic_api_analysis_no_api_key(self, mock_post):
        """Test analysis API call without API key"""
        with patch('dental_consultation_pipeline.analysis.LLM_CONFIG', {"anthropic_api_key": None, "anthropic_model": "test"}):
            result = call_anthropic_api_analysis("Test transcript", "Test Patient")
            assert result is None
            mock_post.assert_not_called()
    
    @patch('dental_consultation_pipeline.analysis.requests.post')
    def test_call_anthropic_api_analysis_http_error(self, mock_post):
        """Test analysis API call with HTTP error"""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("HTTP Error")
        mock_post.return_value = mock_response
        
        result = call_anthropic_api_analysis("Test transcript", "Test Patient")
        assert result is None
    
    @patch('dental_consultation_pipeline.analysis.requests.post')
    def test_call_anthropic_api_analysis_timeout(self, mock_post):
        """Test analysis API call with timeout"""
        mock_post.side_effect = Exception("Timeout")
        
        result = call_anthropic_api_analysis("Test transcript", "Test Patient")
        assert result is None
    
    @patch('dental_consultation_pipeline.analysis.requests.post')
    def test_call_anthropic_api_summary_success(self, mock_post):
        """Test successful summary API call"""
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": [{"text": "Test summary content"}],
            "usage": {"input_tokens": 800, "output_tokens": 300}
        }
        mock_post.return_value = mock_response
        
        # Mock file operations
        with patch('builtins.open', mock_open()) as mock_file:
            with patch('dental_consultation_pipeline.analysis.JSON_DEBUG_DIR') as mock_dir:
                mock_dir.mkdir.return_value = None
                
                # Test the function
                result = call_anthropic_api_summary("Test transcript", "Test Patient")
                assert result == "Test summary content"
                
                # Verify API call
                mock_post.assert_called_once()
                call_args = mock_post.call_args
                assert call_args[1]['json']['model'] == LLM_CONFIG["anthropic_model"]
                assert "Test transcript" in call_args[1]['json']['messages'][0]['content']
    
    @patch('dental_consultation_pipeline.analysis.requests.post')
    def test_call_anthropic_api_summary_no_api_key(self, mock_post):
        """Test summary API call without API key"""
        with patch('dental_consultation_pipeline.analysis.LLM_CONFIG', {"anthropic_api_key": None, "anthropic_model": "test"}):
            result = call_anthropic_api_summary("Test transcript", "Test Patient")
            assert result is None
            mock_post.assert_not_called()
    
    @patch('dental_consultation_pipeline.analysis.requests.post')
    def test_generate_summary_with_llm_success(self, mock_post):
        """Test successful summary generation"""
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": [{"text": "Test summary content"}]
        }
        mock_post.return_value = mock_response
        
        # Test the function
        result = generate_summary_with_llm("Test transcript", "Test Patient")
        assert result == "Test summary content"
    
    @patch('dental_consultation_pipeline.analysis.requests.post')
    def test_generate_summary_with_llm_failure(self, mock_post):
        """Test failed summary generation"""
        # Mock failed API response
        mock_post.side_effect = Exception("API Error")
        
        # Test the function
        result = generate_summary_with_llm("Test transcript", "Test Patient")
        assert result is None
    
    @patch('dental_consultation_pipeline.analysis.requests.post')
    def test_generate_analysis_with_llm_success(self, mock_post):
        """Test successful analysis generation"""
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": [{"text": "Test analysis content"}]
        }
        mock_post.return_value = mock_response
        
        # Test the function
        result = generate_analysis_with_llm("Test transcript", "Test Patient")
        assert result == "Test analysis content"
    
    def test_save_summary_to_markdown(self):
        """Test saving summary to markdown"""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            summary_content = "# Test Summary\n\nThis is a test summary."
            
            # Patch the OUTPUT_DIR to use our temporary directory
            with patch('dental_consultation_pipeline.analysis.OUTPUT_DIR', output_dir):
                # Test saving
                result = save_summary_to_markdown(summary_content, "Test Patient")
                
                # Check if file was created
                expected_file = output_dir / "Test Patient_summary.md"
                assert expected_file.exists()
                
                # Check content
                with open(expected_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                assert content == summary_content
    
    def test_save_summary_to_markdown_error(self):
        """Test saving summary to markdown with error"""
        with patch('builtins.open', side_effect=Exception("Write error")):
            result = save_summary_to_markdown("Test content", "Test Patient")
            assert result is None
    
    def test_save_analysis_to_markdown(self):
        """Test saving analysis to markdown"""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            analysis_content = "# Test Analysis\n\nThis is a test analysis."
            
            # Patch the OUTPUT_DIR to use our temporary directory
            with patch('dental_consultation_pipeline.analysis.OUTPUT_DIR', output_dir):
                # Test saving
                result = save_analysis_to_markdown(analysis_content, "Test Patient")
                
                # Check if file was created
                expected_file = output_dir / "Test Patient_analysis.md"
                assert expected_file.exists()
                
                # Check content
                with open(expected_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                assert content == analysis_content
    
    def test_save_analysis_to_markdown_error(self):
        """Test saving analysis to markdown with error"""
        with patch('builtins.open', side_effect=Exception("Write error")):
            result = save_analysis_to_markdown("Test content", "Test Patient")
            assert result is None
    
    @patch('dental_consultation_pipeline.analysis.generate_summary_with_llm')
    @patch('dental_consultation_pipeline.analysis.generate_analysis_with_llm')
    @patch('dental_consultation_pipeline.analysis.save_summary_to_markdown')
    @patch('dental_consultation_pipeline.analysis.save_analysis_to_markdown')
    def test_process_transcript_with_llm_success(self, mock_save_analysis, mock_save_summary, 
                                               mock_generate_analysis, mock_generate_summary):
        """Test successful transcript processing"""
        # Mock successful responses
        mock_generate_summary.return_value = "Test summary"
        mock_generate_analysis.return_value = "Test analysis"
        mock_save_summary.return_value = Path("test_summary.md")
        mock_save_analysis.return_value = Path("test_analysis.md")
        
        # Create a temporary test file
        with tempfile.NamedTemporaryFile(mode='w', suffix='_clean.txt', delete=False) as f:
            f.write("Test transcript content")
            temp_file = Path(f.name)
        
        try:
            # Test processing
            result = process_transcript_with_llm(temp_file)
            assert result is True
            
            # Verify functions were called
            mock_generate_summary.assert_called_once()
            mock_generate_analysis.assert_called_once()
            mock_save_summary.assert_called_once()
            mock_save_analysis.assert_called_once()
        finally:
            # Clean up
            os.unlink(temp_file)
    
    @patch('dental_consultation_pipeline.analysis.read_transcript')
    def test_process_transcript_with_llm_read_failure(self, mock_read):
        """Test transcript processing when reading fails"""
        mock_read.return_value = None
        
        # Create a temporary test file
        with tempfile.NamedTemporaryFile(mode='w', suffix='_clean.txt', delete=False) as f:
            f.write("Test transcript content")
            temp_file = Path(f.name)
        
        try:
            # Test processing
            result = process_transcript_with_llm(temp_file)
            assert result is False
        finally:
            # Clean up
            os.unlink(temp_file)
    
    @patch('dental_consultation_pipeline.analysis.generate_summary_with_llm')
    @patch('dental_consultation_pipeline.analysis.generate_analysis_with_llm')
    @patch('dental_consultation_pipeline.analysis.save_summary_to_markdown')
    @patch('dental_consultation_pipeline.analysis.save_analysis_to_markdown')
    def test_process_transcript_with_llm_partial_failure(self, mock_save_analysis, mock_save_summary, 
                                                        mock_generate_analysis, mock_generate_summary):
        """Test transcript processing with partial failure"""
        # Mock partial success - summary succeeds, analysis fails
        mock_generate_summary.return_value = "Test summary"
        mock_generate_analysis.return_value = None
        mock_save_summary.return_value = Path("test_summary.md")
        mock_save_analysis.return_value = None
        
        # Create a temporary test file
        with tempfile.NamedTemporaryFile(mode='w', suffix='_clean.txt', delete=False) as f:
            f.write("Test transcript content")
            temp_file = Path(f.name)
        
        try:
            # Test processing
            result = process_transcript_with_llm(temp_file)
            assert result is False  # Should fail because not both summary and analysis succeeded
        finally:
            # Clean up
            os.unlink(temp_file)
    
    @patch('dental_consultation_pipeline.analysis.get_clean_transcript_files')
    @patch('dental_consultation_pipeline.analysis.process_transcript_with_llm')
    def test_analyze_clean_transcripts_no_new_files(self, mock_process, mock_get_files):
        """Test analyze_clean_transcripts when no new files exist"""
        # Mock no new files to process
        mock_get_files.return_value = []
        
        result = analyze_clean_transcripts()
        assert result == []
        mock_process.assert_not_called()
    
    @patch('dental_consultation_pipeline.analysis.get_clean_transcript_files')
    @patch('dental_consultation_pipeline.analysis.process_transcript_with_llm')
    @patch('dental_consultation_pipeline.analysis.OUTPUT_DIR')
    def test_analyze_clean_transcripts_with_new_files(self, mock_output_dir, mock_process, mock_get_files):
        """Test analyze_clean_transcripts with new files to process"""
        # Mock files that need processing
        mock_file1 = MagicMock()
        mock_file1.name = "test1_clean.txt"
        mock_file2 = MagicMock()
        mock_file2.name = "test2_clean.txt"
        mock_get_files.return_value = [mock_file1, mock_file2]
        
        # Mock output directory and file existence checks
        mock_output_dir.__truediv__.return_value.exists.return_value = False
        mock_process.return_value = True
        
        result = analyze_clean_transcripts()
        assert len(result) == 2
        assert mock_process.call_count == 2
    
    def test_llm_config_structure(self):
        """Test LLM configuration structure"""
        assert "anthropic_api_key" in LLM_CONFIG
        assert "anthropic_model" in LLM_CONFIG
        assert isinstance(LLM_CONFIG["anthropic_model"], str)
    
    def test_calculate_token_usage_no_directory(self):
        """Test token usage calculation with no directory"""
        with patch('dental_consultation_pipeline.analysis.JSON_DEBUG_DIR') as mock_dir:
            mock_dir.exists.return_value = False
            result = calculate_token_usage()
            assert result is None
    
    def test_calculate_token_usage_no_files(self):
        """Test token usage calculation with no files"""
        with patch('dental_consultation_pipeline.analysis.JSON_DEBUG_DIR') as mock_dir:
            mock_dir.exists.return_value = True
            mock_dir.glob.return_value = []
            result = calculate_token_usage()
            assert result is None
    
    @patch('dental_consultation_pipeline.analysis.JSON_DEBUG_DIR')
    def test_calculate_token_usage_with_files(self, mock_json_dir):
        """Test token usage calculation with mock files"""
        # Create a mock JSON file structure
        mock_json_dir.exists.return_value = True
        mock_json_dir.glob.return_value = [
            Path("test_response.json")
        ]
        
        # Mock file reading
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = '''
            {
                "usage": {
                    "input_tokens": 1000,
                    "output_tokens": 500
                }
            }
            '''
            
            # Test the function
            result = calculate_token_usage()
            # Function should complete without error
            assert result is None
    
    @patch('dental_consultation_pipeline.analysis.JSON_DEBUG_DIR')
    def test_calculate_token_usage_with_invalid_json(self, mock_json_dir):
        """Test token usage calculation with invalid JSON"""
        mock_json_dir.exists.return_value = True
        mock_json_dir.glob.return_value = [Path("invalid.json")]
        
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = "invalid json"
            
            result = calculate_token_usage()
            assert result is None
    
    @patch('dental_consultation_pipeline.analysis.JSON_DEBUG_DIR')
    def test_calculate_token_usage_with_missing_usage(self, mock_json_dir):
        """Test token usage calculation with missing usage data"""
        mock_json_dir.exists.return_value = True
        mock_json_dir.glob.return_value = [Path("no_usage.json")]
        
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = '''
            {
                "content": "some content"
            }
            '''
            
            result = calculate_token_usage()
            assert result is None
    
    @patch('sys.argv', ['analysis.py', 'analyze'])
    @patch('dental_consultation_pipeline.analysis.analyze_clean_transcripts')
    def test_main_analyze(self, mock_analyze):
        """Test main function with analyze command"""
        main()
        mock_analyze.assert_called_once()
    
    @patch('sys.argv', ['analysis.py', 'tokens'])
    @patch('dental_consultation_pipeline.analysis.calculate_token_usage')
    def test_main_tokens(self, mock_tokens):
        """Test main function with tokens command"""
        main()
        mock_tokens.assert_called_once()
    
    @patch('sys.argv', ['analysis.py', 'invalid'])
    @patch('builtins.print')
    def test_main_invalid_command(self, mock_print):
        """Test main function with invalid command"""
        main()
        mock_print.assert_called()
    
    @patch('sys.argv', ['analysis.py'])
    @patch('builtins.print')
    def test_main_no_args(self, mock_print):
        """Test main function with no arguments"""
        main()
        mock_print.assert_called()
    
    def test_extract_patient_name_special_characters(self):
        """Test patient name extraction with special characters"""
        test_cases = [
            ("John-Doe consult_clean.txt", "John-Doe"),
            ("Mary Jane O'Connor consult_clean.txt", "Mary Jane O'Connor"),
            ("José García consult_clean.txt", "José García"),
            ("Dr. Smith consult_clean.txt", "Dr. Smith"),
            ("Patient_123 consult_clean.txt", "Patient_123"),
        ]
        
        for filename, expected in test_cases:
            result = extract_patient_name(filename)
            assert result == expected
    
    def test_extract_patient_name_case_sensitivity(self):
        """Test patient name extraction case sensitivity"""
        test_cases = [
            ("ANDREW WOULTERS consult_clean.txt", "ANDREW WOULTERS"),
            ("andrew woulters CONSULT_clean.txt", "andrew woulters"),
            ("Christina king CINSULT_clean.txt", "Christina king"),
        ]
        
        for filename, expected in test_cases:
            result = extract_patient_name(filename)
            assert result == expected
    
    def test_extract_patient_name_multiple_voice_recordings(self):
        """Test that multiple voice recordings get unique names"""
        test_cases = [
            ("Voice 250618_115741_clean.txt", "Voice Recording 250618_115741"),
            ("Voice 250701_161733_clean.txt", "Voice Recording 250701_161733"),
            ("Voice 250702_120000_clean.txt", "Voice Recording 250702_120000"),
        ]
        
        for filename, expected in test_cases:
            result = extract_patient_name(filename)
            assert result == expected 