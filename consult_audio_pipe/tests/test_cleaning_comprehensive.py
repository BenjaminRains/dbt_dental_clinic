"""
Comprehensive tests for the cleaning module
"""

import pytest
import tempfile
import os
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from dental_consultation_pipeline.cleaning import (
    load_corrections,
    apply_corrections_to_text,
    format_money_amounts,
    get_clean_filename,
    process_file,
    process_all_files,
    process_specific_file,
    show_corrections_summary,
    get_available_transcript_files,
    show_available_transcript_files,
    clean_transcripts
)

class TestCleaningModule:
    """Test the cleaning module functionality"""
    
    def test_load_corrections_success(self):
        """Test loading corrections from JSON file"""
        # Create a temporary corrections file
        corrections_data = {
            "dental_corrections": {
                "cavity": "cavity",
                "root canal": "root canal",
                "dental implant": "dental implant"
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(corrections_data, f)
            temp_file = f.name
        
        try:
            # Patch the CORRECTIONS_FILE path
            with patch('dental_consultation_pipeline.cleaning.CORRECTIONS_FILE', Path(temp_file)):
                result = load_corrections()
                assert result == corrections_data["dental_corrections"]
        finally:
            os.unlink(temp_file)
    
    def test_load_corrections_file_not_found(self):
        """Test loading corrections when file doesn't exist"""
        with patch('dental_consultation_pipeline.cleaning.CORRECTIONS_FILE', Path("nonexistent.json")):
            result = load_corrections()
            assert result == {}
    
    def test_load_corrections_invalid_json(self):
        """Test loading corrections with invalid JSON"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content")
            temp_file = f.name
        
        try:
            with patch('dental_consultation_pipeline.cleaning.CORRECTIONS_FILE', Path(temp_file)):
                result = load_corrections()
                assert result == {}
        finally:
            os.unlink(temp_file)
    
    def test_apply_corrections_to_text(self):
        """Test applying corrections to text"""
        corrections = {
            "cavity": "cavity",
            "root canal": "root canal",
            "dental implant": "dental implant"
        }
        
        text = "The patient has a cavity and needs a root canal treatment."
        result = apply_corrections_to_text(text, corrections)
        
        # Should return the same text since corrections are identical
        assert result == text
    
    def test_apply_corrections_with_actual_changes(self):
        """Test applying corrections that actually change the text"""
        corrections = {
            "cavity": "cavity",
            "root kanal": "root canal",  # Common misspelling
            "dental implent": "dental implant"  # Common misspelling
        }
        
        text = "The patient has a cavity and needs a root kanal. We discussed dental implent options."
        result = apply_corrections_to_text(text, corrections)
        
        expected = "The patient has a cavity and needs a root canal. We discussed dental implant options."
        assert result == expected
    
    def test_get_clean_filename(self):
        """Test generating clean filenames"""
        test_cases = [
            ("test.txt", "test_clean.txt"),
            ("patient_consult.txt", "patient_consult_clean.txt"),
            ("file_with_spaces.txt", "file_with_spaces_clean.txt"),
        ]
        
        for input_name, expected in test_cases:
            result = get_clean_filename(input_name)
            assert result == expected
    
    def test_process_file_success(self):
        """Test processing a single file successfully"""
        # Create test corrections
        corrections = {
            "cavity": "cavity",
            "root kanal": "root canal"
        }
        
        # Create test input file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("The patient has a cavity and needs a root kanal treatment.")
            input_file = Path(f.name)
        
        # Create temporary output directory
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            
            try:
                # Patch the CLEANED_DIR
                with patch('dental_consultation_pipeline.cleaning.CLEANED_DIR', output_dir):
                    result = process_file(input_file, corrections)
                    
                    # Check if processing was successful
                    assert result is not None
                    assert result.exists()
                    
                    # Check if the cleaned file was created
                    expected_filename = get_clean_filename(input_file.name)
                    expected_file = output_dir / expected_filename
                    assert expected_file.exists()
                    
                    # Check content
                    with open(expected_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                    assert "root canal" in content  # Should be corrected
                    assert "root kanal" not in content  # Should be replaced
            finally:
                os.unlink(input_file)
    
    def test_process_file_no_changes_needed(self):
        """Test processing a file that doesn't need corrections"""
        corrections = {
            "cavity": "cavity",
            "root kanal": "root canal"
        }
        
        # Create test input file with no corrections needed
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("The patient has a cavity and needs a root canal treatment.")
            input_file = Path(f.name)
        
        try:
            result = process_file(input_file, corrections)
            # Should return None when no changes are needed
            assert result is None
        finally:
            os.unlink(input_file)
    
    def test_process_file_read_error(self):
        """Test processing a file that can't be read"""
        # Create a file that will cause a read error (directory)
        with tempfile.TemporaryDirectory() as temp_dir:
            input_file = Path(temp_dir)
            
            corrections = {"test": "test"}
            result = process_file(input_file, corrections)
            assert result is None
    
    def test_get_available_transcript_files(self):
        """Test getting available transcript files"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create some test files
            test_files = ["file1.txt", "file2.txt", "file3.md"]
            for filename in test_files:
                (Path(temp_dir) / filename).touch()
            
            # Patch the TRANSCRIPTS_DIR
            with patch('dental_consultation_pipeline.cleaning.TRANSCRIPTS_DIR', Path(temp_dir)):
                result = get_available_transcript_files()
                
                # Should only return .txt files
                assert len(result) == 2
                assert "file1.txt" in result
                assert "file2.txt" in result
                assert "file3.md" not in result
    
    def test_get_available_transcript_files_empty(self):
        """Test getting transcript files when directory is empty"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch('dental_consultation_pipeline.cleaning.TRANSCRIPTS_DIR', Path(temp_dir)):
                result = get_available_transcript_files()
                assert result == []
    
    def test_get_available_transcript_files_nonexistent(self):
        """Test getting transcript files when directory doesn't exist"""
        with patch('dental_consultation_pipeline.cleaning.TRANSCRIPTS_DIR', Path("nonexistent")):
            result = get_available_transcript_files()
            assert result == []
    
    @patch('dental_consultation_pipeline.cleaning.load_corrections')
    @patch('dental_consultation_pipeline.cleaning.get_available_transcript_files')
    @patch('dental_consultation_pipeline.cleaning.process_file')
    def test_process_all_files(self, mock_process_file, mock_get_files, mock_load_corrections):
        """Test processing all files"""
        # Mock corrections
        mock_load_corrections.return_value = {"test": "test"}
        
        # Mock available files
        mock_get_files.return_value = ["file1.txt", "file2.txt"]
        
        # Mock process_file to return a path for successful processing
        mock_process_file.return_value = Path("processed_file.txt")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create actual test files in the temp directory
            test_files = ["file1.txt", "file2.txt"]
            for filename in test_files:
                (Path(temp_dir) / filename).touch()
            
            with patch('dental_consultation_pipeline.cleaning.TRANSCRIPTS_DIR', Path(temp_dir)):
                result = process_all_files()
                
                # Should return list of processed files
                assert isinstance(result, list)
                assert len(result) == 2
                
                # Should call process_file for each file
                assert mock_process_file.call_count == 2
    
    @patch('dental_consultation_pipeline.cleaning.load_corrections')
    @patch('dental_consultation_pipeline.cleaning.process_file')
    def test_process_specific_file(self, mock_process_file, mock_load_corrections):
        """Test processing a specific file"""
        # Mock corrections
        mock_load_corrections.return_value = {"test": "test"}
        
        # Mock process_file
        mock_process_file.return_value = Path("processed_file.txt")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a test file
            test_file = Path(temp_dir) / "test.txt"
            test_file.touch()
            
            with patch('dental_consultation_pipeline.cleaning.TRANSCRIPTS_DIR', Path(temp_dir)):
                result = process_specific_file("test.txt")
                
                # Should return the result from process_file
                assert result == Path("processed_file.txt")
                mock_process_file.assert_called_once()
    
    def test_process_specific_file_not_found(self):
        """Test processing a specific file that doesn't exist"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch('dental_consultation_pipeline.cleaning.TRANSCRIPTS_DIR', Path(temp_dir)):
                result = process_specific_file("nonexistent.txt")
                assert result is None
    
    @patch('dental_consultation_pipeline.cleaning.load_corrections')
    def test_show_corrections_summary(self, mock_load_corrections):
        """Test showing corrections summary"""
        # Mock corrections with different types
        mock_load_corrections.return_value = {
            "cavity": "cavity",  # dental term
            "root canal": "root canal",  # compound word
            "dr": "doctor",  # abbreviation
            "implant": "implant"  # dental term
        }
        
        # This should run without error
        result = show_corrections_summary()
        assert result is None
    
    @patch('dental_consultation_pipeline.cleaning.load_corrections')
    def test_show_corrections_summary_empty(self, mock_load_corrections):
        """Test showing corrections summary with empty corrections"""
        mock_load_corrections.return_value = {}
        
        result = show_corrections_summary()
        assert result is None
    
    @patch('dental_consultation_pipeline.cleaning.show_corrections_summary')
    @patch('dental_consultation_pipeline.cleaning.show_available_transcript_files')
    @patch('dental_consultation_pipeline.cleaning.process_all_files')
    def test_clean_transcripts(self, mock_process_all, mock_show_files, mock_show_corrections):
        """Test the main clean_transcripts function"""
        # Mock process_all_files to return some processed files
        mock_process_all.return_value = [Path("file1_clean.txt"), Path("file2_clean.txt")]
        
        result = clean_transcripts()
        
        # Should call all the expected functions
        mock_show_corrections.assert_called_once()
        mock_show_files.assert_called_once()
        mock_process_all.assert_called_once()
        
        # Should return the list of processed files
        assert result == [Path("file1_clean.txt"), Path("file2_clean.txt")]
    
    def test_format_money_amounts_basic(self):
        """Test basic money formatting"""
        text = "The implant costs 4000 dollars"
        result = format_money_amounts(text)
        expected = "The implant costs $4000"
        assert result == expected
    
    def test_format_money_amounts_with_bucks(self):
        """Test money formatting with 'bucks'"""
        text = "The crowns are 1500 bucks a piece"
        result = format_money_amounts(text)
        expected = "The crowns are $1500 a piece"
        assert result == expected
    
    def test_format_money_amounts_with_thousand(self):
        """Test money formatting with 'thousand'"""
        text = "It's around 15 thousand"
        result = format_money_amounts(text)
        expected = "It's around $15,000"
        assert result == expected
    
    def test_format_money_amounts_with_hundred(self):
        """Test money formatting with 'hundred'"""
        text = "The fillings are 500 hundred"
        result = format_money_amounts(text)
        expected = "The fillings are $500,00"
        assert result == expected
    
    def test_format_money_amounts_standalone_numbers(self):
        """Test formatting standalone numbers that are likely money amounts"""
        text = "The implant is 8000 and the crowns are 1500"
        result = format_money_amounts(text)
        expected = "The implant is $8000 and the crowns are $1500"
        assert result == expected
    
    def test_format_money_amounts_with_right_there(self):
        """Test money formatting with 'right there' context"""
        text = "That's 8000 right there a couple crowns"
        result = format_money_amounts(text)
        expected = "That's $8000 right there a couple crowns"
        assert result == expected
    
    def test_format_money_amounts_with_ranges(self):
        """Test money formatting with ranges"""
        text = "It's around 15 to 24 thousand"
        result = format_money_amounts(text)
        expected = "It's around $15,000 to $24,000"
        assert result == expected
    
    def test_format_money_amounts_with_give_or_take(self):
        """Test money formatting with 'give or take'"""
        text = "You're probably 12 13,000 give or take"
        result = format_money_amounts(text)
        expected = "You're probably $12,$13,000 give or take"
        assert result == expected
    
    def test_format_money_amounts_complex_dental_pricing(self):
        """Test complex dental pricing scenarios"""
        text = """
        The implant is probably about 4000
        The crowns are about 1500 bucks a piece
        That's 8000 right there a couple crowns
        You're probably 12 13,000 give or take
        It's around 15 to 24 thousand
        The fillings are three to 500 300 400 something like that
        """
        result = format_money_amounts(text)
        
        # Check that dollar signs were added
        assert "$4000" in result
        assert "$1500" in result
        assert "$8000" in result
        assert "$15,000" in result
        assert "$24,000" in result
    
    def test_format_money_amounts_no_changes(self):
        """Test that text without money amounts is unchanged"""
        text = "The patient has a cavity and needs a root canal treatment."
        result = format_money_amounts(text)
        assert result == text
    
    def test_format_money_amounts_already_formatted(self):
        """Test that already formatted money amounts are not double-formatted"""
        text = "The cost is $5000 and the implant is $3000"
        result = format_money_amounts(text)
        assert result == text
    
    def test_format_money_amounts_mixed_content(self):
        """Test money formatting with mixed dental and financial content"""
        text = """
        Patient has severe gum disease. The implant costs 4000 dollars.
        We discussed options ranging from 1500 to 8000.
        The crowns are 1500 bucks each.
        """
        result = format_money_amounts(text)
        
        # Check key transformations
        assert "$4000" in result
        assert "$1500" in result
        assert "$8000" in result
        assert "gum disease" in result  # Non-money content preserved
        assert "Patient has severe" in result  # Non-money content preserved 