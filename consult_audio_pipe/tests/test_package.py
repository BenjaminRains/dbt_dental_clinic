"""
Test package import and basic functionality
"""

import pytest
from pathlib import Path

def test_package_import():
    """Test that the package can be imported"""
    import dental_consultation_pipeline
    assert dental_consultation_pipeline.__version__ == "1.0.0"

def test_analysis_functions():
    """Test that analysis functions can be imported"""
    from dental_consultation_pipeline import analyze_clean_transcripts, process_transcript_with_llm
    assert callable(analyze_clean_transcripts)
    assert callable(process_transcript_with_llm)

def test_conversion_functions():
    """Test that conversion functions can be imported"""
    from dental_consultation_pipeline import convert_to_html, convert_to_pdf, convert_all_formats
    assert callable(convert_to_html)
    assert callable(convert_to_pdf)
    assert callable(convert_all_formats)

def test_transcription_functions():
    """Test that transcription functions can be imported"""
    from dental_consultation_pipeline import transcribe_audio, transcribe_all_audio_files
    assert callable(transcribe_audio)
    assert callable(transcribe_all_audio_files)

def test_cleaning_functions():
    """Test that cleaning functions can be imported"""
    from dental_consultation_pipeline import clean_transcripts, process_all_files
    assert callable(clean_transcripts)
    assert callable(process_all_files) 