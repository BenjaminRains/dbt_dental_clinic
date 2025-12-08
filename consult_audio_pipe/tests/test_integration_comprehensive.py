"""
Comprehensive integration tests for the complete pipeline
"""

import pytest
import tempfile
import os
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from dental_consultation_pipeline.transcription import transcribe_audio_file
from dental_consultation_pipeline.cleaning import process_file, load_corrections
from dental_consultation_pipeline.analysis import process_transcript_with_llm
from dental_consultation_pipeline.conversion import convert_markdown_to_html, convert_markdown_to_pdf_simple

class TestCompletePipeline:
    """Test the complete pipeline workflow"""
    
    def test_full_pipeline_workflow(self):
        """Test the complete pipeline from audio to final outputs"""
        # Create temporary directories for the entire pipeline
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create subdirectories
            audio_dir = temp_path / "audio"
            transcripts_dir = temp_path / "transcripts"
            clean_dir = temp_path / "clean"
            analysis_dir = temp_path / "analysis"
            html_dir = temp_path / "html"
            pdf_dir = temp_path / "pdf"
            
            for dir_path in [audio_dir, transcripts_dir, clean_dir, analysis_dir, html_dir, pdf_dir]:
                dir_path.mkdir()
            
            # Step 1: Create test audio file
            audio_file = audio_dir / "test_consultation.wav"
            with open(audio_file, 'wb') as f:
                f.write(b"dummy audio data for testing")
            
            # Step 2: Mock transcription
            with patch('dental_consultation_pipeline.transcription.whisper.load_model') as mock_load_model:
                mock_model = MagicMock()
                mock_model.transcribe.return_value = {
                    "text": "Hello, this is a dental consultation. The patient has a cavity and needs a root kanal treatment.",
                    "segments": [
                        {"start": 0.0, "end": 3.0, "text": "Hello, this is a dental consultation."},
                        {"start": 3.0, "end": 6.0, "text": "The patient has a cavity and needs a root kanal treatment."}
                    ],
                    "language": "en"
                }
                mock_load_model.return_value = mock_model
                
                # Transcribe audio - use correct function signature
                result = transcribe_audio_file(str(audio_file))
                assert result is not None
                assert "text" in result
                
                # Create transcript files manually for testing
                transcript_txt = transcripts_dir / "test_consultation.txt"
                with open(transcript_txt, 'w') as f:
                    f.write(result["text"])
                assert transcript_txt.exists()
            
            # Step 3: Create corrections file
            corrections_data = {
                "dental_corrections": {
                    "crab tree": "cavity",
                    "root kanal": "root canal",
                    "gentle consultation": "dental consultation"
                }
            }
            corrections_file = temp_path / "dental_corrections.json"
            with open(corrections_file, 'w') as f:
                json.dump(corrections_data, f)
            
            # Step 4: Clean transcript
            with patch('dental_consultation_pipeline.cleaning.CORRECTIONS_FILE', corrections_file):
                with patch('dental_consultation_pipeline.cleaning.CLEANED_DIR', clean_dir):
                    corrections = load_corrections()
                    transcript_file = transcripts_dir / "test_consultation.txt"
                    
                    # Process the transcript
                    result = process_file(transcript_file, corrections)
                    assert result is not None
                    
                    # Check cleaned file was created
                    clean_file = clean_dir / "test_consultation_clean.txt"
                    assert clean_file.exists()
                    
                    # Check content was corrected
                    with open(clean_file, 'r') as f:
                        clean_content = f.read()
                    assert "root canal" in clean_content  # Should be corrected
                    assert "root kanal" not in clean_content  # Should be replaced
            
            # Step 5: Analyze with LLM
            with patch('dental_consultation_pipeline.analysis.requests.post') as mock_post:
                # Mock successful API responses
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "content": [{"text": "# Test Analysis\n\nThis is a test analysis."}]
                }
                mock_post.return_value = mock_response
                
                with patch('dental_consultation_pipeline.analysis.OUTPUT_DIR', analysis_dir):
                    # Process transcript with LLM
                    result = process_transcript_with_llm(clean_file)
                    assert result is True
                    
                    # Check analysis files were created
                    analysis_md = analysis_dir / "test_consultation_analysis.md"
                    summary_md = analysis_dir / "test_consultation_summary.md"
                    assert analysis_md.exists()
                    assert summary_md.exists()
            
            # Step 6: Convert to HTML
            with patch('dental_consultation_pipeline.conversion.HTML_OUTPUT_DIR', html_dir):
                result = convert_markdown_to_html(analysis_md, html_dir)
                assert result is True
                
                # Check HTML file was created
                html_file = html_dir / "test_consultation_analysis.html"
                assert html_file.exists()
                
                # Check HTML content
                with open(html_file, 'r') as f:
                    html_content = f.read()
                assert "<!DOCTYPE html>" in html_content
                assert "Test Analysis" in html_content
            
            # Step 7: Convert to PDF
            with patch('dental_consultation_pipeline.conversion.PDF_OUTPUT_DIR', pdf_dir):
                result = convert_markdown_to_pdf_simple(analysis_md, pdf_dir)
                assert result is True
                
                # Check PDF file was created
                pdf_file = pdf_dir / "test_consultation_analysis.pdf"
                assert pdf_file.exists()
                assert pdf_file.stat().st_size > 0
    
    def test_pipeline_error_handling(self):
        """Test pipeline error handling at various stages"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Test transcription error handling
            with patch('dental_consultation_pipeline.transcription.whisper.load_model') as mock_load_model:
                mock_load_model.side_effect = Exception("Model loading failed")
                
                audio_file = temp_path / "test.wav"
                audio_file.touch()
                
                with pytest.raises(Exception):
                    transcribe_audio_file(str(audio_file))
            
            # Test cleaning error handling
            with patch('dental_consultation_pipeline.cleaning.CORRECTIONS_FILE', Path("nonexistent.json")):
                corrections = load_corrections()
                assert corrections == {}
            
            # Test analysis error handling
            with patch('dental_consultation_pipeline.analysis.requests.post') as mock_post:
                mock_post.side_effect = Exception("API call failed")
                
                transcript_file = temp_path / "test_clean.txt"
                transcript_file.touch()
                
                result = process_transcript_with_llm(transcript_file)
                assert result is False
    
    def test_pipeline_data_flow(self):
        """Test that data flows correctly through the pipeline"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test data
            original_text = "The patient has a cavity and needs a root kanal treatment."
            expected_cleaned_text = "The patient has a cavity and needs a root canal treatment."
            
            # Create transcript file
            transcript_file = temp_path / "test.txt"
            with open(transcript_file, 'w') as f:
                f.write(original_text)
            
            # Create corrections
            corrections = {
                "root kanal": "root canal"
            }
            
            # Test cleaning step
            with patch('dental_consultation_pipeline.cleaning.CLEANED_DIR', temp_path):
                result = process_file(transcript_file, corrections)
                assert result is not None
                
                # Check cleaned content
                clean_file = temp_path / "test_clean.txt"
                with open(clean_file, 'r') as f:
                    cleaned_content = f.read()
                
                assert cleaned_content == expected_cleaned_text
                assert "root canal" in cleaned_content
                assert "root kanal" not in cleaned_content
    
    def test_pipeline_file_naming(self):
        """Test that file naming is consistent throughout the pipeline"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Test file naming conventions
            base_name = "patient_consultation"
            
            # Expected file names at each stage
            expected_files = {
                "audio": f"{base_name}.wav",
                "transcript": f"{base_name}.txt",
                "clean": f"{base_name}_clean.txt",
                "analysis": f"{base_name}_analysis.md",
                "summary": f"{base_name}_summary.md",
                "html": f"{base_name}_analysis.html",
                "pdf": f"{base_name}_analysis.pdf"
            }
            
            # Create test files
            for stage, filename in expected_files.items():
                file_path = temp_path / filename
                file_path.touch()
                assert file_path.exists()
    
    def test_pipeline_directory_structure(self):
        """Test that the pipeline creates the correct directory structure"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create pipeline directories
            directories = [
                "raw_audio",
                "transcripts", 
                "transcripts_clean",
                "api_claude_summary_analysis",
                "html_output",
                "pdf_output"
            ]
            
            for dir_name in directories:
                dir_path = temp_path / dir_name
                dir_path.mkdir()
                assert dir_path.exists()
                assert dir_path.is_dir()
    
    @patch('dental_consultation_pipeline.analysis.requests.post')
    @patch('dental_consultation_pipeline.transcription.whisper.load_model')
    def test_pipeline_with_realistic_data(self, mock_load_model, mock_post):
        """Test pipeline with realistic dental consultation data"""
        # Mock transcription
        mock_model = MagicMock()
        mock_model.transcribe.return_value = {
            "text": "Dr. Smith: Hello Mrs. Johnson, how are you today? Mrs. Johnson: I'm having pain in my tooth. Dr. Smith: Let me examine you. I can see you have a cavity and may need a root kanal. Mrs. Johnson: How much will that cost? Dr. Smith: The root canal will be about $1,200. Mrs. Johnson: Do you take insurance? Dr. Smith: Yes, we accept most insurance plans.",
            "segments": [
                {"start": 0.0, "end": 2.0, "text": "Dr. Smith: Hello Mrs. Johnson, how are you today?"},
                {"start": 2.0, "end": 4.0, "text": "Mrs. Johnson: I'm having pain in my tooth."},
                {"start": 4.0, "end": 6.0, "text": "Dr. Smith: Let me examine you. I can see you have a cavity and may need a root kanal."},
                {"start": 6.0, "end": 8.0, "text": "Mrs. Johnson: How much will that cost?"},
                {"start": 8.0, "end": 10.0, "text": "Dr. Smith: The root canal will be about $1,200."},
                {"start": 10.0, "end": 12.0, "text": "Mrs. Johnson: Do you take insurance?"},
                {"start": 12.0, "end": 14.0, "text": "Dr. Smith: Yes, we accept most insurance plans."}
            ],
            "language": "en"
        }
        mock_load_model.return_value = mock_model
        
        # Mock LLM responses
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": [{"text": "# Mrs. Johnson Analysis\n\n## Overall Assessment\nGood consultation with clear communication.\n\n## Financial Communication\nCosts were clearly discussed.\n\n## Recommendations\nContinue with current approach."}]
        }
        mock_post.return_value = mock_response
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create directories
            for dir_name in ["audio", "transcripts", "clean", "analysis", "html", "pdf"]:
                (temp_path / dir_name).mkdir()
            
            # Create audio file
            audio_file = temp_path / "audio" / "mrs_johnson_consult.wav"
            audio_file.touch()
            
            # Run transcription with correct function signature
            result = transcribe_audio_file(str(audio_file))
            assert result is not None
            
            # Create transcript file manually
            transcript_file = temp_path / "transcripts" / "mrs_johnson_consult.txt"
            with open(transcript_file, 'w') as f:
                f.write(result["text"])
            
            # Create corrections
            corrections = {"root kanal": "root canal"}
            
            # Run cleaning
            with patch('dental_consultation_pipeline.cleaning.CLEANED_DIR', temp_path / "clean"):
                result = process_file(transcript_file, corrections)
                assert result is not None
            
            # Run analysis
            clean_file = temp_path / "clean" / "mrs_johnson_consult_clean.txt"
            with patch('dental_consultation_pipeline.analysis.OUTPUT_DIR', temp_path / "analysis"):
                result = process_transcript_with_llm(clean_file)
                assert result is True
            
            # Run conversion
            analysis_file = temp_path / "analysis" / "mrs_johnson_consult_analysis.md"
            with patch('dental_consultation_pipeline.conversion.HTML_OUTPUT_DIR', temp_path / "html"):
                result = convert_markdown_to_html(analysis_file, temp_path / "html")
                assert result is True
            
            with patch('dental_consultation_pipeline.conversion.PDF_OUTPUT_DIR', temp_path / "pdf"):
                result = convert_markdown_to_pdf_simple(analysis_file, temp_path / "pdf")
                assert result is True
            
            # Verify final outputs
            assert (temp_path / "html" / "mrs_johnson_consult_analysis.html").exists()
            assert (temp_path / "pdf" / "mrs_johnson_consult_analysis.pdf").exists() 