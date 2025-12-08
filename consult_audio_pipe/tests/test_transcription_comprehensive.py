"""
Comprehensive tests for the transcription module
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from dental_consultation_pipeline.transcription import (
    transcribe_audio_file,
    get_available_audio_files,
    transcribe_all_audio_files,
    process_specific_audio_file,
    show_available_audio_files,
    transcribe_audio,
    RAW_AUDIO_DIR,
    OUTPUT_DIR
)

class TestTranscriptionModule:
    """Test the transcription module functionality"""
    
    def test_get_available_audio_files_with_files(self):
        """Test getting audio files when they exist"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create some test audio files
            test_files = ["file1.m4a", "file2.wav", "file3.mp3", "file4.txt"]
            for filename in test_files:
                (Path(temp_dir) / filename).touch()
            
            # Patch the RAW_AUDIO_DIR
            with patch('dental_consultation_pipeline.transcription.RAW_AUDIO_DIR', Path(temp_dir)):
                result = get_available_audio_files()
                
                # Should only return audio files
                assert len(result) == 3
                assert "file1.m4a" in result
                assert "file2.wav" in result
                assert "file3.mp3" in result
                assert "file4.txt" not in result
    
    def test_get_available_audio_files_empty(self):
        """Test getting audio files when directory is empty"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch('dental_consultation_pipeline.transcription.RAW_AUDIO_DIR', Path(temp_dir)):
                result = get_available_audio_files()
                assert result == []
    
    def test_get_available_audio_files_nonexistent(self):
        """Test getting audio files when directory doesn't exist"""
        with patch('dental_consultation_pipeline.transcription.RAW_AUDIO_DIR', Path("nonexistent")):
            result = get_available_audio_files()
            assert result == []
    
    @patch('dental_consultation_pipeline.transcription.whisper.load_model')
    def test_transcribe_audio_file_success(self, mock_load_model):
        """Test successful audio transcription"""
        # Mock whisper model
        mock_model = MagicMock()
        mock_model.transcribe.return_value = {
            "text": "This is a test transcription.",
            "segments": [
                {"start": 0.0, "end": 2.0, "text": "This is a test transcription."}
            ]
        }
        mock_load_model.return_value = mock_model
        
        # Create a temporary audio file
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as f:
            # Write some dummy audio data (just bytes)
            f.write(b"dummy audio data")
            audio_file = f.name
        
        try:
            # Test transcription
            result = transcribe_audio_file(audio_file)
            
            # Should return the transcription result
            assert result is not None
            assert "text" in result
            assert "segments" in result
            
            # Verify whisper model was called
            mock_model.transcribe.assert_called_once()
            
        finally:
            os.unlink(audio_file)
    
    def test_transcribe_audio_file_nonexistent(self):
        """Test transcription with non-existent audio file"""
        # This should raise an error or return None
        with pytest.raises(Exception):
            transcribe_audio_file("nonexistent.wav")
    
    @patch('dental_consultation_pipeline.transcription.whisper.load_model')
    def test_transcribe_audio_file_whisper_error(self, mock_load_model):
        """Test transcription with whisper error"""
        # Mock whisper to raise an error
        mock_load_model.side_effect = Exception("Whisper model error")
        
        # Create a temporary audio file
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as f:
            f.write(b"dummy audio data")
            audio_file = f.name
        
        try:
            # Test transcription
            with pytest.raises(Exception):
                transcribe_audio_file(audio_file)
        finally:
            os.unlink(audio_file)
    
    @patch('dental_consultation_pipeline.transcription.get_available_audio_files')
    @patch('dental_consultation_pipeline.transcription.process_specific_audio_file')
    def test_transcribe_all_audio_files(self, mock_process, mock_get_files):
        """Test transcribing all audio files"""
        # Mock audio files
        mock_get_files.return_value = ["file1.wav", "file2.m4a"]
        
        # Mock transcription results
        mock_process.side_effect = [True, False]  # First succeeds, second fails
        
        result = transcribe_all_audio_files()
        
        # Should return list of successfully transcribed files
        assert isinstance(result, list)
        assert len(result) == 1  # Only one successful transcription
        
        # Should call process_specific_audio_file for each file
        assert mock_process.call_count == 2
    
    @patch('dental_consultation_pipeline.transcription.transcribe_audio_file')
    @patch('dental_consultation_pipeline.transcription.save_outputs')
    def test_process_specific_audio_file(self, mock_save, mock_transcribe):
        """Test processing a specific audio file"""
        # Mock transcription
        mock_transcribe.return_value = {
            "text": "Test transcription",
            "segments": [{"start": 0, "end": 2, "text": "Test transcription"}]
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a test audio file
            test_file = Path(temp_dir) / "test.wav"
            test_file.touch()
            
            with patch('dental_consultation_pipeline.transcription.RAW_AUDIO_DIR', Path(temp_dir)):
                with patch('dental_consultation_pipeline.transcription.OUTPUT_DIR', Path(temp_dir)):
                    result = process_specific_audio_file("test.wav")
                    
                    # Should return True for successful processing
                    assert result is True
                    mock_transcribe.assert_called_once()
                    mock_save.assert_called_once()
    
    def test_process_specific_audio_file_not_found(self):
        """Test processing a specific audio file that doesn't exist"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch('dental_consultation_pipeline.transcription.RAW_AUDIO_DIR', Path(temp_dir)):
                result = process_specific_audio_file("nonexistent.wav")
                assert result is False
    
    @patch('dental_consultation_pipeline.transcription.get_available_audio_files')
    def test_show_available_audio_files(self, mock_get_files):
        """Test showing available audio files"""
        # Mock audio files
        mock_get_files.return_value = ["file1.wav", "file2.m4a"]
        
        # This should run without error
        result = show_available_audio_files()
        assert result is None
    
    def test_transcribe_audio_function(self):
        """Test the transcribe_audio function"""
        with patch('dental_consultation_pipeline.transcription.transcribe_audio_file') as mock_transcribe:
            mock_transcribe.return_value = {
                "text": "Test transcription",
                "segments": [{"start": 0, "end": 2, "text": "Test transcription"}]
            }
            
            result = transcribe_audio("test.wav")
            assert result == "Test transcription"
    
    def test_audio_file_extensions(self):
        """Test that only audio file extensions are recognized"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create files with various extensions
            test_files = [
                "audio1.wav", "audio2.mp3", "audio3.m4a", "audio4.flac",
                "audio5.ogg", "audio6.txt", "audio7.pdf", "audio8.doc"
            ]
            
            for filename in test_files:
                (Path(temp_dir) / filename).touch()
            
            with patch('dental_consultation_pipeline.transcription.RAW_AUDIO_DIR', Path(temp_dir)):
                result = get_available_audio_files()
                
                # Should only return audio files
                assert len(result) == 5  # wav, mp3, m4a, flac, ogg
                
                # Check specific extensions
                assert "audio1.wav" in result
                assert "audio2.mp3" in result
                assert "audio3.m4a" in result
                assert "audio4.flac" in result
                assert "audio5.ogg" in result
                
                # Should not include non-audio files
                assert "audio6.txt" not in result
                assert "audio7.pdf" not in result
                assert "audio8.doc" not in result
    
    @patch('dental_consultation_pipeline.transcription.whisper.load_model')
    def test_transcription_output_formats(self, mock_load_model):
        """Test that transcription creates multiple output formats"""
        # Mock whisper model with detailed output
        mock_model = MagicMock()
        mock_model.transcribe.return_value = {
            "text": "This is a test transcription with multiple segments.",
            "segments": [
                {"start": 0.0, "end": 2.0, "text": "This is a test"},
                {"start": 2.0, "end": 4.0, "text": "transcription with multiple"},
                {"start": 4.0, "end": 6.0, "text": "segments."}
            ],
            "language": "en"
        }
        mock_load_model.return_value = mock_model
        
        # Create a temporary audio file
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as f:
            f.write(b"dummy audio data")
            audio_file = f.name
        
        try:
            # Test transcription
            result = transcribe_audio_file(audio_file)
            
            # Should return the transcription result
            assert result is not None
            assert "text" in result
            assert "segments" in result
            assert "language" in result
            assert result["text"] == "This is a test transcription with multiple segments."
            assert len(result["segments"]) == 3
            
        finally:
            os.unlink(audio_file)
    
    def test_whisper_model_loading(self):
        """Test that whisper model loading is handled correctly"""
        # This test verifies that the model loading path is correct
        # We'll mock the whisper.load_model function to see if it's called correctly
        
        with patch('dental_consultation_pipeline.transcription.whisper.load_model') as mock_load_model:
            mock_model = MagicMock()
            mock_load_model.return_value = mock_model
            
            # Create a temporary audio file
            with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as f:
                f.write(b"dummy audio data")
                audio_file = f.name
            
            try:
                # Test transcription
                transcribe_audio_file(audio_file)
                
                # Should call whisper.load_model
                mock_load_model.assert_called_once()
                
            finally:
                os.unlink(audio_file) 