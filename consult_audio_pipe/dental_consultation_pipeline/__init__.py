"""
Dental Consultation Audio Analysis Pipeline

A complete automated pipeline for processing dental consultation audio recordings,
from transcription to AI-powered analysis with multiple output formats.
"""

__version__ = "1.0.0"
__author__ = "Dental Analysis Team"
__email__ = "your-email@example.com"

# Import main functions for easy access
from .transcription import transcribe_audio, transcribe_all_audio_files
from .cleaning import clean_transcripts, process_all_files
from .analysis import analyze_clean_transcripts, process_transcript_with_llm
from .conversion import convert_to_html, convert_to_pdf, convert_all_formats

__all__ = [
    "__version__",
    "__author__",
    "__email__",
    "transcribe_audio",
    "transcribe_all_audio_files",
    "clean_transcripts", 
    "process_all_files",
    "analyze_clean_transcripts",
    "process_transcript_with_llm",
    "convert_to_html",
    "convert_to_pdf",
    "convert_all_formats",
] 