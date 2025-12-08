#!/usr/bin/env python3
"""
DEPRECATED: Pipeline Orchestrator
Coordinates the complete workflow from audio files to analysis

⚠️  DEPRECATION NOTICE ⚠️
This file is deprecated and no longer maintained.
Please use the active pipeline at: dental_consultation_pipeline/pipeline.py

This file will be removed in a future version.
"""

import sys
import subprocess
import logging
from pathlib import Path
from datetime import datetime

# Import the LLM analysis module
import sys
sys.path.append(str(Path(__file__).parent.parent))
from llm_analysis_integration import analyze_clean_transcripts

# === Configuration ===
# Get the project root directory (parent of scripts directory)
PROJECT_ROOT = Path(__file__).parent.parent
RAW_AUDIO_DIR = PROJECT_ROOT / "raw_audio"
TRANSCRIPTS_DIR = PROJECT_ROOT / "transcripts"
TRANSCRIPTS_CLEAN_DIR = PROJECT_ROOT / "transcripts_clean"
LOGS_DIR = PROJECT_ROOT / "logs"

def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if it doesn't exist
    LOGS_DIR.mkdir(exist_ok=True)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"pipeline_run_{timestamp}.log"
    log_path = LOGS_DIR / log_filename
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)  # Also log to console
        ]
    )
    
    logging.info(f"Pipeline logging initialized. Log file: {log_path}")
    return log_path

def log_status(message: str, level: str = "INFO"):
    """Log status message with specified level"""
    if level.upper() == "INFO":
        logging.info(message)
    elif level.upper() == "WARNING":
        logging.warning(message)
    elif level.upper() == "ERROR":
        logging.error(message)
    elif level.upper() == "DEBUG":
        logging.debug(message)
    else:
        logging.info(message)

def get_audio_files():
    """Get list of audio files in raw_audio directory"""
    if not RAW_AUDIO_DIR.exists():
        # Use print for status command, log_status for pipeline runs
        if logging.getLogger().handlers:
            log_status(f"Raw audio directory not found: {RAW_AUDIO_DIR}", "ERROR")
        else:
            print(f"❌ Raw audio directory not found: {RAW_AUDIO_DIR}")
        return []
    
    audio_extensions = {".m4a", ".wav", ".mp3", ".ogg", ".flac", ".aac"}
    audio_files = []
    
    for file_path in RAW_AUDIO_DIR.iterdir():
        if file_path.is_file() and file_path.suffix.lower() in audio_extensions:
            audio_files.append(file_path)
    
    # Only log if logging is set up (pipeline run)
    if logging.getLogger().handlers:
        log_status(f"Found {len(audio_files)} audio files in {RAW_AUDIO_DIR}")
    return audio_files

def get_transcript_files():
    """Get list of transcript files in transcripts directory"""
    if not TRANSCRIPTS_DIR.exists():
        return []
    
    return list(TRANSCRIPTS_DIR.glob("*.txt"))

def find_new_audio_files():
    """Find audio files that don't have corresponding transcript files"""
    audio_files = get_audio_files()
    new_audio_files = []
    
    for audio_file in audio_files:
        base_name = audio_file.stem
        transcript_path = TRANSCRIPTS_DIR / f"{base_name}.txt"
        
        if not transcript_path.exists():
            new_audio_files.append(audio_file)
    
    return new_audio_files

def validate_transcript_file(file_path):
    """Validate if a transcript file is worth processing"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        # Check if file is empty or too short
        if len(content) < 10:
            return False, "File too short (less than 10 characters)"
        
        # Check if file contains mostly repeated characters (like "a a a a a a")
        words = content.split()
        if len(words) > 0:
            # Check if more than 80% of words are the same
            word_counts = {}
            for word in words:
                word_counts[word] = word_counts.get(word, 0) + 1
            
            most_common_word = max(word_counts.items(), key=lambda x: x[1])
            if most_common_word[1] > len(words) * 0.8:
                return False, f"File contains mostly repeated word: '{most_common_word[0]}'"
        
        # Check if file contains mostly punctuation or special characters
        non_alpha_chars = sum(1 for c in content if not c.isalnum() and not c.isspace())
        if non_alpha_chars > len(content) * 0.5:
            return False, "File contains mostly non-alphanumeric characters"
        
        return True, "File appears valid"
        
    except Exception as e:
        return False, f"Error reading file: {e}"

def find_new_transcripts_to_clean():
    """Find transcript files that don't have corresponding clean versions"""
    transcript_files = get_transcript_files()
    new_transcript_files = []
    
    for transcript_file in transcript_files:
        base_name = transcript_file.stem
        clean_path = TRANSCRIPTS_CLEAN_DIR / f"{base_name}_clean.txt"
        
        if not clean_path.exists():
            # Validate the transcript file before adding it
            is_valid, reason = validate_transcript_file(transcript_file)
            if is_valid:
                new_transcript_files.append(transcript_file)
            else:
                log_status(f"⚠️ Skipping invalid transcript file: {transcript_file.name} - {reason}", "WARNING")
    
    return new_transcript_files

def find_new_markdown_files_to_convert():
    """Find markdown files that don't have corresponding HTML/PDF versions"""
    llm_output_dir = PROJECT_ROOT / "api_claude_summary_analysis"
    html_output_dir = PROJECT_ROOT / "html_output"
    pdf_output_dir = PROJECT_ROOT / "pdf_output"
    
    if not llm_output_dir.exists():
        return []
    
    markdown_files = list(llm_output_dir.glob("*.md"))
    new_markdown_files = []
    
    for md_file in markdown_files:
        base_name = md_file.stem
        html_path = html_output_dir / f"{base_name}.html"
        pdf_path = pdf_output_dir / f"{base_name}.pdf"
        
        # If either HTML or PDF doesn't exist, consider it new
        if not (html_path.exists() and pdf_path.exists()):
            new_markdown_files.append(md_file)
    
    return new_markdown_files

def show_pipeline_status():
    """Show current status of the pipeline"""
    print("📊 Pipeline Status Report")
    print("=" * 50)
    
    # Audio files
    audio_files = get_audio_files()
    print(f"🎵 Raw audio files: {len(audio_files)}")
    for audio_file in audio_files:
        print(f"  • {audio_file.name}")
    
    # Transcript files
    transcript_files = get_transcript_files()
    print(f"\n📝 Raw transcript files: {len(transcript_files)}")
    for transcript_file in transcript_files:
        print(f"  • {transcript_file.name}")
    
    # Clean transcript files
    clean_transcript_files = []
    if TRANSCRIPTS_CLEAN_DIR.exists():
        clean_transcript_files = list(TRANSCRIPTS_CLEAN_DIR.glob("*_clean.txt"))
    print(f"\n🧹 Clean transcript files: {len(clean_transcript_files)}")
    for clean_transcript_file in clean_transcript_files:
        print(f"  • {clean_transcript_file.name}")
    
    # LLM Analysis and Summary files
    llm_output_dir = PROJECT_ROOT / "api_claude_summary_analysis"
    llm_files = []
    if llm_output_dir.exists():
        llm_files = list(llm_output_dir.glob("*"))
    print(f"\n🤖 LLM Analysis & Summary files: {len(llm_files)}")
    for llm_file in llm_files:
        print(f"  • {llm_file.name}")
    
    # HTML output files
    html_output_dir = PROJECT_ROOT / "html_output"
    html_files = []
    if html_output_dir.exists():
        html_files = list(html_output_dir.glob("*.html"))
    print(f"\n🌐 HTML output files: {len(html_files)}")
    for html_file in html_files:
        print(f"  • {html_file.name}")
    
    # PDF output files
    pdf_output_dir = PROJECT_ROOT / "pdf_output"
    pdf_files = []
    if pdf_output_dir.exists():
        pdf_files = list(pdf_output_dir.glob("*.pdf"))
    print(f"\n📄 PDF output files: {len(pdf_files)}")
    for pdf_file in pdf_files:
        print(f"  • {pdf_file.name}")
    
    # New files to process
    new_audio_files = find_new_audio_files()
    new_transcript_files = find_new_transcripts_to_clean()
    new_markdown_files = find_new_markdown_files_to_convert()
    
    print(f"\n🆕 Files ready for processing:")
    print(f"  • New audio files to transcribe: {len(new_audio_files)}")
    print(f"  • New transcripts to clean: {len(new_transcript_files)}")
    print(f"  • New markdown files to convert: {len(new_markdown_files)}")

def run_full_pipeline():
    """Run the complete pipeline from audio to analysis"""
    log_status("📊 Complete Pipeline Orchestrator")
    log_status("=" * 50)
    log_status("Flow: Audio → Transcription → Cleaning → LLM Analysis → HTML/PDF Conversion")
    log_status("Using Anthropic Claude API for analysis and summaries")
    
    # Step 1: Check for new audio files and transcribe them
    log_status("🎵 Step 1: Checking for new audio files...")
    new_audio_files = find_new_audio_files()
    
    if new_audio_files:
        log_status(f"🆕 Found {len(new_audio_files)} new audio file(s) to transcribe:")
        for audio_file in new_audio_files:
            log_status(f"  • {audio_file.name}")
        
        log_status("🎵 Transcribing audio files using audio_transcriber.py...")
        for audio_file in new_audio_files:
            try:
                result = subprocess.run([
                    sys.executable, str(PROJECT_ROOT / "scripts" / "audio_transcriber.py"), "--process", audio_file.name
                ], capture_output=True, text=True, check=True)
                log_status(f"✅ Transcribed: {audio_file.name}")
            except subprocess.CalledProcessError as e:
                log_status(f"❌ Failed to transcribe {audio_file.name}: {e}", "ERROR")
                log_status(f"Error details: {e.stderr}", "ERROR")
    else:
        log_status("✅ No new audio files found")
    
    # Step 2: Check for new transcripts to clean
    log_status("🧹 Step 2: Checking for new transcripts to clean...")
    new_transcript_files = find_new_transcripts_to_clean()
    
    cleaning_successful = True  # Track if cleaning was successful
    
    if new_transcript_files:
        log_status(f"🆕 Found {len(new_transcript_files)} new transcript file(s) to clean:")
        for transcript_file in new_transcript_files:
            log_status(f"  • {transcript_file.name}")
        
        log_status("🧹 Cleaning transcript files using transcript_cleaner.py...")
        try:
            # Add timeout to prevent hanging on problematic files
            result = subprocess.run([
                sys.executable, str(PROJECT_ROOT / "scripts" / "transcript_cleaner.py")
            ], capture_output=True, text=True, check=True, timeout=300)  # 5 minute timeout
            log_status("✅ Applied corrections to all transcript files")
        except subprocess.TimeoutExpired:
            log_status("❌ Transcript cleaning timed out (5 minutes). Some files may be corrupted or too large.", "ERROR")
            log_status("Consider removing problematic transcript files and re-transcribing the audio.", "WARNING")
            cleaning_successful = False
        except subprocess.CalledProcessError as e:
            log_status(f"❌ Failed to clean transcripts: {e}", "ERROR")
            log_status(f"Error details: {e.stderr}", "ERROR")
            cleaning_successful = False
    else:
        log_status("✅ No new transcripts to clean")
    
    # Abort pipeline if transcript cleaning failed
    if not cleaning_successful:
        log_status("🛑 Pipeline aborted due to transcript cleaning failure.", "ERROR")
        log_status("Please fix the transcript issues and run the pipeline again.", "WARNING")
        return
    
    # Step 3: Analyze clean transcripts using LLM
    log_status("🤖 Step 3: Analyzing clean transcripts...")
    try:
        analyze_clean_transcripts()
        log_status("✅ LLM analysis completed")
    except Exception as e:
        log_status(f"❌ Failed to run LLM analysis: {e}", "ERROR")
    
    # Step 4: Convert markdown files to HTML and PDF
    log_status("🌐 Step 4: Converting markdown files to HTML and PDF...")
    new_markdown_files = find_new_markdown_files_to_convert()
    
    if new_markdown_files:
        log_status(f"🆕 Found {len(new_markdown_files)} new markdown file(s) to convert:")
        for md_file in new_markdown_files:
            log_status(f"  • {md_file.name}")
        
        log_status("🌐 Converting markdown files to HTML...")
        try:
            result = subprocess.run([
                sys.executable, str(PROJECT_ROOT / "scripts" / "convert_markdown_to_html.py")
            ], capture_output=True, text=True, check=True)
            log_status("✅ Converted markdown files to HTML")
        except subprocess.CalledProcessError as e:
            log_status(f"❌ Failed to convert to HTML: {e}", "ERROR")
            log_status(f"Error details: {e.stderr}", "ERROR")
        
        log_status("📄 Converting markdown files to PDF...")
        try:
            result = subprocess.run([
                sys.executable, str(PROJECT_ROOT / "scripts" / "convert_markdown_to_pdf.py")
            ], capture_output=True, text=True, check=True)
            log_status("✅ Converted markdown files to PDF")
        except subprocess.CalledProcessError as e:
            log_status(f"❌ Failed to convert to PDF: {e}", "ERROR")
            log_status(f"Error details: {e.stderr}", "ERROR")
    else:
        log_status("✅ No new markdown files to convert")
    
    log_status("✅ Pipeline complete!")

def validate_all_transcripts():
    """Validate all transcript files and report issues"""
    transcript_files = get_transcript_files()
    
    if not transcript_files:
        print("📝 No transcript files found to validate")
        return
    
    print("🔍 Validating transcript files...")
    print("=" * 50)
    
    valid_files = []
    invalid_files = []
    
    for transcript_file in transcript_files:
        is_valid, reason = validate_transcript_file(transcript_file)
        if is_valid:
            valid_files.append(transcript_file)
            print(f"✅ {transcript_file.name}")
        else:
            invalid_files.append(transcript_file)
            print(f"❌ {transcript_file.name} - {reason}")
    
    print("=" * 50)
    print(f"📊 Validation Results:")
    print(f"  ✅ Valid files: {len(valid_files)}")
    print(f"  ❌ Invalid files: {len(invalid_files)}")
    
    if invalid_files:
        print(f"\n⚠️ Invalid files detected. Consider removing these files and re-transcribing:")
        for file in invalid_files:
            print(f"  • {file.name}")
        print(f"\nTo remove invalid files, run:")
        print(f"  python scripts/run_pipeline.py cleanup")

def cleanup_invalid_transcripts():
    """Remove invalid transcript files"""
    transcript_files = get_transcript_files()
    
    if not transcript_files:
        print("📝 No transcript files found to clean up")
        return
    
    print("🧹 Cleaning up invalid transcript files...")
    print("=" * 50)
    
    removed_count = 0
    for transcript_file in transcript_files:
        is_valid, reason = validate_transcript_file(transcript_file)
        if not is_valid:
            try:
                transcript_file.unlink()
                print(f"🗑️ Removed: {transcript_file.name} - {reason}")
                removed_count += 1
            except Exception as e:
                print(f"❌ Failed to remove {transcript_file.name}: {e}")
    
    print("=" * 50)
    print(f"🗑️ Removed {removed_count} invalid transcript files")

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python scripts/run_pipeline.py run                    # Run full pipeline")
        print("  python scripts/run_pipeline.py status                 # Show pipeline status")
        print("  python scripts/run_pipeline.py validate               # Validate transcript files")
        print("  python scripts/run_pipeline.py cleanup                # Remove invalid transcript files")
        print("\nExamples:")
        print("  python scripts/run_pipeline.py run")
        print("  python scripts/run_pipeline.py status")
        print("  python scripts/run_pipeline.py validate")
        print("  python scripts/run_pipeline.py cleanup")
        return
    
    command = sys.argv[1]
    
    # Initialize logging for run command
    if command == "run":
        log_path = setup_logging()
        log_status(f"Starting pipeline run. Log file: {log_path}")
        run_full_pipeline()
    elif command == "status":
        # For status command, use simple console output
        show_pipeline_status()
    elif command == "validate":
        validate_all_transcripts()
    elif command == "cleanup":
        cleanup_invalid_transcripts()
    else:
        print(f"❌ Invalid command: {command}")
        print("Available commands: run, status, validate, cleanup")

if __name__ == "__main__":
    main() 