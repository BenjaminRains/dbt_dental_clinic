#!/usr/bin/env python3
"""
Pipeline Orchestrator
Coordinates the complete workflow from audio files to analysis
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime

# Import the LLM analysis module
from .analysis import analyze_clean_transcripts

# === Configuration ===
# Get the project root directory (parent of dental_consultation_pipeline directory)
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
    claude_output_dir = PROJECT_ROOT / "api_claude_summary_analysis"
    chatgpt_output_dir = PROJECT_ROOT / "api_chatgpt_summary_analysis"
    html_output_dir = PROJECT_ROOT / "html_output"
    pdf_output_dir = PROJECT_ROOT / "pdf_output"
    
    markdown_files = []
    
    # Check Claude output directory
    if claude_output_dir.exists():
        markdown_files.extend(list(claude_output_dir.glob("*.md")))
    
    # Check ChatGPT output directory
    if chatgpt_output_dir.exists():
        markdown_files.extend(list(chatgpt_output_dir.glob("*.md")))
    
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
    claude_output_dir = PROJECT_ROOT / "api_claude_summary_analysis"
    chatgpt_output_dir = PROJECT_ROOT / "api_chatgpt_summary_analysis"
    
    claude_files = []
    if claude_output_dir.exists():
        claude_files = list(claude_output_dir.glob("*"))
    
    chatgpt_files = []
    if chatgpt_output_dir.exists():
        chatgpt_files = list(chatgpt_output_dir.glob("*"))
    
    total_llm_files = len(claude_files) + len(chatgpt_files)
    print(f"\n🤖 LLM Analysis & Summary files: {total_llm_files}")
    print(f"  📁 Claude outputs ({len(claude_files)} files):")
    for llm_file in claude_files:
        print(f"    • {llm_file.name}")
    print(f"  📁 ChatGPT outputs ({len(chatgpt_files)} files):")
    for llm_file in chatgpt_files:
        print(f"    • {llm_file.name}")
    
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
    log_status("Using Anthropic Claude API and/or OpenAI ChatGPT 5 for analysis and summaries")
    
    # Step 1: Check for new audio files and transcribe them
    log_status("🎵 Step 1: Checking for new audio files...")
    new_audio_files = find_new_audio_files()
    
    transcription_successful = True  # Track if transcription was successful
    
    if new_audio_files:
        log_status(f"🆕 Found {len(new_audio_files)} new audio file(s) to transcribe:")
        for audio_file in new_audio_files:
            log_status(f"  • {audio_file.name}")
        
        log_status("🎵 Transcribing audio files using transcription module...")
        try:
            # Import and use the transcription module directly
            from .transcription import transcribe_all_audio_files
            processed_files = transcribe_all_audio_files()
            if processed_files:
                log_status(f"✅ Successfully transcribed {len(processed_files)} files")
            else:
                log_status("❌ No files were transcribed", "ERROR")
                transcription_successful = False
        except Exception as e:
            log_status(f"❌ Failed to transcribe audio files: {e}", "ERROR")
            transcription_successful = False
    else:
        log_status("✅ No new audio files found")
    
    # Step 2: Check for new transcripts to clean (only if transcription was successful or no transcription was needed)
    if transcription_successful:
        log_status("🧹 Step 2: Checking for new transcripts to clean...")
        new_transcript_files = find_new_transcripts_to_clean()
        
        cleaning_successful = True  # Track if cleaning was successful
        
        if new_transcript_files:
            log_status(f"🆕 Found {len(new_transcript_files)} new transcript file(s) to clean:")
            for transcript_file in new_transcript_files:
                log_status(f"  • {transcript_file.name}")
            
            log_status("🧹 Cleaning transcript files using cleaning module...")
            try:
                # Import and use the cleaning module directly
                from .cleaning import clean_transcripts
                processed_files = clean_transcripts()
                if processed_files:
                    log_status(f"✅ Applied corrections to {len(processed_files)} transcript files")
                else:
                    log_status("✅ No transcript files needed cleaning")
            except Exception as e:
                log_status(f"❌ Failed to clean transcripts: {e}", "ERROR")
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
            # Check if we should use both models
            use_both_models = os.getenv("USE_BOTH_MODELS", "false").lower() == "true"
            if use_both_models:
                log_status("🔄 Using both Claude and ChatGPT 5 models for analysis...")
                from .analysis import analyze_clean_transcripts_with_both_models
                analyze_clean_transcripts_with_both_models()
            else:
                # Use specified LLM model (default to claude if not specified)
                llm_model = os.getenv("LLM_MODEL", "claude").lower()
                if llm_model not in ["claude", "chatgpt"]:
                    log_status(f"⚠️ Invalid LLM model '{llm_model}'. Using Claude as default.", "WARNING")
                    llm_model = "claude"
                
                log_status(f"🤖 Using {llm_model.upper()} for analysis...")
                from .analysis import analyze_clean_transcripts
                analyze_clean_transcripts(llm_model)
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
            
            log_status("🌐 Converting markdown files to HTML and PDF...")
            try:
                # Import and use the conversion module directly
                from .conversion import convert_all_formats
                convert_all_formats()
                log_status("✅ Converted markdown files to HTML and PDF")
            except Exception as e:
                log_status(f"❌ Failed to convert markdown files: {e}", "ERROR")
        else:
            log_status("✅ No new markdown files to convert")
    else:
        log_status("🛑 Pipeline aborted due to transcription failure.", "ERROR")
        log_status("Please fix the transcription issues and run the pipeline again.", "WARNING")
        return
    
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
        print(f"  python -m dental_consultation_pipeline.pipeline cleanup")

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
        print("  python -m dental_consultation_pipeline.pipeline run [llm]              # Run full pipeline")
        print("  python -m dental_consultation_pipeline.pipeline status                 # Show pipeline status")
        print("  python -m dental_consultation_pipeline.pipeline validate               # Validate transcript files")
        print("  python -m dental_consultation_pipeline.pipeline cleanup                # Remove invalid transcript files")
        print("\nLLM Options:")
        print("  claude   - Use Anthropic Claude 3.5 Sonnet (default)")
        print("  chatgpt  - Use OpenAI ChatGPT 5")
        print("\nExamples:")
        print("  python -m dental_consultation_pipeline.pipeline run claude")
        print("  python -m dental_consultation_pipeline.pipeline run chatgpt")
        print("  python -m dental_consultation_pipeline.pipeline run")
        print("  python -m dental_consultation_pipeline.pipeline status")
        print("  python -m dental_consultation_pipeline.pipeline validate")
        print("  python -m dental_consultation_pipeline.pipeline cleanup")
        return
    
    command = sys.argv[1]
    
    # Initialize logging for run command
    if command == "run":
        # Check for LLM argument
        llm_model = "claude"  # default
        if len(sys.argv) > 2:
            llm_model = sys.argv[2].lower()
            if llm_model not in ["claude", "chatgpt"]:
                print(f"❌ Invalid LLM model: {llm_model}")
                print("Available LLM models: claude, chatgpt")
                return
        
        # Set environment variable for the pipeline to use
        os.environ["LLM_MODEL"] = llm_model
        
        log_path = setup_logging()
        log_status(f"Starting pipeline run with {llm_model.upper()}. Log file: {log_path}")
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