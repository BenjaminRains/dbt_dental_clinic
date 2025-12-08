"""
Audio Transcription Module

Handles audio file transcription using OpenAI Whisper.
"""

import sys
import os
import whisper
from datetime import datetime
from pathlib import Path

# === Configuration ===
# Get the project root directory (parent of dental_consultation_pipeline directory)
PROJECT_ROOT = Path(__file__).parent.parent
RAW_AUDIO_DIR = PROJECT_ROOT / "raw_audio"
OUTPUT_DIR = PROJECT_ROOT / "transcripts"
log_file = PROJECT_ROOT / "logs" / "transcription_progress.log"

def log_status(message: str):
    """Log status messages with timestamp"""
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    line = f"{timestamp} {message}"
    print(line)
    
    # Ensure logs directory exists
    log_file.parent.mkdir(exist_ok=True)
    
    with open(log_file, "a", encoding="utf-8") as log:
        log.write(line + "\n")

def transcribe_audio_file(audio_path: str):
    """Transcribe an audio file using OpenAI Whisper"""
    log_status("Loading Whisper model...")
    model = whisper.load_model("base")

    log_status(f"Starting transcription for {audio_path}...")
    result = model.transcribe(audio_path)
    log_status("Transcription completed.")
    return result

def save_outputs(result, txt_path, tsv_path, srt_path, vtt_path):
    """Save transcription results in multiple formats"""
    log_status(f"Saving TXT to {txt_path}")
    with open(txt_path, "w", encoding="utf-8") as f:
        # Write formatted text with line breaks for readability
        for segment in result["segments"]:
            text = segment["text"].strip()
            if text:  # Only write non-empty segments
                f.write(f"{text}\n")

    def write_srt(segments, path):
        """Write SRT subtitle format"""
        with open(path, "w", encoding="utf-8") as f:
            for i, segment in enumerate(segments, start=1):
                start = segment["start"]
                end = segment["end"]
                text = segment["text"].strip()

                def format_time(t):
                    h = int(t // 3600)
                    m = int((t % 3600) // 60)
                    s = int(t % 60)
                    ms = int((t - int(t)) * 1000)
                    return f"{h:02}:{m:02}:{s:02},{ms:03}"

                f.write(f"{i}\n{format_time(start)} --> {format_time(end)}\n{text}\n\n")

    def write_vtt(segments, path):
        """Write VTT subtitle format"""
        with open(path, "w", encoding="utf-8") as f:
            f.write("WEBVTT\n\n")
            for segment in segments:
                start = segment["start"]
                end = segment["end"]
                text = segment["text"].strip()

                def format_time(t):
                    h = int(t // 3600)
                    m = int((t % 3600) // 60)
                    s = int(t % 60)
                    ms = int((t - int(t)) * 1000)
                    return f"{h:02}:{m:02}:{s:02}.{ms:03}"

                f.write(f"{format_time(start)} --> {format_time(end)}\n{text}\n\n")

    def write_tsv(segments, path):
        """Write TSV format with timestamps"""
        with open(path, "w", encoding="utf-8") as f:
            f.write("start\tend\ttext\n")
            for segment in segments:
                f.write(f"{segment['start']}\t{segment['end']}\t{segment['text'].strip()}\n")

    log_status(f"Saving TSV to {tsv_path}")
    write_tsv(result["segments"], tsv_path)

    log_status(f"Saving SRT to {srt_path}")
    write_srt(result["segments"], srt_path)

    log_status(f"Saving VTT to {vtt_path}")
    write_vtt(result["segments"], vtt_path)

    log_status("All files saved successfully.")

def get_available_audio_files():
    """Get list of available audio files in raw_audio directory"""
    if not RAW_AUDIO_DIR.exists():
        print(f"❌ Raw audio directory not found: {RAW_AUDIO_DIR}")
        return []
    
    audio_files = []
    for ext in ['*.m4a', '*.mp3', '*.wav', '*.ogg', '*.flac']:
        audio_files.extend(RAW_AUDIO_DIR.glob(ext))
    
    return [file.name for file in audio_files]

def show_available_audio_files():
    """Show list of available audio files"""
    audio_files = get_available_audio_files()
    
    if not audio_files:
        print("🎵 No audio files found in raw_audio directory")
        return
    
    print(f"\n🎵 Available Audio Files ({len(audio_files)}):")
    print("=" * 50)
    
    for i, filename in enumerate(audio_files, 1):
        print(f"  {i:2d}. {filename}")
    
    print()

def process_specific_audio_file(filename):
    """Process a specific audio file from raw_audio directory"""
    audio_path = RAW_AUDIO_DIR / filename
    
    if not audio_path.exists():
        print(f"❌ Audio file not found: {audio_path}")
        return False
    
    print(f"🎵 Processing: {filename}")
    
    # Set up output paths
    base_filename = audio_path.stem
    txt_path = OUTPUT_DIR / f"{base_filename}.txt"
    tsv_path = OUTPUT_DIR / f"{base_filename}.tsv"
    srt_path = OUTPUT_DIR / f"{base_filename}.srt"
    vtt_path = OUTPUT_DIR / f"{base_filename}.vtt"
    
    # Create output directory if it doesn't exist
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Transcribe and save
    result = transcribe_audio_file(str(audio_path))
    save_outputs(result, txt_path, tsv_path, srt_path, vtt_path)
    
    return True

def transcribe_audio(audio_file_path: str) -> str:
    """
    Transcribe an audio file using OpenAI Whisper.
    
    Args:
        audio_file_path: Path to the audio file
        
    Returns:
        Transcribed text
    """
    result = transcribe_audio_file(audio_file_path)
    
    # Extract just the text content
    text_content = ""
    for segment in result["segments"]:
        text = segment["text"].strip()
        if text:
            text_content += text + "\n"
    
    return text_content.strip()

def transcribe_all_audio_files():
    """Transcribe all audio files in the raw_audio directory"""
    audio_files = get_available_audio_files()
    
    if not audio_files:
        print("❌ No audio files found to transcribe")
        return []
    
    print(f"🎵 Found {len(audio_files)} audio files to transcribe...")
    print(f"📁 Input: {RAW_AUDIO_DIR}")
    print(f"📁 Output: {OUTPUT_DIR}")
    print("=" * 50)
    
    processed_files = []
    for filename in audio_files:
        if process_specific_audio_file(filename):
            processed_files.append(filename)
        print()  # Add spacing
    
    print("=" * 50)
    print(f"🎉 Successfully transcribed {len(processed_files)}/{len(audio_files)} files")
    print(f"📁 Transcript files saved in: {OUTPUT_DIR}")
    
    return processed_files

def main():
    """Main function for command line usage"""
    if len(sys.argv) > 1:
        if sys.argv[1] == "transcribe":
            transcribe_all_audio_files()
        elif sys.argv[1] == "list":
            show_available_audio_files()
        elif sys.argv[1] == "process" and len(sys.argv) > 2:
            filename = sys.argv[2]
            if process_specific_audio_file(filename):
                print("✅ Processing complete.")
            else:
                print("❌ Processing failed.")
        elif sys.argv[1] == "file" and len(sys.argv) > 2:
            # Original functionality for direct file path
            audio_file = sys.argv[2]
            if not os.path.exists(audio_file):
                print(f"Error: File '{audio_file}' does not exist.")
                return
            
            log_status(f"Processing: {audio_file}")
            result = transcribe_audio_file(audio_file)
            
            # Set up output paths
            base_filename = os.path.splitext(os.path.basename(audio_file))[0]
            txt_path = OUTPUT_DIR / f"{base_filename}.txt"
            tsv_path = OUTPUT_DIR / f"{base_filename}.tsv"
            srt_path = OUTPUT_DIR / f"{base_filename}.srt"
            vtt_path = OUTPUT_DIR / f"{base_filename}.vtt"
            
            # Create output directory if it doesn't exist
            OUTPUT_DIR.mkdir(exist_ok=True)
            
            save_outputs(result, txt_path, tsv_path, srt_path, vtt_path)
            log_status("Audio transcription complete.")
        else:
            print("Usage:")
            print("  python -m dental_consultation_pipeline.transcription transcribe - Transcribe all audio files")
            print("  python -m dental_consultation_pipeline.transcription list       - Show available audio files")
            print("  python -m dental_consultation_pipeline.transcription process <filename> - Process specific file")
            print("  python -m dental_consultation_pipeline.transcription file <path> - Process file by path")
    else:
        # Interactive mode
        print("🎵 Audio Transcription Module")
        print("=" * 40)
        
        # Show available audio files
        show_available_audio_files()
        
        # Option to process a specific file
        print(f"\n🎯 Process a specific file? (y/n): ", end="")
        response = input().lower().strip()
        
        if response in ['y', 'yes']:
            print("Enter the filename (e.g., 'consult1.m4a'): ", end="")
            filename = input().strip()
            if process_specific_audio_file(filename):
                print("✅ Processing complete.")
            else:
                print("❌ Processing failed.")
        
        # Option to process all files
        print(f"\n🔄 Process all audio files? (y/n): ", end="")
        response = input().lower().strip()
        
        if response in ['y', 'yes']:
            transcribe_all_audio_files()
        
        print(f"\n📁 Transcript files saved in: {OUTPUT_DIR}")

if __name__ == "__main__":
    main() 