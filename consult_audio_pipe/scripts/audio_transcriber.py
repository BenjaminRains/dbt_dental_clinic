import sys
import os
import whisper
from datetime import datetime
from pathlib import Path

# === Configuration ===
RAW_AUDIO_DIR = Path("raw_audio")
OUTPUT_DIR = Path("transcripts")
log_file = os.path.join(os.path.dirname(__file__), "progress.log")

def log_status(message: str):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    line = f"{timestamp} {message}"
    print(line)
    with open(log_file, "a", encoding="utf-8") as log:
        log.write(line + "\n")

def transcribe_audio(audio_path: str):
    log_status("Loading Whisper model...")
    model = whisper.load_model("base")

    log_status(f"Starting transcription for {audio_path}...")
    result = model.transcribe(audio_path)
    log_status("Transcription completed.")
    return result

def save_outputs(result, txt_path, tsv_path, srt_path, vtt_path):
    log_status(f"Saving TXT to {txt_path}")
    with open(txt_path, "w", encoding="utf-8") as f:
        # Write formatted text with line breaks for readability
        for segment in result["segments"]:
            text = segment["text"].strip()
            if text:  # Only write non-empty segments
                f.write(f"{text}\n")

    def write_srt(segments, path):
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
    result = transcribe_audio(str(audio_path))
    save_outputs(result, txt_path, tsv_path, srt_path, vtt_path)
    
    return True

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

# === Handle command-line argument ===
if len(sys.argv) < 2:
    print("Usage: python audio_transcriber.py <audio_file>")
    print("   or: python audio_transcriber.py --list")
    print("   or: python audio_transcriber.py --process <filename>")
    print("   or: python audio_transcriber.py --transcribe-all")
    exit(1)

# Check for special commands
if sys.argv[1] == "--list":
    show_available_audio_files()
    exit(0)

if sys.argv[1] == "--process":
    if len(sys.argv) < 3:
        print("Usage: python audio_transcriber.py --process <filename>")
        exit(1)
    filename = sys.argv[2]
    if process_specific_audio_file(filename):
        print("✅ Processing complete.")
    else:
        print("❌ Processing failed.")
    exit(0)

if sys.argv[1] == "--transcribe-all":
    transcribe_all_audio_files()
    exit(0)

# Original functionality for direct file path
audio_file = sys.argv[1]
if not os.path.exists(audio_file):
    print(f"Error: File '{audio_file}' does not exist.")
    exit(1)

# === Output paths for original functionality ===
base_filename = os.path.splitext(os.path.basename(audio_file))[0]
output_dir = os.path.join(os.path.dirname(__file__), "transcripts")
os.makedirs(output_dir, exist_ok=True)

txt_path = os.path.join(output_dir, f"{base_filename}.txt")
tsv_path = os.path.join(output_dir, f"{base_filename}.tsv")
srt_path = os.path.join(output_dir, f"{base_filename}.srt")
vtt_path = os.path.join(output_dir, f"{base_filename}.vtt")

def main():
    log_status(f"Processing: {audio_file}")
    result = transcribe_audio(audio_file)
    
    # Set up output paths for original functionality
    base_filename = os.path.splitext(os.path.basename(audio_file))[0]
    txt_path = os.path.join(output_dir, f"{base_filename}.txt")
    tsv_path = os.path.join(output_dir, f"{base_filename}.tsv")
    srt_path = os.path.join(output_dir, f"{base_filename}.srt")
    vtt_path = os.path.join(output_dir, f"{base_filename}.vtt")
    
    save_outputs(result, txt_path, tsv_path, srt_path, vtt_path)
    log_status("Audio transcription complete.")

if __name__ == "__main__":
    main()
