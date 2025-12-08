#!/usr/bin/env python3
"""
Transcript Cleaner Script
Uses the dental corrections dictionary to clean transcript files
"""

import re
import json
from pathlib import Path
from datetime import datetime

# === Configuration ===
TRANSCRIPTS_DIR = Path("transcripts")
CLEANED_DIR = Path("transcripts_clean")
CORRECTIONS_FILE = Path("dental_corrections.json")

def load_corrections():
    """Load corrections from JSON file"""
    try:
        with open(CORRECTIONS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data['dental_corrections']
    except Exception as e:
        print(f"❌ Error loading corrections: {e}")
        return {}

def apply_corrections_to_text(text, corrections):
    """Apply corrections to text"""
    corrected_text = text
    
    # Sort corrections by length (longest first) to avoid partial replacements
    sorted_corrections = sorted(corrections.items(), key=lambda x: len(x[0]), reverse=True)
    
    for incorrect, correct in sorted_corrections:
        # Use word boundaries to avoid partial matches
        pattern = r'\b' + re.escape(incorrect) + r'\b'
        corrected_text = re.sub(pattern, correct, corrected_text, flags=re.IGNORECASE)
    
    return corrected_text

def get_clean_filename(original_filename):
    """Convert filename to clean format: patient_name_consult_clean.txt"""
    # Remove .txt extension
    name_without_ext = original_filename.replace('.txt', '')
    
    # Convert to clean format
    clean_name = f"{name_without_ext}_clean.txt"
    
    return clean_name

def process_file(file_path, corrections):
    """Process a single transcript file"""
    print(f"🔄 Processing: {file_path.name}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            original_text = f.read()
    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")
        return None
    
    # Apply corrections
    corrected_text = apply_corrections_to_text(original_text, corrections)
    
    # Check if any changes were made
    if corrected_text == original_text:
        print(f"✅ No changes needed for {file_path.name}")
        return None
    
    # Count changes
    changes = []
    for incorrect, correct in corrections.items():
        pattern = r'\b' + re.escape(incorrect) + r'\b'
        original_count = len(re.findall(pattern, original_text, flags=re.IGNORECASE))
        corrected_count = len(re.findall(pattern, corrected_text, flags=re.IGNORECASE))
        
        if original_count > 0:
            changes.append({
                'incorrect': incorrect,
                'correct': correct,
                'original_count': original_count,
                'corrected_count': corrected_count
            })
    
    # Display changes
    print(f"📝 Changes made:")
    for change in changes:
        print(f"  • '{change['incorrect']}' → '{change['correct']}': {change['original_count']} → {change['corrected_count']} instances")
    
    # Create cleaned directory if it doesn't exist
    CLEANED_DIR.mkdir(exist_ok=True)
    
    # Generate clean filename
    clean_filename = get_clean_filename(file_path.name)
    cleaned_file_path = CLEANED_DIR / clean_filename
    
    try:
        with open(cleaned_file_path, 'w', encoding='utf-8') as f:
            f.write(corrected_text)
        print(f"✅ Cleaned file saved: {cleaned_file_path}")
        return cleaned_file_path
    except Exception as e:
        print(f"❌ Error saving cleaned file: {e}")
        return None

def process_all_files():
    """Process all transcript files"""
    # Load corrections
    corrections = load_corrections()
    if not corrections:
        print("❌ No corrections loaded")
        return
    
    print(f"📋 Loaded {len(corrections)} corrections")
    
    # Check if transcripts directory exists
    if not TRANSCRIPTS_DIR.exists():
        print(f"❌ Transcripts directory not found: {TRANSCRIPTS_DIR}")
        return
    
    # Get all transcript files
    txt_files = list(TRANSCRIPTS_DIR.glob("*.txt"))
    if not txt_files:
        print(f"📝 No .txt files found in {TRANSCRIPTS_DIR}")
        return
    
    print(f"🔄 Processing {len(txt_files)} files...")
    print("=" * 50)
    
    processed_files = []
    for file_path in txt_files:
        result = process_file(file_path, corrections)
        if result:
            processed_files.append(result)
        print()  # Add spacing
    
    print(f"✅ Successfully processed {len(processed_files)} files")
    return processed_files

def process_specific_file(filename):
    """Process a specific transcript file"""
    corrections = load_corrections()
    if not corrections:
        print("❌ No corrections loaded")
        return None
    
    file_path = TRANSCRIPTS_DIR / filename
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return None
    
    return process_file(file_path, corrections)

def show_corrections_summary():
    """Show a summary of available corrections"""
    corrections = load_corrections()
    if not corrections:
        print("❌ No corrections loaded")
        return
    
    print("📋 Available Corrections:")
    print("=" * 40)
    
    # Group by category
    dental_terms = []
    compound_words = []
    abbreviations = []
    
    for incorrect, correct in corrections.items():
        if ' ' in incorrect:
            compound_words.append((incorrect, correct))
        elif len(incorrect) <= 4:
            abbreviations.append((incorrect, correct))
        else:
            dental_terms.append((incorrect, correct))
    
    print(f"🦷 Dental Terms ({len(dental_terms)}):")
    for incorrect, correct in dental_terms[:10]:  # Show first 10
        print(f"  • '{incorrect}' → '{correct}'")
    if len(dental_terms) > 10:
        print(f"  ... and {len(dental_terms) - 10} more")
    
    print(f"\n🔗 Compound Words ({len(compound_words)}):")
    for incorrect, correct in compound_words[:10]:  # Show first 10
        print(f"  • '{incorrect}' → '{correct}'")
    if len(compound_words) > 10:
        print(f"  ... and {len(compound_words) - 10} more")
    
    print(f"\n📝 Abbreviations ({len(abbreviations)}):")
    for incorrect, correct in abbreviations[:10]:  # Show first 10
        print(f"  • '{incorrect}' → '{correct}'")
    if len(abbreviations) > 10:
        print(f"  ... and {len(abbreviations) - 10} more")

def get_available_transcript_files():
    """Get list of available transcript .txt files"""
    if not TRANSCRIPTS_DIR.exists():
        print(f"❌ Transcripts directory not found: {TRANSCRIPTS_DIR}")
        return []
    
    txt_files = list(TRANSCRIPTS_DIR.glob("*.txt"))
    return [file.name for file in txt_files]

def show_available_transcript_files():
    """Show list of available transcript files"""
    transcript_files = get_available_transcript_files()
    
    if not transcript_files:
        print("📝 No .txt files found in transcripts directory")
        return
    
    print(f"\n📄 Available Transcript Files ({len(transcript_files)}):")
    print("=" * 50)
    
    for i, filename in enumerate(transcript_files, 1):
        print(f"  {i:2d}. {filename}")
    
    print()

def main():
    """Main function"""
    print("🔧 Transcript Cleaner Script")
    print("=" * 40)
    
    # Show corrections summary
    show_corrections_summary()
    
    # Show available transcript files
    show_available_transcript_files()
    
    # Option to process a specific file
    print(f"\n🎯 Process a specific file? (y/n): ", end="")
    response = input().lower().strip()
    
    if response in ['y', 'yes']:
        print("Enter the filename (e.g., 'Christina King consult.txt'): ", end="")
        filename = input().strip()
        process_specific_file(filename)
    
    # Option to process all files
    print(f"\n🔄 Process all transcript files? (y/n): ", end="")
    response = input().lower().strip()
    
    if response in ['y', 'yes']:
        process_all_files()
    
    print(f"\n📁 Cleaned files saved to: {CLEANED_DIR}")

if __name__ == "__main__":
    main() 