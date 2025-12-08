"""
Transcript Cleaning Module

Handles cleaning and correction of transcript files using dental corrections.
"""

import re
import json
from pathlib import Path
from datetime import datetime

# === Configuration ===
# Get the project root directory (parent of dental_consultation_pipeline directory)
PROJECT_ROOT = Path(__file__).parent.parent
TRANSCRIPTS_DIR = PROJECT_ROOT / "transcripts"
CLEANED_DIR = PROJECT_ROOT / "transcripts_clean"
CORRECTIONS_FILE = PROJECT_ROOT / "dental_corrections.json"

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

def format_money_amounts(text):
    """Format money amounts in text to include proper dollar signs"""
    formatted_text = text
    
    # First, handle numbers with explicit money words (highest priority)
    # Numbers followed by "dollars", "bucks", "grand", etc.
    formatted_text = re.sub(r'\b(\d+(?:,\d{3})*(?:\.\d{2})?)\s*(dollars?|bucks?|grand|k)\b', r'$\1', formatted_text, flags=re.IGNORECASE)
    
    # Numbers followed by "thousand", "thousands"
    formatted_text = re.sub(r'\b(\d+(?:,\d{3})*(?:\.\d{2})?)\s*(thousand|thousands)\b', r'$\1,000', formatted_text, flags=re.IGNORECASE)
    
    # Numbers followed by "hundred", "hundreds"
    formatted_text = re.sub(r'\b(\d+(?:,\d{3})*(?:\.\d{2})?)\s*(hundred|hundreds)\b', r'$\1,00', formatted_text, flags=re.IGNORECASE)
    
    # Handle "X hundred" and "X thousand" patterns
    formatted_text = re.sub(r'\b(\d{1,3})\s*(hundred|hundreds)\b', r'$\1,00', formatted_text, flags=re.IGNORECASE)
    formatted_text = re.sub(r'\b(\d{1,2})\s*(thousand|thousands)\b', r'$\1,000', formatted_text, flags=re.IGNORECASE)
    
    # Handle special cases for dental pricing
    # "8,000 right there" -> "$8,000 right there"
    formatted_text = re.sub(r'\b(\d{1,3}(?:,\d{3})*)\s+(right there|a piece|each|per)\b', r'$\1 \2', formatted_text)
    
    # Handle ranges like "15 to 24" -> "$15,000 to $24,000"
    formatted_text = re.sub(r'\b(\d{1,2})\s+to\s+(\d{1,2})\b', r'$\1,000 to $\2,000', formatted_text)
    
    # Handle "give or take" amounts
    formatted_text = re.sub(r'\b(\d{1,2})\s+(\d{1,3})\s+(give or take)\b', r'$\1,$\2 \3', formatted_text)
    
    # Finally, handle standalone numbers that are likely money amounts
    # Only match numbers that don't already have dollar signs
    formatted_text = re.sub(r'\b(?<!\$)(\d{4,})\b(?!\s*(?:dollars?|bucks?|grand|k|thousand|thousands|hundred|hundreds))', r'$\1', formatted_text)
    
    # Handle common dental amounts that might be standalone
    common_amounts = [1500, 2000, 3000, 4000, 5000, 8000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 51000]
    for amount in common_amounts:
        # Only replace if not already formatted and not followed by money words
        pattern = rf'\b(?<!\$)({amount})\b(?!\s*(?:dollars?|bucks?|grand|k|thousand|thousands|hundred|hundreds))'
        formatted_text = re.sub(pattern, rf'$\1', formatted_text)
    
    return formatted_text

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
    
    # Apply money formatting
    money_formatted_text = format_money_amounts(corrected_text)
    
    # Check if any changes were made
    if money_formatted_text == original_text:
        print(f"✅ No changes needed for {file_path.name}")
        # Still create the cleaned file even if no changes were made
        print(f"📝 Creating cleaned file (no corrections needed)")
    else:
        # Count changes from corrections
        correction_changes = []
        for incorrect, correct in corrections.items():
            pattern = r'\b' + re.escape(incorrect) + r'\b'
            original_count = len(re.findall(pattern, original_text, flags=re.IGNORECASE))
            corrected_count = len(re.findall(pattern, corrected_text, flags=re.IGNORECASE))
            
            if original_count > 0:
                correction_changes.append({
                    'incorrect': incorrect,
                    'correct': correct,
                    'original_count': original_count,
                    'corrected_count': corrected_count
                })
        
        # Count money formatting changes
        money_changes = []
        if money_formatted_text != corrected_text:
            # Count dollar signs added
            original_dollars = len(re.findall(r'\$', original_text))
            final_dollars = len(re.findall(r'\$', money_formatted_text))
            if final_dollars > original_dollars:
                money_changes.append(f"Added {final_dollars - original_dollars} dollar signs")
        
        # Display changes
        if correction_changes:
            print(f"📝 Correction changes made:")
            for change in correction_changes:
                print(f"  • '{change['incorrect']}' → '{change['correct']}': {change['original_count']} → {change['corrected_count']} instances")
        
        if money_changes:
            print(f"💰 Money formatting changes:")
            for change in money_changes:
                print(f"  • {change}")
    
    # Use the money formatted text as final result
    corrected_text = money_formatted_text
    
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
        return []
    
    print(f"📋 Loaded {len(corrections)} corrections")
    
    # Check if transcripts directory exists
    if not TRANSCRIPTS_DIR.exists():
        print(f"❌ Transcripts directory not found: {TRANSCRIPTS_DIR}")
        return []
    
    # Get all transcript files
    txt_files = list(TRANSCRIPTS_DIR.glob("*.txt"))
    if not txt_files:
        print(f"📝 No .txt files found in {TRANSCRIPTS_DIR}")
        return []
    
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

def clean_transcripts():
    """Clean all transcript files using dental corrections"""
    print("🔧 Transcript Cleaning Process")
    print("=" * 40)
    
    # Show corrections summary
    show_corrections_summary()
    
    # Show available transcript files
    show_available_transcript_files()
    
    # Process all files
    processed_files = process_all_files()
    
    if processed_files:
        print(f"\n📁 Cleaned files saved to: {CLEANED_DIR}")
        return processed_files
    else:
        print(f"\n📁 No files were processed. Check {TRANSCRIPTS_DIR} for transcript files.")
        return []

def main():
    """Main function for command line usage"""
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "clean":
            clean_transcripts()
        elif sys.argv[1] == "corrections":
            show_corrections_summary()
        elif sys.argv[1] == "files":
            show_available_transcript_files()
        elif sys.argv[1] == "process" and len(sys.argv) > 2:
            process_specific_file(sys.argv[2])
        else:
            print("Usage:")
            print("  python -m dental_consultation_pipeline.cleaning clean        - Clean all transcripts")
            print("  python -m dental_consultation_pipeline.cleaning corrections - Show available corrections")
            print("  python -m dental_consultation_pipeline.cleaning files       - Show available transcript files")
            print("  python -m dental_consultation_pipeline.cleaning process <filename> - Process specific file")
    else:
        # Interactive mode
        print("🔧 Transcript Cleaner Module")
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