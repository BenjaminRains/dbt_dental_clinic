import os
import json
import requests
from pathlib import Path
from datetime import datetime

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
except Exception as e:
    print(f"⚠️  Error loading .env file: {e}")

# === Configuration ===
# Get the project root directory (parent of scripts directory)
PROJECT_ROOT = Path(__file__).parent.parent
TRANSCRIPTS_CLEAN_DIR = PROJECT_ROOT / "transcripts_clean"
OUTPUT_DIR = PROJECT_ROOT / "api_claude_summary_analysis"
JSON_DEBUG_DIR = PROJECT_ROOT / "api_debug_responses"

# LLM Configuration
LLM_CONFIG = {
    "anthropic_api_key": os.getenv("ANTHROPIC_API_KEY"),
    "anthropic_model": "claude-3-5-sonnet-20241022",
}



SUMMARY_PROMPT = """You are tasked with creating a clear, clean summary of a dental consultation conversation. 

TRANSCRIPT:
{transcript}

Please create a professional summary that includes:

# {patient_name} Consultation Summary

## Consultation Overview
- Date and type of consultation
- Main reason for visit
- Key topics discussed

## Patient Information
- Current dental concerns
- Medical history mentioned
- Current medications (if discussed)

## Treatment Discussion
- Treatment options presented
- Recommendations made
- Treatment plan outlined

## Financial Information
- Costs discussed
- Insurance coverage mentioned
- Payment arrangements

## Medication Information
- Medications discussed
- Medication history
- Medication safety considerations

## Follow-up
- Next steps recommended
- Appointments scheduled
- Instructions given to patient

## Key Points
- Important decisions made
- Critical information shared
- Action items for patient

Write this as a clear, factual summary without analysis or interpretation. Focus on what was actually discussed from the clean transcript."""

ANALYSIS_PROMPT = """You are an expert dental consultant and sales coach analyzing consultation recordings. Please provide a comprehensive analysis of the following consultation transcript.

TRANSCRIPT:
{transcript}

IMPORTANT: Pay special attention to any discussions about:
- Financial matters (costs, fees, payment plans, insurance, pricing)
- Medical information (current health issues, medications, medical history)
- Treatment costs and financial arrangements
- Insurance coverage and benefits
- Payment methods and financing options
- Medical conditions that may affect dental treatment
- Current medications and their impact on dental care
- Health concerns or symptoms mentioned by the patient

Please provide a detailed analysis in the following structured format:

# {patient_name} Consult Analysis

## Overall Assessment
Provide a brief but comprehensive assessment of the consultation quality, including professionalism, effectiveness, and patient engagement. Be critical. The dentist likes critical feedback more than postive feedback.

## Strengths
- Identify 3-5 specific strengths in the consultation
- Focus on communication, clinical presentation, and patient care
- Be specific and actionable

## Areas for Improvement
- Identify 3-5 specific areas that could be improved
- Provide constructive feedback
- Focus on actionable improvements

## Communication Skills
- Analyze verbal communication effectiveness
- Assess listening skills and patient engagement
- Evaluate clarity of explanations

## Clinical Presentation
- Assess clinical knowledge demonstration
- Evaluate treatment planning presentation
- Review patient education quality

## Financial Communication Analysis
- How effectively were costs and fees discussed?
- Was insurance coverage clearly explained?
- Were payment options and financing presented appropriately?
- Any concerns about financial transparency or clarity?

## Medical Information Handling
- How well were current health issues addressed?
- Was medication history properly reviewed?
- Were medical conditions that affect dental treatment identified?
- Any gaps in medical information gathering?

## Recommendations
- Provide 3-5 specific, actionable recommendations
- Focus on immediate improvements
- Include both communication and clinical aspects
- Address any financial or medical communication issues identified

## Key Takeaways
Summarize the main points and provide actionable insights for future consultations.
Include any critical financial or medical information that should be followed up on.

Be professional, constructive, and specific in your analysis. Focus on practical improvements that can enhance consultation quality, with particular attention to financial transparency and medical information accuracy."""

def get_clean_transcript_files():
    """Get list of clean transcript files in transcripts_clean directory"""
    if not TRANSCRIPTS_CLEAN_DIR.exists():
        return []
    
    return list(TRANSCRIPTS_CLEAN_DIR.glob("*_clean.txt"))

def extract_patient_name(filename):
    """Extract patient name from filename"""
    # Remove file extensions
    name = filename.replace('.txt', '').replace('.tsv', '').replace('.srt', '').replace('.vtt', '')
    
    # Remove '_clean' suffix
    if name.endswith('_clean'):
        name = name[:-6]
    
    # Remove common suffixes
    suffixes_to_remove = [' consult', ' cinsult', ' photos']
    for suffix in suffixes_to_remove:
        if name.lower().endswith(suffix.lower()):
            name = name[:-len(suffix)]
            break
    
    # Handle special cases like "Voice 250618_115741" -> "Voice Recording"
    if name.startswith('Voice '):
        name = "Voice Recording"
    elif name == 'consult1':
        name = "Consult 1"
    
    return name

def read_transcript(txt_path):
    """Read transcript from TXT file"""
    try:
        with open(txt_path, 'r', encoding='utf-8') as f:
            return f.read().strip()
    except Exception as e:
        print(f"Error reading {txt_path}: {e}")
        return None

def call_anthropic_api_analysis(transcript, patient_name):
    """Generate analysis using Anthropic Claude API and save full JSON response."""
    if not LLM_CONFIG["anthropic_api_key"]:
        print("❌ Anthropic API key not found. Set ANTHROPIC_API_KEY environment variable.")
        return None
    
    headers = {
        "x-api-key": LLM_CONFIG["anthropic_api_key"],
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01"
    }
    
    data = {
        "model": LLM_CONFIG["anthropic_model"],
        "max_tokens": 2000,
        "messages": [
            {
                "role": "user",
                "content": ANALYSIS_PROMPT.format(transcript=transcript, patient_name=patient_name)
            }
        ]
    }
    
    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=data,
            timeout=60
        )
        response.raise_for_status()
        
        # Get the full JSON response
        json_response = response.json()
        
        # Save the full JSON response for debugging
        JSON_DEBUG_DIR.mkdir(exist_ok=True)
        json_filename = f"{patient_name}_analysis_response.json"
        json_path = JSON_DEBUG_DIR / json_filename
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_response, f, indent=2)
        print(f"📄 JSON response saved: {json_path}")
        
        # Extract and return the text content
        text_content = json_response["content"][0]["text"]
        return text_content
    except Exception as e:
        print(f"❌ Anthropic API error: {e}")
        return None

def generate_analysis_with_llm(transcript, patient_name):
    """Generate analysis using Anthropic Claude API"""
    print(f"🤖 Generating analysis using Anthropic Claude...")
    return call_anthropic_api_analysis(transcript, patient_name)

def process_transcript_with_llm(clean_transcript_path):
    """Process a single clean transcript file with LLM analysis and summary"""
    patient_name = extract_patient_name(clean_transcript_path.name.replace('_clean', ''))
    
    print(f"🟢 Processing: {patient_name}")
    
    # Read clean transcript
    transcript = read_transcript(clean_transcript_path)
    if not transcript:
        print(f"❌ Failed to read clean transcript for: {patient_name}")
        return False
    
    success_count = 0
    
    # Generate summary
    print(f"📝 Generating clean summary...")
    summary = generate_summary_with_llm(transcript, patient_name)
    if summary:
        # Save as Markdown
        summary_md_path = save_summary_to_markdown(summary, patient_name)
        if summary_md_path:
            print(f"✅ Summary Markdown saved: {summary_md_path}")
            success_count += 1
    else:
        print(f"❌ Failed to generate summary for: {patient_name}")
    
    # Generate analysis
    print(f"🤖 Generating skill analysis...")
    analysis = generate_analysis_with_llm(transcript, patient_name)
    if analysis:
        # Save as Markdown
        analysis_md_path = save_analysis_to_markdown(analysis, patient_name)
        if analysis_md_path:
            print(f"✅ Analysis Markdown saved: {analysis_md_path}")
            success_count += 1
    else:
        print(f"❌ Failed to generate analysis for: {patient_name}")
    
    return success_count >= 2  # At least summary and analysis must succeed (2 files)

def generate_summary_with_llm(transcript, patient_name):
    """Generate clean summary using Anthropic Claude API"""
    print(f"📝 Generating summary using Anthropic Claude...")
    return call_anthropic_api_summary(transcript, patient_name)

def call_anthropic_api_summary(transcript, patient_name):
    """Generate summary using Anthropic Claude API and save full JSON response."""
    if not LLM_CONFIG["anthropic_api_key"]:
        print("❌ Anthropic API key not found. Set ANTHROPIC_API_KEY environment variable.")
        return None
    
    headers = {
        "x-api-key": LLM_CONFIG["anthropic_api_key"],
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01"
    }
    
    data = {
        "model": LLM_CONFIG["anthropic_model"],
        "max_tokens": 1500,
        "messages": [
            {
                "role": "user",
                "content": SUMMARY_PROMPT.format(transcript=transcript, patient_name=patient_name)
            }
        ]
    }
    
    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=data,
            timeout=60
        )
        response.raise_for_status()
        
        # Get the full JSON response
        json_response = response.json()
        
        # Save the full JSON response for debugging
        JSON_DEBUG_DIR.mkdir(exist_ok=True)
        json_filename = f"{patient_name}_summary_response.json"
        json_path = JSON_DEBUG_DIR / json_filename
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_response, f, indent=2)
        print(f"📄 JSON response saved: {json_path}")
        
        # Extract and return the text content
        text_content = json_response["content"][0]["text"]
        return text_content
    except Exception as e:
        print(f"❌ Anthropic API error: {e}")
        return None

def save_summary_to_markdown(summary, patient_name):
    """Save summary to Markdown file"""
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    output_filename = f"{patient_name}_summary.md"
    output_path = OUTPUT_DIR / output_filename
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(summary)
        return output_path
    except Exception as e:
        print(f"❌ Error saving Markdown summary: {e}")
        return None

def save_analysis_to_markdown(analysis, patient_name):
    """Save analysis to Markdown file"""
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    output_filename = f"{patient_name}_analysis.md"
    output_path = OUTPUT_DIR / output_filename
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(analysis)
        return output_path
    except Exception as e:
        print(f"❌ Error saving Markdown analysis: {e}")
        return None



def analyze_clean_transcripts():
    """Analyze all clean transcripts that don't have analysis files yet"""
    clean_transcript_files = get_clean_transcript_files()
    new_clean_transcript_files = []
    
    for clean_transcript_file in clean_transcript_files:
        patient_name = extract_patient_name(clean_transcript_file.name.replace('_clean', ''))
        
        # Check if analysis files exist
        analysis_md = OUTPUT_DIR / f"{patient_name}_analysis.md"
        summary_md = OUTPUT_DIR / f"{patient_name}_summary.md"
        
        # If any of these files don't exist, consider it new
        if not (analysis_md.exists() and summary_md.exists()):
            new_clean_transcript_files.append(clean_transcript_file)
    
    if not new_clean_transcript_files:
        print("✅ No new clean transcripts to analyze")
        return []
    
    print(f"🤖 Found {len(new_clean_transcript_files)} new clean transcript(s) to analyze:")
    for clean_transcript_file in new_clean_transcript_files:
        print(f"  • {clean_transcript_file.name}")
    
    processed_count = 0
    for clean_transcript_file in new_clean_transcript_files:
        if process_transcript_with_llm(clean_transcript_file):
            processed_count += 1
    
    if processed_count > 0:
        print(f"\n🎉 Successfully processed {processed_count} transcript(s)")
        print(f"📁 Check {OUTPUT_DIR} for summary and analysis files (.md)")
    else:
        print("\n❌ No transcripts were successfully processed")
    
    return new_clean_transcript_files

def calculate_token_usage():
    """Calculate total token usage from all saved JSON response files"""
    if not JSON_DEBUG_DIR.exists():
        print("❌ No JSON debug directory found. Run analysis first to generate token data.")
        return
    
    json_files = list(JSON_DEBUG_DIR.glob("*.json"))
    if not json_files:
        print("❌ No JSON response files found. Run analysis first to generate token data.")
        return
    
    total_input_tokens = 0
    total_output_tokens = 0
    total_cost = 0
    file_count = 0
    
    print("📊 Token Usage Summary")
    print("=" * 50)
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract token usage
            usage = data.get('usage', {})
            input_tokens = usage.get('input_tokens', 0)
            output_tokens = usage.get('output_tokens', 0)
            
            # Calculate cost (approximate - check current rates)
            # Claude 3.5 Sonnet rates (as of 2024): $3/1M input, $15/1M output
            input_cost = (input_tokens / 1_000_000) * 3.00
            output_cost = (output_tokens / 1_000_000) * 15.00
            file_cost = input_cost + output_cost
            
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            total_cost += file_cost
            file_count += 1
            
            # Extract patient name from filename
            patient_name = json_file.stem.replace('_analysis_response', '').replace('_summary_response', '')
            
            print(f"📄 {patient_name}: {input_tokens:,} input + {output_tokens:,} output = ${file_cost:.4f}")
            
        except Exception as e:
            print(f"❌ Error reading {json_file.name}: {e}")
    
    print("=" * 50)
    print(f"📈 Total Files Processed: {file_count}")
    print(f"🔢 Total Input Tokens: {total_input_tokens:,}")
    print(f"🔢 Total Output Tokens: {total_output_tokens:,}")
    print(f"💰 Estimated Total Cost: ${total_cost:.4f}")
    print(f"📊 Average Cost per File: ${total_cost/file_count:.4f}" if file_count > 0 else "📊 No files processed")

if __name__ == "__main__":
    # Simple command line interface for testing
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == "analyze":
            analyze_clean_transcripts()
        elif sys.argv[1] == "tokens":
            calculate_token_usage()
        else:
            print("Usage:")
            print("  python scripts/llm_analysis_integration.py analyze  - Process transcripts")
            print("  python scripts/llm_analysis_integration.py tokens   - Show token usage")
    else:
        print("Usage:")
        print("  python scripts/llm_analysis_integration.py analyze  - Process transcripts")
        print("  python scripts/llm_analysis_integration.py tokens   - Show token usage") 