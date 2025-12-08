# Dental Consultation Audio Analysis Pipeline

This project provides a complete automated pipeline for processing dental consultation audio recordings, from transcription to AI-powered analysis with multiple output formats.

## 🎯 Overview

The pipeline consists of five main stages:
1. **Audio Transcription** - Convert audio files to text using OpenAI Whisper
2. **Transcript Cleaning** - Apply dental corrections using the corrections library
3. **LLM Analysis** - Generate summaries and detailed skill analysis using Claude AI
4. **HTML Conversion** - Convert markdown files to web-ready HTML
5. **PDF Conversion** - Convert markdown files to printable PDF documents

## 🏗️ Technical Architecture

### Tech Stack

**Core Technologies:**
- **Python 3.8+** - Primary programming language
- **OpenAI Whisper** - Speech-to-text transcription
- **Anthropic Claude AI** - LLM analysis and summarization
- **PyTorch** - Deep learning framework (required by Whisper)
- **NumPy** - Numerical computing
- **FFmpeg** - Audio processing (handled by Whisper)

**Document Processing:**
- **Markdown2** - Markdown to HTML conversion
- **ReportLab** - PDF generation
- **Python-docx** - Word document processing
- **Jinja2** - Template engine

**Development & Testing:**
- **Pytest** - Testing framework
- **Coverage.py** - Code coverage
- **Black** - Code formatting
- **Flake8** - Linting

**Infrastructure:**
- **Requests** - HTTP client for API calls
- **Python-dotenv** - Environment variable management
- **Pathlib** - Modern path handling
- **Logging** - Structured logging

### Pipeline Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Raw Audio     │    │   Transcription  │    │   Raw Transcript│
│   Files (.m4a)  │───▶│   (Whisper AI)   │───▶│   (.txt, .srt)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Clean         │◀───│   Dental         │◀───│   Raw Transcript│
│   Transcript    │    │   Corrections    │    │   Files         │
│   (.txt)        │    │   (JSON Rules)   │    └─────────────────┘
└─────────────────┘    └──────────────────┘
         │
         ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   LLM Analysis  │───▶│   Claude AI      │───▶│   Markdown      │
│   (Claude API)  │    │   (Anthropic)    │    │   (.md)         │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                                              │
         ▼                                              ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   HTML Output   │◀───│   Markdown       │───▶│   PDF Output    │
│   (.html)       │    │   Conversion     │    │   (.pdf)        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Data Flow & Processing

#### 1. Audio Transcription Stage
**Technology**: OpenAI Whisper (Large Language Model)
- **Input**: Audio files (`.m4a`, `.wav`, `.mp3`, `.ogg`, `.flac`, `.aac`)
- **Process**: 
  - Loads Whisper "base" model (~244MB)
  - Processes audio in chunks for memory efficiency
  - Generates timestamped segments with confidence scores
- **Output**: Multiple formats for flexibility
  - `.txt` - Plain text with line breaks
  - `.tsv` - Tab-separated with timestamps
  - `.srt` - Subtitle format for video
  - `.vtt` - Web video text tracks

#### 2. Transcript Cleaning Stage
**Technology**: Custom Python processing with JSON rules
- **Input**: Raw transcript files from Whisper
- **Process**:
  - Loads dental corrections from `dental_corrections.json`
  - Applies pattern matching and replacement rules
  - Handles dental terminology, abbreviations, and common errors
  - Preserves original timestamps and structure
- **Output**: Cleaned transcript files (`.txt`)

#### 3. LLM Analysis Stage
**Technology**: Anthropic Claude AI (Claude 3.5 Sonnet)
- **Input**: Cleaned transcript files
- **Process**:
  - Sends transcript + structured prompts to Claude API
  - Uses two specialized prompts:
    - **Summary Prompt**: Creates factual consultation summaries
    - **Analysis Prompt**: Generates skill assessment and recommendations
  - Handles API rate limiting and error recovery
  - Saves full JSON responses for debugging
- **Output**: 
  - `*_summary.md` - Structured consultation summaries
  - `*_analysis.md` - Detailed skill analysis reports

#### 4. Document Conversion Stage
**Technology**: Markdown2 + ReportLab + Custom CSS
- **Input**: Markdown files from LLM analysis
- **Process**:
  - **HTML Conversion**: Markdown2 with custom CSS styling
  - **PDF Conversion**: ReportLab with professional formatting
  - Responsive design for multiple screen sizes
  - Professional styling with gradients and typography
- **Output**:
  - `.html` - Web-ready files with responsive design
  - `.pdf` - Print-optimized documents

### Key Technical Features

#### Smart File Processing
- **Incremental Processing**: Only processes new files at each stage
- **File Validation**: Checks for corrupted or incomplete transcripts
- **Error Recovery**: Continues processing even if individual files fail
- **Status Tracking**: Comprehensive logging and status reporting

#### Memory Management
- **Streaming Audio**: Processes large audio files without loading entirely into memory
- **Batch Processing**: Handles multiple files efficiently
- **Model Loading**: Whisper model loaded once and reused
- **Garbage Collection**: Automatic cleanup of temporary data

#### API Integration
- **Rate Limiting**: Respects Claude API rate limits
- **Error Handling**: Retries failed API calls with exponential backoff
- **Response Caching**: Saves full API responses for debugging
- **Token Tracking**: Monitors API usage and costs

#### Quality Assurance
- **Transcript Validation**: Checks for minimum content and structure
- **Correction Verification**: Validates dental corrections are applied
- **Output Validation**: Ensures generated files are complete
- **Logging**: Comprehensive audit trail for debugging

### Performance Characteristics

#### Processing Speed
- **Audio Transcription**: ~1-2 minutes per minute of audio (CPU dependent)
- **Transcript Cleaning**: Near-instantaneous (text processing)
- **LLM Analysis**: ~10-30 seconds per transcript (API dependent)
- **Document Conversion**: ~1-5 seconds per file

#### Resource Requirements
- **CPU**: Multi-core recommended for transcription
- **RAM**: 4GB minimum, 8GB+ recommended
- **Storage**: ~10MB per minute of audio processed
- **Network**: Stable internet for Claude API calls

#### Scalability
- **Parallel Processing**: Can process multiple files simultaneously
- **Batch Operations**: Efficient handling of large file sets
- **Modular Design**: Each stage can be run independently
- **Extensible**: Easy to add new processing stages

### Security & Privacy

#### Data Handling
- **Local Processing**: Audio files processed locally, not uploaded
- **API Security**: Secure API key management via environment variables
- **No Data Retention**: Generated files stored locally only
- **Audit Trail**: Complete logging for compliance

#### API Security
- **Environment Variables**: API keys stored securely
- **Request Signing**: Proper authentication with Claude API
- **Error Handling**: No sensitive data in error messages
- **Rate Limiting**: Prevents API abuse

## 📁 Project Structure

```
WhisperProject/
├── raw_audio/                    # Original audio files (input)
├── transcripts/                  # Raw transcript files (TXT, TSV, SRT, VTT)
├── transcripts_clean/            # Cleaned transcript files (after corrections)
├── manual_claude_summary_analysis/ # Manual LLM analysis (web-based Claude)
├── api_claude_summary_analysis/  # API-generated LLM analysis (Claude API)
├── html_output/                  # Web-ready HTML files
├── pdf_output/                   # Printable PDF files
├── api_debug_responses/          # JSON debug responses from Claude API
├── logs/                         # Pipeline execution logs
├── whisper_env/                  # Python virtual environment
├── dental_consultation_pipeline/ # Main package (active pipeline)
│   ├── __init__.py              # Package initialization
│   ├── pipeline.py              # Main pipeline orchestrator ⭐
│   ├── transcription.py         # Audio transcription module
│   ├── cleaning.py              # Transcript cleaning module
│   ├── analysis.py              # LLM analysis module
│   └── conversion.py            # HTML/PDF conversion module
├── scripts/                      # Legacy scripts (deprecated)
│   ├── run_pipeline.py          # ⚠️ DEPRECATED - Use package instead
│   ├── audio_transcriber.py     # ⚠️ DEPRECATED - Use package instead
│   ├── transcript_cleaner.py    # ⚠️ DEPRECATED - Use package instead
│   ├── llm_analysis_integration.py # ⚠️ DEPRECATED - Use package instead
│   ├── convert_markdown_to_html.py # ⚠️ DEPRECATED - Use package instead
│   └── convert_markdown_to_pdf.py  # ⚠️ DEPRECATED - Use package instead
├── templates/                    # Document templates
├── tests/                        # Test files
├── dental_corrections.json       # Dental corrections library
└── README.md                     # This file
```

### Analysis Directory Notes

**`manual_claude_summary_analysis/`** - Contains analysis files generated by manually submitting the `prompt.md` template and clean transcript files to the web-based Claude model. These files provide more comprehensive, detailed analysis with expert-level insights suitable for professional development and practice improvement.

**`api_claude_summary_analysis/`** - Contains analysis files generated automatically using the Claude API through the pipeline. These files provide good general summaries and standard analysis suitable for quick overviews and basic documentation.

## 🚀 Quick Start

### Option 1: Install as Python Package (Recommended)

**Install the package:**
```bash
# Install from local directory
pip install -e .

# Or install with development dependencies
pip install -e .[dev]
```

**Use the command-line tools:**
```bash
# Run complete pipeline (Audio → Transcription → Cleaning → LLM Analysis → HTML/PDF)
python -m dental_consultation_pipeline.pipeline run

# Check pipeline status
python -m dental_consultation_pipeline.pipeline status

# Validate transcript files
python -m dental_consultation_pipeline.pipeline validate

# Clean up invalid transcript files
python -m dental_consultation_pipeline.pipeline cleanup

# Individual commands
dental-transcribe --process filename.m4a
dental-clean
dental-analyze
dental-convert
```

**Use as Python module:**
```python
from dental_consultation_pipeline import run_full_pipeline, show_pipeline_status

# Run the complete pipeline
run_full_pipeline()

# Show status
show_pipeline_status()
```

### Option 2: Development Setup

**1. Activate Virtual Environment**

**PowerShell:**
```powershell
whisper_env\Scripts\Activate.ps1
```

**Command Prompt:**
```cmd
whisper_env\Scripts\activate.bat
```

**Git Bash:**
```bash
source whisper_env/Scripts/activate
```

**2. Complete Pipeline (Recommended)**

**Add audio files to `raw_audio/` directory, then run:**

```bash
# Run complete pipeline (Audio → Transcription → Cleaning → LLM Analysis → HTML/PDF)
python -m dental_consultation_pipeline.pipeline run

# Check pipeline status
python -m dental_consultation_pipeline.pipeline status
```

**Individual script usage (Legacy - Deprecated):**
```bash
# ⚠️ DEPRECATED: These scripts are no longer maintained
# Audio transcription only
python scripts/audio_transcriber.py --process filename.m4a

# Transcript cleaning only
python scripts/transcript_cleaner.py

# LLM analysis only
python scripts/llm_analysis_integration.py analyze

# HTML conversion only
python scripts/convert_markdown_to_html.py

# PDF conversion only
python scripts/convert_markdown_to_pdf.py
```

### 3. Pipeline Status

**Check what files need processing:**
```bash
python -m dental_consultation_pipeline.pipeline status
```

This will show:
- Raw audio files
- Raw transcript files
- Clean transcript files
- LLM analysis & summary files
- HTML output files
- PDF output files
- Files ready for processing at each step

## 📋 Complete Pipeline Features

### `dental_consultation_pipeline.pipeline` ⭐ **Main Pipeline Orchestrator**

**Purpose**: Orchestrates the complete workflow from audio files to multiple output formats

**Data Flow:**
1. **Audio Detection** → Scans `raw_audio/` for new files
2. **Transcription** → Uses Whisper to create 4 format outputs in `transcripts/`
3. **Cleaning** → Applies dental corrections, saves to `transcripts_clean/`
4. **LLM Analysis** → Generates summaries + skill analysis using Claude AI
5. **HTML Conversion** → Converts markdown to web-ready HTML files
6. **PDF Conversion** → Converts markdown to printable PDF files

**Features:**
- **Complete Automation** - Detects and processes new files automatically
- **Smart Processing** - Only processes files that need processing
- **Multiple Output Formats** - Markdown, HTML, and PDF outputs
- **Error Handling** - Robust error handling and continues on failures
- **Status Reporting** - Comprehensive status checking and file counts
- **Modular Design** - Each step can be run independently
- **Validation Tools** - Built-in transcript validation and cleanup

**Usage:**
```bash
# Run complete pipeline
python -m dental_consultation_pipeline.pipeline run

# Check status
python -m dental_consultation_pipeline.pipeline status

# Validate transcript files
python -m dental_consultation_pipeline.pipeline validate

# Clean up invalid transcript files
python -m dental_consultation_pipeline.pipeline cleanup
```

## 📊 Output Formats

### 1. LLM Analysis & Summary

#### Manual Analysis (`manual_claude_summary_analysis/` folder)
**Purpose**: Expert-level analysis generated by manually submitting prompts to web-based Claude

**Characteristics:**
- **Comprehensive detail**: 128+ lines with extensive subsections
- **Expert insights**: Professional-level critique and recommendations
- **Clinical depth**: Detailed treatment planning and risk assessment
- **Practice management**: Focus on workflow, time management, and liability
- **Actionable feedback**: Specific, detailed improvement recommendations

**File Types:**
- `*_summary.md` - Detailed consultation summaries with specific costs, timelines, and action items
- `*_analysis.md` - Comprehensive skill analysis with expert-level critique

#### API Analysis (`api_claude_summary_analysis/` folder)
**Purpose**: Automated analysis generated using Claude API through the pipeline

**Characteristics:**
- **Concise format**: 50-60 lines with focused overview
- **Standard evaluation**: Consistent but less detailed assessment
- **General recommendations**: Common improvement suggestions
- **Quick reference**: Suitable for basic documentation and overviews

**File Types:**
- `*_summary.md` - Clean consultation summaries with key points
- `*_analysis.md` - Standard skill analysis reports

**Summary Sections:**
- **Consultation Overview** - Date, type, main reason for visit
- **Patient Information** - Dental concerns, medical history, medications
- **Treatment Discussion** - Options presented, recommendations, treatment plan
- **Financial Information** - Costs, insurance, payment arrangements
- **Follow-up** - Next steps, appointments, patient instructions
- **Key Points** - Important decisions, critical information, action items

**Analysis Sections:**
- **Overall Assessment** - Quality assessment and professionalism
- **Strengths** - 3-5 specific strengths identified
- **Areas for Improvement** - 3-5 actionable improvements
- **Communication Skills** - Verbal communication analysis
- **Clinical Presentation** - Clinical knowledge and presentation
- **Financial Communication Analysis** - Cost discussion effectiveness
- **Medical Information Handling** - Health issue and medication review
- **Recommendations** - 3-5 specific, actionable recommendations
- **Key Takeaways** - Summary and insights

### 2. HTML Output (`html_output/` folder)
**Purpose**: Web-ready HTML files with professional styling

**Features:**
- Responsive design for mobile and desktop
- Professional styling with gradients and shadows
- Clean typography and spacing
- Generated timestamps

### 3. PDF Output (`pdf_output/` folder)
**Purpose**: Printable PDF documents for professional use

**Features:**
- Professional formatting with custom styles
- Proper page breaks and spacing
- Clean typography optimized for printing

## 🔧 Configuration

### Environment Variables

For LLM integration, set these environment variables:

```bash
# Anthropic Claude API (Required for analysis)
export ANTHROPIC_API_KEY="your-anthropic-api-key"
```

**Note**: The pipeline currently uses Anthropic Claude AI for analysis. Make sure to set your API key in the `.env` file or as an environment variable.

### Directory Structure

The pipeline automatically creates these directories:
- `raw_audio/` - Place new audio files here
- `transcripts/` - Raw Whisper outputs (auto-generated)
- `transcripts_clean/` - Cleaned transcripts (auto-generated)
- `manual_claude_summary_analysis/` - Manual LLM analysis files (manually generated)
- `api_claude_summary_analysis/` - API-generated LLM analysis files (auto-generated)
- `html_output/` - Web-ready HTML files (auto-generated)
- `pdf_output/` - Printable PDF files (auto-generated)
- `api_debug_responses/` - JSON debug responses (auto-generated)
- `logs/` - Pipeline execution logs (auto-generated)

## 🔄 Complete Workflow

1. **Audio Recording** → Place in `raw_audio/` directory
2. **Automatic Detection** → Pipeline detects new audio files
3. **Transcription** → Whisper converts audio to text (4 formats)
4. **Cleaning** → Dental corrections applied to transcripts
5. **LLM Analysis** → Claude AI generates summaries and skill analysis (API)
6. **HTML Conversion** → Markdown files converted to web-ready HTML
7. **PDF Conversion** → Markdown files converted to printable PDF
8. **Multiple Outputs** → Files available in Markdown, HTML, and PDF formats

### Manual Analysis Workflow (Optional)

For expert-level analysis:
1. **Clean Transcript** → Use files from `transcripts_clean/`
2. **Manual Submission** → Submit `prompt.md` template + clean transcript to web-based Claude
3. **Expert Analysis** → Generate comprehensive analysis with detailed insights
4. **Save Results** → Store in `manual_claude_summary_analysis/` directory

## 🧪 Testing

Run the pipeline status to verify your setup:

```bash
python -m dental_consultation_pipeline.pipeline status
```

This will show:
- Directory structure and file counts
- Files ready for processing at each step
- Current pipeline status

## 📝 Individual Scripts

### Core Processing Scripts

#### `dental_consultation_pipeline.transcription`
- **Purpose**: Transcribe individual audio files using Whisper
- **Input**: Audio files in `raw_audio/` directory
- **Output**: TXT, TSV, SRT, VTT files in `transcripts/` folder
- **Usage**: `dental-transcribe --process filename.m4a`

#### `dental_consultation_pipeline.cleaning`
- **Purpose**: Clean transcript files using dental corrections
- **Input**: Raw transcript files in `transcripts/`
- **Output**: Cleaned files in `transcripts_clean/`
- **Usage**: `dental-clean`

#### `dental_consultation_pipeline.analysis`
- **Purpose**: Generate summaries and analysis using Claude AI
- **Input**: Clean transcript files in `transcripts_clean/`
- **Output**: Markdown files in `api_claude_summary_analysis/`
- **Usage**: `dental-analyze`

#### `dental_consultation_pipeline.conversion`
- **Purpose**: Convert markdown files to HTML and PDF
- **Input**: Markdown files in `api_claude_summary_analysis/`
- **Output**: HTML files in `html_output/` and PDF files in `pdf_output/`
- **Usage**: `dental-convert`

### Legacy Scripts (Deprecated)

⚠️ **Note**: The following scripts in the `scripts/` directory are deprecated and no longer maintained. Use the package modules instead.

- `scripts/audio_transcriber.py` - Use `dental-transcribe` command
- `scripts/transcript_cleaner.py` - Use `dental-clean` command  
- `scripts/llm_analysis_integration.py` - Use `dental-analyze` command
- `scripts/convert_markdown_to_html.py` - Use `dental-convert` command
- `scripts/convert_markdown_to_pdf.py` - Use `dental-convert` command
- `scripts/run_pipeline.py` - Use `python -m dental_consultation_pipeline.pipeline run`

## 🚀 Next Steps

1. **Set up API key** for Anthropic Claude AI
2. **Add audio files** to the `raw_audio/` directory
3. **Run the complete pipeline**: `python -m dental_consultation_pipeline.pipeline run`
4. **Check results** in `api_claude_summary_analysis/`, `html_output/`, and `pdf_output/` directories
5. **Use status command** to monitor pipeline: `python -m dental_consultation_pipeline.pipeline status`

## 📦 Building and Distribution

### Build the Package
```bash
# Build source distribution
python -m build --sdist

# Build wheel distribution
python -m build --wheel

# Build both
python -m build
```

### Install from Built Package
```bash
# Install from wheel
pip install dist/dental_consultation_pipeline-*.whl

# Install from source distribution
pip install dist/dental_consultation_pipeline-*.tar.gz
```

### Publish to PyPI (Optional)
```bash
# Upload to PyPI (requires account)
python -m twine upload dist/*

# Upload to Test PyPI first
python -m twine upload --repository testpypi dist/*
```

## 📞 Support

For issues or questions:
1. Check the logs in the `logs/` directory
2. Check pipeline status: `python -m dental_consultation_pipeline.pipeline status`
3. Run individual commands to isolate issues 