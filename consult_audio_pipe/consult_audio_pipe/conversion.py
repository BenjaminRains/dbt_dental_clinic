"""
Document Conversion Module

Handles conversion of markdown files to HTML and PDF formats.
"""

import os
import markdown2
from pathlib import Path
from datetime import datetime
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor

# === Configuration ===
# Get the project root directory (parent of dental_consultation_pipeline directory)
PROJECT_ROOT = Path(__file__).parent.parent
INPUT_DIR = PROJECT_ROOT / "api_claude_summary_analysis"
HTML_OUTPUT_DIR = PROJECT_ROOT / "html_output"
PDF_OUTPUT_DIR = PROJECT_ROOT / "pdf_output"

def convert_markdown_to_html(md_file_path, output_dir):
    """Convert a single markdown file to HTML"""
    try:
        # Read markdown content
        with open(md_file_path, 'r', encoding='utf-8') as f:
            md_content = f.read()
        
        # Convert markdown to HTML
        html_content = markdown2.markdown(md_content, extras=['tables', 'fenced-code-blocks', 'code-friendly'])
        
        # Determine LLM type based on the source directory
        llm_suffix = ""
        if "chatgpt" in str(md_file_path.parent).lower():
            llm_suffix = "_chatgpt"
        elif "claude" in str(md_file_path.parent).lower():
            llm_suffix = "_claude"
        
        # Create full HTML document with styling
        full_html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{md_file_path.stem}{llm_suffix}</title>
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 800px;
                    margin: 0 auto;
                    padding: 20px;
                    background-color: #f8f9fa;
                }}
                
                .container {{
                    background: white;
                    padding: 40px;
                    border-radius: 8px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }}
                
                h1 {{
                    color: #1e3a8a;
                    background: linear-gradient(135deg, #dbeafe 0%, #bfdbfe 100%);
                    border-bottom: 3px solid #3b82f6;
                    padding: 15px 20px;
                    margin-bottom: 20px;
                    font-size: 2.2em;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(59, 130, 246, 0.1);
                }}
                
                h2 {{
                    color: #1e40af;
                    background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
                    border-bottom: 2px solid #60a5fa;
                    padding: 10px 15px;
                    margin-top: 30px;
                    margin-bottom: 15px;
                    font-size: 1.6em;
                    border-radius: 6px;
                    box-shadow: 0 1px 3px rgba(59, 130, 246, 0.1);
                }}
                
                h3 {{
                    color: #2563eb;
                    background: linear-gradient(135deg, #f8fafc 0%, #eff6ff 100%);
                    padding: 8px 12px;
                    margin-top: 25px;
                    margin-bottom: 10px;
                    font-size: 1.3em;
                    border-radius: 4px;
                    border-left: 4px solid #60a5fa;
                }}
                
                p {{
                    margin-bottom: 15px;
                }}
                
                ul, ol {{
                    margin-left: 20px;
                    margin-bottom: 15px;
                }}
                
                li {{
                    margin-bottom: 5px;
                }}
                
                strong {{
                    color: #2c3e50;
                    font-weight: 600;
                }}
                
                em {{
                    color: #7f8c8d;
                }}
                
                code {{
                    background-color: #f4f4f4;
                    padding: 2px 4px;
                    border-radius: 3px;
                    font-family: 'Courier New', monospace;
                    font-size: 0.9em;
                }}
                
                pre {{
                    background-color: #f4f4f4;
                    padding: 15px;
                    border-radius: 5px;
                    overflow-x: auto;
                    margin: 15px 0;
                }}
                
                pre code {{
                    background: none;
                    padding: 0;
                }}
                
                blockquote {{
                    border-left: 4px solid #3498db;
                    padding-left: 15px;
                    margin: 15px 0;
                    color: #555;
                    font-style: italic;
                }}
                
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin: 15px 0;
                }}
                
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px 12px;
                    text-align: left;
                }}
                
                th {{
                    background-color: #f8f9fa;
                    font-weight: 600;
                }}
                
                .header {{
                    text-align: center;
                    margin-bottom: 30px;
                    padding-bottom: 20px;
                    border-bottom: 2px solid #ecf0f1;
                }}
                
                .footer {{
                    margin-top: 40px;
                    padding-top: 20px;
                    border-top: 1px solid #ecf0f1;
                    text-align: center;
                    color: #7f8c8d;
                    font-size: 0.9em;
                }}
                
                @media (max-width: 768px) {{
                    body {{
                        padding: 10px;
                    }}
                    
                    .container {{
                        padding: 20px;
                    }}
                    
                    h1 {{
                        font-size: 1.8em;
                    }}
                    
                    h2 {{
                        font-size: 1.4em;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>{md_file_path.stem.replace('_', ' ').title()}</h1>
                    <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                {html_content}
                
                <div class="footer">
                    <p>Generated by Claude AI Analysis System</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Create output directory if it doesn't exist
        output_dir.mkdir(exist_ok=True)
        
        # Generate HTML filename with LLM suffix
        llm_suffix = ""
        if "chatgpt" in str(md_file_path.parent).lower():
            llm_suffix = "_chatgpt"
        elif "claude" in str(md_file_path.parent).lower():
            llm_suffix = "_claude"
        
        html_filename = md_file_path.stem + llm_suffix + ".html"
        html_path = output_dir / html_filename
        
        # Check if file already exists
        file_exists = html_path.exists()
        
        # Write HTML file (overwrites if exists)
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(full_html)
        
        if file_exists:
            print(f"✅ Overwrote: {md_file_path.name} → {html_filename}")
        else:
            print(f"✅ Converted: {md_file_path.name} → {html_filename}")
        return True
        
    except Exception as e:
        print(f"❌ Error converting {md_file_path.name}: {e}")
        return False

def convert_markdown_to_pdf_simple(md_file_path, output_dir):
    """Convert a single markdown file to PDF using reportlab"""
    try:
        # Read markdown content
        with open(md_file_path, 'r', encoding='utf-8') as f:
            md_content = f.read()
        
        # Convert markdown to HTML first
        html_content = markdown2.markdown(md_content, extras=['tables', 'fenced-code-blocks', 'code-friendly'])
        
        # Create output directory if it doesn't exist
        output_dir.mkdir(exist_ok=True)
        
        # Generate PDF filename with LLM suffix
        llm_suffix = ""
        if "chatgpt" in str(md_file_path.parent).lower():
            llm_suffix = "_chatgpt"
        elif "claude" in str(md_file_path.parent).lower():
            llm_suffix = "_claude"
        
        pdf_filename = md_file_path.stem + llm_suffix + ".pdf"
        pdf_path = output_dir / pdf_filename
        
        # Create PDF document
        doc = SimpleDocTemplate(str(pdf_path), pagesize=A4)
        styles = getSampleStyleSheet()
        
        # Create custom styles
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=18,
            spaceAfter=20,
            textColor=HexColor('#2c3e50'),
            alignment=1  # Center alignment
        )
        
        heading1_style = ParagraphStyle(
            'CustomHeading1',
            parent=styles['Heading1'],
            fontSize=16,
            spaceAfter=12,
            spaceBefore=20,
            textColor=HexColor('#34495e')
        )
        
        heading2_style = ParagraphStyle(
            'CustomHeading2',
            parent=styles['Heading2'],
            fontSize=14,
            spaceAfter=10,
            spaceBefore=15,
            textColor=HexColor('#7f8c8d')
        )
        
        normal_style = ParagraphStyle(
            'CustomNormal',
            parent=styles['Normal'],
            fontSize=11,
            spaceAfter=8,
            leading=14
        )
        
        # Parse HTML and convert to reportlab elements
        story = []
        
        # Add title
        title = Paragraph(md_file_path.stem.replace('_', ' ').title(), title_style)
        story.append(title)
        story.append(Spacer(1, 20))
        
        # Split HTML content into lines and process
        lines = html_content.split('\n')
        current_text = ""
        
        for line in lines:
            line = line.strip()
            if not line:
                if current_text:
                    # Process accumulated text
                    p = Paragraph(current_text, normal_style)
                    story.append(p)
                    current_text = ""
                continue
            
            if line.startswith('<h1>'):
                if current_text:
                    p = Paragraph(current_text, normal_style)
                    story.append(p)
                    current_text = ""
                # Extract text between <h1> tags
                text = line.replace('<h1>', '').replace('</h1>', '').strip()
                p = Paragraph(text, heading1_style)
                story.append(p)
            elif line.startswith('<h2>'):
                if current_text:
                    p = Paragraph(current_text, normal_style)
                    story.append(p)
                    current_text = ""
                # Extract text between <h2> tags
                text = line.replace('<h2>', '').replace('</h2>', '').strip()
                p = Paragraph(text, heading2_style)
                story.append(p)
            elif line.startswith('<h3>'):
                if current_text:
                    p = Paragraph(current_text, normal_style)
                    story.append(p)
                    current_text = ""
                # Extract text between <h3> tags
                text = line.replace('<h3>', '').replace('</h3>', '').strip()
                p = Paragraph(text, heading2_style)
                story.append(p)
            elif line.startswith('<ul>') or line.startswith('</ul>') or line.startswith('<ol>') or line.startswith('</ol>'):
                continue
            elif line.startswith('<li>'):
                if current_text:
                    p = Paragraph(current_text, normal_style)
                    story.append(p)
                    current_text = ""
                # Extract text between <li> tags
                text = line.replace('<li>', '• ').replace('</li>', '').strip()
                p = Paragraph(text, normal_style)
                story.append(p)
            elif line.startswith('<p>'):
                # Extract text between <p> tags
                text = line.replace('<p>', '').replace('</p>', '').strip()
                if text:
                    current_text += text + " "
            elif line.startswith('</p>'):
                if current_text:
                    p = Paragraph(current_text, normal_style)
                    story.append(p)
                    current_text = ""
            else:
                # Regular text line
                if line:
                    current_text += line + " "
        
        # Add any remaining text
        if current_text:
            p = Paragraph(current_text, normal_style)
            story.append(p)
        
        # Build PDF
        doc.build(story)
        
        print(f"✅ Converted: {md_file_path.name} → {pdf_filename}")
        return True
        
    except Exception as e:
        print(f"❌ Error converting {md_file_path.name}: {e}")
        return False

def get_markdown_files(input_dir=None):
    """Get list of markdown files in the input directory that correspond to audio files in raw_audio"""
    if input_dir is None:
        input_dir = INPUT_DIR
    
    if not input_dir.exists():
        print(f"❌ Input directory not found: {input_dir}")
        return []
    
    # Get list of audio files in raw_audio directory
    raw_audio_dir = PROJECT_ROOT / "raw_audio"
    if not raw_audio_dir.exists():
        print(f"❌ Raw audio directory not found: {raw_audio_dir}")
        return []
    
    # Get audio file names (without extensions)
    audio_files = []
    for audio_file in raw_audio_dir.iterdir():
        if audio_file.is_file() and audio_file.suffix.lower() in {'.m4a', '.wav', '.mp3', '.ogg', '.flac', '.aac'}:
            audio_files.append(audio_file.stem)
    
    if not audio_files:
        print("✅ No audio files found in raw_audio directory")
        return []
    
    # Get all markdown files
    all_md_files = list(input_dir.glob("*.md"))
    if not all_md_files:
        print(f"❌ No markdown files found in {input_dir}")
        return []
    
    # Filter markdown files to only include those corresponding to audio files
    filtered_md_files = []
    for md_file in all_md_files:
        # Extract patient name from markdown filename (remove _analysis.md or _summary.md)
        base_name = md_file.stem
        if base_name.endswith('_analysis'):
            base_name = base_name[:-9]  # Remove '_analysis'
        elif base_name.endswith('_summary'):
            base_name = base_name[:-8]  # Remove '_summary'
        
        # Check if this corresponds to an audio file
        # First try exact match
        if base_name in audio_files:
            filtered_md_files.append(md_file)
        else:
            # Try matching by removing "consult" suffix from audio file names
            for audio_file in audio_files:
                # Remove " consult" suffix from audio file name
                audio_base = audio_file.replace(' consult', '')
                if base_name == audio_base:
                    filtered_md_files.append(md_file)
                    break
    
    if not filtered_md_files:
        print("✅ No markdown files found that correspond to audio files in raw_audio directory")
        return []
    
    return filtered_md_files

def convert_to_html(input_dir=None):
    """Convert all markdown files to HTML format"""
    md_files = get_markdown_files(input_dir)
    if not md_files:
        return []
    
    print(f"🔄 Found {len(md_files)} markdown files to convert to HTML...")
    if input_dir:
        print(f"📁 Input: {input_dir}")
    else:
        print(f"📁 Input: {INPUT_DIR}")
    print(f"📁 Output: {HTML_OUTPUT_DIR}")
    print("=" * 50)
    
    success_count = 0
    converted_files = []
    for md_file in md_files:
        if convert_markdown_to_html(md_file, HTML_OUTPUT_DIR):
            success_count += 1
            converted_files.append(md_file)
    
    print("=" * 50)
    print(f"🎉 Successfully converted {success_count}/{len(md_files)} files to HTML")
    print(f"📁 HTML files saved in: {HTML_OUTPUT_DIR}")
    
    return converted_files

def convert_to_pdf(input_dir=None):
    """Convert all markdown files to PDF format"""
    md_files = get_markdown_files(input_dir)
    if not md_files:
        return []
    
    print(f"🔄 Found {len(md_files)} markdown files to convert to PDF...")
    if input_dir:
        print(f"📁 Input: {input_dir}")
    else:
        print(f"📁 Input: {INPUT_DIR}")
    print(f"📁 Output: {PDF_OUTPUT_DIR}")
    print("=" * 50)
    
    success_count = 0
    converted_files = []
    for md_file in md_files:
        if convert_markdown_to_pdf_simple(md_file, PDF_OUTPUT_DIR):
            success_count += 1
            converted_files.append(md_file)
    
    print("=" * 50)
    print(f"🎉 Successfully converted {success_count}/{len(md_files)} files to PDF")
    print(f"📁 PDF files saved in: {PDF_OUTPUT_DIR}")
    
    return converted_files

def convert_all_formats():
    """Convert all markdown files to both HTML and PDF formats"""
    print("🔄 Document Conversion Process")
    print("=" * 40)
    
    # Define both input directories
    claude_dir = PROJECT_ROOT / "api_claude_summary_analysis"
    chatgpt_dir = PROJECT_ROOT / "api_chatgpt_summary_analysis"
    
    total_converted = 0
    
    # Process Claude files
    if claude_dir.exists():
        print("🤖 Processing Claude files...")
        claude_files = get_markdown_files(claude_dir)
        if claude_files:
            print(f"📄 Found {len(claude_files)} Claude markdown files")
            print("🌐 Converting Claude files to HTML...")
            convert_to_html(claude_dir)
            print("📄 Converting Claude files to PDF...")
            convert_to_pdf(claude_dir)
            total_converted += len(claude_files)
        else:
            print("✅ No Claude files to convert")
        print()
    
    # Process ChatGPT files
    if chatgpt_dir.exists():
        print("🤖 Processing ChatGPT files...")
        chatgpt_files = get_markdown_files(chatgpt_dir)
        if chatgpt_files:
            print(f"📄 Found {len(chatgpt_files)} ChatGPT markdown files")
            print("🌐 Converting ChatGPT files to HTML...")
            convert_to_html(chatgpt_dir)
            print("📄 Converting ChatGPT files to PDF...")
            convert_to_pdf(chatgpt_dir)
            total_converted += len(chatgpt_files)
        else:
            print("✅ No ChatGPT files to convert")
        print()
    
    if total_converted == 0:
        print("❌ No markdown files found to convert")
        return
    
    print("🎉 Conversion process completed!")
    print(f"📁 HTML files: {HTML_OUTPUT_DIR}")
    print(f"📁 PDF files: {PDF_OUTPUT_DIR}")
    print(f"📊 Total files processed: {total_converted}")

def main():
    """Main function for command line usage"""
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "html":
            convert_to_html()
        elif sys.argv[1] == "pdf":
            convert_to_pdf()
        elif sys.argv[1] == "all":
            convert_all_formats()
        elif sys.argv[1] == "files":
            md_files = get_markdown_files()
            if md_files:
                print(f"📄 Available Markdown Files ({len(md_files)}):")
                print("=" * 50)
                for i, md_file in enumerate(md_files, 1):
                    print(f"  {i:2d}. {md_file.name}")
                print()
        else:
            print("Usage:")
            print("  python -m dental_consultation_pipeline.conversion html - Convert to HTML")
            print("  python -m dental_consultation_pipeline.conversion pdf  - Convert to PDF")
            print("  python -m dental_consultation_pipeline.conversion all  - Convert to both formats")
            print("  python -m dental_consultation_pipeline.conversion files - Show available files")
    else:
        # Default: convert all formats
        convert_all_formats()

if __name__ == "__main__":
    main() 