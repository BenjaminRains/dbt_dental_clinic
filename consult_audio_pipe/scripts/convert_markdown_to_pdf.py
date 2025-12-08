import os
import markdown2
from pathlib import Path
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor

# Configuration
INPUT_DIR = Path("api_claude_summary_analysis")
OUTPUT_DIR = Path("pdf_output")

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
        
        # Generate PDF filename
        pdf_filename = md_file_path.stem + ".pdf"
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

def convert_all_markdown_files():
    """Convert all markdown files in the input directory to PDF"""
    if not INPUT_DIR.exists():
        print(f"❌ Input directory not found: {INPUT_DIR}")
        return
    
    md_files = list(INPUT_DIR.glob("*.md"))
    if not md_files:
        print(f"❌ No markdown files found in {INPUT_DIR}")
        return
    
    print(f"🔄 Found {len(md_files)} markdown files to convert...")
    print(f"📁 Input: {INPUT_DIR}")
    print(f"📁 Output: {OUTPUT_DIR}")
    print("=" * 50)
    
    success_count = 0
    for md_file in md_files:
        if convert_markdown_to_pdf_simple(md_file, OUTPUT_DIR):
            success_count += 1
    
    print("=" * 50)
    print(f"🎉 Successfully converted {success_count}/{len(md_files)} files")
    print(f"📁 PDF files saved in: {OUTPUT_DIR}")

if __name__ == "__main__":
    convert_all_markdown_files() 