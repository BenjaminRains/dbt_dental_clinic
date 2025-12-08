import os
import markdown2
from pathlib import Path
from datetime import datetime

# Configuration
INPUT_DIR = Path("api_claude_summary_analysis")
OUTPUT_DIR = Path("html_output")

def convert_markdown_to_html(md_file_path, output_dir):
    """Convert a single markdown file to HTML"""
    try:
        # Read markdown content
        with open(md_file_path, 'r', encoding='utf-8') as f:
            md_content = f.read()
        
        # Convert markdown to HTML
        html_content = markdown2.markdown(md_content, extras=['tables', 'fenced-code-blocks', 'code-friendly'])
        
        # Create full HTML document with styling
        full_html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{md_file_path.stem}</title>
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
        
        # Generate HTML filename
        html_filename = md_file_path.stem + ".html"
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

def convert_all_markdown_files():
    """Convert all markdown files in the input directory to HTML"""
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
        if convert_markdown_to_html(md_file, OUTPUT_DIR):
            success_count += 1
    
    print("=" * 50)
    print(f"🎉 Successfully converted {success_count}/{len(md_files)} files")
    print(f"📁 HTML files saved in: {OUTPUT_DIR}")

if __name__ == "__main__":
    convert_all_markdown_files() 