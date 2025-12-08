#!/usr/bin/env python3
"""
ChatGPT 5 Integration Demo Script

This script demonstrates how to use ChatGPT 5 for dental consultation analysis.
"""

import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import the pipeline modules
sys.path.append(str(Path(__file__).parent.parent))

from dental_consultation_pipeline.analysis import (
    analyze_clean_transcripts,
    analyze_clean_transcripts_with_both_models,
    calculate_token_usage
)

def main():
    """Demo the ChatGPT 5 integration"""
    print("🤖 ChatGPT 5 Integration Demo")
    print("=" * 50)
    
    # Check if OpenAI API key is set
    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        print("❌ OPENAI_API_KEY environment variable not set!")
        print("Please set your OpenAI API key:")
        print("  export OPENAI_API_KEY='your-api-key-here'")
        print("  or add it to your .env file")
        return
    
    print("✅ OpenAI API key found")
    
    # Check if Anthropic API key is set
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")
    if not anthropic_key:
        print("⚠️  ANTHROPIC_API_KEY environment variable not set!")
        print("Claude analysis will not work without this key")
    else:
        print("✅ Anthropic API key found")
    
    print("\n📋 Available options:")
    print("1. Analyze with ChatGPT 5 only")
    print("2. Analyze with Claude only")
    print("3. Analyze with both models")
    print("4. Show token usage")
    print("5. Exit")
    
    while True:
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == "1":
            print("\n🔄 Running analysis with ChatGPT 5...")
            analyze_clean_transcripts("chatgpt5")
            break
        elif choice == "2":
            print("\n🔄 Running analysis with Claude...")
            analyze_clean_transcripts("claude")
            break
        elif choice == "3":
            print("\n🔄 Running analysis with both models...")
            analyze_clean_transcripts_with_both_models()
            break
        elif choice == "4":
            print("\n📊 Calculating token usage...")
            calculate_token_usage()
            break
        elif choice == "5":
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please enter 1-5.")

if __name__ == "__main__":
    main()
