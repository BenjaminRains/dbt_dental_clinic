import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def test_claude_api():
    """Test Claude API connection"""
    # Import the LLM_CONFIG from the analysis module
    from dental_consultation_pipeline.analysis import LLM_CONFIG
    
    api_key = LLM_CONFIG["anthropic_api_key"]
    
    if not api_key:
        print("❌ ANTHROPIC_API_KEY not found")
        return False
    
    print("🔑 Found ANTHROPIC_API_KEY in .env file")
    print(f"🔑 API Key (first 10 chars): {api_key[:10]}...")
    
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    
    data = {
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "messages": [
            {"role": "user", "content": "Hello! Please respond with 'API test successful' if you can see this message."}
        ]
    }
    
    try:
        print("🚀 Testing Claude API connection...")
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=data,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result["content"][0]["text"]
            print("✅ Claude API test successful!")
            print(f"🤖 Response: {content}")
            return True
        else:
            print(f"❌ API request failed with status code: {response.status_code}")
            print(f"❌ Error: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing Claude API Connection")
    print("=" * 40)
    
    success = test_claude_api()
    
    if success:
        print("\n🎉 Claude API is working correctly!")
        print("💡 You can now use the analysis module in the pipeline")
    else:
        print("\n❌ Claude API test failed")
        print("💡 Check your API key and internet connection") 