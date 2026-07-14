import os
import requests
from pathlib import Path
from dotenv import load_dotenv

_env_file = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_env_file, override=False)

def test_openai_api():
    """Test OpenAI API connection"""
    # Import the LLM_CONFIG from the analysis module
    from dental_consultation_pipeline.analysis import LLM_CONFIG
    
    api_key = LLM_CONFIG["openai_api_key"]
    model = LLM_CONFIG["openai_model"]
    
    if not api_key:
        print("❌ OPENAI_API_KEY not found")
        return False
    
    print("🔑 Found OPENAI_API_KEY in .env file")
    print(f"🤖 Model: {model}")
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    data = {
        "model": model,
        "max_tokens": 100,
        "messages": [
            {"role": "user", "content": "Hello! Please respond with 'OpenAI API test successful' if you can see this message."}
        ]
    }
    
    try:
        print("🚀 Testing OpenAI API connection...")
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=data,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            print("✅ OpenAI API test successful!")
            print(f"🤖 Response: {content}")
            
            # Also show token usage
            usage = result.get("usage", {})
            if usage:
                print(f"📊 Tokens used: {usage.get('prompt_tokens', 0)} input, {usage.get('completion_tokens', 0)} output")
            
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

def test_openai_client():
    """Test OpenAI client library connection"""
    try:
        from openai import OpenAI
        from dental_consultation_pipeline.analysis import LLM_CONFIG
        
        api_key = LLM_CONFIG["openai_api_key"]
        model = LLM_CONFIG["openai_model"]
        
        if not api_key:
            print("❌ OPENAI_API_KEY not found for client test")
            return False
        
        print("🚀 Testing OpenAI client library...")
        client = OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model=model,
            max_tokens=100,
            messages=[
                {"role": "user", "content": "Hello! Please respond with 'OpenAI client test successful' if you can see this message."}
            ]
        )
        
        content = response.choices[0].message.content
        print("✅ OpenAI client test successful!")
        print(f"🤖 Response: {content}")
        
        # Show token usage
        if hasattr(response, 'usage') and response.usage:
            print(f"📊 Tokens used: {response.usage.prompt_tokens} input, {response.usage.completion_tokens} output")
        
        return True
        
    except ImportError:
        print("❌ OpenAI client library not installed. Install with: pip install openai")
        return False
    except Exception as e:
        print(f"❌ OpenAI client error: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing OpenAI API Connection")
    print("=" * 40)
    
    print("\n📡 Testing with requests library...")
    success1 = test_openai_api()
    
    print("\n📡 Testing with OpenAI client library...")
    success2 = test_openai_client()
    
    if success1 and success2:
        print("\n🎉 OpenAI API is working correctly!")
        print("💡 You can now use ChatGPT in the analysis module")
    elif success1:
        print("\n⚠️  OpenAI API works with requests but client library failed")
        print("💡 Install OpenAI client: pip install openai")
    elif success2:
        print("\n⚠️  OpenAI client works but requests failed")
        print("💡 This is unusual but the client library is working")
    else:
        print("\n❌ OpenAI API tests failed")
        print("💡 Check your API key, model name, and internet connection")
