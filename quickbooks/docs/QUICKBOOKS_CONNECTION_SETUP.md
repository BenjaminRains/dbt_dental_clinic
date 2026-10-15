# QuickBooks API Connection Setup Guide

> Stage files are `quickbooks/.env_local`, `quickbooks/.env_clinic`, and `quickbooks/.env_test`. `clinic` is the live company and uses the Intuit production API. Examples below that say `.env_production` or `QUICKBOOKS_ENVIRONMENT=production` are out of date.

**Date**: 2026-01-08  
**Purpose**: Step-by-step guide for connecting to clinic's QuickBooks Online API using Python

---

## Overview

To connect to a clinic's QuickBooks Online API, you need to:

1. **Register an app** with Intuit Developer Portal
2. **Complete OAuth 2.0 flow** to get authorization from clinic
3. **Obtain access tokens** (access token + refresh token + realm ID)
4. **Use tokens** to connect via python-quickbooks library

---

## Step 1: Register Application with Intuit Developer Portal

### 1.1 Create Developer Account

1. Go to https://developer.intuit.com/
2. Sign in with Intuit account (or create one)
3. If this is your first app, you'll need to create a developer profile

### 1.2 Create New App

1. Click **"Create an App"** or go to https://developer.intuit.com/app/developer/myapps
2. Select **"QuickBooks Online and Payments"** as the platform
3. Fill in app details:
   - **App Name**: e.g., "MDC Analytics ETL" or "Dental Clinic Analytics"
   - **App Type**: Choose based on your use case
   - **Production App**: Check if connecting to production QuickBooks, or use Sandbox for testing
4. Click **"Create App"**

### 1.3 Get Credentials

After creating the app, go to the **"Keys & OAuth"** tab:

1. **Client ID** (also called Consumer Key) - Copy this value
2. **Client Secret** (also called Consumer Secret) - Copy this value
3. **Sandbox Base URL**: `https://sandbox-quickbooks.api.intuit.com` (for testing)
4. **Production Base URL**: `https://quickbooks.api.intuit.com` (for production)

### 1.4 Configure Redirect URI

1. Under **"Redirect URIs"**, add your redirect URI(s)
   - For local development: `http://localhost:8000/callback` or `http://localhost:8080/callback`
   - For production: Your actual callback URL
2. Click **"Save"**

**Important**: The redirect URI must match exactly (including http/https, port, trailing slashes)

---

## Step 2: Install Python Libraries

### Option A: Using `python-quickbooks` (Older but Stable)

```bash
pip install python-quickbooks
```

**Note**: This library may require additional setup. Check GitHub: https://github.com/sidecars/python-quickbooks

### Option B: Using `intuitlib` (Official Intuit Library - Recommended)

```bash
pip install intuitlib
```

**Note**: `intuitlib` is Intuit's official Python library for OAuth 2.0. You can use it with `python-quickbooks` or use `requests` directly.

### Option C: Manual OAuth with `requests` (Full Control)

No additional installation needed - `requests` is already in your dependencies.

---

## Step 3: Complete OAuth 2.0 Flow

### 3.1 Understanding the OAuth Flow

The OAuth flow requires **user interaction** (the clinic owner/admin must authorize your app):

```
1. Your app redirects user to Intuit authorization page
2. User (clinic admin) logs in and authorizes your app
3. Intuit redirects back to your callback URL with authorization code
4. You exchange authorization code for tokens
5. You store tokens for future API calls
```

### 3.2 Initial Authorization (One-Time Setup)

#### Option A: Using `intuitlib` (Recommended for OAuth)

Create a script to handle the initial OAuth flow:

```python
# scripts/quickbooks_oauth_setup.py

from intuitlib.client import AuthClient
from intuitlib.enums import Scopes
from intuitlib.exceptions import AuthClientError
import os
import webbrowser

def setup_quickbooks_oauth(entity_name='MDC'):
    """
    One-time setup script to get OAuth tokens for QuickBooks.
    
    Run this script once per clinic entity to get initial tokens.
    User will be redirected to browser for authorization.
    """
    
    # Get credentials from environment or input
    client_id = os.getenv(f'{entity_name}_QUICKBOOKS_CLIENT_ID')
    client_secret = os.getenv(f'{entity_name}_QUICKBOOKS_CLIENT_SECRET')
    redirect_uri = os.getenv('QUICKBOOKS_REDIRECT_URI', 'http://localhost:8080/callback')
    
    if not client_id or not client_secret:
        print(f"Please set {entity_name}_QUICKBOOKS_CLIENT_ID and {entity_name}_QUICKBOOKS_CLIENT_SECRET")
        return None
    
    # Initialize auth client
    # Use 'sandbox' for testing, 'production' for live QuickBooks
    environment = os.getenv('QUICKBOOKS_ENVIRONMENT', 'sandbox')
    
    auth_client = AuthClient(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        environment=environment  # 'sandbox' or 'production'
    )
    
    # Get authorization URL
    # Scopes define what data your app can access
    scopes = [
        Scopes.ACCOUNTING,  # Access to accounting data
    ]
    
    auth_url = auth_client.get_authorization_url(scopes)
    
    print(f"\n{'='*60}")
    print(f"Step 1: Authorize the application")
    print(f"{'='*60}")
    print(f"\nOpening browser to authorize...")
    print(f"If browser doesn't open, visit this URL manually:")
    print(f"\n{auth_url}\n")
    
    # Open browser automatically
    webbrowser.open(auth_url)
    
    # Wait for user to complete authorization
    print("\nWaiting for authorization...")
    print("After authorizing, you'll be redirected to the callback URL.")
    print("Copy the full callback URL (including all parameters) and paste it below.\n")
    
    callback_url = input("Paste the callback URL here: ").strip()
    
    # Extract authorization code from callback URL
    try:
        # Parse the callback URL to get authorization code
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(callback_url)
        params = parse_qs(parsed.query)
        
        auth_code = params.get('code', [None])[0]
        realm_id = params.get('realmId', [None])[0]  # This is the Company ID!
        
        if not auth_code:
            print("Error: No authorization code found in callback URL")
            return None
        
        print(f"\n{'='*60}")
        print(f"Step 2: Exchanging authorization code for tokens")
        print(f"{'='*60}\n")
        
        # Exchange authorization code for tokens
        auth_client.get_bearer_token(auth_code, realm_id=realm_id)
        
        # Get tokens
        access_token = auth_client.access_token
        refresh_token = auth_client.refresh_token
        
        print(f"\n{'='*60}")
        print(f"✅ Successfully connected to QuickBooks!")
        print(f"{'='*60}")
        print(f"\nRealm ID (Company ID): {realm_id}")
        print(f"Access Token: {access_token[:20]}... (truncated)")
        print(f"Refresh Token: {refresh_token[:20]}... (truncated)")
        print(f"\n{'='*60}")
        print(f"Add these to your .env_production file:")
        print(f"{'='*60}\n")
        print(f"{entity_name}_QUICKBOOKS_REALM_ID={realm_id}")
        print(f"{entity_name}_QUICKBOOKS_ACCESS_TOKEN={access_token}")
        print(f"{entity_name}_QUICKBOOKS_REFRESH_TOKEN={refresh_token}")
        print(f"\n⚠️  IMPORTANT: Store these securely and never commit them to git!")
        
        return {
            'realm_id': realm_id,
            'access_token': access_token,
            'refresh_token': refresh_token
        }
        
    except AuthClientError as e:
        print(f"Error during token exchange: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

if __name__ == '__main__':
    # Run for MDC
    setup_quickbooks_oauth('MDC')
    
    # Uncomment to run for GLIC (if separate QuickBooks company)
    # setup_quickbooks_oauth('GLIC')
```

#### Option B: Manual OAuth with `requests`

If you prefer full control or don't want to use `intuitlib`:

```python
# scripts/quickbooks_oauth_manual.py

import requests
import webbrowser
from urllib.parse import urlparse, parse_qs
import os

def setup_oauth_manual(entity_name='MDC'):
    """Manual OAuth 2.0 setup using requests library"""
    
    client_id = os.getenv(f'{entity_name}_QUICKBOOKS_CLIENT_ID')
    client_secret = os.getenv(f'{entity_name}_QUICKBOOKS_CLIENT_SECRET')
    redirect_uri = os.getenv('QUICKBOOKS_REDIRECT_URI', 'http://localhost:8080/callback')
    environment = os.getenv('QUICKBOOKS_ENVIRONMENT', 'sandbox')
    
    # Step 1: Get authorization URL
    base_url = 'https://appcenter.intuit.com/connect/oauth2' if environment == 'production' else 'https://appcenter.intuit.com/connect/oauth2'
    
    auth_url = (
        f"{base_url}?"
        f"client_id={client_id}&"
        f"response_type=code&"
        f"scope=com.intuit.quickbooks.accounting&"
        f"redirect_uri={redirect_uri}"
    )
    
    print(f"Open this URL in browser: {auth_url}")
    webbrowser.open(auth_url)
    
    # Step 2: Get callback URL from user
    callback_url = input("\nPaste the callback URL here: ").strip()
    parsed = urlparse(callback_url)
    params = parse_qs(parsed.query)
    auth_code = params.get('code', [None])[0]
    realm_id = params.get('realmId', [None])[0]
    
    # Step 3: Exchange code for tokens
    token_url = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'
    
    response = requests.post(token_url, data={
        'grant_type': 'authorization_code',
        'code': auth_code,
        'redirect_uri': redirect_uri
    }, auth=(client_id, client_secret), headers={
        'Accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded'
    })
    
    if response.status_code == 200:
        tokens = response.json()
        print(f"\n✅ Tokens obtained!")
        print(f"Realm ID: {realm_id}")
        print(f"Access Token: {tokens['access_token'][:20]}...")
        print(f"Refresh Token: {tokens['refresh_token'][:20]}...")
        return {
            'realm_id': realm_id,
            'access_token': tokens['access_token'],
            'refresh_token': tokens['refresh_token']
        }
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

if __name__ == '__main__':
    setup_oauth_manual('MDC')
```

#### Option C: Simple Web Server for Callback (Better UX)

For a better user experience, create a simple web server to handle the callback:

```python
# scripts/quickbooks_oauth_server.py

from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import webbrowser
from intuitlib.client import AuthClient
from intuitlib.enums import Scopes
import os

class CallbackHandler(BaseHTTPRequestHandler):
    """HTTP handler for OAuth callback"""
    
    auth_client = None
    tokens = None
    
    def do_GET(self):
        """Handle OAuth callback"""
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        
        auth_code = params.get('code', [None])[0]
        realm_id = params.get('realmId', [None])[0]
        
        if auth_code and CallbackHandler.auth_client:
            try:
                # Exchange code for tokens
                CallbackHandler.auth_client.get_bearer_token(auth_code, realm_id=realm_id)
                
                CallbackHandler.tokens = {
                    'realm_id': realm_id,
                    'access_token': CallbackHandler.auth_client.access_token,
                    'refresh_token': CallbackHandler.auth_client.refresh_token
                }
                
                # Send success response
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(b"""
                    <html>
                    <body>
                        <h1>Authorization Successful!</h1>
                        <p>You can close this window and return to the terminal.</p>
                        <script>setTimeout(function(){window.close();}, 2000);</script>
                    </body>
                    </html>
                """)
            except Exception as e:
                self.send_response(400)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(f"<html><body><h1>Error: {e}</h1></body></html>".encode())
        else:
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"<html><body><h1>No authorization code received</h1></body></html>")
    
    def log_message(self, format, *args):
        """Suppress log messages"""
        pass

def setup_oauth_with_server(entity_name='MDC', port=8080):
    """OAuth setup with local web server for callback"""
    
    client_id = os.getenv(f'{entity_name}_QUICKBOOKS_CLIENT_ID')
    client_secret = os.getenv(f'{entity_name}_QUICKBOOKS_CLIENT_SECRET')
    redirect_uri = f'http://localhost:{port}/callback'
    environment = os.getenv('QUICKBOOKS_ENVIRONMENT', 'sandbox')
    
    # Initialize auth client
    auth_client = AuthClient(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        environment=environment
    )
    
    CallbackHandler.auth_client = auth_client
    
    # Get authorization URL
    auth_url = auth_client.get_authorization_url([Scopes.ACCOUNTING])
    
    print(f"\n{'='*60}")
    print(f"QuickBooks OAuth Setup for {entity_name}")
    print(f"{'='*60}\n")
    print(f"1. Starting local web server on port {port}...")
    print(f"2. Opening browser for authorization...\n")
    
    # Start web server in background
    server = HTTPServer(('localhost', port), CallbackHandler)
    
    # Open browser
    webbrowser.open(auth_url)
    
    # Wait for callback (with timeout)
    import signal
    import sys
    
    def timeout_handler(signum, frame):
        print("\nTimeout: No response received. Please try again.")
        server.shutdown()
        sys.exit(1)
    
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(300)  # 5 minute timeout
    
    print("Waiting for authorization (server running on http://localhost:8080)...")
    print("After authorizing in browser, the callback will be handled automatically.\n")
    
    # Handle one request (the callback)
    server.handle_request()
    
    signal.alarm(0)  # Cancel timeout
    
    if CallbackHandler.tokens:
        print(f"\n✅ Successfully connected!")
        print(f"\nRealm ID: {CallbackHandler.tokens['realm_id']}")
        print(f"Access Token: {CallbackHandler.tokens['access_token'][:30]}...")
        print(f"Refresh Token: {CallbackHandler.tokens['refresh_token'][:30]}...")
        print(f"\nAdd to .env_production:")
        print(f"{entity_name}_QUICKBOOKS_REALM_ID={CallbackHandler.tokens['realm_id']}")
        print(f"{entity_name}_QUICKBOOKS_ACCESS_TOKEN={CallbackHandler.tokens['access_token']}")
        print(f"{entity_name}_QUICKBOOKS_REFRESH_TOKEN={CallbackHandler.tokens['refresh_token']}")
        return CallbackHandler.tokens
    else:
        print("\n❌ Failed to get tokens. Please try again.")
        return None

if __name__ == '__main__':
    setup_oauth_with_server('MDC')
```

---

## Step 4: Store Credentials Securely

After getting tokens, add them to your environment files:

### `.env_production` (or `.env_test` for sandbox):

```bash
# MDC QuickBooks Configuration
MDC_QUICKBOOKS_CLIENT_ID=your_client_id_from_intuit_portal
MDC_QUICKBOOKS_CLIENT_SECRET=your_client_secret_from_intuit_portal
MDC_QUICKBOOKS_REALM_ID=1234567890  # From OAuth callback
MDC_QUICKBOOKS_ACCESS_TOKEN=eyJraWQiOi...  # From OAuth flow
MDC_QUICKBOOKS_REFRESH_TOKEN=L011546037639...  # From OAuth flow

# GLIC QuickBooks Configuration (if separate company)
GLIC_QUICKBOOKS_CLIENT_ID=your_client_id
GLIC_QUICKBOOKS_CLIENT_SECRET=your_client_secret
GLIC_QUICKBOOKS_REALM_ID=0987654321
GLIC_QUICKBOOKS_ACCESS_TOKEN=eyJraWQiOi...
GLIC_QUICKBOOKS_REFRESH_TOKEN=L011546037639...

# QuickBooks API Settings
QUICKBOOKS_ENVIRONMENT=production  # or 'sandbox' for testing
QUICKBOOKS_REDIRECT_URI=http://localhost:8080/callback
QUICKBOOKS_API_MINOR_VERSION=65
```

**⚠️ Security Note**: Never commit these values to git. They should be in `.env_production` which should be in `.gitignore`.

---

## Step 5: Connect Using python-quickbooks Library

### 5.1 Using `python-quickbooks` with Tokens

Once you have tokens, you can connect:

```python
# etl_pipeline/etl_pipeline/extractors/quickbooks_extractor.py

from quickbooks import QuickBooks
from quickbooks.objects import Customer, Invoice, Payment
import os
from etl_pipeline.config import get_settings

class QuickBooksExtractor:
    def __init__(self, entity_id='MDC'):
        """Initialize QuickBooks client for entity"""
        self.entity_id = entity_id
        self.settings = get_settings()
        
        # Get credentials from environment
        self.client_id = os.getenv(f'{entity_id}_QUICKBOOKS_CLIENT_ID')
        self.client_secret = os.getenv(f'{entity_id}_QUICKBOOKS_CLIENT_SECRET')
        self.realm_id = os.getenv(f'{entity_id}_QUICKBOOKS_REALM_ID')
        self.access_token = os.getenv(f'{entity_id}_QUICKBOOKS_ACCESS_TOKEN')
        self.refresh_token = os.getenv(f'{entity_id}_QUICKBOOKS_REFRESH_TOKEN')
        
        # Determine environment
        environment = os.getenv('QUICKBOOKS_ENVIRONMENT', 'sandbox')
        sandbox = (environment == 'sandbox')
        
        # Initialize QuickBooks client
        self.client = QuickBooks(
            sandbox=sandbox,
            consumer_key=self.client_id,
            consumer_secret=self.client_secret,
            access_token=self.access_token,
            access_token_secret=self.refresh_token,  # Note: python-quickbooks uses this for refresh
            company_id=self.realm_id
        )
    
    def get_customers(self):
        """Get all customers"""
        customers = Customer.query("", qb=self.client)
        return customers
    
    def get_invoices(self, last_updated_time=None):
        """Get invoices, optionally filtered by last updated time"""
        if last_updated_time:
            query = f"SELECT * FROM Invoice WHERE MetaData.LastUpdatedTime > '{last_updated_time}'"
        else:
            query = "SELECT * FROM Invoice"
        
        invoices = Invoice.query(query, qb=self.client)
        return invoices
```

### 5.2 Test Connection

Create a test script to verify connection:

```python
# scripts/test_quickbooks_connection.py

from etl_pipeline.extractors.quickbooks_extractor import QuickBooksExtractor

def test_connection(entity_id='MDC'):
    """Test QuickBooks connection"""
    print(f"Testing QuickBooks connection for {entity_id}...")
    
    try:
        extractor = QuickBooksExtractor(entity_id)
        
        # Try to get a few customers
        print("Fetching customers...")
        customers = extractor.get_customers()
        print(f"✅ Connected! Found {len(customers)} customers")
        
        # Show first customer
        if customers:
            first_customer = customers[0]
            print(f"\nFirst customer: {first_customer.DisplayName}")
            print(f"  ID: {first_customer.Id}")
            print(f"  Balance: {first_customer.Balance if hasattr(first_customer, 'Balance') else 'N/A'}")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == '__main__':
    test_connection('MDC')
```

---

## Step 6: Handle Token Refresh

Access tokens expire after 1 hour. You need to refresh them using the refresh token:

```python
# etl_pipeline/etl_pipeline/extractors/quickbooks_auth.py

import requests
from datetime import datetime, timedelta
import os
import logging

logger = logging.getLogger(__name__)

class QuickBooksTokenManager:
    """Manages QuickBooks OAuth tokens and refresh"""
    
    def __init__(self, entity_id='MDC'):
        self.entity_id = entity_id
        self.client_id = os.getenv(f'{entity_id}_QUICKBOOKS_CLIENT_ID')
        self.client_secret = os.getenv(f'{entity_id}_QUICKBOOKS_CLIENT_SECRET')
        self.refresh_token = os.getenv(f'{entity_id}_QUICKBOOKS_REFRESH_TOKEN')
        self.access_token = os.getenv(f'{entity_id}_QUICKBOOKS_ACCESS_TOKEN')
        self.token_expires_at = None
        
        # Calculate expiration (tokens typically last 1 hour)
        # We'll refresh 5 minutes before expiration
        self.token_expires_at = datetime.now() + timedelta(hours=1, minutes=-5)
    
    def get_valid_access_token(self):
        """Get a valid access token, refreshing if needed"""
        if self.is_token_valid():
            return self.access_token
        
        logger.info(f"Access token expired for {self.entity_id}, refreshing...")
        return self.refresh_access_token()
    
    def is_token_valid(self):
        """Check if current access token is still valid"""
        if not self.access_token or not self.token_expires_at:
            return False
        return datetime.now() < self.token_expires_at
    
    def refresh_access_token(self):
        """Refresh access token using refresh token"""
        url = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'
        
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token
        }
        
        response = requests.post(
            url,
            data=data,
            auth=(self.client_id, self.client_secret),
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
        )
        
        if response.status_code == 200:
            tokens = response.json()
            self.access_token = tokens['access_token']
            
            # Update refresh token if new one provided (some APIs do this)
            if 'refresh_token' in tokens:
                self.refresh_token = tokens['refresh_token']
                # TODO: Update environment variable or secure storage
            
            expires_in = tokens.get('expires_in', 3600)  # Default 1 hour
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 300)  # 5 min buffer
            
            logger.info(f"Successfully refreshed access token for {self.entity_id}")
            return self.access_token
        else:
            logger.error(f"Failed to refresh token: {response.status_code} - {response.text}")
            raise Exception(f"Token refresh failed: {response.text}")
```

---

## Step 7: For Production - Getting Clinic Authorization

### 7.1 Who Needs to Authorize?

The **clinic owner or QuickBooks admin** must authorize your app. This is a one-time process per QuickBooks company.

### 7.2 Options for Getting Authorization

#### Option A: In-Person/Remote Screen Share
- Run the OAuth setup script with clinic admin present
- They log into QuickBooks and authorize the app
- You capture the tokens immediately

#### Option B: Send Authorization Link
- Create a simple web page that generates the authorization URL
- Clinic admin clicks link, authorizes, gets redirected to your callback
- You receive tokens via webhook or email notification

#### Option C: Desktop Application
- Create a simple desktop app that guides clinic admin through authorization
- Store tokens locally or send to your server

#### Option D: Manual Token Exchange
- Provide clinic admin with instructions
- They complete OAuth flow and send you the authorization code
- You exchange code for tokens

### 7.3 What the Clinic Admin Will See

When clinic admin authorizes your app, they will see:
- **App Name**: Your app name from Intuit Developer Portal
- **Permissions**: What data your app can access (accounting data)
- **Authorization Button**: "Authorize" or "Connect"
- **Company Selection**: If they have multiple QuickBooks companies, they'll select which one

**Note**: They can revoke access at any time from their QuickBooks account settings.

---

## Troubleshooting

### Issue: "Invalid redirect URI"
**Solution**: Make sure redirect URI in code matches exactly what's configured in Intuit Developer Portal (including http/https, port, trailing slashes)

### Issue: "Invalid client credentials"
**Solution**: Double-check Client ID and Client Secret are correct and match the environment (sandbox vs production)

### Issue: "Access token expired"
**Solution**: Implement token refresh logic (see Step 6)

### Issue: "Realm ID not found"
**Solution**: Realm ID is obtained from OAuth callback, not from API. Make sure you capture it during initial OAuth flow.

### Issue: "Rate limit exceeded"
**Solution**: QuickBooks allows 500 requests/minute. Implement rate limiting and retry logic with exponential backoff.

---

## Quick Reference: Complete Setup Flow

```
1. Register app at https://developer.intuit.com/
   ↓ Get Client ID and Client Secret

2. Run OAuth setup script
   ↓ Clinic admin authorizes app
   ↓ Get Realm ID, Access Token, Refresh Token

3. Store tokens in .env_production
   ↓ Never commit to git

4. Use tokens in python-quickbooks
   ↓ Initialize QuickBooks client
   ↓ Make API calls

5. Implement token refresh
   ↓ Access tokens expire after 1 hour
   ↓ Use refresh token to get new access token
   ↓ Refresh tokens last 100 days
```

---

## Next Steps

1. **Create Intuit Developer Account** and register your app
2. **Get Client ID and Client Secret** from Developer Portal
3. **Run OAuth setup script** to get initial tokens (one-time per clinic)
4. **Store tokens securely** in environment variables
5. **Test connection** with a simple script
6. **Implement token refresh** logic
7. **Build extraction pipeline** using the tokens

---

**Document Version**: 1.0  
**Last Updated**: 2026-01-08
