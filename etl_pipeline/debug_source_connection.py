import os
import pymysql
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def debug_source_connection():
    """Debug the source connection step by step."""
    print("🔍 DEBUGGING SOURCE CONNECTION")
    print("=" * 50)
    
    # Step 1: Check environment variables
    print("\n1️⃣ Checking Environment Variables:")
    source_vars = {
        'OPENDENTAL_SOURCE_HOST': os.getenv('OPENDENTAL_SOURCE_HOST'),
        'OPENDENTAL_SOURCE_PORT': os.getenv('OPENDENTAL_SOURCE_PORT'),
        'OPENDENTAL_SOURCE_DB': os.getenv('OPENDENTAL_SOURCE_DB'),
        'OPENDENTAL_SOURCE_USER': os.getenv('OPENDENTAL_SOURCE_USER'),
        'OPENDENTAL_SOURCE_PW': os.getenv('OPENDENTAL_SOURCE_PW')  # Changed from OPENDENTAL_SOURCE_PASSWORD
    }
    
    missing_vars = []
    for var, value in source_vars.items():
        if not value:
            print(f"❌ {var}: MISSING or EMPTY")
            missing_vars.append(var)
        else:
            if 'PW' in var or 'PASSWORD' in var:  # Handle both PW and PASSWORD
                print(f"✅ {var}: SET (***)")
            else:
                print(f"✅ {var}: {value}")
    
    if missing_vars:
        print(f"\n❌ Missing variables: {', '.join(missing_vars)}")
        print("\n💡 Add these to your .env file:")
        for var in missing_vars:
            if var == 'OPENDENTAL_SOURCE_HOST':
                print(f"{var}=your.opendental.server.com  # or IP address")
            elif var == 'OPENDENTAL_SOURCE_PORT':
                print(f"{var}=3306  # default MySQL port")
            elif var == 'OPENDENTAL_SOURCE_DB':
                print(f"{var}=opendental  # database name")
            elif var == 'OPENDENTAL_SOURCE_USER':
                print(f"{var}=readonly_user  # read-only username")
            elif var == 'OPENDENTAL_SOURCE_PW':
                print(f"{var}=your_password  # database password")
        return False
    
    host = source_vars['OPENDENTAL_SOURCE_HOST']
    port = int(source_vars['OPENDENTAL_SOURCE_PORT'])
    database = source_vars['OPENDENTAL_SOURCE_DB']
    user = source_vars['OPENDENTAL_SOURCE_USER']
    password = source_vars['OPENDENTAL_SOURCE_PW']  # Changed from OPENDENTAL_SOURCE_PASSWORD
    
    # Step 2: Test basic network connectivity
    print(f"\n2️⃣ Testing Network Connectivity to {host}:{port}:")
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"✅ Network connection to {host}:{port} successful")
        else:
            print(f"❌ Cannot connect to {host}:{port}")
            print("💡 Possible issues:")
            print("  - Server is down or not accessible")
            print("  - Firewall blocking the connection")
            print("  - Wrong host/port in .env file")
            print("  - VPN required to access the server")
            return False
    except Exception as e:
        print(f"❌ Network test failed: {str(e)}")
        return False
    
    # Step 3: Test MySQL connection with PyMySQL directly
    print(f"\n3️⃣ Testing Direct PyMySQL Connection:")
    try:
        # Test without SSL first
        connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            ssl_disabled=True,
            charset='utf8mb4'
        )
        
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            print(f"✅ Direct PyMySQL connection successful: {result}")
        
        connection.close()
        
    except pymysql.err.OperationalError as e:
        print(f"❌ PyMySQL connection failed: {str(e)}")
        error_code = e.args[0] if e.args else 0
        
        if error_code == 1045:
            print("💡 This is an authentication error:")
            print("  - Wrong username or password")
            print("  - User doesn't have access to the database")
        elif error_code == 2003:
            print("💡 This is a connection error:")
            print("  - MySQL server is not running")
            print("  - Wrong host or port")
            print("  - Firewall blocking connection")
        elif error_code == 1049:
            print("💡 Database doesn't exist:")
            print(f"  - Database '{database}' not found on server")
            print("  - Check OPENDENTAL_SOURCE_DB value")
        else:
            print(f"💡 MySQL error code {error_code}")
        
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False
    
    # Step 4: Test SQLAlchemy connection
    print(f"\n4️⃣ Testing SQLAlchemy Connection:")
    try:
        # Try different connection string variations
        connection_strings = [
            # Standard connection
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?ssl_disabled=true&charset=utf8mb4",
            # Alternative SSL settings
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?ssl=false&charset=utf8mb4",
            # Minimal connection
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
        ]
        
        for i, conn_str in enumerate(connection_strings, 1):
            print(f"\n  Attempt {i}: Testing connection string variation...")
            try:
                engine = create_engine(
                    conn_str,
                    connect_args={
                        "ssl_disabled": True,
                        "ssl_verify_cert": False,
                        "ssl_verify_identity": False,
                        "charset": "utf8mb4"
                    }
                )
                
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT 1"))
                    print(f"  ✅ SQLAlchemy connection successful with attempt {i}")
                    
                    # Test read access
                    result = conn.execute(text("SELECT CURRENT_USER()"))
                    current_user = result.scalar()
                    print(f"  Connected as: {current_user}")
                    
                    return True
                    
            except Exception as e:
                print(f"  ❌ Attempt {i} failed: {str(e)}")
                continue
        
        print("❌ All SQLAlchemy connection attempts failed")
        return False
        
    except Exception as e:
        print(f"❌ SQLAlchemy test failed: {str(e)}")
        return False
    
    return True

def suggest_fixes():
    """Suggest potential fixes based on common issues."""
    print("\n🔧 POTENTIAL FIXES:")
    print("=" * 30)
    
    print("\n1. Check .env file location and format:")
    print("   - Make sure .env is in the etl_pipeline directory")
    print("   - No spaces around = sign: VARIABLE=value")
    print("   - No quotes around values unless needed")
    
    print("\n2. Test manual MySQL connection:")
    print("   mysql -h [HOST] -P [PORT] -u [USER] -p [DATABASE]")
    print("   mysql -h [HOST] -P [PORT] -u [USER] -p [DATABASE] --ssl-mode=DISABLED")
    
    print("\n3. Common OpenDental connection settings:")
    print("   - Host: Usually the server where OpenDental is installed")
    print("   - Port: Usually 3306 (default MySQL)")
    print("   - Database: Usually 'opendental' or similar")
    print("   - User: Should be read-only user, NOT root")
    
    print("\n4. Network/Firewall issues:")
    print("   - Ensure MySQL port is open")
    print("   - Check if VPN is required")
    print("   - Verify with your IT admin")
    
    print("\n5. Alternative: Skip source for now:")
    print("   - You can test staging and target first")
    print("   - Add source connection later")
    print("   - Comment out source tests temporarily")

if __name__ == "__main__":
    success = debug_source_connection()
    if not success:
        suggest_fixes()
    else:
        print("\n🎉 Source connection debugging completed successfully!")
        print("✅ Your OpenDental connection should work now.")