#!/usr/bin/env python3
"""
Script to create MySQL dump from client server.
Supports both direct streaming and file-based approaches.
"""

import os
import sys
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from environment variables."""
    load_dotenv()
    
    config = {
        'source': {
            'host': os.getenv('OPENDENTAL_SOURCE_HOST'),
            'port': int(os.getenv('OPENDENTAL_SOURCE_PORT', '3306')),
            'user': os.getenv('OPENDENTAL_SOURCE_USER'),
            'password': os.getenv('OPENDENTAL_SOURCE_PASSWORD'),
            'database': os.getenv('OPENDENTAL_SOURCE_DB', 'opendental')
        },
        'target': {
            'host': os.getenv('MYSQL_REPLICATION_HOST', 'localhost'),
            'port': int(os.getenv('MYSQL_REPLICATION_PORT', '3306')),
            'user': os.getenv('MYSQL_REPLICATION_USER'),
            'password': os.getenv('MYSQL_REPLICATION_PASSWORD'),
            'database': os.getenv('MYSQL_REPLICATION_DB', 'opendental_replication')
        }
    }
    
    # Validate required config
    required_vars = [
        'OPENDENTAL_SOURCE_HOST',
        'OPENDENTAL_SOURCE_USER',
        'OPENDENTAL_SOURCE_PASSWORD'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    return config

def create_dump_file(config, output_dir='dumps'):
    """Create a MySQL dump file."""
    # Create dumps directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    dump_file = f"{output_dir}/opendental_dump_{timestamp}.sql"
    
    # Build mysqldump command
    dump_cmd = [
        'mysqldump',
        f"--host={config['source']['host']}",
        f"--port={config['source']['port']}",
        f"--user={config['source']['user']}",
        f"--password={config['source']['password']}",
        '--single-transaction',
        '--routines',
        '--triggers',
        '--events',
        '--set-gtid-purged=OFF',
        config['source']['database']
    ]
    
    try:
        logger.info(f"Creating dump file: {dump_file}")
        with open(dump_file, 'w') as f:
            process = subprocess.Popen(
                dump_cmd,
                stdout=f,
                stderr=subprocess.PIPE
            )
            _, stderr = process.communicate()
            
            if process.returncode != 0:
                raise Exception(f"mysqldump failed: {stderr.decode()}")
        
        logger.info(f"Dump file created successfully: {dump_file}")
        return dump_file
        
    except Exception as e:
        logger.error(f"Error creating dump file: {e}")
        raise

def direct_dump(config):
    """Perform direct dump from source to target database."""
    try:
        # Build mysqldump command for direct pipe
        dump_cmd = [
            'mysqldump',
            f"--host={config['source']['host']}",
            f"--port={config['source']['port']}",
            f"--user={config['source']['user']}",
            f"--password={config['source']['password']}",
            '--single-transaction',
            '--routines',
            '--triggers',
            '--events',
            '--set-gtid-purged=OFF',
            config['source']['database']
        ]
        
        # Build mysql import command
        import_cmd = [
            'mysql',
            f"--host={config['target']['host']}",
            f"--port={config['target']['port']}",
            f"--user={config['target']['user']}",
            f"--password={config['target']['password']}",
            config['target']['database']
        ]
        
        logger.info("Starting direct dump from source to target")
        
        # Create pipe between mysqldump and mysql
        dump_process = subprocess.Popen(
            dump_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        import_process = subprocess.Popen(
            import_cmd,
            stdin=dump_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait for both processes to complete
        dump_process.stdout.close()
        _, import_stderr = import_process.communicate()
        
        if import_process.returncode != 0:
            raise Exception(f"Import failed: {import_stderr.decode()}")
        
        logger.info("Direct dump completed successfully")
        
    except Exception as e:
        logger.error(f"Error during direct dump: {e}")
        raise

def main():
    """Main function to handle MySQL dump process."""
    try:
        # Load configuration
        config = load_config()
        
        # Check command line arguments
        if len(sys.argv) > 1 and sys.argv[1] == '--direct':
            # Perform direct dump
            direct_dump(config)
        else:
            # Create dump file
            dump_file = create_dump_file(config)
            print(f"\nDump file created: {dump_file}")
            print("\nTo import this dump, run:")
            print(f"python scripts/setup_replication_db.py {dump_file}")
        
    except Exception as e:
        logger.error(f"Process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 