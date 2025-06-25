#!/usr/bin/env python3
"""
Script to update pipeline.yml configuration.
This script provides a programmatic way to add, modify, or validate pipeline configuration.
"""

import yaml
import argparse
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PipelineConfigManager:
    """Manages pipeline.yml configuration updates."""
    
    def __init__(self, config_path: str = "etl_pipeline/config/pipeline.yml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load the current pipeline configuration."""
        if not self.config_path.exists():
            logger.error(f"Configuration file not found: {self.config_path}")
            return {}
        
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            return {}
    
    def _save_config(self) -> bool:
        """Save the current configuration to file."""
        try:
            # Create backup
            backup_path = self.config_path.with_suffix('.yml.backup')
            if self.config_path.exists():
                import shutil
                shutil.copy2(self.config_path, backup_path)
                logger.info(f"Backup created: {backup_path}")
            
            # Save new configuration
            with open(self.config_path, 'w') as f:
                yaml.dump(self.config, f, default_flow_style=False, indent=2)
            
            logger.info(f"Configuration saved to: {self.config_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            return False
    
    def add_setting(self, section: str, key: str, value: Any) -> bool:
        """Add or update a setting in the configuration."""
        try:
            # Navigate to section (create if doesn't exist)
            if section not in self.config:
                self.config[section] = {}
            
            # Set the value
            self.config[section][key] = value
            logger.info(f"Added/updated {section}.{key} = {value}")
            return True
        except Exception as e:
            logger.error(f"Failed to add setting: {e}")
            return False
    
    def add_nested_setting(self, path: str, value: Any) -> bool:
        """Add or update a nested setting using dot notation (e.g., 'general.batch_size')."""
        try:
            parts = path.split('.')
            current = self.config
            
            # Navigate to the parent level
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            # Set the final value
            current[parts[-1]] = value
            logger.info(f"Added/updated {path} = {value}")
            return True
        except Exception as e:
            logger.error(f"Failed to add nested setting: {e}")
            return False
    
    def add_connection_config(self, db_type: str, **kwargs) -> bool:
        """Add or update database connection configuration."""
        try:
            if 'connections' not in self.config:
                self.config['connections'] = {}
            
            if db_type not in self.config['connections']:
                self.config['connections'][db_type] = {}
            
            self.config['connections'][db_type].update(kwargs)
            logger.info(f"Updated connection config for {db_type}")
            return True
        except Exception as e:
            logger.error(f"Failed to add connection config: {e}")
            return False
    
    def add_stage_config(self, stage: str, **kwargs) -> bool:
        """Add or update pipeline stage configuration."""
        try:
            if 'stages' not in self.config:
                self.config['stages'] = {}
            
            if stage not in self.config['stages']:
                self.config['stages'][stage] = {}
            
            self.config['stages'][stage].update(kwargs)
            logger.info(f"Updated stage config for {stage}")
            return True
        except Exception as e:
            logger.error(f"Failed to add stage config: {e}")
            return False
    
    def add_alert_config(self, alert_type: str, **kwargs) -> bool:
        """Add or update alerting configuration."""
        try:
            if 'monitoring' not in self.config:
                self.config['monitoring'] = {}
            if 'alerts' not in self.config['monitoring']:
                self.config['monitoring']['alerts'] = {}
            
            if alert_type not in self.config['monitoring']['alerts']:
                self.config['monitoring']['alerts'][alert_type] = {}
            
            self.config['monitoring']['alerts'][alert_type].update(kwargs)
            logger.info(f"Updated alert config for {alert_type}")
            return True
        except Exception as e:
            logger.error(f"Failed to add alert config: {e}")
            return False
    
    def validate_config(self) -> bool:
        """Validate the current configuration structure."""
        try:
            required_sections = ['general', 'connections', 'stages']
            missing_sections = [section for section in required_sections if section not in self.config]
            
            if missing_sections:
                logger.warning(f"Missing required sections: {missing_sections}")
                return False
            
            logger.info("Configuration validation passed")
            return True
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False
    
    def show_config(self, section: Optional[str] = None) -> None:
        """Display the current configuration."""
        try:
            if section:
                if section in self.config:
                    print(f"\n=== {section.upper()} ===")
                    print(yaml.dump(self.config[section], default_flow_style=False, indent=2))
                else:
                    logger.warning(f"Section '{section}' not found")
            else:
                print("\n=== FULL CONFIGURATION ===")
                print(yaml.dump(self.config, default_flow_style=False, indent=2))
        except Exception as e:
            logger.error(f"Failed to show configuration: {e}")

def validate_configuration(config):
    """Validate the configuration structure."""
    try:
        # Check required top-level keys
        required_keys = ['metadata', 'tables']
        for key in required_keys:
            if key not in config:
                print(f"Missing required key: {key}")
                return False
        
        # Validate metadata
        metadata = config.get('metadata', {})
        if not isinstance(metadata, dict):
            print("Metadata must be a dictionary")
            return False
        
        # Validate tables
        tables = config.get('tables', {})
        if not isinstance(tables, dict):
            print("Tables must be a dictionary")
            return False
        
        # Validate each table configuration
        for table_name, table_config in tables.items():
            if not isinstance(table_config, dict):
                print(f"Table {table_name} configuration must be a dictionary")
                return False
            
            # Check required table keys
            required_table_keys = ['columns', 'incremental_column', 'batch_size']
            for key in required_table_keys:
                if key not in table_config:
                    print(f"Table {table_name} missing required key: {key}")
                    return False
        
        print("Configuration is valid")
        return True
        
    except Exception as e:
        print(f"Configuration validation failed: {str(e)}")
        return False

def save_configuration(config, output_path):
    """Save configuration to file."""
    try:
        with open(output_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        print("Configuration saved successfully")
        return True
    except Exception as e:
        print(f"Failed to save configuration: {str(e)}")
        return False

def main():
    """Main function to update pipeline configuration."""
    parser = argparse.ArgumentParser(description='Update pipeline configuration')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--save', action='store_true', help='Save changes to file')
    parser.add_argument('--table', help='Specific table to update')
    parser.add_argument('--incremental-column', help='Set incremental column for table')
    parser.add_argument('--batch-size', type=int, help='Set batch size for table')
    parser.add_argument('--priority', choices=['high', 'medium', 'low'], help='Set table priority')
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"Failed to load configuration: {str(e)}")
        return 1
    
    # Validate configuration
    if not validate_configuration(config):
        return 1
    
    # Apply updates
    changes_made = False
    
    if args.table:
        if args.table not in config.get('tables', {}):
            print(f"Table {args.table} not found in configuration")
            return 1
        
        table_config = config['tables'][args.table]
        
        if args.incremental_column:
            table_config['incremental_column'] = args.incremental_column
            changes_made = True
            print(f"Updated incremental column for {args.table}: {args.incremental_column}")
        
        if args.batch_size:
            table_config['batch_size'] = args.batch_size
            changes_made = True
            print(f"Updated batch size for {args.table}: {args.batch_size}")
        
        if args.priority:
            table_config['priority'] = args.priority
            changes_made = True
            print(f"Updated priority for {args.table}: {args.priority}")
    
    # Save if requested
    if args.save and changes_made:
        if save_configuration(config, args.config):
            return 0
        else:
            return 1
    elif changes_made:
        print("Changes made but not saved. Use --save to persist changes.")
    
    return 0

if __name__ == "__main__":
    main() 