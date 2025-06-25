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

def main():
    """Main CLI interface."""
    parser = argparse.ArgumentParser(description="Update pipeline.yml configuration")
    parser.add_argument('--config-path', default='etl_pipeline/config/pipeline.yml', 
                       help='Path to pipeline.yml file')
    parser.add_argument('--show', action='store_true', help='Show current configuration')
    parser.add_argument('--section', help='Show specific section only')
    parser.add_argument('--validate', action='store_true', help='Validate configuration')
    parser.add_argument('--add-setting', nargs=3, metavar=('SECTION', 'KEY', 'VALUE'),
                       help='Add/update a setting (section key value)')
    parser.add_argument('--add-nested', nargs=2, metavar=('PATH', 'VALUE'),
                       help='Add/update nested setting using dot notation (e.g., general.batch_size 2000)')
    parser.add_argument('--add-connection', nargs=2, metavar=('DB_TYPE', 'CONFIG_FILE'),
                       help='Add connection config from JSON file')
    parser.add_argument('--add-stage', nargs=2, metavar=('STAGE', 'CONFIG_FILE'),
                       help='Add stage config from JSON file')
    parser.add_argument('--add-alert', nargs=2, metavar=('ALERT_TYPE', 'CONFIG_FILE'),
                       help='Add alert config from JSON file')
    parser.add_argument('--save', action='store_true', help='Save changes to file')
    
    args = parser.parse_args()
    
    # Initialize manager
    manager = PipelineConfigManager(args.config_path)
    
    # Handle commands
    if args.show:
        manager.show_config(args.section)
        return
    
    if args.validate:
        if manager.validate_config():
            print("✅ Configuration is valid")
        else:
            print("❌ Configuration validation failed")
        return
    
    # Track if we made changes
    changes_made = False
    
    if args.add_setting:
        section, key, value = args.add_setting
        # Try to convert value to appropriate type
        try:
            if value.lower() in ('true', 'false'):
                value = value.lower() == 'true'
            elif value.isdigit():
                value = int(value)
            elif value.replace('.', '').isdigit():
                value = float(value)
        except:
            pass  # Keep as string if conversion fails
        
        if manager.add_setting(section, key, value):
            changes_made = True
    
    if args.add_nested:
        path, value = args.add_nested
        # Try to convert value to appropriate type
        try:
            if value.lower() in ('true', 'false'):
                value = value.lower() == 'true'
            elif value.isdigit():
                value = int(value)
            elif value.replace('.', '').isdigit():
                value = float(value)
        except:
            pass  # Keep as string if conversion fails
        
        if manager.add_nested_setting(path, value):
            changes_made = True
    
    if args.add_connection:
        db_type, config_file = args.add_connection
        try:
            import json
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            if manager.add_connection_config(db_type, **config_data):
                changes_made = True
        except Exception as e:
            logger.error(f"Failed to load connection config: {e}")
    
    if args.add_stage:
        stage, config_file = args.add_stage
        try:
            import json
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            if manager.add_stage_config(stage, **config_data):
                changes_made = True
        except Exception as e:
            logger.error(f"Failed to load stage config: {e}")
    
    if args.add_alert:
        alert_type, config_file = args.add_alert
        try:
            import json
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            if manager.add_alert_config(alert_type, **config_data):
                changes_made = True
        except Exception as e:
            logger.error(f"Failed to load alert config: {e}")
    
    # Save if requested and changes were made
    if args.save and changes_made:
        if manager._save_config():
            print("✅ Configuration saved successfully")
        else:
            print("❌ Failed to save configuration")
            sys.exit(1)
    elif changes_made:
        print("⚠️  Changes made but not saved. Use --save to persist changes.")
    elif not args.show and not args.validate:
        print("No changes specified. Use --help for usage information.")

if __name__ == "__main__":
    main() 