"""
Step 1.1: Create etl_pipeline/config/providers.py

This file provides the configuration provider pattern for dependency injection.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
from pathlib import Path
import os
import yaml
import logging

logger = logging.getLogger(__name__)


class ConfigProvider(ABC):
    """Abstract configuration provider interface."""
    
    @abstractmethod
    def get_config(self, config_type: str) -> Dict[str, Any]:
        """Get configuration by type (pipeline, tables, env)."""
        pass


class FileConfigProvider(ConfigProvider):
    """File-based configuration provider."""
    
    def __init__(self, config_dir: Path):
        self.config_dir = Path(config_dir)
    
    def get_config(self, config_type: str) -> Dict[str, Any]:
        """Load configuration from files or environment."""
        if config_type == 'pipeline':
            return self._load_yaml_config('pipeline')
        elif config_type == 'tables':
            return self._load_yaml_config('tables')
        elif config_type == 'env':
            return dict(os.environ)
        else:
            raise ValueError(f"Unknown config type: {config_type}")
    
    def _load_yaml_config(self, name: str) -> Dict[str, Any]:
        """Load YAML configuration file."""
        for ext in ['.yml', '.yaml']:
            config_path = self.config_dir / f"{name}{ext}"
            if config_path.exists():
                try:
                    with open(config_path, 'r') as f:
                        return yaml.safe_load(f) or {}
                except Exception as e:
                    logger.error(f"Failed to load {name} config: {e}")
                    return {}
        
        logger.warning(f"No {name} config file found in {self.config_dir}")
        return {}


class DictConfigProvider(ConfigProvider):
    """Dictionary-based configuration provider for testing."""
    
    def __init__(self, **configs):
        """Initialize with configuration dictionaries."""
        self.configs = {
            'pipeline': configs.get('pipeline', {}),
            'tables': configs.get('tables', {'tables': {}}),
            'env': configs.get('env', {})
        }
    
    def get_config(self, config_type: str) -> Dict[str, Any]:
        """Get configuration by type."""
        return self.configs.get(config_type, {})