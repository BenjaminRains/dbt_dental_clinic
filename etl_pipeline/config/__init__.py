"""
Configuration package for the ETL pipeline.
Contains database, pipeline, and table configurations.
"""
from .database import DatabaseConfig
from etl_pipeline.loaders.config_loader import PipelineConfig

__all__ = ['DatabaseConfig', 'PipelineConfig'] 