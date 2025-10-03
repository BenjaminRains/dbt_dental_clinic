# DBT model metadata and lineage information
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from database import Base
from sqlalchemy.dialects.postgresql import JSONB

class DBTModelMetadata(Base):
    """Model metadata from DBT documentation and lineage"""
    __tablename__ = "dbt_model_metadata"
    
    id = Column(Integer, primary_key=True, index=True)
    model_name = Column(String(255), unique=True, index=True, nullable=False)
    model_type = Column(String(50), nullable=False)  # 'staging', 'intermediate', 'mart', 'dimension'
    schema_name = Column(String(100), nullable=False)
    description = Column(Text)
    business_context = Column(Text)
    technical_specs = Column(Text)
    dependencies = Column(JSONB)  # Array of dependent model names
    downstream_models = Column(JSONB)  # Array of models that depend on this one
    data_quality_notes = Column(Text)
    refresh_frequency = Column(String(50))
    grain_definition = Column(Text)
    source_tables = Column(JSONB)  # Array of source table names
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    is_active = Column(Boolean, default=True)

class DBTMetricLineage(Base):
    """Mapping of UI metrics to their DBT model sources"""
    __tablename__ = "dbt_metric_lineage"
    
    id = Column(Integer, primary_key=True, index=True)
    metric_name = Column(String(255), nullable=False, index=True)
    metric_display_name = Column(String(255), nullable=False)
    source_model = Column(String(255), nullable=False)
    source_column = Column(String(255))
    calculation_logic = Column(Text)
    business_definition = Column(Text)
    data_freshness = Column(String(100))
    last_updated = Column(DateTime)
    is_active = Column(Boolean, default=True)
