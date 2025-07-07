"""
OpenDental Schema Analysis Script

This is THE single source of schema analysis for the entire ETL pipeline.
All schema analysis, table relationship discovery, and configuration generation
happens here using direct database connections and modern architecture.

Usage:
    python etl_pipeline/scripts/analyze_opendental_schema.py

Output:
    - etl_pipeline/config/tables.yml (pipeline configuration)
    - etl_pipeline/logs/schema_analysis_YYYYMMDD_HHMMSS.json (detailed analysis)
    - Console summary report

This script uses modern connection handling and direct database analysis.
"""

import os
import yaml
import json
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine

from etl_pipeline.core.connections import ConnectionFactory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/schema_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants for filtering tables
EXCLUDED_PATTERNS = ['tmp_', 'temp_', 'backup_', '#', 'test_']

# DBT model patterns for discovery
DBT_STAGING_PATTERNS = ['stg_opendental__*.sql', 'stg_*.sql']
DBT_MART_PATTERNS = ['dim_*.sql', 'fact_*.sql', 'mart_*.sql']
DBT_INTERMEDIATE_PATTERNS = ['int_*.sql', 'intermediate_*.sql']

class OpenDentalSchemaAnalyzer:
    """Coordinates complete OpenDental schema analysis using direct database connections."""
    
    def __init__(self):
        """Initialize with source database connection using modern connection handling."""
        # Use explicit production connection
        self.source_engine = ConnectionFactory.get_opendental_source_connection()
        self.source_db = os.getenv('OPENDENTAL_SOURCE_DB')
        
        if not self.source_db:
            raise ValueError("OPENDENTAL_SOURCE_DB environment variable is required")
        
        # Get dbt project root (current directory)
        self.dbt_project_root = str(Path(__file__).parent.parent.parent)
        
        # Initialize inspector for schema analysis
        self.inspector = inspect(self.source_engine)
    
    def discover_all_tables(self) -> List[str]:
        """Discover all tables in the source database."""
        try:
            tables = self.inspector.get_table_names()
            logger.info(f"Discovered {len(tables)} tables in database")
            return tables
        except Exception as e:
            logger.error(f"Failed to discover tables: {e}")
            raise
    
    def get_table_schema(self, table_name: str) -> Dict:
        """Get detailed schema information for a table."""
        try:
            columns = self.inspector.get_columns(table_name)
            primary_keys = self.inspector.get_pk_constraint(table_name)
            foreign_keys = self.inspector.get_foreign_keys(table_name)
            indexes = self.inspector.get_indexes(table_name)
            
            return {
                'table_name': table_name,
                'columns': {col['name']: {
                    'type': str(col['type']),
                    'nullable': col['nullable'],
                    'default': col['default'],
                    'primary_key': col['name'] in primary_keys.get('constrained_columns', [])
                } for col in columns},
                'primary_keys': primary_keys.get('constrained_columns', []),
                'foreign_keys': foreign_keys,
                'indexes': indexes
            }
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return {'table_name': table_name, 'error': str(e)}
    
    def get_table_size_info(self, table_name: str) -> Dict:
        """Get table size and row count information."""
        try:
            with self.source_engine.connect() as conn:
                # Get row count
                row_count_result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                row_count = row_count_result.scalar()
                
                # Get table size (approximate)
                size_result = conn.execute(text(f"""
                    SELECT 
                        ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
                    FROM information_schema.tables 
                    WHERE table_schema = '{self.source_db}' 
                    AND table_name = '{table_name}'
                """))
                size_mb = size_result.scalar() or 0
                
                return {
                    'table_name': table_name,
                    'row_count': row_count,
                    'size_mb': size_mb,
                    'source': 'database_query'
                }
        except Exception as e:
            logger.error(f"Failed to get size info for table {table_name}: {e}")
            return {
                'table_name': table_name,
                'row_count': 0,
                'size_mb': 0,
                'error': str(e)
            }
    
    def discover_dbt_models(self) -> Dict[str, List[str]]:
        """Discover dbt models in the project."""
        dbt_models = {
            'staging': [],
            'mart': [],
            'intermediate': []
        }
        
        try:
            models_dir = Path(self.dbt_project_root) / 'models'
            if not models_dir.exists():
                logger.warning(f"dbt models directory not found: {models_dir}")
                return dbt_models
            
            # Discover staging models
            staging_dir = models_dir / 'staging'
            if staging_dir.exists():
                for sql_file in staging_dir.rglob('*.sql'):
                    dbt_models['staging'].append(sql_file.stem)
            
            # Discover mart models
            marts_dir = models_dir / 'marts'
            if marts_dir.exists():
                for sql_file in marts_dir.rglob('*.sql'):
                    dbt_models['mart'].append(sql_file.stem)
            
            # Discover intermediate models
            intermediate_dir = models_dir / 'intermediate'
            if intermediate_dir.exists():
                for sql_file in intermediate_dir.rglob('*.sql'):
                    dbt_models['intermediate'].append(sql_file.stem)
            
            logger.info(f"Discovered dbt models: {sum(len(models) for models in dbt_models.values())} total")
            return dbt_models
            
        except Exception as e:
            logger.error(f"Failed to discover dbt models: {e}")
            return dbt_models
    
    def determine_table_importance(self, table_name: str, schema_info: Dict, size_info: Dict) -> str:
        """Determine table importance based on schema and size analysis."""
        # Critical tables (core business entities)
        critical_tables = ['patient', 'appointment', 'procedurelog', 'claimproc', 'payment']
        if table_name.lower() in critical_tables:
            return 'critical'
        
        # Important tables (frequently used)
        important_tables = ['provider', 'carrier', 'insplan', 'patplan', 'fee', 'procedurecode']
        if table_name.lower() in important_tables:
            return 'important'
        
        # Large tables (performance consideration)
        if size_info.get('row_count', 0) > 1000000:  # 1M+ rows
            return 'important'
        
        # Reference tables (lookup data)
        if 'def' in table_name.lower() or 'type' in table_name.lower():
            return 'reference'
        
        # Audit tables (logging/history)
        if 'log' in table_name.lower() or 'hist' in table_name.lower():
            return 'audit'
        
        return 'standard'
    
    def determine_extraction_strategy(self, table_name: str, schema_info: Dict, size_info: Dict) -> str:
        """Determine optimal extraction strategy for a table."""
        row_count = size_info.get('row_count', 0)
        
        # Small tables: full table
        if row_count < 10000:
            return 'full_table'
        
        # Medium tables: incremental if possible
        if row_count < 1000000:
            # Check for timestamp or ID columns for incremental
            columns = schema_info.get('columns', {})
            incremental_candidates = ['date_created', 'date_modified', 'date_added', 'id']
            
            for col in incremental_candidates:
                if col in columns:
                    return 'incremental'
            
            return 'full_table'
        
        # Large tables: chunked incremental
        return 'chunked_incremental'
    
    def find_incremental_columns(self, table_name: str, schema_info: Dict) -> List[str]:
        """Find suitable incremental columns for a table."""
        columns = schema_info.get('columns', {})
        incremental_candidates = []
        
        # Look for timestamp columns
        for col_name, col_info in columns.items():
            col_type = str(col_info.get('type', '')).lower()
            if any(time_type in col_type for time_type in ['timestamp', 'datetime', 'date']):
                incremental_candidates.append(col_name)
        
        # Look for ID columns that might be auto-incrementing
        for col_name in columns.keys():
            if col_name.lower() in ['id', 'pk', 'primary_key']:
                incremental_candidates.append(col_name)
        
        return incremental_candidates[:3]  # Return top 3 candidates
    
    def generate_complete_configuration(self, output_dir: str) -> Dict:
        """Generate complete configuration for all tables."""
        logger.info("Generating complete configuration...")
        
        # Discover dbt models
        dbt_models = self.discover_dbt_models()
        
        # Discover tables
        all_tables = self.discover_all_tables()
        tables = [t for t in all_tables if not any(pattern in t.lower() for pattern in EXCLUDED_PATTERNS)]
        
        config = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'source_database': self.source_db,
                'total_tables': len(tables),
                'configuration_version': '3.0',
                'analyzer_version': '3.0'
            },
            'tables': {}
        }
        
        # Process each table
        for table_name in tqdm(tables, desc="Analyzing tables"):
            try:
                # Get schema and size information
                schema_info = self.get_table_schema(table_name)
                size_info = self.get_table_size_info(table_name)
                
                # Determine table characteristics
                importance = self.determine_table_importance(table_name, schema_info, size_info)
                extraction_strategy = self.determine_extraction_strategy(table_name, schema_info, size_info)
                incremental_columns = self.find_incremental_columns(table_name, schema_info)
                
                # Check if table has dbt models
                is_modeled = False
                dbt_model_types = []
                
                # Check staging models
                staging_models = [model for model in dbt_models['staging'] 
                                if table_name.lower() in model.lower()]
                if staging_models:
                    is_modeled = True
                    dbt_model_types.append('staging')
                
                # Check mart models
                mart_models = [model for model in dbt_models['mart'] 
                             if table_name.lower() in model.lower()]
                if mart_models:
                    is_modeled = True
                    dbt_model_types.append('mart')
                
                # Check intermediate models
                intermediate_models = [model for model in dbt_models['intermediate'] 
                                    if table_name.lower() in model.lower()]
                if intermediate_models:
                    is_modeled = True
                    dbt_model_types.append('intermediate')
                
                # Build table configuration
                table_config = {
                    'table_name': table_name,
                    'table_importance': importance,
                    'extraction_strategy': extraction_strategy,
                    'estimated_rows': size_info.get('row_count', 0),
                    'estimated_size_mb': size_info.get('size_mb', 0),
                    'batch_size': 10000 if size_info.get('row_count', 0) > 100000 else 5000,
                    'incremental_column': incremental_columns[0] if incremental_columns else None,
                    'incremental_columns': incremental_columns,
                    'is_modeled': is_modeled,
                    'dbt_model_types': dbt_model_types,
                    'monitoring': {
                        'alert_on_failure': importance in ['critical', 'important'],
                        'alert_on_slow_extraction': size_info.get('row_count', 0) > 100000
                    },
                    'schema_hash': hash(str(schema_info)),  # Simple hash for change detection
                    'last_analyzed': datetime.now().isoformat()
                }
                
                config['tables'][table_name] = table_config
                
            except Exception as e:
                logger.error(f"Failed to process table {table_name}: {e}")
                # Add error entry
                config['tables'][table_name] = {
                    'table_name': table_name,
                    'error': str(e),
                    'table_importance': 'standard',
                    'extraction_strategy': 'full_table'
                }
        
        return config
    
    def analyze_complete_schema(self, output_dir: str = 'etl_pipeline/config') -> Dict[str, str]:
        """Perform complete schema analysis and generate all outputs."""
        logger.info("Starting complete OpenDental schema analysis...")
        
        try:
            # Ensure output directories exist
            os.makedirs(output_dir, exist_ok=True)
            os.makedirs('logs', exist_ok=True)
            
            # Generate complete configuration
            config = self.generate_complete_configuration(output_dir)
            
            # Save tables.yml
            tables_yml_path = os.path.join(output_dir, 'tables.yml')
            with open(tables_yml_path, 'w') as f:
                yaml.dump(config, f, default_flow_style=False, sort_keys=False)
            
            # Generate detailed analysis report
            logger.info("Generating detailed analysis report...")
            analysis_report = self._generate_detailed_analysis_report(config)
            
            # Save detailed analysis
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            analysis_path = f'logs/schema_analysis_{timestamp}.json'
            with open(analysis_path, 'w') as f:
                json.dump(analysis_report, f, indent=2, default=str)
            
            # Generate summary report
            self._generate_summary_report(config, tables_yml_path, analysis_path)
            
            logger.info("Schema analysis completed successfully!")
            
            return {
                'tables_config': tables_yml_path,
                'analysis_report': analysis_path,
                'analysis_log': f'logs/schema_analysis_{timestamp}.log'
            }
            
        except Exception as e:
            logger.error(f"Schema analysis failed: {str(e)}")
            raise
    
    def _generate_detailed_analysis_report(self, config: Dict) -> Dict:
        """Generate detailed analysis report with all metadata."""
        logger.info("Generating detailed analysis report...")
        
        analysis = {
            'analysis_metadata': {
                'generated_at': datetime.now().isoformat(),
                'source_database': self.source_db,
                'total_tables_analyzed': len(config.get('tables', {})),
                'analyzer_version': '3.0'
            },
            'table_analysis': {},
            'dbt_model_analysis': self.discover_dbt_models(),
            'performance_analysis': {},
            'recommendations': []
        }
        
        # Analyze each table
        for table_name, table_config in config.get('tables', {}).items():
            if 'error' not in table_config:
                analysis['table_analysis'][table_name] = {
                    'schema_info': self.get_table_schema(table_name),
                    'size_info': self.get_table_size_info(table_name),
                    'configuration': table_config
                }
        
        # Generate recommendations
        total_tables = len(config.get('tables', {}))
        critical_tables = sum(1 for t in config.get('tables', {}).values() 
                            if t.get('table_importance') == 'critical')
        modeled_tables = sum(1 for t in config.get('tables', {}).values() 
                           if t.get('is_modeled', False))
        
        analysis['recommendations'] = [
            f"Critical tables identified: {critical_tables}",
            f"Tables with dbt models: {modeled_tables} ({modeled_tables/total_tables*100:.1f}%)",
            "Consider adding dbt models for critical tables without models" if critical_tables > modeled_tables else "Good dbt model coverage"
        ]
        
        return analysis
    
    def _generate_summary_report(self, config: Dict, output_path: str, analysis_path: str) -> None:
        """Generate and display summary report."""
        tables = config.get('tables', {})
        total_tables = len(tables)
        
        # Calculate statistics
        importance_stats = {}
        for table_config in tables.values():
            importance = table_config.get('table_importance', 'standard')
            importance_stats[importance] = importance_stats.get(importance, 0) + 1
        
        # Total size estimates
        total_size_mb = sum(table.get('estimated_size_mb', 0) for table in tables.values())
        total_rows = sum(table.get('estimated_rows', 0) for table in tables.values())
        
        # Monitoring stats
        monitored_tables = sum(1 for table in tables.values() 
                             if table.get('monitoring', {}).get('alert_on_failure', False))
        
        # DBT modeling stats
        modeled_tables = sum(1 for table in tables.values() 
                           if table.get('is_modeled', False))
        dbt_model_types = {}
        for table_config in tables.values():
            model_types = table_config.get('dbt_model_types', [])
            for model_type in model_types:
                dbt_model_types[model_type] = dbt_model_types.get(model_type, 0) + 1
        
        # Extraction strategy stats
        strategy_stats = {}
        for table_config in tables.values():
            strategy = table_config.get('extraction_strategy', 'full_table')
            strategy_stats[strategy] = strategy_stats.get(strategy, 0) + 1
        
        # Generate report
        report = f"""
OpenDental Schema Analysis Summary
=================================

Total Tables Analyzed: {total_tables:,}
Total Estimated Size: {total_size_mb:,.1f} MB
Total Estimated Rows: {total_rows:,}

Table Classification:
- Critical: {importance_stats.get('critical', 0)}
- Important: {importance_stats.get('important', 0)}
- Reference: {importance_stats.get('reference', 0)}
- Audit: {importance_stats.get('audit', 0)}
- Standard: {importance_stats.get('standard', 0)}

DBT Modeling Status:
- Tables with DBT Models: {modeled_tables} ({modeled_tables/total_tables*100:.1f}%)
- Staging Models: {dbt_model_types.get('staging', 0)}
- Mart Models: {dbt_model_types.get('mart', 0)}

Extraction Strategies:
- Incremental: {strategy_stats.get('incremental', 0)}
- Full Table: {strategy_stats.get('full_table', 0)}
- Chunked Incremental: {strategy_stats.get('chunked_incremental', 0)}

Tables with Monitoring: {monitored_tables}

Configuration saved to: {output_path}
Detailed analysis saved to: {analysis_path}

Ready to run ETL pipeline!
"""
        
        # Print to console
        print(report)
        
        # Save to file
        report_path = output_path.replace('.yml', '_summary.txt')
        with open(report_path, 'w') as f:
            f.write(report)
        
        logger.info(f"Summary report saved to: {report_path}")

def main():
    """Main function - generate complete schema analysis and configuration."""
    try:
        logger.info("Starting OpenDental Schema Analysis")
        logger.info("=" * 50)
        
        # Create analyzer and run complete analysis
        analyzer = OpenDentalSchemaAnalyzer()
        results = analyzer.analyze_complete_schema()
        
        print(f"\nAnalysis complete! Files generated:")
        for name, path in results.items():
            print(f"   {name}: {path}")
            
        logger.info("Schema analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 