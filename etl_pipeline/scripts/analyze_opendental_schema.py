"""
OpenDental Schema Analysis Script

This is THE single source of schema analysis for the entire ETL pipeline.
All schema analysis, table relationship discovery, and configuration generation
happens here through the SchemaDiscovery class.

Usage:
    python etl_pipeline/scripts/analyze_opendental_schema.py

Output:
    - etl_pipeline/config/tables.yml (pipeline configuration)
    - etl_pipeline/logs/schema_analysis_YYYYMMDD_HHMMSS.json (detailed analysis)
    - Console summary report

This script replaces generate_table_config.py entirely.
"""

import os
import yaml
import json
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, List
from tqdm import tqdm

from etl_pipeline.core.schema_discovery import SchemaDiscovery
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

class OpenDentalSchemaAnalyzer:
    """Coordinates complete OpenDental schema analysis using SchemaDiscovery."""
    
    def __init__(self):
        """Initialize with source database connection only."""
        self.source_engine = ConnectionFactory.get_opendental_source_connection()
        self.source_db = os.getenv('OPENDENTAL_SOURCE_DB')
        
        if not self.source_db:
            raise ValueError("OPENDENTAL_SOURCE_DB environment variable is required")
        
        # Get dbt project root (current directory)
        dbt_project_root = str(Path(__file__).parent.parent.parent)
        
        # Single SchemaDiscovery instance with dbt project root
        self.schema_discovery = SchemaDiscovery(
            source_engine=self.source_engine,
            source_db=self.source_db,
            dbt_project_root=dbt_project_root
        )
    
    def analyze_complete_schema(self, output_dir: str = 'etl_pipeline/config') -> Dict[str, str]:
        """Perform complete schema analysis and generate all outputs."""
        logger.info("Starting complete OpenDental schema analysis...")
        
        try:
            # Ensure output directories exist
            os.makedirs(output_dir, exist_ok=True)
            os.makedirs('logs', exist_ok=True)
            
            # Discover all tables
            logger.info("Discovering tables...")
            all_tables = self.schema_discovery.discover_all_tables()
            
            # Filter out excluded tables
            tables = [t for t in all_tables if not any(pattern in t.lower() for pattern in EXCLUDED_PATTERNS)]
            total_tables = len(tables)
            logger.info(f"Found {total_tables} tables to process (filtered from {len(all_tables)} total)")
            
            # Generate complete configuration using SchemaDiscovery
            logger.info("Generating complete configuration...")
            config = self.schema_discovery.generate_complete_configuration(output_dir)
            
            # Save tables.yml
            tables_yml_path = os.path.join(output_dir, 'tables.yml')
            with open(tables_yml_path, 'w') as f:
                yaml.dump(config, f, default_flow_style=False, sort_keys=False)
            
            # Generate detailed analysis report
            logger.info("Generating detailed analysis report...")
            analysis_report = self._generate_detailed_analysis_report(tables, config)
            
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
    
    def _generate_detailed_analysis_report(self, tables: List[str], config: Dict) -> Dict:
        """Generate detailed analysis report with all metadata."""
        logger.info("Analyzing table relationships and usage patterns...")
        
        # Get detailed analysis from SchemaDiscovery
        analysis = self.schema_discovery.analyze_complete_schema(tables)
        
        # Add configuration metadata
        analysis['configuration_metadata'] = {
            'generated_at': datetime.now().isoformat(),
            'total_tables_configured': len(config.get('tables', {})),
            'configuration_version': '3.0',
            'schema_discovery_version': '3.0'
        }
        
        # Add table-by-table configuration details
        analysis['table_configurations'] = {}
        for table_name in tables:
            table_config = config.get('tables', {}).get(table_name, {})
            analysis['table_configurations'][table_name] = {
                'configuration': table_config,
                'schema_info': self.schema_discovery.get_table_schema(table_name),
                'size_info': self.schema_discovery.get_table_size_info(table_name),
                'incremental_columns': self.schema_discovery.get_incremental_columns(table_name)
            }
        
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
- Streaming Incremental: {strategy_stats.get('streaming_incremental', 0)}

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