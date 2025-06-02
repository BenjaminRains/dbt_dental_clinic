"""
Business rules transformer for data quality rules and business logic transformations.
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

class BusinessRulesTransformer:
    """Handles business rules and data quality transformations."""
    
    def __init__(self, source_engine: Engine, target_engine: Engine):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self._rule_cache = {}
    
    def apply_business_rules(self, table_name: str, rules: List[Dict[str, Any]]) -> bool:
        """
        Apply business rules to a table.
        
        Args:
            table_name: Name of the table
            rules: List of business rules to apply
            
        Returns:
            bool: True if rules were applied successfully
        """
        try:
            for rule in rules:
                if not self._apply_rule(table_name, rule):
                    logger.error(f"Failed to apply rule {rule['name']} to table {table_name}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error applying business rules to table {table_name}: {str(e)}")
            return False
    
    def _apply_rule(self, table_name: str, rule: Dict[str, Any]) -> bool:
        """
        Apply a single business rule.
        
        Args:
            table_name: Name of the table
            rule: Rule definition
            
        Returns:
            bool: True if rule was applied successfully
        """
        try:
            rule_type = rule.get('type', 'validation')
            
            if rule_type == 'validation':
                return self._apply_validation_rule(table_name, rule)
            elif rule_type == 'transformation':
                return self._apply_transformation_rule(table_name, rule)
            elif rule_type == 'calculation':
                return self._apply_calculation_rule(table_name, rule)
            else:
                logger.error(f"Unknown rule type: {rule_type}")
                return False
                
        except Exception as e:
            logger.error(f"Error applying rule {rule.get('name')}: {str(e)}")
            return False
    
    def _apply_validation_rule(self, table_name: str, rule: Dict[str, Any]) -> bool:
        """Apply a validation rule."""
        try:
            query = f"""
            SELECT COUNT(*) as invalid_count
            FROM {table_name}
            WHERE {rule['condition']}
            """
            
            with self.target_engine.connect() as conn:
                result = conn.execute(text(query))
                invalid_count = result.scalar()
            
            if invalid_count > rule.get('threshold', 0):
                logger.error(
                    f"Validation rule {rule['name']} failed: "
                    f"{invalid_count} invalid records found"
                )
                return False
            
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Error applying validation rule {rule['name']}: {str(e)}")
            return False
    
    def _apply_transformation_rule(self, table_name: str, rule: Dict[str, Any]) -> bool:
        """Apply a transformation rule."""
        try:
            query = f"""
            UPDATE {table_name}
            SET {rule['update_expression']}
            WHERE {rule['condition']}
            """
            
            with self.target_engine.connect() as conn:
                result = conn.execute(text(query))
                rows_updated = result.rowcount
            
            logger.info(
                f"Applied transformation rule {rule['name']}: "
                f"updated {rows_updated} rows"
            )
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Error applying transformation rule {rule['name']}: {str(e)}")
            return False
    
    def _apply_calculation_rule(self, table_name: str, rule: Dict[str, Any]) -> bool:
        """Apply a calculation rule."""
        try:
            query = f"""
            UPDATE {table_name}
            SET {rule['target_column']} = {rule['calculation']}
            WHERE {rule.get('condition', 'TRUE')}
            """
            
            with self.target_engine.connect() as conn:
                result = conn.execute(text(query))
                rows_updated = result.rowcount
            
            logger.info(
                f"Applied calculation rule {rule['name']}: "
                f"updated {rows_updated} rows"
            )
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Error applying calculation rule {rule['name']}: {str(e)}")
            return False
    
    def validate_data_quality(self, table_name: str, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate data quality against a set of rules.
        
        Args:
            table_name: Name of the table
            rules: List of data quality rules
            
        Returns:
            Dict[str, Any]: Validation results
        """
        try:
            results = {
                'table_name': table_name,
                'timestamp': datetime.now(),
                'rules': [],
                'passed': True
            }
            
            for rule in rules:
                rule_result = self._validate_rule(table_name, rule)
                results['rules'].append(rule_result)
                
                if not rule_result['passed']:
                    results['passed'] = False
            
            return results
            
        except Exception as e:
            logger.error(f"Error validating data quality for table {table_name}: {str(e)}")
            return {
                'table_name': table_name,
                'timestamp': datetime.now(),
                'error': str(e),
                'passed': False
            }
    
    def _validate_rule(self, table_name: str, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a single data quality rule."""
        try:
            query = f"""
            SELECT COUNT(*) as total_count,
                   COUNT(CASE WHEN {rule['condition']} THEN 1 END) as valid_count
            FROM {table_name}
            """
            
            with self.target_engine.connect() as conn:
                result = conn.execute(text(query))
                row = result.fetchone()
                total_count = row[0]
                valid_count = row[1]
            
            passed = valid_count / total_count >= rule.get('threshold', 1.0)
            
            return {
                'name': rule['name'],
                'description': rule.get('description', ''),
                'total_count': total_count,
                'valid_count': valid_count,
                'valid_percentage': valid_count / total_count if total_count > 0 else 0,
                'threshold': rule.get('threshold', 1.0),
                'passed': passed
            }
            
        except SQLAlchemyError as e:
            logger.error(f"Error validating rule {rule['name']}: {str(e)}")
            return {
                'name': rule['name'],
                'error': str(e),
                'passed': False
            }
    
    def get_default_rules(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get default business rules for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of default rules
        """
        if table_name in self._rule_cache:
            return self._rule_cache[table_name]
        
        # Common rules for all tables
        common_rules = [
            {
                'name': 'not_null_check',
                'type': 'validation',
                'description': 'Check for null values in required columns',
                'condition': 'column_name IS NULL',
                'threshold': 0.0
            },
            {
                'name': 'data_type_check',
                'type': 'validation',
                'description': 'Validate data types',
                'condition': 'column_name::text ~ \'^[0-9]+$\'',
                'threshold': 1.0
            }
        ]
        
        # Table-specific rules
        table_rules = self._get_table_specific_rules(table_name)
        
        # Combine and cache rules
        all_rules = common_rules + table_rules
        self._rule_cache[table_name] = all_rules
        
        return all_rules
    
    def _get_table_specific_rules(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table-specific business rules."""
        # Example rules for specific tables
        rules = {
            'patient': [
                {
                    'name': 'valid_dob',
                    'type': 'validation',
                    'description': 'Check for valid date of birth',
                    'condition': 'dob <= CURRENT_DATE',
                    'threshold': 1.0
                },
                {
                    'name': 'valid_phone',
                    'type': 'validation',
                    'description': 'Check for valid phone number format',
                    'condition': 'phone ~ \'^[0-9]{10}$\'',
                    'threshold': 0.95
                }
            ],
            'appointment': [
                {
                    'name': 'valid_appointment_date',
                    'type': 'validation',
                    'description': 'Check for valid appointment dates',
                    'condition': 'appointment_date >= CURRENT_DATE',
                    'threshold': 1.0
                },
                {
                    'name': 'valid_duration',
                    'type': 'validation',
                    'description': 'Check for valid appointment duration',
                    'condition': 'duration > 0 AND duration <= 480',
                    'threshold': 1.0
                }
            ]
        }
        
        return rules.get(table_name, []) 