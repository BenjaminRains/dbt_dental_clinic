"""
Data validation utilities for the ETL pipeline.
"""
import logging
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
import re
import json
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ValidationRule:
    """Data validation rule definition."""
    name: str
    description: str
    validator: Callable[[Any], bool]
    error_message: str
    severity: str = 'error'  # error, warning, info

class DataValidator:
    """Handles data validation for the ETL pipeline."""
    
    def __init__(self):
        self.rules: Dict[str, List[ValidationRule]] = {}
        self.validation_results: Dict[str, List[Dict[str, Any]]] = {}
    
    def add_rule(self, table_name: str, rule: ValidationRule) -> None:
        """
        Add a validation rule for a table.
        
        Args:
            table_name: Name of the table
            rule: Validation rule to add
        """
        if table_name not in self.rules:
            self.rules[table_name] = []
        self.rules[table_name].append(rule)
        logger.info(f"Added validation rule {rule.name} for table {table_name}")
    
    def validate_data(self, table_name: str, data: Any) -> Dict[str, Any]:
        """
        Validate data against rules.
        
        Args:
            table_name: Name of the table
            data: Data to validate
            
        Returns:
            Dict[str, Any]: Validation results
        """
        if table_name not in self.rules:
            logger.warning(f"No validation rules found for table {table_name}")
            return {'valid': True, 'message': 'No validation rules defined'}
        
        results = {
            'table_name': table_name,
            'timestamp': datetime.now(),
            'valid': True,
            'errors': [],
            'warnings': [],
            'info': []
        }
        
        for rule in self.rules[table_name]:
            try:
                if not rule.validator(data):
                    message = {
                        'rule': rule.name,
                        'description': rule.description,
                        'message': rule.error_message
                    }
                    
                    if rule.severity == 'error':
                        results['valid'] = False
                        results['errors'].append(message)
                    elif rule.severity == 'warning':
                        results['warnings'].append(message)
                    else:
                        results['info'].append(message)
                        
            except Exception as e:
                logger.error(f"Error validating rule {rule.name}: {str(e)}")
                results['valid'] = False
                results['errors'].append({
                    'rule': rule.name,
                    'description': rule.description,
                    'message': f'Validation error: {str(e)}'
                })
        
        self.validation_results[table_name] = results
        return results
    
    def get_validation_results(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get validation results.
        
        Args:
            table_name: Optional table name to filter results
            
        Returns:
            Dict[str, Any]: Validation results
        """
        if table_name:
            return self.validation_results.get(table_name, {})
        return self.validation_results
    
    def clear_results(self, table_name: Optional[str] = None) -> None:
        """
        Clear validation results.
        
        Args:
            table_name: Optional table name to clear results for
        """
        if table_name:
            self.validation_results.pop(table_name, None)
        else:
            self.validation_results.clear()
    
    @staticmethod
    def create_numeric_rule(name: str, min_value: Optional[float] = None,
                           max_value: Optional[float] = None) -> ValidationRule:
        """Create a numeric validation rule."""
        def validator(value: Any) -> bool:
            try:
                num = float(value)
                if min_value is not None and num < min_value:
                    return False
                if max_value is not None and num > max_value:
                    return False
                return True
            except (ValueError, TypeError):
                return False
        
        return ValidationRule(
            name=name,
            description=f"Validate numeric value between {min_value} and {max_value}",
            validator=validator,
            error_message=f"Value must be between {min_value} and {max_value}"
        )
    
    @staticmethod
    def create_string_rule(name: str, min_length: Optional[int] = None,
                          max_length: Optional[int] = None,
                          pattern: Optional[str] = None) -> ValidationRule:
        """Create a string validation rule."""
        def validator(value: Any) -> bool:
            if not isinstance(value, str):
                return False
            if min_length is not None and len(value) < min_length:
                return False
            if max_length is not None and len(value) > max_length:
                return False
            if pattern is not None and not re.match(pattern, value):
                return False
            return True
        
        return ValidationRule(
            name=name,
            description=f"Validate string with length {min_length}-{max_length} and pattern {pattern}",
            validator=validator,
            error_message=f"String must match requirements"
        )
    
    @staticmethod
    def create_date_rule(name: str, min_date: Optional[datetime] = None,
                        max_date: Optional[datetime] = None) -> ValidationRule:
        """Create a date validation rule."""
        def validator(value: Any) -> bool:
            try:
                if isinstance(value, str):
                    date = datetime.fromisoformat(value)
                elif isinstance(value, datetime):
                    date = value
                else:
                    return False
                
                if min_date is not None and date < min_date:
                    return False
                if max_date is not None and date > max_date:
                    return False
                return True
            except (ValueError, TypeError):
                return False
        
        return ValidationRule(
            name=name,
            description=f"Validate date between {min_date} and {max_date}",
            validator=validator,
            error_message=f"Date must be between {min_date} and {max_date}"
        )
    
    @staticmethod
    def create_enum_rule(name: str, allowed_values: List[Any]) -> ValidationRule:
        """Create an enum validation rule."""
        def validator(value: Any) -> bool:
            return value in allowed_values
        
        return ValidationRule(
            name=name,
            description=f"Validate value is one of {allowed_values}",
            validator=validator,
            error_message=f"Value must be one of {allowed_values}"
        )
    
    @staticmethod
    def create_json_rule(name: str, schema: Dict[str, Any]) -> ValidationRule:
        """Create a JSON validation rule."""
        def validator(value: Any) -> bool:
            try:
                if isinstance(value, str):
                    data = json.loads(value)
                else:
                    data = value
                
                # Basic schema validation
                for key, expected_type in schema.items():
                    if key not in data:
                        return False
                    if not isinstance(data[key], expected_type):
                        return False
                return True
            except (json.JSONDecodeError, TypeError):
                return False
        
        return ValidationRule(
            name=name,
            description=f"Validate JSON against schema {schema}",
            validator=validator,
            error_message=f"JSON must match schema {schema}"
        ) 