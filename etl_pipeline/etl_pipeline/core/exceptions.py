"""
Custom exceptions for the ETL pipeline.
Provides specific exception types for different error scenarios.

CLEANED UP AND SIMPLIFIED
=========================
This exceptions module has been cleaned up to remove unused exception classes.

Current Status:
- ETLPipelineError: Base exception for ETL pipeline errors (ACTIVELY USED)
- Removed 12 unused exception classes to reduce complexity
- Simplified to only include what's actually needed
- Exception hierarchy removed in favor of standard Python exceptions

Usage Analysis:
- ETLPipelineError: Used in main.py for configuration/health check failures
- All other custom exceptions were unused and have been removed
- Codebase uses standard Python exceptions (ValueError, RuntimeError, etc.)

Benefits of Cleanup:
- Reduced code complexity and maintenance burden
- Follows YAGNI principle (You Aren't Gonna Need It)
- Simplified imports and exports
- Clearer codebase with only necessary components

Future Considerations:
- Add specific exceptions only when actually needed
- Use standard Python exceptions when possible
- Implement comprehensive exception handling patterns when required
"""

class ETLPipelineError(Exception):
    """Base exception for ETL pipeline errors."""
    pass