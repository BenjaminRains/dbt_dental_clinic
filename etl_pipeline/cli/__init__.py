"""ETL Pipeline CLI Module"""

def get_cli():
    """Lazy import of CLI to avoid circular dependencies."""
    from .main import cli
    return cli

__all__ = ['get_cli'] 