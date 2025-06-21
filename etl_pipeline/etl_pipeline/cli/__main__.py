"""
CLI module entry point for running etl_pipeline.cli as a module.

This allows running the CLI via:
    python -m etl_pipeline.cli --help
    python -m etl_pipeline.cli run --config config.yaml
"""

from etl_pipeline.cli.main import cli

def main():
    """Main CLI entry point."""
    cli()

if __name__ == "__main__":
    main() 