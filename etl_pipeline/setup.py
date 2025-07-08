#!/usr/bin/env python3
"""
Setup script for ETL Pipeline package.
"""

from setuptools import setup, find_packages

setup(
    name="etl_pipeline",
    version="0.1.0",
    description="ETL Pipeline for Dental Clinic Data",
    author="Your Name",
    packages=find_packages(),
    install_requires=[
        "click>=8.0.0",
        "sqlalchemy>=1.4.0",
        "pymysql>=1.0.0",
        "psycopg2-binary>=2.9.0",
        "pandas>=1.3.0",
        "pyyaml>=6.0",
        "tabulate>=0.8.0",
        "pytest>=7.0.0",
        "pytest-cov>=4.0.0",
        "pytest-order>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-order>=1.0.0",
            "black>=22.0.0",
            "isort>=5.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "etl=etl_pipeline.cli.main:main",
        ],
    },
    python_requires=">=3.8",
) 