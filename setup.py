from setuptools import setup, find_packages

setup(
    name="elt_pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "click",
        "pyyaml",
        "python-dotenv",
        "sqlalchemy",
        "psycopg2-binary",
        "pymysql",
        "pandas",
        "tabulate"
    ],
    entry_points={
        "console_scripts": [
            "elt=elt_pipeline:main",
        ],
    },
) 