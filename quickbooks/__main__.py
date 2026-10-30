"""Repo-root entry point: ``python -m quickbooks`` from the repository root.

The client package lives in ``quickbooks/quickbooks``. This module is what
Python finds when the current directory is the repository root.
"""

from quickbooks.quickbooks.__main__ import main

if __name__ == "__main__":
    raise SystemExit(main())
