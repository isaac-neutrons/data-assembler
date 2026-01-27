"""
Command-line interface for data-assembler.

Provides commands for ingesting, validating, and processing
reflectivity data into the lakehouse.
"""

from .main import app, main

__all__ = ["main", "app"]
