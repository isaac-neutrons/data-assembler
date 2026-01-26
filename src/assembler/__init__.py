"""
Data Assembler - Automated reflectivity data ingestion workflow.

This package provides tools to ingest reflectometry data from multiple sources
and assemble them into a structured format for the scientific data lakehouse.
"""

__version__ = "0.1.0"

from assembler.compat import (
    get_parquet_schema_mapping,
    is_raven_ai_available,
    validate_against_raven,
)
from assembler.models import (
    Environment,
    Layer,
    Material,
    Measurement,
    Reflectivity,
    Sample,
)

__all__ = [
    "Material",
    "Layer",
    "Sample",
    "Measurement",
    "Reflectivity",
    "Environment",
    # Compatibility utilities
    "is_raven_ai_available",
    "validate_against_raven",
    "get_parquet_schema_mapping",
]
