"""
Data Assembler - Automated reflectivity data ingestion workflow.

This package provides tools to ingest reflectometry data from multiple sources
and assemble them into a structured format for the scientific data lakehouse.
"""

__version__ = "0.1.0"

from assembler.tools import (
    FileFinder,
    FileInfo,
    FileType,
    RelatedFiles,
    detect_file_type,
    extract_ipts,
    extract_run_number,
)
from assembler.workflow import AssemblyResult, DataAssembler
from assembler.writers import ParquetWriter, write_assembly_to_parquet

__all__ = [
    # File detection tools
    "FileType",
    "FileInfo",
    "RelatedFiles",
    "FileFinder",
    "detect_file_type",
    "extract_run_number",
    "extract_ipts",
    # Workflow
    "DataAssembler",
    "AssemblyResult",
    # Writers
    "ParquetWriter",
    "write_assembly_to_parquet",
]
