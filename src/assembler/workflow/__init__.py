"""
Workflow module for data assembly and ingestion.

This module provides the main data assembly workflow that combines
parsed data from multiple sources into schema-ready records for the lakehouse.

Module structure:
- result.py: AssemblyResult dataclass
- record_builders.py: Record builder functions for Reflectivity, Sample, Environment
- assembler.py: Main DataAssembler orchestrator class
"""

from typing import Optional

# Re-export assembler
from .assembler import DataAssembler

# Re-export builders for advanced use
from .record_builders import (
    build_environment_record,
    build_reflectivity_record,
    build_sample_record,
    detect_facility,
)

# Re-export result types
from .result import AssemblyResult

__all__ = [
    # Main classes
    "AssemblyResult",
    "DataAssembler",
    # Record builders (for advanced/custom workflows)
    "build_reflectivity_record",
    "build_environment_record",
    "build_sample_record",
    "detect_facility",
    # Convenience function
    "assemble_from_files",
]


def assemble_from_files(
    reduced_path: Optional[str] = None,
    parquet_dir: Optional[str] = None,
    model_path: Optional[str] = None,
) -> AssemblyResult:
    """
    Convenience function to assemble data from file paths.

    This is a high-level function that handles parsing and assembly
    in one step. For more control, use the parsers and DataAssembler
    directly.

    Args:
        reduced_path: Path to reduced data file (.txt)
        parquet_dir: Directory containing parquet files
        model_path: Path to model JSON file

    Returns:
        AssemblyResult with assembled records

    Example:
        result = assemble_from_files(
            reduced_path="/data/REF_L_218386_reduced.txt",
            parquet_dir="/data/parquet/218386",
            model_path="/data/model_218386.json",
        )

        if result.is_complete:
            refl = result.reflectivity
            print(f"Assembled measurement with {len(refl['reflectivity']['q'])} points")
    """
    from assembler.parsers import ModelParser, ParquetParser, ReducedParser

    reduced = None
    parquet = None
    model = None

    if reduced_path:
        parser = ReducedParser()
        reduced = parser.parse(reduced_path)

    if parquet_dir:
        parser = ParquetParser()
        parquet = parser.parse_directory(parquet_dir)

    if model_path:
        parser = ModelParser()
        model = parser.parse(model_path)

    assembler = DataAssembler()
    result = assembler.assemble(reduced=reduced, parquet=parquet, model=model)

    if parquet_dir:
        result.parquet_dir = parquet_dir

    return result
