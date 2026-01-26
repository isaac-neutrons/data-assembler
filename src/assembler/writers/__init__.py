"""
Writers module for outputting assembled data to Parquet files.

This module provides functionality to write assembled Reflectivity, Sample,
and Environment records to Parquet files suitable for Apache Iceberg tables.

Module structure:
- schemas.py: PyArrow schema definitions for lakehouse tables
- serializers.py: Model-to-record conversion utilities
- parquet_writer.py: Main ParquetWriter class
"""

from pathlib import Path

from assembler.workflow import AssemblyResult

# Re-export writer class
from .parquet_writer import ParquetWriter

# Re-export schemas
from .schemas import (
    ENVIRONMENT_SCHEMA,
    REFLECTIVITY_SCHEMA,
    SAMPLE_SCHEMA,
    get_schema_for_model,
)

# Re-export serializers
from .serializers import (
    environment_to_record,
    reflectivity_to_record,
    sample_to_record,
    serialize_value,
)

__all__ = [
    # Schemas
    "REFLECTIVITY_SCHEMA",
    "SAMPLE_SCHEMA",
    "ENVIRONMENT_SCHEMA",
    "get_schema_for_model",
    # Serializers
    "serialize_value",
    "reflectivity_to_record",
    "sample_to_record",
    "environment_to_record",
    # Writer
    "ParquetWriter",
    # Convenience function
    "write_assembly_to_parquet",
]


def write_assembly_to_parquet(result: AssemblyResult, output_dir: str | Path) -> dict[str, Path]:
    """
    Convenience function to write all results from an assembly.

    Args:
        result: The AssemblyResult from DataAssembler
        output_dir: Directory for output files

    Returns:
        Dict mapping table names to written file paths

    Example:
        assembler = DataAssembler()
        result = assembler.assemble(reduced_data, parquet_data)

        paths = write_assembly_to_parquet(result, "/data/lakehouse")
        print(f"Wrote reflectivity to: {paths['reflectivity']}")
    """
    writer = ParquetWriter(output_dir)
    paths: dict[str, Path] = {}

    if result.reflectivity:
        paths["reflectivity"] = writer.write_reflectivity(result.reflectivity)

    if result.sample:
        paths["sample"] = writer.write_sample(result.sample)

    if result.environment:
        paths["environment"] = writer.write_environment(result.environment)

    return paths
