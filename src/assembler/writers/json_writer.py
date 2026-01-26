"""
JSON writer for outputting assembled data in AI-ready JSON format.

This module provides functionality to write assembled Reflectivity, Sample,
and Environment records to JSON files, maintaining schema compatibility
with the Parquet output for consumers who prefer JSON.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from assembler.models.environment import Environment
from assembler.models.measurement import Reflectivity
from assembler.models.sample import Sample
from assembler.workflow import AssemblyResult

from .serializers import (
    environment_to_record,
    reflectivity_to_record,
    sample_to_record,
)


class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime, UUID, and Path objects."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, Path):
            return str(obj)
        return super().default(obj)


class JSONWriter:
    """
    Writes assembled data to JSON files.

    This writer produces JSON files with the same schema as the Parquet
    output, making it easy for consumers to switch between formats.
    """

    def __init__(self, output_dir: str | Path):
        """
        Initialize the JSON writer.

        Args:
            output_dir: Base directory for output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_reflectivity(self, measurement: Reflectivity) -> Path:
        """
        Write a Reflectivity measurement to JSON.

        Args:
            measurement: The Reflectivity model to write

        Returns:
            Path to the written JSON file
        """
        record = reflectivity_to_record(measurement)
        
        # Create output path
        output_path = self.output_dir / "reflectivity.json"
        
        with open(output_path, "w") as f:
            json.dump(record, f, cls=JSONEncoder, indent=2)
        
        return output_path

    def write_sample(self, sample: Sample) -> Path:
        """
        Write a Sample to JSON.

        Args:
            sample: The Sample model to write

        Returns:
            Path to the written JSON file
        """
        record = sample_to_record(sample)
        
        output_path = self.output_dir / "sample.json"
        
        with open(output_path, "w") as f:
            json.dump(record, f, cls=JSONEncoder, indent=2)
        
        return output_path

    def write_environment(self, env: Environment) -> Path:
        """
        Write an Environment to JSON.

        Args:
            env: The Environment model to write

        Returns:
            Path to the written JSON file
        """
        record = environment_to_record(env)
        
        output_path = self.output_dir / "environment.json"
        
        with open(output_path, "w") as f:
            json.dump(record, f, cls=JSONEncoder, indent=2)
        
        return output_path

    def write_all(self, result: AssemblyResult) -> dict[str, Path]:
        """
        Write all assembled data to JSON files.

        Args:
            result: The AssemblyResult from DataAssembler

        Returns:
            Dict mapping table names to written file paths
        """
        paths: dict[str, Path] = {}

        if result.reflectivity:
            paths["reflectivity"] = self.write_reflectivity(result.reflectivity)

        if result.sample:
            paths["sample"] = self.write_sample(result.sample)

        if result.environment:
            paths["environment"] = self.write_environment(result.environment)

        return paths


def write_assembly_to_json(result: AssemblyResult, output_dir: str | Path) -> dict[str, Path]:
    """
    Convenience function to write all results from an assembly to JSON.

    Args:
        result: The AssemblyResult from DataAssembler
        output_dir: Directory for output files

    Returns:
        Dict mapping table names to written file paths

    Example:
        assembler = DataAssembler()
        result = assembler.assemble(reduced_data, parquet_data)

        paths = write_assembly_to_json(result, "/data/output")
        print(f"Wrote reflectivity to: {paths['reflectivity']}")
    """
    writer = JSONWriter(output_dir)
    return writer.write_all(result)
