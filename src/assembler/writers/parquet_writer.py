"""
Parquet file writer for lakehouse output.

This module provides the main writer class for outputting assembled data
to Parquet files suitable for Apache Iceberg tables.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq

from assembler.models.environment import Environment
from assembler.models.measurement import Reflectivity
from assembler.models.sample import Sample

from .schemas import ENVIRONMENT_SCHEMA, REFLECTIVITY_SCHEMA, SAMPLE_SCHEMA
from .serializers import environment_to_record, reflectivity_to_record, sample_to_record

if TYPE_CHECKING:
    from assembler.workflow import AssemblyResult


class ParquetWriter:
    """
    Writes assembled data models to Parquet files.

    Supports partitioned output by facility and year for Iceberg compatibility.

    Example:
        writer = ParquetWriter("/data/lakehouse")
        writer.write(reflectivity_measurement)
        writer.write(sample_instance)

        # Or write an assembly result
        writer.write(assembly_result)
    """

    def __init__(
        self,
        output_dir: str | Path,
        partition_by_facility: bool = True,
        partition_by_year: bool = True,
    ):
        """
        Initialize the writer with an output directory.

        Args:
            output_dir: Base directory for output files. Subdirectories
                       will be created for partitions.
            partition_by_facility: Whether to partition by facility (default True)
            partition_by_year: Whether to partition by year (default True)
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.partition_by_facility = partition_by_facility
        self.partition_by_year = partition_by_year

    def _get_partition_path(
        self,
        table_name: str,
        facility: str | None = None,
        year: int | None = None,
    ) -> Path:
        """
        Build a partition path for Iceberg-style layout.

        Args:
            table_name: The table name (e.g., 'reflectivity', 'sample')
            facility: Optional facility for partitioning (e.g., 'SNS')
            year: Optional year for partitioning

        Returns:
            Path to the partition directory
        """
        parts = [self.output_dir, table_name]
        if facility and self.partition_by_facility:
            parts.append(f"facility={facility}")
        if year and self.partition_by_year:
            parts.append(f"year={year}")
        return Path(*parts)

    def write(
        self,
        model: Reflectivity | Sample | Environment | AssemblyResult,
        **partition_kwargs: Any,
    ) -> Path | dict[str, str]:
        """
        Write a model instance or assembly result to Parquet.

        Automatically detects model type and writes to appropriate table.
        If an AssemblyResult is passed, writes all contained models.

        Args:
            model: The model instance or AssemblyResult to write
            **partition_kwargs: Partitioning options (facility, year)

        Returns:
            Path to the written file (for single models), or
            Dict mapping table names to paths (for AssemblyResult)
        """
        # Check if it's an AssemblyResult (duck typing to avoid circular import)
        if (
            hasattr(model, "reflectivity")
            and hasattr(model, "sample")
            and hasattr(model, "environment")
        ):
            return self._write_assembly_result(model, **partition_kwargs)

        if isinstance(model, Reflectivity):
            return self.write_reflectivity(model, **partition_kwargs)
        elif isinstance(model, Sample):
            return self.write_sample(model, **partition_kwargs)
        elif isinstance(model, Environment):
            return self.write_environment(model, **partition_kwargs)
        else:
            raise TypeError(f"Unsupported model type: {type(model)}")

    def _write_assembly_result(
        self,
        result: AssemblyResult,
        **partition_kwargs: Any,
    ) -> dict[str, str]:
        """
        Write all models from an AssemblyResult.

        Args:
            result: The assembly result containing models to write
            **partition_kwargs: Partitioning options

        Returns:
            Dict mapping table names to written file paths
        """
        paths: dict[str, str] = {}

        if result.reflectivity:
            path = self.write_reflectivity(result.reflectivity, **partition_kwargs)
            paths["reflectivity"] = str(path)

        if result.sample:
            path = self.write_sample(result.sample, **partition_kwargs)
            paths["sample"] = str(path)

        if result.environment:
            path = self.write_environment(result.environment, **partition_kwargs)
            paths["environment"] = str(path)

        return paths

    def write_reflectivity(
        self,
        measurement: Reflectivity,
        facility: str | None = None,
        year: int | None = None,
    ) -> Path:
        """
        Write a reflectivity measurement to Parquet.

        Args:
            measurement: The Reflectivity instance
            facility: Partition by facility (auto-detected if not provided)
            year: Partition by year (auto-detected from run_start if not provided)

        Returns:
            Path to the written file
        """
        # Auto-detect partitions from data
        if facility is None and measurement.facility:
            fac = measurement.facility
            facility = fac.value if hasattr(fac, "value") else fac
        if year is None and measurement.run_start:
            year = measurement.run_start.year

        record = reflectivity_to_record(measurement)
        table = pa.Table.from_pylist([record], schema=REFLECTIVITY_SCHEMA)

        partition_dir = self._get_partition_path("reflectivity", facility, year)
        partition_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{measurement.run_number or measurement.id}.parquet"
        output_path = partition_dir / filename

        pq.write_table(table, output_path)
        return output_path

    def write_sample(self, sample: Sample) -> Path:
        """
        Write a sample to Parquet.

        Samples are not partitioned (relatively small dataset).

        Args:
            sample: The Sample instance

        Returns:
            Path to the written file
        """
        record = sample_to_record(sample)
        table = pa.Table.from_pylist([record], schema=SAMPLE_SCHEMA)

        partition_dir = self._get_partition_path("sample")
        partition_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{sample.id}.parquet"
        output_path = partition_dir / filename

        pq.write_table(table, output_path)
        return output_path

    def write_environment(self, env: Environment) -> Path:
        """
        Write an environment to Parquet.

        Environments are not partitioned (relatively small dataset).

        Args:
            env: The Environment instance

        Returns:
            Path to the written file
        """
        record = environment_to_record(env)
        table = pa.Table.from_pylist([record], schema=ENVIRONMENT_SCHEMA)

        partition_dir = self._get_partition_path("environment")
        partition_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{env.id}.parquet"
        output_path = partition_dir / filename

        pq.write_table(table, output_path)
        return output_path

    def write_batch(
        self,
        models: list[Reflectivity | Sample | Environment],
        **partition_kwargs: Any,
    ) -> list[Path]:
        """
        Write multiple models to Parquet files.

        Args:
            models: List of model instances
            **partition_kwargs: Partitioning options passed to each write

        Returns:
            List of paths to written files
        """
        return [self.write(model, **partition_kwargs) for model in models]
