"""
Parquet file writer for lakehouse output.

This module provides the main writer class for outputting assembled data
to Parquet files suitable for Apache Iceberg tables.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq

from .schemas import (
    ENVIRONMENT_SCHEMA,
    REFLECTIVITY_MODEL_SCHEMA,
    REFLECTIVITY_SCHEMA,
    SAMPLE_SCHEMA,
)

if TYPE_CHECKING:
    from assembler.workflow import AssemblyResult


class ParquetWriter:
    """
    Writes assembled data records to Parquet files.

    Supports partitioned output by facility and year for Iceberg compatibility.

    Example:
        writer = ParquetWriter("/data/lakehouse")
        writer.write_reflectivity(reflectivity_record)
        writer.write_sample(sample_record)

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
        data: dict[str, Any] | AssemblyResult,
        table_type: str | None = None,
        **partition_kwargs: Any,
    ) -> Path | dict[str, str]:
        """
        Write a record or assembly result to Parquet.

        Automatically detects record type and writes to appropriate table.
        If an AssemblyResult is passed, writes all contained records.

        Args:
            data: The record dict or AssemblyResult to write
            table_type: Type of table ('reflectivity', 'sample', 'environment')
                       Required if data is a dict
            **partition_kwargs: Partitioning options (facility, year)

        Returns:
            Path to the written file (for single records), or
            Dict mapping table names to paths (for AssemblyResult)
        """
        # Check if it's an AssemblyResult
        if (
            hasattr(data, "reflectivity")
            and hasattr(data, "sample")
            and hasattr(data, "environment")
        ):
            return self._write_assembly_result(data, **partition_kwargs)

        # It's a dict record - need table_type
        if table_type is None:
            # Try to detect from record structure
            if "reflectivity" in data and isinstance(data["reflectivity"], dict):
                table_type = "reflectivity"
            elif "layers" in data or "layers_json" in data:
                table_type = "sample"
            elif "ambient_medium" in data or "temperature" in data:
                table_type = "environment"
            else:
                raise ValueError("Cannot detect table type. Please specify table_type parameter.")

        if table_type == "reflectivity":
            return self.write_reflectivity(data, **partition_kwargs)
        elif table_type == "sample":
            return self.write_sample(data)
        elif table_type == "environment":
            return self.write_environment(data)
        elif table_type == "reflectivity_model":
            return self.write_reflectivity_model(data)
        else:
            raise ValueError(f"Unknown table type: {table_type}")

    def _write_assembly_result(
        self,
        result: AssemblyResult,
        **partition_kwargs: Any,
    ) -> dict[str, str]:
        """
        Write all records from an AssemblyResult.

        Args:
            result: The assembly result containing records to write
            **partition_kwargs: Partitioning options

        Returns:
            Dict mapping table names to written file paths
        """
        paths: dict[str, str] = {}

        # Write every run record (a multi-angle state has several). Each is
        # keyed by run_number in its filename, so the N files don't collide.
        refl_paths = []
        for refl in result.reflectivities:
            path = self.write_reflectivity(refl, **partition_kwargs)
            refl_paths.append(str(path))
        if refl_paths:
            paths["reflectivity"] = refl_paths[0]
            if len(refl_paths) > 1:
                paths["reflectivities"] = refl_paths

        # A multi-state run has one sample/environment per state (each keyed by
        # id in its filename, so the writes don't collide).
        sample_paths = [str(self.write_sample(s)) for s in result.samples]
        if sample_paths:
            paths["sample"] = sample_paths[0]
            if len(sample_paths) > 1:
                paths["samples"] = sample_paths

        env_paths = [str(self.write_environment(e)) for e in result.environments]
        if env_paths:
            paths["environment"] = env_paths[0]
            if len(env_paths) > 1:
                paths["environments"] = env_paths

        if result.reflectivity_model:
            path = self.write_reflectivity_model(result.reflectivity_model)
            paths["reflectivity_model"] = str(path)

        return paths

    def write_reflectivity(
        self,
        record: dict[str, Any],
        facility: str | None = None,
        year: int | None = None,
    ) -> Path:
        """
        Write a reflectivity record to Parquet.

        Args:
            record: The reflectivity record dict matching REFLECTIVITY_SCHEMA
            facility: Partition by facility (auto-detected if not provided)
            year: Partition by year (auto-detected from run_start if not provided)

        Returns:
            Path to the written file
        """
        # Auto-detect partitions from data
        if facility is None:
            facility = record.get("facility")
        if year is None:
            run_start = record.get("run_start")
            if run_start and hasattr(run_start, "year"):
                year = run_start.year

        table = pa.Table.from_pylist([record], schema=REFLECTIVITY_SCHEMA)

        partition_dir = self._get_partition_path("reflectivity", facility, year)
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Key on the always-unique record id: run_number can repeat across a
        # state's partials, or be "Unknown" when unparseable — keying on it would
        # silently overwrite distinct runs. The run_number stays a queryable column.
        record_id = record.get("id") or record.get("run_number") or "unknown"
        filename = f"{record_id}.parquet"
        output_path = partition_dir / filename

        pq.write_table(table, output_path)
        return output_path

    def write_sample(self, record: dict[str, Any]) -> Path:
        """
        Write a sample record to Parquet.

        Samples are not partitioned (relatively small dataset).

        Args:
            record: The sample record dict matching SAMPLE_SCHEMA

        Returns:
            Path to the written file
        """
        table = pa.Table.from_pylist([record], schema=SAMPLE_SCHEMA)

        partition_dir = self._get_partition_path("sample")
        partition_dir.mkdir(parents=True, exist_ok=True)

        sample_id = record.get("id") or "unknown"
        filename = f"{sample_id}.parquet"
        output_path = partition_dir / filename

        pq.write_table(table, output_path)
        return output_path

    def write_environment(self, record: dict[str, Any]) -> Path:
        """
        Write an environment record to Parquet.

        Environments are not partitioned (relatively small dataset).

        Args:
            record: The environment record dict matching ENVIRONMENT_SCHEMA

        Returns:
            Path to the written file
        """
        table = pa.Table.from_pylist([record], schema=ENVIRONMENT_SCHEMA)

        partition_dir = self._get_partition_path("environment")
        partition_dir.mkdir(parents=True, exist_ok=True)

        env_id = record.get("id") or "unknown"
        filename = f"{env_id}.parquet"
        output_path = partition_dir / filename

        pq.write_table(table, output_path)
        return output_path

    def write_reflectivity_model(self, record: dict[str, Any]) -> Path:
        """
        Write a reflectivity model record to Parquet.

        Reflectivity models are not partitioned (relatively small dataset).

        Args:
            record: The model record dict matching REFLECTIVITY_MODEL_SCHEMA

        Returns:
            Path to the written file
        """
        table = pa.Table.from_pylist([record], schema=REFLECTIVITY_MODEL_SCHEMA)

        partition_dir = self._get_partition_path("reflectivity_model")
        partition_dir.mkdir(parents=True, exist_ok=True)

        model_id = record.get("id") or "unknown"
        filename = f"{model_id}.parquet"
        output_path = partition_dir / filename

        pq.write_table(table, output_path)
        return output_path

    def write_batch(
        self,
        records: list[dict[str, Any]],
        table_type: str,
        **partition_kwargs: Any,
    ) -> list[Path]:
        """
        Write multiple records to Parquet files.

        Args:
            records: List of record dicts
            table_type: Type of records ('reflectivity', 'sample', 'environment')
            **partition_kwargs: Partitioning options passed to each write

        Returns:
            List of paths to written files
        """
        return [self.write(record, table_type=table_type, **partition_kwargs) for record in records]


def write_assembly_to_parquet(result: AssemblyResult, output_dir: str | Path) -> dict[str, Path]:
    """
    Convenience function to write all records from an assembly.

    Args:
        result: The AssemblyResult from DataAssembler
        output_dir: Directory for output files

    Returns:
        Dict mapping table names to written file paths

    Example::

        assembler = DataAssembler()
        result = assembler.assemble(reduced_data, parquet_data)

        paths = write_assembly_to_parquet(result, "/data/lakehouse")
        print(f"Wrote reflectivity to: {paths['reflectivity']}")
    """
    writer = ParquetWriter(output_dir)
    paths: dict[str, Path] = {}

    # Write every run record (a multi-angle state has several). Each file is
    # keyed by run_number, so the N files do not collide.
    refl_paths = [writer.write_reflectivity(refl) for refl in result.reflectivities]
    if refl_paths:
        paths["reflectivity"] = refl_paths[0]
        if len(refl_paths) > 1:
            paths["reflectivities"] = refl_paths

    sample_paths = [writer.write_sample(s) for s in result.samples]
    if sample_paths:
        paths["sample"] = sample_paths[0]
        if len(sample_paths) > 1:
            paths["samples"] = sample_paths

    env_paths = [writer.write_environment(e) for e in result.environments]
    if env_paths:
        paths["environment"] = env_paths[0]
        if len(env_paths) > 1:
            paths["environments"] = env_paths

    if result.reflectivity_model:
        paths["reflectivity_model"] = writer.write_reflectivity_model(result.reflectivity_model)

    return paths
