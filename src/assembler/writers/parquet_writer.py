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

from .schemas import ENVIRONMENT_SCHEMA, REFLECTIVITY_SCHEMA, SAMPLE_SCHEMA

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
        if hasattr(data, "reflectivity") and hasattr(data, "sample") and hasattr(data, "environment"):
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

        if result.reflectivity:
            path = self.write_reflectivity(result.reflectivity, **partition_kwargs)
            paths["reflectivity"] = str(path)

        if result.sample:
            path = self.write_sample(result.sample)
            paths["sample"] = str(path)

        if result.environment:
            path = self.write_environment(result.environment)
            paths["environment"] = str(path)

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

        run_number = record.get("run_number") or record.get("id") or "unknown"
        filename = f"{run_number}.parquet"
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
