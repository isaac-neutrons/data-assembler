"""
JSON writer for outputting assembled data in AI-ready JSON format.

This module provides functionality to write assembled records to JSON files,
maintaining schema compatibility with the Parquet output for consumers who prefer JSON.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from assembler.workflow import AssemblyResult


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

    def write_reflectivity(self, record: dict[str, Any]) -> Path:
        """
        Write a reflectivity record to JSON.

        Args:
            record: The reflectivity record dict

        Returns:
            Path to the written JSON file
        """
        output_path = self.output_dir / "reflectivity.json"

        with open(output_path, "w") as f:
            json.dump(record, f, cls=JSONEncoder, indent=2)

        return output_path

    def write_sample(self, record: dict[str, Any]) -> Path:
        """
        Write a sample record to JSON.

        Args:
            record: The sample record dict

        Returns:
            Path to the written JSON file
        """
        output_path = self.output_dir / "sample.json"

        with open(output_path, "w") as f:
            json.dump(record, f, cls=JSONEncoder, indent=2)

        return output_path

    def write_environment(self, record: dict[str, Any]) -> Path:
        """
        Write an environment record to JSON.

        Args:
            record: The environment record dict

        Returns:
            Path to the written JSON file
        """
        output_path = self.output_dir / "environment.json"

        with open(output_path, "w") as f:
            json.dump(record, f, cls=JSONEncoder, indent=2)

        return output_path

    def write_reflectivity_model(self, record: dict[str, Any]) -> Path:
        """
        Write a reflectivity model record to JSON.

        Args:
            record: The reflectivity model record dict

        Returns:
            Path to the written JSON file
        """
        output_path = self.output_dir / "reflectivity_model.json"

        with open(output_path, "w") as f:
            json.dump(record, f, cls=JSONEncoder, indent=2)

        return output_path

    def _write_keyed(self, record: dict[str, Any], table: str) -> Path:
        """Write *record* to ``<output_dir>/<table>/<id>.json`` (id-keyed, no collision)."""
        sub = self.output_dir / table
        sub.mkdir(parents=True, exist_ok=True)
        output_path = sub / f"{record.get('id', table)}.json"
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

        refls = result.reflectivities
        if len(refls) <= 1:
            # Single run: flat json/reflectivity.json (back-compat).
            if refls:
                paths["reflectivity"] = self.write_reflectivity(refls[0])
        else:
            # Multi-angle state: one file per run under json/<run>/ to avoid
            # collision; shared sample/environment/fit stay at the top level.
            refl_paths = []
            for i, refl in enumerate(refls):
                # Key the subdir on the always-unique id (run_number can repeat
                # across a state's partials or be "Unknown" → would collide).
                key = refl.get("id") or refl.get("run_number") or f"run{i}"
                sub = JSONWriter(self.output_dir / str(key))
                refl_paths.append(sub.write_reflectivity(refl))
            paths["reflectivity"] = refl_paths[0]
            paths["reflectivities"] = refl_paths

        # Single sample/environment stays flat (back-compat); multiple (one per
        # state in a multi-state run) go under <table>/<id>.json so all are
        # discoverable and none collide.
        samples = result.samples
        if len(samples) <= 1:
            if samples:
                paths["sample"] = self.write_sample(samples[0])
        else:
            sps = [self._write_keyed(s, "sample") for s in samples]
            paths["sample"] = sps[0]
            paths["samples"] = sps

        envs = result.environments
        if len(envs) <= 1:
            if envs:
                paths["environment"] = self.write_environment(envs[0])
        else:
            eps = [self._write_keyed(e, "environment") for e in envs]
            paths["environment"] = eps[0]
            paths["environments"] = eps

        if result.reflectivity_model:
            paths["reflectivity_model"] = self.write_reflectivity_model(result.reflectivity_model)

        return paths


def write_assembly_to_json(result: AssemblyResult, output_dir: str | Path) -> dict[str, Path]:
    """
    Convenience function to write all results from an assembly to JSON.

    Args:
        result: The AssemblyResult from DataAssembler
        output_dir: Directory for output files

    Returns:
        Dict mapping table names to written file paths
    """
    writer = JSONWriter(output_dir)
    return writer.write_all(result)
